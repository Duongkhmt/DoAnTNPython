"""
ETL Pipeline: Raw Data to MongoDB -> Clean/Transform -> TimescaleDB (Production-Grade Full Version)

Đây là bản nâng cấp toàn diện 100% của file 'etl_mongo_to_timescale.py', đảm bảo đầy đủ tất cả
các cơ chế sản xuất (production) tương tự như 'crawler_pipeline.py' (gồm: đa luồng song song,
cơ chế checkpoint lưu trạng thái, thông báo lỗi qua Telegram, kiểm tra mức độ hoàn thiện dữ liệu,
và tự động làm mới view/aggregate).

Khác biệt cốt lõi:
  - 'crawler_pipeline.py' cào dữ liệu và ghi trực tiếp vào TimescaleDB.
  - 'etl_mongo_to_timescale.py' thực hiện đúng 3 pha ETL tách biệt:
      1. EXTRACT: Cào thô và ghi thẳng tài liệu JSON vào MongoDB (Raw Storage).
      2. TRANSFORM: Đọc từ MongoDB, xử lý định dạng, kiểu dữ liệu, khuyết thiếu bằng Pandas trong bộ nhớ.
      3. LOAD: Nạp dữ liệu sạch vào TimescaleDB bằng câu lệnh Upsert.
"""

import sys
import math
import time
import uuid
import argparse
import os
import json
import threading
import concurrent.futures
import traceback
import gc
from datetime import datetime, timedelta
from functools import wraps
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Đảm bảo import đầy đủ các thư viện và helpers của dự án
docker_venv_path = "/opt/airflow/dags/venv/Lib/site-packages"
if docker_venv_path not in sys.path:
    sys.path.append(docker_venv_path)

import pandas as pd
from sqlalchemy import text
from vnstock_data import Listing, Quote, Finance, Company, Trading
from mongo_utils import MongoManager
from timescale_utils import DatabaseManager

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

print("=" * 80)
print("ETL PIPELINE CHUNG KHOAN (RAW MONGODB -> CLEAN TIMESCALEDB)")
print("=" * 80)

# Khởi tạo hai bộ kết nối database duy nhất
db = DatabaseManager(database="vnstock_ts")
db.create_all_tables()
mongo = MongoManager()

# Khởi tạo định danh Run ID toàn cục cho phiên ETL
RUN_ID = uuid.uuid4().hex[:12]
TODAY = datetime.now().strftime("%Y-%m-%d")

# ==========================================
# CẤU HÌNH COMMAND LINE ARGUMENTS
# ==========================================
def parse_args():
    parser = argparse.ArgumentParser(description="ETL Pipeline VNStock data")
    parser.add_argument("--daily", action="store_true", help="Chế độ đồng bộ hàng ngày")
    parser.add_argument("--backfill", action="store_true", help="Làm mới aggregate sau khi nạp dữ liệu")
    parser.add_argument("--symbols", nargs="*", help="Chỉ chạy các mã chỉ định")
    parser.add_argument("--limit", type=int, help="Giới hạn số mã chạy")
    parser.add_argument("--batch-size", type=int, default=25, help="Kích thước batch")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["quotes", "company", "finance", "trading"],
        help="Chỉ chạy các stage chỉ định",
    )
    parser.add_argument("--stage-sleep", type=float, default=0.5, help="Nghỉ giữa các request")
    parser.add_argument("--batch-sleep", type=float, default=5.0, help="Nghỉ giữa các batch")
    parser.add_argument("--workers", type=int, default=4, help="Số luồng chạy song song")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Giới hạn kết nối API đồng thời")
    parser.add_argument("--dry-run", action="store_true", help="Không ghi vào DB")
    return parser.parse_args()

ARGS = parse_args()

if ARGS.daily:
    START_DATE = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    print(f"[*] ETL DAILY MODE: {START_DATE} đến nay")
else:
    START_DATE = "2020-01-01"
    print(f"[*] ETL BACKFILL MODE: {START_DATE} đến nay")

# ==========================================
# CƠ CHẾ KHÓA AN TOÀN ĐA LUỒNG (THREAD SAFETY)
# ==========================================
print_lock = threading.Lock()
checkpoint_lock = threading.Lock()
semaphore = threading.Semaphore(ARGS.max_concurrent)

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def log_stage(stage, symbol, status, message=None, extra=None):
    """Ghi nhật ký tiến trình ETL vào MongoDB"""
    mongo.log_crawl(
        run_id=RUN_ID,
        stage=stage,
        symbol=symbol,
        status=status,
        message=message,
        extra=extra or {},
    )


# =============================================================================
# CHƯƠNG TRÌNH THÔNG BÁO BÁO CÁO QUA TELEGRAM (ALERT SYSTEM)
# =============================================================================
class AlertNotifier:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        
    def send_message(self, text: str):
        if not self.token or not self.chat_id:
            safe_print("[WARNING] Chưa cấu hình Telegram credentials. Bỏ qua gửi tin nhắn.")
            return
            
        max_len = 4096
        chunks = [text[i:i + max_len] for i in range(0, len(text), max_len)]
        
        for chunk in chunks[:2]:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": chunk, "parse_mode": "HTML"}
            try:
                requests.post(url, json=payload, timeout=10)
            except Exception as e:
                safe_print(f"[ERROR] Không gửi được thông báo Telegram: {e}")

    def send_summary(self, total: int, failed: list, incomplete: list):
        fail_pct = (len(failed) / total * 100) if total > 0 else 0
        
        msg = f"<b>[ETL PIPELINE SUMMARY]</b>\nRun ID: {RUN_ID}\nDate: {TODAY}\n"
        msg += f"Tổng mã: {total} | Lỗi: {len(failed)} ({fail_pct:.1f}%) | Chưa hoàn thành: {len(incomplete)}\n"
        
        if len(failed) == 0 and len(incomplete) == 0:
            msg = "✅ " + msg + "\nTrạng thái: THÀNH CÔNG TOÀN BỘ"
        else:
            msg = "⚠️ " + msg + "\n"
            if failed:
                msg += "\n<b>Mã bị lỗi:</b>\n" + ", ".join(failed[:50])
                if len(failed) > 50:
                    msg += f" ... (+{len(failed)-50} mã khác)"
            if incomplete:
                msg += "\n\n<b>Mã chưa hoàn thiện:</b>\n" + ", ".join(incomplete[:50])
                if len(incomplete) > 50:
                    msg += f" ... (+{len(incomplete)-50} mã khác)"

        self.send_message(msg)


# =============================================================================
# CÁC HÀM XỬ LÝ ETL CHI TIẾT CHO TỪNG GIAI ĐOẠN (ETL STAGES)
# =============================================================================

# ----------------- STAGE 1: LISTING -----------------
def etl_sync_listing():
    safe_print("\n[STAGE 1] ĐANG TRÍCH XUẤT DANH SÁCH MÃ CHỨNG KHOÁN (LISTING)...")
    
    # 1. EXTRACT
    try:
        lst = Listing(source="VND")
        df_listing = lst.all_symbols()
        mongo.save_raw_payload(
            dataset="listing_raw",
            symbol=None,
            source="VND",
            payload=df_listing.to_dict(orient="records"),
            run_id=RUN_ID,
            metadata={"rows": len(df_listing)},
        )
    except Exception as exc:
        log_stage("listing", None, "extract_error", message=str(exc))
        return []

    # 2. TRANSFORM
    try:
        raw_docs = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "dataset": "listing_raw"}))
        if not raw_docs:
            return []
        df_raw = pd.DataFrame(raw_docs[0]["payload"])
        
        if "type" in df_raw.columns:
            df_raw = df_raw[df_raw["type"].isin(["STOCK", "Cổ phiếu", "CP"])]

        if len(df_raw) == 0:
            log_stage("listing", None, "empty")
            return []

        df_clean = pd.DataFrame()
        df_clean["symbol"] = df_raw["symbol"].str.upper().str.strip()
        df_clean["organ_name"] = df_raw.get("company_name", None)
        df_clean["exchange"] = df_raw.get("exchange", None)
        df_clean["company_type"] = df_raw.get("type", None)
        df_clean = df_clean.dropna(subset=["symbol"])
    except Exception as exc:
        log_stage("listing", None, "transform_error", message=str(exc))
        return []

    # 3. LOAD
    try:
        if not ARGS.dry_run:
            db.upsert_dataframe(df_clean, "listing", conflict_cols=["symbol"])
        log_stage("listing", None, "success", extra={"symbols": len(df_clean)})
        return df_clean["symbol"].tolist()
    except Exception as exc:
        log_stage("listing", None, "load_error", message=str(exc))
        return []


# ----------------- STAGE 2: QUOTES -----------------
def etl_quotes_extract(symbol):
    """Pha trích xuất dữ liệu giá nến lưu vào MongoDB"""
    q = Quote(symbol=symbol, source="VND")
    df = q.history(start=START_DATE, end=TODAY, interval="1D")
    mongo.save_raw_payload(
        dataset="quotes_raw",
        symbol=symbol,
        source="VND",
        payload=df.to_dict(orient="records"),
        run_id=RUN_ID,
        metadata={"rows": len(df), "start_date": START_DATE, "end_date": TODAY},
    )
    return len(df)

def etl_quotes_transform(symbol):
    """Pha làm sạch dữ liệu giá nến"""
    raw_docs = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "symbol": symbol, "dataset": "quotes_raw"}))
    if not raw_docs or len(raw_docs[0]["payload"]) == 0:
        return pd.DataFrame()
        
    df_raw = pd.DataFrame(raw_docs[0]["payload"])
    df_clean = df_raw.copy()
    
    if "time" in df_clean.columns and "trading_date" not in df_clean.columns:
        df_clean = df_clean.rename(columns={"time": "trading_date"})
    if "symbol" not in df_clean.columns:
        df_clean["symbol"] = symbol

    expected_cols = ["symbol", "trading_date", "open", "high", "low", "close", "volume"]
    for col in expected_cols:
        if col not in df_clean.columns:
            df_clean[col] = None
            
    df_clean = df_clean[expected_cols]
    df_clean["trading_date"] = pd.to_datetime(df_clean["trading_date"]).dt.date
    df_clean["symbol"] = df_clean["symbol"].str.upper()
    df_clean = df_clean.dropna(subset=["symbol", "trading_date", "close"])
    return df_clean

def etl_quotes_load(symbol, df_clean):
    """Pha nạp dữ liệu giá nến vào TimescaleDB"""
    if len(df_clean) > 0 and not ARGS.dry_run:
        db.upsert_dataframe(df_clean, "quote_history", conflict_cols=["symbol", "trading_date"])


# ----------------- STAGE 3: COMPANY -----------------
def etl_company_extract(symbol):
    """Pha trích xuất tổng quan doanh nghiệp lưu thô vào MongoDB"""
    c = Company(symbol=symbol, source="KBS")
    df = c.overview()
    mongo.save_raw_payload(
        dataset="company_raw",
        symbol=symbol,
        source="KBS",
        payload=df.to_dict(orient="records"),
        run_id=RUN_ID,
        metadata={"rows": len(df)},
    )
    return len(df)

def etl_company_transform(symbol):
    """Pha xử lý làm sạch thông tin ngành doanh nghiệp"""
    raw_docs = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "symbol": symbol, "dataset": "company_raw"}))
    if not raw_docs or len(raw_docs[0]["payload"]) == 0:
        return pd.DataFrame()
        
    df_raw = pd.DataFrame(raw_docs[0]["payload"])
    df_clean = pd.DataFrame()
    df_clean["symbol"] = [symbol]
    
    industry_api = df_raw.get("icb_name2", None)
    if isinstance(industry_api, pd.Series):
        industry_api = industry_api.iloc[0] if not industry_api.empty else None

    # Load JSON fallback map
    if not hasattr(etl_company_transform, "fallback_map"):
        map_path = os.path.join(os.path.dirname(__file__), 'industry_mapping.json')
        if os.path.exists(map_path):
            etl_company_transform.fallback_map = json.load(open(map_path, 'r', encoding='utf-8'))
        else:
            etl_company_transform.fallback_map = {}
    
    fallback_map = etl_company_transform.fallback_map
    final_industry = industry_api
    
    if final_industry is None or pd.isna(final_industry):
        if symbol in fallback_map:
            final_industry = fallback_map[symbol].get('industry')
            df_clean["sector"] = fallback_map[symbol].get('sector', 'Khác')
        if final_industry is None or pd.isna(final_industry):
            final_industry = 'Khác'
            df_clean["sector"] = df_raw.get("icb_name3", 'Khác')
    else:
        df_clean["sector"] = df_raw.get("icb_name3", 'Khác')
                
    df_clean["industry"] = final_industry
    df_clean["symbol"] = df_clean["symbol"].str.upper()
    return df_clean

def etl_company_load(symbol, df_clean):
    """Pha nạp thông tin doanh nghiệp"""
    if len(df_clean) > 0 and not ARGS.dry_run:
        db.upsert_dataframe(df_clean, "company", conflict_cols=["symbol"])


# ----------------- STAGE 4: FINANCE -----------------
def etl_finance_extract(symbol):
    """Pha trích xuất báo cáo tài chính thô lưu vào MongoDB"""
    f = Finance(symbol=symbol, source="VCI")
    df = f.ratio()
    mongo.save_raw_payload(
        dataset="finance_raw",
        symbol=symbol,
        source="VCI",
        payload=df.reset_index().to_dict(orient="records") if len(df) > 0 else [],
        run_id=RUN_ID,
        metadata={"rows": len(df)},
    )
    return len(df)

def etl_finance_transform(symbol):
    """Pha xoay bảng dọc chỉ số tài chính"""
    raw_docs = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "symbol": symbol, "dataset": "finance_raw"}))
    if not raw_docs or len(raw_docs[0]["payload"]) == 0:
        return pd.DataFrame()
        
    df_raw = pd.DataFrame(raw_docs[0]["payload"])
    period_col = df_raw.columns[0]
    cols_to_drop = [period_col, "report_period", "Ratio TTM Id", "Ratio Type", "Ratio Year Id", "ticker", "symbol"]
    val_cols = [c for c in df_raw.columns if c not in cols_to_drop]

    df_melted = pd.melt(
        df_raw,
        id_vars=[period_col],
        value_vars=val_cols,
        var_name="item_name",
        value_name="value",
    )
    df_melted = df_melted.rename(columns={period_col: "report_period"})
    df_melted["symbol"] = symbol
    df_melted["report_type"] = "ratio"
    df_melted["value"] = pd.to_numeric(df_melted["value"], errors="coerce")
    df_melted = df_melted.dropna(subset=["value"])

    df_clean = df_melted[["symbol", "report_type", "report_period", "item_name", "value"]].copy()
    df_clean = df_clean.drop_duplicates(
        subset=["symbol", "report_type", "report_period", "item_name"],
        keep="last",
    )
    return df_clean

def etl_finance_load(symbol, df_clean):
    """Pha nạp các chỉ số tài chính"""
    if len(df_clean) > 0 and not ARGS.dry_run:
        db.upsert_dataframe(
            df_clean,
            "finance",
            conflict_cols=["symbol", "report_type", "report_period", "item_name"],
        )


# ----------------- STAGE 5: TRADING -----------------
def etl_trading_extract(symbol):
    """Pha trích xuất dữ liệu tự doanh, khối ngoại và lệnh mua bán"""
    tr = Trading(symbol=symbol, source="VCI")
    
    # 1. Khối ngoại thô
    try:
        df_f = tr.foreign_trade(start=START_DATE, end=TODAY)
        if df_f is not None and len(df_f) > 0:
            mongo.save_raw_payload("trading_foreign_raw", symbol, "VCI", df_f.to_dict(orient="records"), run_id=RUN_ID)
    except: pass
    
    # 2. Tự doanh thô
    try:
        df_p = tr.prop_trade(start=START_DATE, end=TODAY)
        if df_p is not None and len(df_p) > 0:
            mongo.save_raw_payload("trading_prop_raw", symbol, "VCI", df_p.to_dict(orient="records"), run_id=RUN_ID)
    except: pass

    # 3. Lịch sử khớp chi tiết thô
    try:
        df_ph = tr.price_history(start=START_DATE, end=TODAY)
        if df_ph is not None and len(df_ph) > 0:
            mongo.save_raw_payload("trading_price_raw", symbol, "VCI", df_ph.to_dict(orient="records"), run_id=RUN_ID)
    except: pass
    
    return 1

def etl_trading_transform(symbol):
    """Pha xử lý tính toán dòng tiền ròng các khối và cung cầu đặt lệnh"""
    # A. Dòng tiền ròng (Foreign + Prop)
    raw_f = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "symbol": symbol, "dataset": "trading_foreign_raw"}))
    raw_p = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "symbol": symbol, "dataset": "trading_prop_raw"}))
    df_f = pd.DataFrame(raw_f[0]["payload"]) if raw_f else pd.DataFrame()
    df_p = pd.DataFrame(raw_p[0]["payload"]) if raw_p else pd.DataFrame()
    
    df_trading_clean = pd.DataFrame()
    if len(df_f) > 0 or len(df_p) > 0:
        all_dates = set()
        if len(df_f) > 0: all_dates.update(df_f["trading_date"].tolist())
        if len(df_p) > 0: all_dates.update(df_p["trading_date"].tolist())

        fr_map   = df_f.set_index("trading_date").to_dict("index") if len(df_f) > 0 else {}
        prop_map = df_p.set_index("trading_date").to_dict("index") if len(df_p) > 0 else {}

        records = []
        for t_date in all_dates:
            f_row = fr_map.get(t_date, {})
            p_row = prop_map.get(t_date, {})
            records.append({
                "symbol":           symbol,
                "trading_date":     t_date,
                "fr_buy_volume":    float(f_row.get("fr_buy_volume_matched",  0) or 0),
                "fr_sell_volume":   float(f_row.get("fr_sell_volume_matched", 0) or 0),
                "fr_buy_value":     float(f_row.get("fr_buy_value_matched",   0) or 0) / 1e9,
                "fr_sell_value":    float(f_row.get("fr_sell_value_matched",  0) or 0) / 1e9,
                "prop_buy_volume":  float(p_row.get("total_buy_trade_volume", 0) or 0),
                "prop_sell_volume": float(p_row.get("total_sell_trade_volume",0) or 0),
                "prop_buy_value":   float(p_row.get("total_buy_trade_value",  0) or 0) / 1e9,
                "prop_sell_value":  float(p_row.get("total_sell_trade_value", 0) or 0) / 1e9,
            })
        df_trading_clean = pd.DataFrame(records)
        df_trading_clean["trading_date"] = pd.to_datetime(df_trading_clean["trading_date"]).dt.date

    # B. Lịch sử khớp lệnh & Sổ lệnh
    raw_price = list(mongo.db.raw_payloads.find({"run_id": RUN_ID, "symbol": symbol, "dataset": "trading_price_raw"}))
    df_ph = pd.DataFrame(raw_price[0]["payload"]) if raw_price else pd.DataFrame()
    
    df_summary_clean = pd.DataFrame()
    df_order_clean = pd.DataFrame()
    
    if len(df_ph) > 0:
        df_ph = df_ph.copy()
        df_ph["symbol"]       = symbol
        df_ph["trading_date"] = pd.to_datetime(df_ph["trading_date"]).dt.date

        df_summary_clean = pd.DataFrame({
            "symbol":            df_ph["symbol"],
            "trading_date":      df_ph["trading_date"],
            "total_trading_vol": pd.to_numeric(df_ph["total_volume"],  errors="coerce"),
            "total_trading_val": pd.to_numeric(df_ph["total_value"],   errors="coerce"),
            "open_price":        pd.to_numeric(df_ph["open"],          errors="coerce"),
            "highest_price":     pd.to_numeric(df_ph["high"],          errors="coerce"),
            "lowest_price":      pd.to_numeric(df_ph["low"],           errors="coerce"),
            "close_price":       pd.to_numeric(df_ph["close"],         errors="coerce"),
        })

        for col in ["total_buy_trade", "total_sell_trade", "total_buy_trade_volume", 
                    "total_sell_trade_volume", "average_buy_trade_volume", "average_sell_trade_volume"]:
            if col not in df_ph.columns:
                df_ph[col] = None

        df_order_clean = pd.DataFrame({
            "symbol":               df_ph["symbol"],
            "trading_date":         df_ph["trading_date"],
            "buy_orders":           pd.to_numeric(df_ph["total_buy_trade"],           errors="coerce"),
            "sell_orders":          pd.to_numeric(df_ph["total_sell_trade"],          errors="coerce"),
            "buy_volume":           pd.to_numeric(df_ph["total_buy_trade_volume"],    errors="coerce"),
            "sell_volume":          pd.to_numeric(df_ph["total_sell_trade_volume"],   errors="coerce"),
            "avg_buy_order_volume": pd.to_numeric(df_ph["average_buy_trade_volume"],  errors="coerce"),
            "avg_sell_order_volume":pd.to_numeric(df_ph["average_sell_trade_volume"], errors="coerce"),
        })

    return df_trading_clean, df_summary_clean, df_order_clean

def etl_trading_load(symbol, df_trading, df_summary, df_order):
    """Pha nạp dữ liệu giao dịch sạch vào các bảng TimescaleDB"""
    if not ARGS.dry_run:
        if len(df_trading) > 0:
            db.upsert_dataframe(df_trading, "trading", conflict_cols=["symbol", "trading_date"])
        if len(df_summary) > 0:
            db.upsert_dataframe(df_summary, "trading_summary", conflict_cols=["symbol", "trading_date"])
        if len(df_order) > 0:
            db.upsert_dataframe(df_order, "trading_order_stats", conflict_cols=["symbol", "trading_date"])


# =============================================================================
# CƠ CHẾ TỰ ĐỘNG THỬ LẠI (STAGE-LEVEL RETRY DECORATOR)
# =============================================================================
def retry(max_attempts=2, backoff_factor=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(symbol, *args, **kwargs):
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return func(symbol, *args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise e
                    sleep_time = backoff_factor * attempt
                    time.sleep(sleep_time)
                    attempt += 1
        return wrapper
    return decorator


@retry(max_attempts=2, backoff_factor=1.0)
def execute_quotes_etl(symbol):
    rows = etl_quotes_extract(symbol)
    if rows > 0:
        df_clean = etl_quotes_transform(symbol)
        etl_quotes_load(symbol, df_clean)
        log_stage("quotes", symbol, "success", extra={"rows": len(df_clean)})
    else:
        log_stage("quotes", symbol, "empty")

@retry(max_attempts=2, backoff_factor=1.0)
def execute_company_etl(symbol):
    rows = etl_company_extract(symbol)
    if rows > 0:
        df_clean = etl_company_transform(symbol)
        etl_company_load(symbol, df_clean)
        log_stage("company", symbol, "success", extra={"rows": len(df_clean)})
    else:
        log_stage("company", symbol, "empty")

@retry(max_attempts=2, backoff_factor=1.0)
def execute_finance_etl(symbol):
    rows = etl_finance_extract(symbol)
    if rows > 0:
        df_clean = etl_finance_transform(symbol)
        etl_finance_load(symbol, df_clean)
        log_stage("finance", symbol, "success", extra={"rows": len(df_clean)})
    else:
        log_stage("finance", symbol, "empty")

@retry(max_attempts=2, backoff_factor=1.0)
def execute_trading_etl(symbol):
    etl_trading_extract(symbol)
    df_trading, df_summary, df_order = etl_trading_transform(symbol)
    etl_trading_load(symbol, df_trading, df_summary, df_order)
    log_stage("trading", symbol, "success")


# =============================================================================
# CƠ CHẾ ĐIỀU PHỐI VÀ QUẢN LÝ TIẾN ĐỘ CHẠY (ETL ORCHESTRATOR)
# =============================================================================
class ETLOrchestrator:
    def __init__(self, stages):
        self.stages = stages
        self.checkpoint_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_checkpoint.json")
        self.checkpoints = self.load_checkpoint()

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                if ARGS.daily:
                    cutoff = datetime.now() - timedelta(hours=16)
                    valid_data = {}
                    for sym, info in data.items():
                        try:
                            ts = datetime.fromisoformat(info.get("timestamp", "2000-01-01T00:00:00"))
                            if ts >= cutoff:
                                valid_data[sym] = info
                        except ValueError:
                            pass
                    data = valid_data
                return data
            except Exception as e:
                safe_print(f"Không đọc được checkpoint: {e}")
                return {}
        return {}

    def save_checkpoint(self, symbol, status, error=None):
        with checkpoint_lock:
            self.checkpoints[symbol] = {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "error": error
            }
            if not ARGS.dry_run:
                try:
                    with open(self.checkpoint_file, 'w') as f:
                        json.dump(self.checkpoints, f, indent=4)
                except Exception as e:
                    safe_print(f"[WARNING] Lưu checkpoint thất bại cho {symbol}: {e}")

    def process_symbol(self, symbol):
        """Xử lý tuần tự Extract -> Transform -> Load của 1 mã"""
        if ARGS.dry_run:
            safe_print(f"[DRY-RUN] Đang xử lý {symbol} - Stages: {self.stages}")
            time.sleep(0.1)
            return "SUCCESS"

        errors = []
        with semaphore:
            if "quotes" in self.stages:
                try:
                    execute_quotes_etl(symbol)
                except Exception as e:
                    errors.append(f"quotes: {e}")
            if "company" in self.stages:
                try:
                    execute_company_etl(symbol)
                except Exception as e:
                    errors.append(f"company: {e}")
            if "finance" in self.stages:
                try:
                    execute_finance_etl(symbol)
                except Exception as e:
                    errors.append(f"finance: {e}")
            if "trading" in self.stages:
                try:
                    execute_trading_etl(symbol)
                except Exception as e:
                    errors.append(f"trading: {e}")
        
        if errors:
            raise Exception(" | ".join(errors))
        return "SUCCESS"

    def run_parallel(self, symbols) -> list:
        to_process = []
        for sym in symbols:
            if self.checkpoints.get(sym, {}).get("status") == "SUCCESS":
                continue
            to_process.append(sym)

        if not to_process:
            return []

        safe_print(f"\n[ORCHESTRATOR] Bắt đầu chạy đa luồng song song cho {len(to_process)} mã...")
        failed_symbols = []
        
        if tqdm:
            pbar = tqdm(total=len(to_process), desc="ETL Processing", unit="sym")
        else:
            pbar = None

        # Khởi tạo ThreadPoolExecutor để chạy song song
        with concurrent.futures.ThreadPoolExecutor(max_workers=ARGS.workers) as executor:
            futures = {executor.submit(self.process_symbol, sym): sym for sym in to_process}
            
            try:
                for future in concurrent.futures.as_completed(futures):
                    sym = futures[future]
                    try:
                        future.result(timeout=120)
                        self.save_checkpoint(sym, "SUCCESS")
                    except concurrent.futures.TimeoutError:
                        safe_print(f"[TIMEOUT] Mã {sym} chạy vượt quá 120s")
                        failed_symbols.append(sym)
                        self.save_checkpoint(sym, "TIMEOUT", error="Timeout 120s")
                    except Exception as e:
                        failed_symbols.append(sym)
                        self.save_checkpoint(sym, "FAILED", error=str(e))
                    
                    if pbar:
                        pbar.update(1)
                        
                    del futures[future]
                    gc.collect()
            except KeyboardInterrupt:
                safe_print("\n[!] Nhận lệnh dừng (Ctrl+C). Đang hủy các tác vụ...")
                for f in futures:
                    f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                sys.exit(1)

        if pbar:
            pbar.close()
        return failed_symbols

    def retry_failed(self, failed_symbols, max_rounds=1):
        if not failed_symbols:
            return []
            
        safe_print(f"\n[ETL RETRY LEVEL 2] Bắt đầu thực hiện chạy lại cho {len(failed_symbols)} mã lỗi...")
        current_failed = failed_symbols
        
        for round_idx in range(1, max_rounds + 1):
            if not current_failed:
                break
            safe_print(f"\n--- Vòng chạy lại {round_idx}/{max_rounds} cho {len(current_failed)} mã ---")
            
            for sym in current_failed:
                self.save_checkpoint(sym, "PENDING_RETRY")
                
            current_failed = self.run_parallel(current_failed)
        return current_failed

    def check_data_completeness(self, symbols):
        """Đo lường mức độ hoàn thiện dữ liệu giữa MongoDB và TimescaleDB"""
        safe_print("\n[BÁO CÁO MỨC ĐỘ HOÀN THIỆN DỮ LIỆU]")
        report = {}
        missing_or_incomplete = []
        
        if ARGS.dry_run:
            return report, missing_or_incomplete

        start_dt = pd.to_datetime(START_DATE)
        end_dt = pd.to_datetime(TODAY)
        expected_days = len(pd.bdate_range(start=start_dt, end=end_dt))
        if expected_days == 0:
            expected_days = 1
            
        print(f"{'Mã':<10} | {'Giá Nến%':<10} | {'Đặt Lệnh%':<10} | {'Tài Chính':<10} | {'Trạng Thái':<10}")
        print("-" * 65)
        
        for sym in symbols:
            try:
                # 1. Đo độ phủ giá nến trong Timescale
                sql_quote = f"SELECT COUNT(*) FROM quote_history WHERE symbol = '{sym}' AND trading_date >= '{START_DATE}'"
                quote_count = pd.read_sql(text(sql_quote), db.engine).iloc[0,0]
                quote_pct = float((quote_count / expected_days) * 100)
                
                # 2. Đo độ phủ lệnh giao dịch
                sql_trade = f"SELECT COUNT(*) FROM trading_order_stats WHERE symbol = '{sym}' AND trading_date >= '{START_DATE}'"
                trade_count = pd.read_sql(text(sql_trade), db.engine).iloc[0,0]
                trade_pct = float((trade_count / expected_days) * 100)
                
                # 3. Kiểm tra thông số tài chính
                sql_fin = f"SELECT COUNT(*) FROM finance WHERE symbol = '{sym}'"
                fin_count = pd.read_sql(text(sql_fin), db.engine).iloc[0,0]
                fin_exists = bool(fin_count > 0)
                
                status = "SUCCESS"
                if not fin_exists:
                    status = "MISSING"
                elif quote_pct < 80 or trade_pct < 80:
                    status = "INCOMPLETE"
                    
                report[sym] = {
                    "quote_coverage": round(quote_pct, 2),
                    "trading_coverage": round(trade_pct, 2),
                    "finance_exists": fin_exists,
                    "status": status
                }
                
                print(f"{sym:<10} | {quote_pct:<9.1f}% | {trade_pct:<9.1f}% | {'Có' if fin_exists else 'Không':<10} | {status:<10}")
                
                if status in ["MISSING", "INCOMPLETE"]:
                    missing_or_incomplete.append(sym)
                    if self.checkpoints.get(sym, {}).get("status") != "SUCCESS":
                        self.save_checkpoint(sym, status)
            except Exception as e:
                safe_print(f"[ERROR] Kiểm tra dữ liệu lỗi cho {sym}: {e}")

        # Lưu báo cáo thống kê hoàn thiện ra ổ đĩa
        report_file = os.path.join(os.getcwd(), f"etl_completeness_report_{RUN_ID}_{TODAY}.json")
        try:
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=4)
        except Exception as e:
            safe_print(f"[WARNING] Không lưu được báo cáo: {e}")
            
        return report, missing_or_incomplete


# =============================================================================
# HÀM CHẠY CHÍNH (MAIN ENTRY POINT)
# =============================================================================
def main():
    notifier = AlertNotifier()
    try:
        log_stage("run", None, "started", extra={"daily_mode": ARGS.daily})

        # Bước 1: Đồng bộ danh sách mã chứng khoán (Listing)
        symbols = etl_sync_listing()
        if "VNINDEX" not in symbols:
            symbols.append("VNINDEX")

        # Lọc danh sách mã dựa trên đối số đầu vào (nếu có)
        if ARGS.symbols:
            requested_symbols = {symbol.upper() for symbol in ARGS.symbols}
            symbols = [s for s in symbols if s.upper() in requested_symbols]
            safe_print(f"\nLọc theo --symbols, còn {len(symbols)} mã: {symbols[:10]}")

        # Định nghĩa các phân hệ cần chạy
        selected_stages = ARGS.stages or ["quotes", "company", "finance", "trading"]

        if ARGS.limit:
            symbols = symbols[: ARGS.limit]

        safe_print(f"\nTổng số mã chạy ETL: {len(symbols)}")
        safe_print(f"Các Stage sẽ chạy: {selected_stages}")

        if not symbols:
            safe_print("\nKhông có mã chứng khoán nào khả dụng để chạy!")
            log_stage("run", None, "completed", extra={"remaining_symbols": 0})
            return

        # Khởi tạo bộ điều phối ETL
        orchestrator = ETLOrchestrator(selected_stages)
        
        # 1. Chạy đa luồng song song (Extract -> Transform -> Load)
        failed_symbols = orchestrator.run_parallel(symbols)
        
        # 2. Thử lại cấp 2 cho các mã thất bại
        final_failed = orchestrator.retry_failed(failed_symbols, max_rounds=1)
        
        # 3. Kiểm định mức độ hoàn thiện dữ liệu giữa các DB
        report, incomplete = orchestrator.check_data_completeness(symbols)
        
        final_failed = [s for s in final_failed if orchestrator.checkpoints.get(s, {}).get("status") != "SUCCESS"]

        # 4. Gửi báo cáo hoàn thành qua Telegram
        notifier.send_summary(total=len(symbols), failed=final_failed, incomplete=incomplete)

        log_stage("run", None, "completed", extra={"processed_symbols": len(symbols)})

        # 5. Làm mới các bảng tổng hợp và view tối ưu hóa
        if ARGS.backfill:
            safe_print("\nĐang làm mới (Refresh) Continuous Aggregates của TimescaleDB...")
            db.refresh_historical_aggregates(start=START_DATE)

        safe_print("\nĐang làm mới các Materialized View trên Dashboard...")
        try:
            import create_dashboard_views
            create_dashboard_views.refresh_views()
        except Exception as e:
            safe_print(f"Lỗi refresh dashboard views: {e}")

    except Exception as e:
        err_trace = traceback.format_exc()
        safe_print(f"CRITICAL ERROR trong main: {e}\n{err_trace}")
        notifier.send_message(f"🚨 <b>[CRITICAL ERROR]</b>\nRun ID: {RUN_ID}\nLỗi:\n<pre>{str(e)}</pre>")

if __name__ == "__main__":
    main()
