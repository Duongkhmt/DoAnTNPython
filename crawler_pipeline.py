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

# Add Windows venv site-packages when the project is mounted into Docker.
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
print("MASTER SYNC CHUNG KHOAN (VNSTOCK DATA) - CONCURRENT PIPELINE")
print("=" * 80)

db = DatabaseManager(database="vnstock_ts")
db.create_all_tables()

mongo = MongoManager()
RUN_ID = uuid.uuid4().hex[:12]

TODAY = datetime.now().strftime("%Y-%m-%d")

def parse_args():
    parser = argparse.ArgumentParser(description="Master sync VNStock data")
    parser.add_argument("--daily", action="store_true", help="Dong bo du lieu gan day")
    parser.add_argument("--backfill", action="store_true", help="Refresh aggregate sau khi crawl")
    parser.add_argument("--symbols", nargs="*", help="Chi crawl cac ma chi dinh")
    parser.add_argument("--limit", type=int, help="Gioi han so ma can crawl")
    parser.add_argument("--batch-size", type=int, default=25, help="So ma moi batch")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["quotes", "company", "finance", "trading"],
        help="Chi chay cac stage duoc chi dinh",
    )
    parser.add_argument("--stage-sleep", type=float, default=0.5, help="Nghi giua tung request")
    parser.add_argument("--batch-sleep", type=float, default=5.0, help="Nghi giua cac batch")
    
    # New concurrent args
    parser.add_argument("--workers", type=int, default=4, help="So worker threads")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Max concurrent API calls")
    parser.add_argument("--dry-run", action="store_true", help="Khong goi API hay ghi DB")
    return parser.parse_args()

ARGS = parse_args()

if ARGS.daily:
    START_DATE = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    print(f"[*] CHE DO DONG BO HANG NGAY: {START_DATE} den nay")
else:
    START_DATE = "2020-01-01"
    print(f"[*] CHE DO DONG BO LICH SU: {START_DATE} den nay")


# ==========================================
# THREAD SAFETY & LOCKS
# ==========================================
print_lock = threading.Lock()
checkpoint_lock = threading.Lock()
semaphore = threading.Semaphore(ARGS.max_concurrent)

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# ==========================================
# DO NOT TOUCH: ORIGINAL LOGIC
# ==========================================
def save_raw(dataset, symbol, source, payload, metadata=None):
    mongo.save_raw_payload(
        dataset=dataset,
        symbol=symbol,
        source=source,
        payload=payload,
        run_id=RUN_ID,
        metadata=metadata or {},
    )

def log_stage(stage, symbol, status, message=None, extra=None):
    mongo.log_crawl(
        run_id=RUN_ID,
        stage=stage,
        symbol=symbol,
        status=status,
        message=message,
        extra=extra or {},
    )

def sync_listing():
    print("\n[1] DANG DONG BO DANH SACH MA CHUNG KHOAN...")

    lst = Listing(source="VND")
    df_listing = lst.all_symbols()
    save_raw(
        "listing",
        None,
        "VND",
        df_listing.to_dict(orient="records"),
        {"rows": len(df_listing)},
    )

    if "type" in df_listing.columns:
        df_listing = df_listing[df_listing["type"].isin(["STOCK", "Cổ phiếu", "CP"])]

    if len(df_listing) == 0:
        print("Khong keo duoc danh sach ma. Thoat!")
        log_stage("listing", None, "empty")
        return []

    df_clean = pd.DataFrame()
    df_clean["symbol"] = df_listing["symbol"]
    df_clean["organ_name"] = df_listing.get("company_name", None)
    df_clean["exchange"] = df_listing.get("exchange", None)
    df_clean["company_type"] = df_listing.get("type", None)

    db.upsert_dataframe(df_clean, "listing", conflict_cols=["symbol"])
    log_stage("listing", None, "success", extra={"symbols": len(df_clean)})
    return df_clean["symbol"].tolist()

def sync_quotes(symbols):
    if len(symbols) > 1:
        print(f"\n[2] DANG DONG BO QUOTE CHO {len(symbols)} MA...")
        print(f"    Tu {START_DATE} den {TODAY}")

    success = 0
    for idx, symbol in enumerate(symbols):
        if len(symbols) > 1:
            print(f"  [{idx + 1}/{len(symbols)}] Quote: {symbol}")
        try:
            q = Quote(symbol=symbol, source="VND")
            df = q.history(start=START_DATE, end=TODAY, interval="1D")
            save_raw(
                "quote_history",
                symbol,
                "VND",
                df.to_dict(orient="records"),
                {"rows": len(df), "start_date": START_DATE, "end_date": TODAY},
            )

            if len(df) > 0:
                df_clean = df.copy()
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

                db.upsert_dataframe(df_clean, "quote_history", conflict_cols=["symbol", "trading_date"])
                log_stage("quotes", symbol, "success", extra={"rows": len(df_clean)})
                success += 1
            else:
                log_stage("quotes", symbol, "empty")

            time.sleep(ARGS.stage_sleep)
        except Exception as exc:
            log_stage("quotes", symbol, "error", message=str(exc))
            raise exc

    if len(symbols) > 1:
        print(f"\n  Quote xong! Thanh cong: {success}/{len(symbols)}.")

def sync_company(symbols):
    if len(symbols) > 1:
        print(f"\n[3] DANG DONG BO COMPANY CHO {len(symbols)} MA...")

    success = 0
    for idx, symbol in enumerate(symbols):
        if len(symbols) > 1:
            print(f"  [{idx + 1}/{len(symbols)}] Company: {symbol}")
        try:
            c = Company(symbol=symbol, source="KBS")
            df = c.overview()
            save_raw(
                "company",
                symbol,
                "VCI",
                df.to_dict(orient="records"),
                {"rows": len(df)},
            )

            if len(df) > 0:
                df_clean = pd.DataFrame()
                df_clean["symbol"] = [symbol]
                
                # Cố gắng lấy từ nguồn API (nếu sau này API hoạt động lại)
                industry_api = df.get("icb_name2", None)
                if isinstance(industry_api, pd.Series):
                    industry_api = industry_api.iloc[0] if not industry_api.empty else None

                # Load JSON fallback map once
                if not hasattr(sync_company, "fallback_map"):
                    import json
                    import os
                    map_path = os.path.join(os.path.dirname(__file__), 'industry_mapping.json')
                    if os.path.exists(map_path):
                        sync_company.fallback_map = json.load(open(map_path, 'r', encoding='utf-8'))
                    else:
                        sync_company.fallback_map = {}
                
                fallback_map = sync_company.fallback_map
                
                final_industry = industry_api
                if final_industry is None or pd.isna(final_industry):
                    if symbol in fallback_map:
                        final_industry = fallback_map[symbol].get('industry')
                        df_clean["sector"] = fallback_map[symbol].get('sector', 'Khác')
                    
                    if final_industry is None or pd.isna(final_industry):
                        final_industry = 'Khác'
                        df_clean["sector"] = df.get("icb_name3", 'Khác')
                else:
                    df_clean["sector"] = df.get("icb_name3", 'Khác')
                            
                df_clean["industry"] = final_industry



                db.upsert_dataframe(df_clean, "company", conflict_cols=["symbol"])
                log_stage("company", symbol, "success", extra={"rows": len(df_clean)})
                success += 1
            else:
                log_stage("company", symbol, "empty")

        except Exception as exc:
            # print(f"    -> Bo qua Company {symbol}: {exc}")
            log_stage("company", symbol, "error", message=str(exc))
            raise exc

        time.sleep(ARGS.stage_sleep)

    if len(symbols) > 1:
        print(f"\n  Company xong! Thanh cong: {success}/{len(symbols)}.")

def sync_finance(symbols):
    if len(symbols) > 1:
        print(f"\n[4] DANG DONG BO FINANCE CHO {len(symbols)} MA...")

    success = 0
    for idx, symbol in enumerate(symbols):
        if len(symbols) > 1:
            print(f"  [{idx + 1}/{len(symbols)}] Finance: {symbol}")
        try:
            f = Finance(symbol=symbol, source="VCI")
            df = f.ratio()
            save_raw(
                "finance_ratio",
                symbol,
                "VCI",
                df.reset_index().to_dict(orient="records") if len(df) > 0 else [],
                {"rows": len(df)},
            )

            if len(df) > 0:
                df = df.reset_index()
                period_col = df.columns[0]

                cols_to_drop = [
                    period_col,
                    "report_period",
                    "Ratio TTM Id",
                    "Ratio Type",
                    "Ratio Year Id",
                    "ticker",
                    "symbol",
                ]
                val_cols = [c for c in df.columns if c not in cols_to_drop]

                df_melted = pd.melt(
                    df,
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

                df_clean = df_melted[["symbol", "report_type", "report_period", "item_name", "value"]]
                df_clean = df_clean.drop_duplicates(
                    subset=["symbol", "report_type", "report_period", "item_name"],
                    keep="last",
                )

                db.upsert_dataframe(
                    df_clean,
                    "finance",
                    conflict_cols=["symbol", "report_type", "report_period", "item_name"],
                )
                log_stage("finance", symbol, "success", extra={"rows": len(df_clean)})
                success += 1
            else:
                log_stage("finance", symbol, "empty")

        except Exception as exc:
            # print(f"    -> Bo qua Finance {symbol}: {exc}")
            log_stage("finance", symbol, "error", message=str(exc))
            raise exc

        time.sleep(ARGS.stage_sleep)

    if len(symbols) > 1:
        print(f"\n  Finance xong! Thanh cong: {success}/{len(symbols)}.")

def sync_trading(symbols):
    if len(symbols) > 1:
        print(f"\n[5] DANG DONG BO TRADING CHO {len(symbols)} MA...")

    success = 0
    for idx, symbol in enumerate(symbols):
        if len(symbols) > 1:
            print(f"  [{idx + 1}/{len(symbols)}] Trading: {symbol}")
        symbol_ok = True

        tr = Trading(symbol=symbol, source="VCI")

        # --- Foreign trade + Prop trade ---
        df_foreign = pd.DataFrame()
        df_prop = pd.DataFrame()

        try:
            df_foreign = tr.foreign_trade(start=START_DATE, end=TODAY)
            if df_foreign is not None and len(df_foreign) > 0:
                save_raw("trading_foreign", symbol, "VCI", df_foreign.to_dict(orient="records"), {"rows": len(df_foreign)})
            else:
                df_foreign = pd.DataFrame()
        except Exception as exc:
            # print(f"    -> Bo qua foreign_trade {symbol}: {exc}")
            df_foreign = pd.DataFrame()

        try:
            df_prop = tr.prop_trade(start=START_DATE, end=TODAY)
            if df_prop is not None and len(df_prop) > 0:
                save_raw("trading_prop", symbol, "VCI", df_prop.to_dict(orient="records"), {"rows": len(df_prop)})
            else:
                df_prop = pd.DataFrame()
        except Exception as exc:
            # print(f"    -> Bo qua prop_trade {symbol}: {exc}")
            df_prop = pd.DataFrame()

        try:
            if len(df_foreign) > 0 or len(df_prop) > 0:
                all_dates = set()
                if len(df_foreign) > 0:
                    all_dates.update(df_foreign["trading_date"].tolist())
                if len(df_prop) > 0:
                    all_dates.update(df_prop["trading_date"].tolist())

                fr_map   = df_foreign.set_index("trading_date").to_dict("index") if len(df_foreign) > 0 else {}
                prop_map = df_prop.set_index("trading_date").to_dict("index")    if len(df_prop)    > 0 else {}

                records = []
                for trading_date in all_dates:
                    f_row = fr_map.get(trading_date, {})
                    p_row = prop_map.get(trading_date, {})
                    records.append({
                        "symbol":           symbol,
                        "trading_date":     trading_date,
                        "fr_buy_volume":    float(f_row.get("fr_buy_volume_matched",  0) or 0),
                        "fr_sell_volume":   float(f_row.get("fr_sell_volume_matched", 0) or 0),
                        "fr_buy_value":     float(f_row.get("fr_buy_value_matched",   0) or 0) / 1e9,
                        "fr_sell_value":    float(f_row.get("fr_sell_value_matched",  0) or 0) / 1e9,
                        "prop_buy_volume":  float(p_row.get("total_buy_trade_volume", 0) or 0),
                        "prop_sell_volume": float(p_row.get("total_sell_trade_volume",0) or 0),
                        "prop_buy_value":   float(p_row.get("total_buy_trade_value",  0) or 0) / 1e9,
                        "prop_sell_value":  float(p_row.get("total_sell_trade_value", 0) or 0) / 1e9,
                    })

                df_clean = pd.DataFrame(records)
                df_clean["trading_date"] = pd.to_datetime(df_clean["trading_date"]).dt.date
                db.upsert_dataframe(df_clean, "trading", conflict_cols=["symbol", "trading_date"])
                log_stage("trading", symbol, "success", extra={"rows": len(df_clean)})
            else:
                log_stage("trading", symbol, "empty")

        except Exception as exc:
            # print(f"    -> Bo qua foreign/prop {symbol}: {exc}")
            log_stage("trading", symbol, "error", message=str(exc))
            symbol_ok = False

        # --- Price history ---
        try:
            df_ph = tr.price_history(start=START_DATE, end=TODAY)
            save_raw("trading_price_history", symbol, "VCI",
                     df_ph.to_dict(orient="records"), {"rows": len(df_ph)})

            if len(df_ph) > 0:
                df_ph = df_ph.copy()
                df_ph["symbol"]       = symbol
                df_ph["trading_date"] = pd.to_datetime(df_ph["trading_date"]).dt.date

                df_sum = pd.DataFrame({
                    "symbol":            df_ph["symbol"],
                    "trading_date":      df_ph["trading_date"],
                    "total_trading_vol": pd.to_numeric(df_ph["total_volume"],  errors="coerce"),
                    "total_trading_val": pd.to_numeric(df_ph["total_value"],   errors="coerce"),
                    "open_price":        pd.to_numeric(df_ph["open"],          errors="coerce"),
                    "highest_price":     pd.to_numeric(df_ph["high"],          errors="coerce"),
                    "lowest_price":      pd.to_numeric(df_ph["low"],           errors="coerce"),
                    "close_price":       pd.to_numeric(df_ph["close"],         errors="coerce"),
                })
                db.upsert_dataframe(df_sum, "trading_summary",
                                    conflict_cols=["symbol", "trading_date"])

                # Dam bao cac cot order ton tai de khong bi KeyError voi cac ma thieu thanh khoan
                for col in ["total_buy_trade", "total_sell_trade", "total_buy_trade_volume", 
                            "total_sell_trade_volume", "average_buy_trade_volume", "average_sell_trade_volume"]:
                    if col not in df_ph.columns:
                        df_ph[col] = None

                df_ord = pd.DataFrame({
                    "symbol":               df_ph["symbol"],
                    "trading_date":         df_ph["trading_date"],
                    "buy_orders":           pd.to_numeric(df_ph["total_buy_trade"],           errors="coerce"),
                    "sell_orders":          pd.to_numeric(df_ph["total_sell_trade"],          errors="coerce"),
                    "buy_volume":           pd.to_numeric(df_ph["total_buy_trade_volume"],    errors="coerce"),
                    "sell_volume":          pd.to_numeric(df_ph["total_sell_trade_volume"],   errors="coerce"),
                    "avg_buy_order_volume": pd.to_numeric(df_ph["average_buy_trade_volume"],  errors="coerce"),
                    "avg_sell_order_volume":pd.to_numeric(df_ph["average_sell_trade_volume"], errors="coerce"),
                })
                db.upsert_dataframe(df_ord, "trading_order_stats",
                                    conflict_cols=["symbol", "trading_date"])
                log_stage("price_history", symbol, "success", extra={"rows": len(df_ph)})

        except Exception as exc:
            # print(f"    -> Bo qua price_history {symbol}: {exc}")
            log_stage("price_history", symbol, "error", message=str(exc))
            raise exc

        # --- Put through ---
        try:
            df_pt = tr.put_through(start=START_DATE, end=TODAY)
            if len(df_pt) > 0:
                save_raw("trading_put_through", symbol, "VCI",
                         df_pt.to_dict(orient="records"), {"rows": len(df_pt)})
        except Exception:
            pass

        # --- Insider deal ---
        try:
            df_ins = tr.insider_deal()
            if len(df_ins) > 0:
                save_raw("trading_insider_deal", symbol, "VCI",
                         df_ins.to_dict(orient="records"), {"rows": len(df_ins)})
        except Exception:
            pass

        if symbol_ok:
            success += 1
        time.sleep(ARGS.stage_sleep)

    if len(symbols) > 1:
        print(f"\n  Trading xong! Thanh cong: {success}/{len(symbols)}.")

def _load_symbol_set(sql, params=None):
    try:
        df = pd.read_sql(text(sql), db.engine, params=params or {})
        if "symbol" not in df.columns:
            return set()
        return set(df["symbol"].dropna().tolist())
    except Exception as exc:
        safe_print(f"Canh bao load_symbol_set: {exc}")
        return set()

def get_done_symbols(selected_stages):
    if not selected_stages:
        return set()

    done_sets = []

    if "quotes" in selected_stages:
        done_sets.append(_load_symbol_set("""
            SELECT DISTINCT symbol
            FROM quote_history
            WHERE trading_date = CURRENT_DATE
        """))

    if "trading" in selected_stages:
        done_sets.append(_load_symbol_set("""
            SELECT DISTINCT symbol
            FROM trading_order_stats
            WHERE trading_date = CURRENT_DATE
        """))

    if "company" in selected_stages:
        done_sets.append(_load_symbol_set("""
            SELECT DISTINCT symbol
            FROM company
            WHERE symbol IS NOT NULL
        """))

    if "finance" in selected_stages:
        done_sets.append(_load_symbol_set("""
            SELECT DISTINCT symbol
            FROM finance
            WHERE symbol IS NOT NULL
        """))

    if not done_sets:
        return set()

    result = done_sets[0]
    for symbols in done_sets[1:]:
        result = result.intersection(symbols)
    return result

# ==========================================
# ORCHESTRATION LAYER (NEW)
# ==========================================

class AlertNotifier:
    def __init__(self):
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        
    def send_message(self, text: str):
        if not self.token or not self.chat_id:
            safe_print("[WARNING] Telegram credentials not set. Skip alert.")
            return
            
        max_len = 4096
        chunks = [text[i:i + max_len] for i in range(0, len(text), max_len)]
        
        for chunk in chunks[:2]: # Batch into max 2 messages
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": chunk, "parse_mode": "HTML"}
            try:
                requests.post(url, json=payload, timeout=10)
            except Exception as e:
                safe_print(f"[ERROR] Failed to send Telegram message: {e}")

    def send_summary(self, total: int, failed: list, incomplete: list):
        fail_pct = (len(failed) / total * 100) if total > 0 else 0
        
        msg = f"<b>[CRAWL SUMMARY]</b>\nRun ID: {RUN_ID}\nDate: {TODAY}\n"
        msg += f"Total: {total} | Failed: {len(failed)} ({fail_pct:.1f}%) | Incomplete/Missing: {len(incomplete)}\n"
        
        if len(failed) == 0 and len(incomplete) == 0:
            msg = "✅ " + msg + "\nStatus: ALL SUCCESS"
        else:
            msg = "⚠️ " + msg + "\n"
            if failed:
                msg += "\n<b>Failed Symbols:</b>\n" + ", ".join(failed[:50])
                if len(failed) > 50:
                    msg += f" ... (+{len(failed)-50} more)"
            if incomplete:
                msg += "\n\n<b>Incomplete Symbols:</b>\n" + ", ".join(incomplete[:50])
                if len(incomplete) > 50:
                    msg += f" ... (+{len(incomplete)-50} more)"

        if len(failed) == 0 and len(incomplete) == 0:
            self.send_message(msg)
        elif fail_pct > 10 or incomplete or failed:
            self.send_message(msg)

def retry(max_attempts=2, backoff_factor=1.0):
    """Level 1: Stage-level retry decorator with linear backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(symbol, *args, **kwargs):
            stage = func.__name__.replace('execute_', '')
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
def execute_quotes(symbol):
    sync_quotes([symbol])

@retry(max_attempts=2, backoff_factor=1.0)
def execute_company(symbol):
    sync_company([symbol])

@retry(max_attempts=2, backoff_factor=1.0)
def execute_finance(symbol):
    sync_finance([symbol])

@retry(max_attempts=2, backoff_factor=1.0)
def execute_trading(symbol):
    sync_trading([symbol])

class CrawlOrchestrator:
    def __init__(self, stages):
        self.stages = stages
        self.checkpoint_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawl_checkpoint.json")
        self.checkpoints = self.load_checkpoint()

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                # Nếu là daily mode, giữ checkpoint của 16h qua để khỏi bị reset khi sang ngày mới
                if ARGS.daily:
                    now = datetime.now()
                    cutoff = now - timedelta(hours=16)
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
                safe_print(f"Failed to load checkpoint: {e}")
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
                    safe_print(f"[WARNING] Save checkpoint failed for {symbol}: {e}")

    def process_symbol(self, symbol):
        """Processes a single symbol ensuring stages run sequentially"""
        if ARGS.dry_run:
            safe_print(f"[DRY-RUN] Processing {symbol} - Stages: {self.stages}")
            time.sleep(0.1) # Simulate some work
            return "SUCCESS"

        errors = []
        with semaphore:
            if "quotes" in self.stages:
                try:
                    execute_quotes(symbol)
                except Exception as e:
                    errors.append(f"quotes: {e}")
            if "company" in self.stages:
                try:
                    execute_company(symbol)
                except Exception as e:
                    errors.append(f"company: {e}")
            if "finance" in self.stages:
                try:
                    execute_finance(symbol)
                except Exception as e:
                    errors.append(f"finance: {e}")
            if "trading" in self.stages:
                try:
                    execute_trading(symbol)
                except Exception as e:
                    errors.append(f"trading: {e}")
        
        if errors:
            raise Exception(" | ".join(errors))
            
        return "SUCCESS"

    def run_parallel(self, symbols) -> list:
        to_process = []
        for sym in symbols:
            # Skip only SUCCESS symbols from checkpoint
            if self.checkpoints.get(sym, {}).get("status") == "SUCCESS":
                continue
            to_process.append(sym)

        if not to_process:
            return []

        safe_print(f"\n[ORCHESTRATOR] Bat dau chay song song {len(to_process)} ma...")
        failed_symbols = []
        
        if tqdm:
            pbar = tqdm(total=len(to_process), desc="Crawling", unit="sym")
        else:
            pbar = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=ARGS.workers) as executor:
            futures = {executor.submit(self.process_symbol, sym): sym for sym in to_process}
            
            try:
                for future in concurrent.futures.as_completed(futures):
                    sym = futures[future]
                    try:
                        # Timeout enforcement explicitly inside as_completed as requested
                        result = future.result(timeout=120)
                        self.save_checkpoint(sym, "SUCCESS")
                    except concurrent.futures.TimeoutError:
                        safe_print(f"[TIMEOUT] Symbol={sym} vuot qua 120s")
                        failed_symbols.append(sym)
                        self.save_checkpoint(sym, "TIMEOUT", error="Timeout 120s")
                    except Exception as e:
                        # safe_print(f"[ERROR] Symbol={sym} fail: {str(e)}")
                        failed_symbols.append(sym)
                        self.save_checkpoint(sym, "FAILED", error=str(e))
                    
                    if pbar:
                        pbar.update(1)
                        
                    # Memory cleanup to avoid bloat when crawling 1000+ symbols
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
            
        safe_print(f"\n[RETRY CẤP 2] Bat dau retry {len(failed_symbols)} ma that bai...")
        current_failed = failed_symbols
        
        for round_idx in range(1, max_rounds + 1):
            if not current_failed:
                break
            safe_print(f"\n--- Retry Round {round_idx}/{max_rounds} cho {len(current_failed)} ma ---")
            
            # Reset checkpoint status so it processes again
            for sym in current_failed:
                self.save_checkpoint(sym, "PENDING_RETRY")
                
            current_failed = self.run_parallel(current_failed)
            
        return current_failed

    def check_data_completeness(self, symbols):
        safe_print("\n[KIỂM TRA HOÀN THIỆN DỮ LIỆU]")
        report = {}
        missing_or_incomplete = []
        
        if ARGS.dry_run:
            safe_print("[DRY-RUN] Skip completeness check.")
            return report, missing_or_incomplete

        # Calculate expected trading days (T2-T6)
        start_dt = pd.to_datetime(START_DATE)
        end_dt = pd.to_datetime(TODAY)
        expected_days = len(pd.bdate_range(start=start_dt, end=end_dt))
        if expected_days == 0:
            expected_days = 1
            
        print(f"{'Symbol':<10} | {'Quote%':<10} | {'Trading%':<10} | {'Finance':<10} | {'Status':<10}")
        print("-" * 60)
        
        for sym in symbols:
            try:
                # 1. Quote coverage
                sql_quote = f"SELECT COUNT(*) FROM quote_history WHERE symbol = '{sym}' AND trading_date >= '{START_DATE}'"
                quote_count = pd.read_sql(text(sql_quote), db.engine).iloc[0,0]
                quote_pct = float((quote_count / expected_days) * 100)
                
                # 2. Trading coverage
                sql_trade = f"SELECT COUNT(*) FROM trading_order_stats WHERE symbol = '{sym}' AND trading_date >= '{START_DATE}'"
                trade_count = pd.read_sql(text(sql_trade), db.engine).iloc[0,0]
                trade_pct = float((trade_count / expected_days) * 100)
                
                # 3. Finance exists
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
                
                print(f"{sym:<10} | {quote_pct:<9.1f}% | {trade_pct:<9.1f}% | {'Yes' if fin_exists else 'No':<10} | {status:<10}")
                
                if status in ["MISSING", "INCOMPLETE"]:
                    missing_or_incomplete.append(sym)
                    # Chỉ ghi đè checkpoint nếu trạng thái hiện tại khác SUCCESS
                    if self.checkpoints.get(sym, {}).get("status") != "SUCCESS":
                        self.save_checkpoint(sym, status)
            except Exception as e:
                safe_print(f"[ERROR] Completeness check fail cho {sym}: {e}")

        # Save completeness report
        report_file = os.path.join(os.getcwd(), f"completeness_report_{RUN_ID}_{TODAY}.json")
        try:
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=4)
        except Exception as e:
            safe_print(f"[WARNING] Could not save completeness report: {e}")
            
        return report, missing_or_incomplete

# ==========================================
# MAIN ENTRY POINT
# ==========================================
def main():
    notifier = AlertNotifier()
    try:
        demo_mode = False
        log_stage("run", None, "started", extra={"daily_mode": ARGS.daily})

        symbols = sync_listing()
        if "VNINDEX" not in symbols:
            symbols.append("VNINDEX")


        if demo_mode:
            symbols = ["TCB", "FPT", "VNM", "SHB", "ACB"]
            safe_print(f"\n[DEMO] Chay thu {len(symbols)} ma: {symbols}")

        if ARGS.symbols:
            requested_symbols = {symbol.upper() for symbol in ARGS.symbols}
            symbols = [s for s in symbols if s.upper() in requested_symbols]
            safe_print(f"\nLoc theo --symbols, con {len(symbols)} ma: {symbols[:10]}")

        selected_stages = ARGS.stages or ["quotes", "company", "finance", "trading"]

        done_symbols = get_done_symbols(selected_stages)
        if not ARGS.symbols:
            symbols = [s for s in symbols if s not in done_symbols]

        if ARGS.limit:
            symbols = symbols[: ARGS.limit]

        safe_print(f"\nTong ma: {len(symbols) + len(done_symbols)}")
        safe_print(f"  Da co: {len(done_symbols)}")
        safe_print(f"  Con crawl: {len(symbols)}")
        safe_print(f"  Stage se chay: {selected_stages}")

        if not symbols:
            safe_print("\nKhong con gi de crawl!")
            log_stage("run", None, "completed", extra={"remaining_symbols": 0})
            if ARGS.backfill:
                db.refresh_historical_aggregates(start=START_DATE)
            notifier.send_message(f"✅ <b>[CRAWL SUMMARY]</b>\nRun ID: {RUN_ID}\nDate: {TODAY}\nStatus: Nothing to crawl")
            return

        orchestrator = CrawlOrchestrator(selected_stages)
        
        # 1. Parallel Crawl
        failed_symbols = orchestrator.run_parallel(symbols)
        
        # 2. Level 2 Retry
        final_failed = orchestrator.retry_failed(failed_symbols, max_rounds=1)
        
        # 3. Completeness Check
        report, incomplete = orchestrator.check_data_completeness(symbols)
        
        # 4. Filter missing data out of failed (khong can auto recovery lai)
        final_failed = [s for s in final_failed if orchestrator.checkpoints.get(s, {}).get("status") != "SUCCESS"]

        # 5. Alert & Cleanup
        notifier.send_summary(total=len(symbols), failed=final_failed, incomplete=incomplete)

        log_stage("run", None, "completed", extra={"processed_symbols": len(symbols)})

        if ARGS.backfill:
            safe_print("\nBackfill Continuous Aggregates...")
            db.refresh_historical_aggregates(start=START_DATE)

        safe_print("\nRefreshing Dashboard Views...")
        try:
            import create_dashboard_views
            create_dashboard_views.refresh_views()
        except Exception as e:
            safe_print(f"Lỗi khi refresh dashboard views: {e}")

    except Exception as e:
        err_trace = traceback.format_exc()
        safe_print(f"CRITICAL ERROR in main: {e}\n{err_trace}")
        notifier.send_message(f"🚨 <b>[CRITICAL ERROR]</b>\nRun ID: {RUN_ID}\nDate: {TODAY}\nError:\n<pre>{str(e)}</pre>")

if __name__ == "__main__":
    main()
