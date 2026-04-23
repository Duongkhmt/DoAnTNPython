import sys
import math
import time
import uuid
import argparse
from datetime import datetime, timedelta

# Add Windows venv site-packages when the project is mounted into Docker.
docker_venv_path = "/opt/airflow/dags/venv/Lib/site-packages"
if docker_venv_path not in sys.path:
    sys.path.append(docker_venv_path)

import pandas as pd
from vnstock_data import Listing, Quote, Finance, Company, Trading

from mongo_utils import MongoManager
from timescale_utils import DatabaseManager

print("=" * 80)
print("MASTER SYNC CHUNG KHOAN (VNSTOCK DATA)")
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
    parser.add_argument("--stage-sleep", type=float, default=0.1, help="Nghi giua tung request")
    parser.add_argument("--batch-sleep", type=float, default=5.0, help="Nghi giua cac batch")
    return parser.parse_args()


ARGS = parse_args()

if ARGS.daily:
    START_DATE = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    print(f"[*] CHE DO DONG BO HANG NGAY: {START_DATE} den nay")
else:
    START_DATE = "2024-01-01"
    print(f"[*] CHE DO DONG BO LICH SU: {START_DATE} den nay")


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
    df_clean["industry"] = None
    df_clean["sector"] = None

    db.upsert_dataframe(df_clean, "listing", conflict_cols=["symbol"])
    log_stage("listing", None, "success", extra={"symbols": len(df_clean)})
    return df_clean["symbol"].tolist()


def sync_quotes(symbols):
    print(f"\n[2] DANG DONG BO QUOTE CHO {len(symbols)} MA...")
    print(f"    Tu {START_DATE} den {TODAY}")

    success = 0
    for idx, symbol in enumerate(symbols):
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

    print(f"\n  Quote xong! Thanh cong: {success}/{len(symbols)}.")


def sync_company(symbols):
    print(f"\n[3] DANG DONG BO COMPANY CHO {len(symbols)} MA...")

    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"  [{idx + 1}/{len(symbols)}] Company: {symbol}")
        try:
            c = Company(symbol=symbol, source="VCI")
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
                df_clean["industry"] = df.get("icb_name2", None)
                df_clean["sector"] = df.get("icb_name3", None)

                db.upsert_dataframe(df_clean, "company", conflict_cols=["symbol"])
                log_stage("company", symbol, "success", extra={"rows": len(df_clean)})
                success += 1
            else:
                log_stage("company", symbol, "empty")

        except Exception as exc:
            print(f"    -> Bo qua Company {symbol}: {exc}")
            log_stage("company", symbol, "error", message=str(exc))

        time.sleep(ARGS.stage_sleep)

    print(f"\n  Company xong! Thanh cong: {success}/{len(symbols)}.")


def sync_finance(symbols):
    print(f"\n[4] DANG DONG BO FINANCE CHO {len(symbols)} MA...")

    success = 0
    for idx, symbol in enumerate(symbols):
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
            print(f"    -> Bo qua Finance {symbol}: {exc}")
            log_stage("finance", symbol, "error", message=str(exc))

        time.sleep(ARGS.stage_sleep)

    print(f"\n  Finance xong! Thanh cong: {success}/{len(symbols)}.")


def sync_trading(symbols):
    print(f"\n[5] DANG DONG BO TRADING CHO {len(symbols)} MA...")

    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"  [{idx + 1}/{len(symbols)}] Trading: {symbol}")
        try:
            tr = Trading(symbol=symbol, source="VCI")
            df_foreign = tr.foreign_trade(start=START_DATE, end=TODAY)
            df_prop = tr.prop_trade(start=START_DATE, end=TODAY)
            save_raw(
                "trading_foreign",
                symbol,
                "VCI",
                df_foreign.to_dict(orient="records"),
                {"rows": len(df_foreign), "start_date": START_DATE, "end_date": TODAY},
            )
            save_raw(
                "trading_prop",
                symbol,
                "VCI",
                df_prop.to_dict(orient="records"),
                {"rows": len(df_prop), "start_date": START_DATE, "end_date": TODAY},
            )

            if len(df_foreign) > 0 or len(df_prop) > 0:
                all_dates = set()
                if len(df_foreign) > 0:
                    all_dates.update(df_foreign["trading_date"].tolist())
                if len(df_prop) > 0:
                    all_dates.update(df_prop["trading_date"].tolist())

                fr_map = df_foreign.set_index("trading_date").to_dict("index") if len(df_foreign) > 0 else {}
                prop_map = df_prop.set_index("trading_date").to_dict("index") if len(df_prop) > 0 else {}

                records = []
                for trading_date in all_dates:
                    f_row = fr_map.get(trading_date, {})
                    p_row = prop_map.get(trading_date, {})
                    records.append(
                        {
                            "symbol": symbol,
                            "trading_date": trading_date,
                            "fr_buy_volume": float(f_row.get("fr_buy_volume_matched", 0) or 0),
                            "fr_sell_volume": float(f_row.get("fr_sell_volume_matched", 0) or 0),
                            "prop_buy_volume": float(p_row.get("total_buy_trade_volume", 0) or 0),
                            "prop_sell_volume": float(p_row.get("total_sell_trade_volume", 0) or 0),
                        }
                    )

                df_clean = pd.DataFrame(records)
                df_clean["trading_date"] = pd.to_datetime(df_clean["trading_date"]).dt.date
                db.upsert_dataframe(df_clean, "trading", conflict_cols=["symbol", "trading_date"])
                log_stage("trading", symbol, "success", extra={"rows": len(df_clean)})
            else:
                log_stage("trading", symbol, "empty")

            try:
                tr_cafe = Trading(symbol=symbol, source="CAFEF")
                df_order = tr_cafe.order_stats(start=START_DATE, end=TODAY)
                save_raw(
                    "trading_order_stats",
                    symbol,
                    "CAFEF",
                    df_order.to_dict(orient="records"),
                    {"rows": len(df_order), "start_date": START_DATE, "end_date": TODAY},
                )
                if len(df_order) > 0:
                    df_clean = df_order.copy()
                    if "symbol" not in df_clean.columns:
                        df_clean["symbol"] = symbol
                    if "trading_date" not in df_clean.columns:
                        if isinstance(df_clean.index, pd.DatetimeIndex) or df_clean.index.name == "date":
                            df_clean["trading_date"] = df_clean.index.date
                    if "trading_date" in df_clean.columns:
                        df_clean["trading_date"] = pd.to_datetime(df_clean["trading_date"]).dt.date
                        db.upsert_dataframe(df_clean, "trading_order_stats", conflict_cols=["symbol", "trading_date"])
            except Exception:
                pass

            try:
                df_sum = tr.summary(start=START_DATE, end=TODAY)
                save_raw(
                    "trading_summary",
                    symbol,
                    "VCI",
                    df_sum.to_dict(orient="records"),
                    {"rows": len(df_sum), "start_date": START_DATE, "end_date": TODAY},
                )
                if len(df_sum) > 0:
                    df_clean = df_sum.copy()
                    df_clean["symbol"] = symbol

                    if "trading_date" not in df_clean.columns:
                        df_clean["trading_date"] = TODAY
                    if "total_volume" in df_clean.columns:
                        df_clean["total_trading_vol"] = df_clean["total_volume"]
                    if "total_value" in df_clean.columns:
                        df_clean["total_trading_val"] = df_clean["total_value"]

                    expected_cols = [
                        "symbol",
                        "trading_date",
                        "total_trading_vol",
                        "total_trading_val",
                        "open_price",
                        "highest_price",
                        "lowest_price",
                        "close_price",
                    ]
                    for col in expected_cols:
                        if col not in df_clean.columns:
                            df_clean[col] = None
                    df_clean = df_clean[expected_cols]

                    df_clean["trading_date"] = pd.to_datetime(df_clean["trading_date"]).dt.date
                    for col in [
                        "total_trading_vol",
                        "total_trading_val",
                        "open_price",
                        "highest_price",
                        "lowest_price",
                        "close_price",
                    ]:
                        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

                    db.upsert_dataframe(df_clean, "trading_summary", conflict_cols=["symbol", "trading_date"])
            except Exception:
                pass

            success += 1
            time.sleep(ARGS.stage_sleep)
        except Exception as exc:
            log_stage("trading", symbol, "error", message=str(exc))

    print(f"\n  Trading xong! Thanh cong: {success}/{len(symbols)}.")


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def get_done_symbols():
    try:
        sql = """
        SELECT DISTINCT symbol
        FROM quote_history
        WHERE trading_date = CURRENT_DATE
        """
        df = pd.read_sql(sql, db.engine)
        return set(df["symbol"].tolist())
    except Exception as exc:
        print(f"Canh bao get_done_symbols: {exc}")
        return set()


def main():
    demo_mode = False
    log_stage("run", None, "started", extra={"daily_mode": ARGS.daily})

    symbols = sync_listing()

    if demo_mode:
        symbols = ["TCB", "FPT", "VNM", "SHB", "ACB"]
        print(f"\n[DEMO] Chay thu {len(symbols)} ma: {symbols}")

    if ARGS.symbols:
        requested_symbols = {symbol.upper() for symbol in ARGS.symbols}
        symbols = [s for s in symbols if s.upper() in requested_symbols]
        print(f"\nLoc theo --symbols, con {len(symbols)} ma: {symbols[:10]}")

    done_symbols = get_done_symbols()
    if not ARGS.symbols:
        symbols = [s for s in symbols if s not in done_symbols]

    if ARGS.limit:
        symbols = symbols[: ARGS.limit]

    selected_stages = ARGS.stages or ["quotes", "company", "finance", "trading"]

    print(f"\nTong ma: {len(symbols) + len(done_symbols)}")
    print(f"  Da co: {len(done_symbols)}")
    print(f"  Con crawl: {len(symbols)}")
    print(f"  Stage se chay: {selected_stages}")

    if not symbols:
        print("\nKhong con gi de crawl!")
        log_stage("run", None, "completed", extra={"remaining_symbols": 0})
        if ARGS.backfill:
            db.refresh_historical_aggregates(start=START_DATE)
        return

    batch_size = max(1, ARGS.batch_size)
    total_batches = math.ceil(len(symbols) / batch_size)
    print(f"\nBat dau {total_batches} batch (moi batch {batch_size} ma)...")

    for i, batch in enumerate(chunk_list(symbols, batch_size)):
        print("\n" + "=" * 55)
        print(f"  BATCH {i + 1}/{total_batches} | {len(batch)} ma | Vi du: {batch[:5]}")
        print("=" * 55)

        for symbol in batch:
            try:
                if "quotes" in selected_stages:
                    sync_quotes([symbol])
                if "company" in selected_stages:
                    sync_company([symbol])
                if "finance" in selected_stages:
                    sync_finance([symbol])
                if "trading" in selected_stages:
                    sync_trading([symbol])
                print(f"  DONE {symbol}")
            except Exception as exc:
                print(f"  Loi {symbol}: {exc}")
                log_stage("symbol", symbol, "error", message=str(exc))
            time.sleep(ARGS.stage_sleep)

        done_now = len(done_symbols) + (i + 1) * batch_size
        total = len(symbols) + len(done_symbols)
        percent = min(100, round(done_now * 100 / total, 2))
        print(f"\n  Tien do: ~{percent}%")
        print(f"  Nghi {ARGS.batch_sleep}s...")
        time.sleep(ARGS.batch_sleep)

    print("\nDONE ALL!")
    log_stage("run", None, "completed", extra={"processed_symbols": len(symbols)})

    if ARGS.backfill:
        print("\nBackfill Continuous Aggregates...")
        db.refresh_historical_aggregates(start=START_DATE)


if __name__ == "__main__":
    main()
