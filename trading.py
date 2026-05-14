# crawl_vnindex.py
import pandas as pd
from vnstock_data import Quote
from timescale_utils import DatabaseManager

db  = DatabaseManager(database="vnstock_ts")
eng = db.engine

print("Đang crawl VNINDEX...")

for source in ["VCI", "VND"]:
    try:
        q  = Quote(symbol="VNINDEX", source=source)
        df = q.history(
            start="2020-01-01",
            end="2026-05-12",
            interval="1D"
        )
        print(f"Source {source}: {len(df)} rows")
        print(df.tail(3))

        if len(df) > 0:
            df_clean = df.copy()

            # Đổi tên cột time → trading_date
            if "time" in df_clean.columns:
                df_clean = df_clean.rename(
                    columns={"time": "trading_date"}
                )

            df_clean["symbol"] = "VNINDEX"
            df_clean["trading_date"] = pd.to_datetime(
                df_clean["trading_date"]
            ).dt.date

            expected = [
                "symbol", "trading_date",
                "open", "high", "low", "close", "volume"
            ]
            for col in expected:
                if col not in df_clean.columns:
                    df_clean[col] = None
            df_clean = df_clean[expected]

            db.upsert_dataframe(
                df_clean, "quote_history",
                conflict_cols=["symbol", "trading_date"]
            )
            print(f"✅ Đã lưu {len(df_clean)} dòng VNINDEX vào DB!")
            break

    except Exception as e:
        print(f"Source {source} lỗi: {e}")
        continue