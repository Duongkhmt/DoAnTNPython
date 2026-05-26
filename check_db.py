import pandas as pd
from sqlalchemy import create_engine

try:
    engine = create_engine("postgresql://postgres:postgres@localhost:5433/vnstock_ts")
    df = pd.read_sql("SELECT * FROM trading_order_stats WHERE trading_date = '2026-05-25' LIMIT 10", engine)
    print("Data from trading_order_stats for 2026-05-25:")
    print(df.to_string())
except Exception as e:
    print(f"Error: {e}")
