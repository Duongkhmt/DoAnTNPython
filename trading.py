from vnstock_data import Trading
from datetime import datetime, timedelta

start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
end   = datetime.now().strftime("%Y-%m-%d")

for source in ["KBS", "VND"]:
    try:
        tr = Trading(symbol="SHB", source=source)
        df = tr.side_stats(start=start, end=end)
        print(f"{source}: {df.columns.tolist()}")
        print(df.head(2))
    except Exception as e:
        print(f"{source}: ERROR - {e}")