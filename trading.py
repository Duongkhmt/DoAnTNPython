# test_sources.py
from vnstock_data import Trading
from datetime import datetime, timedelta

symbol = "SHB"
start = "2026-04-20"
end = "2026-04-25"

# Test tất cả source có thể có
for source in ["VCI", "CAFEF", "KBS"]:
    print(f"\n=== source={source} ===")
    try:
        tr = Trading(symbol=symbol, source=source)
        
        # Test foreign_trade
        df = tr.foreign_trade(start=start, end=end)
        print(f"foreign_trade cols: {df.columns.tolist()}")
        print(df.head(2).to_string())
        
    except Exception as e:
        print(f"foreign_trade lỗi: {e}")
    
    try:
        tr = Trading(symbol=symbol, source=source)
        df = tr.prop_trade(start=start, end=end)
        print(f"prop_trade cols: {df.columns.tolist()}")
        print(df.head(2).to_string())
    except Exception as e:
        print(f"prop_trade lỗi: {e}")

    try:
        tr = Trading(symbol=symbol, source=source)
        df = tr.side_stats(start=start, end=end)
        print(f"side_stats cols: {df.columns.tolist()}")
        print(df.head(2).to_string())
    except Exception as e:
        print(f"side_stats lỗi: {e}")