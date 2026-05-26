from vnstock_data import Trading
from datetime import datetime, timedelta

try:
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    print(f"Checking VCI trading data from {start_date} to {end_date}...")
    
    tr = Trading(symbol="FPT", source="VCI")
    df = tr.price_history(start=start_date, end=end_date)
    
    if df is not None and not df.empty:
        print("\n[SUCCESS] Retrieved data. Latest trading dates available from VCI:")
        print(df['trading_date'].tail().to_string(index=False))
        latest_date = pd.to_datetime(df['trading_date'].iloc[-1]).strftime("%Y-%m-%d")
        if latest_date == end_date:
            print(f"\n=> Dữ liệu hôm nay ({end_date}) ĐÃ CÓ trên VCI!")
        else:
            print(f"\n=> VCI mới chỉ có dữ liệu đến ngày: {latest_date}. Dữ liệu hôm nay ({end_date}) CHƯA CÓ.")
    else:
        print("Data is empty.")
except Exception as e:
    print(f"Error checking VCI API: {e}")
