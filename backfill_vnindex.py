import os
import sys
import pickle
import json
import pandas as pd
from sqlalchemy import text
from timescale_utils import DatabaseManager
from daily_predict import predict_vnindex_trend

def main():
    db = DatabaseManager()
    engine = db.engine
    
    # Query all available dates in technical_indicators for VNINDEX
    sql = """
    SELECT trading_date 
    FROM technical_indicators 
    WHERE symbol = 'VNINDEX'
    ORDER BY trading_date DESC
    LIMIT 40
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
        
    if df.empty:
        print("No VNINDEX indicators found.")
        return
        
    dates = df["trading_date"].tolist()
    print(f"Found {len(dates)} dates. Running backfill...")
    
    for dt in dates:
        # Convert to string date format 'YYYY-MM-DD'
        dt_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
        res = predict_vnindex_trend(engine, dt_str)
        if res:
            print(f"Success for {dt_str}: {res['trend']} (score={res['probability']:.4f})")
        else:
            print(f"Failed or skipped for {dt_str}")

if __name__ == "__main__":
    main()
