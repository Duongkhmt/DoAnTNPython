import sys
import pandas as pd
from datetime import datetime

docker_venv_path = "/opt/airflow/dags/venv/Lib/site-packages"
if docker_venv_path not in sys.path:
    sys.path.append(docker_venv_path)

from vnstock_data import Quote
from timescale_utils import DatabaseManager

def main():
    db = DatabaseManager(database="vnstock_ts")
    symbol = "VNINDEX"
    start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print("Fetching VNINDEX from", start_date, "to", end_date)
    
    try:
        q = Quote(symbol=symbol, source="VND")
        df = q.history(start=start_date, end=end_date, interval="1D")
        
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
            print("Saved", len(df_clean), "rows of VNINDEX into quote_history.")
            
            listing_df = pd.DataFrame([{
                "symbol": "VNINDEX",
                "organ_name": "VN-INDEX",
                "exchange": "HOSE",
                "company_type": "INDEX",
                "industry": "Market",
                "sector": "Market"
            }])
            db.upsert_dataframe(listing_df, "listing", conflict_cols=["symbol"])
            print("Saved VNINDEX into listing.")
            
        else:
            print("Empty result from VNStock API.")
            
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
