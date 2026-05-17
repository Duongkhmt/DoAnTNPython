from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_latest_dates():
    db = DatabaseManager()
    engine = db.engine
    
    queries = {
        "quote_history": "SELECT MAX(trading_date) FROM quote_history",
        "technical_indicators": "SELECT MAX(trading_date) FROM technical_indicators",
        "ml_predictions": "SELECT MAX(predict_date) FROM ml_predictions",
        "trading_summary": "SELECT MAX(trading_date) FROM trading_summary"
    }
    
    results = {}
    with engine.connect() as conn:
        for table, query in queries.items():
            try:
                res = conn.execute(text(query)).scalar()
                results[table] = res
            except Exception as e:
                results[table] = f"Error: {e}"
                
    print("\n--- Latest Dates in Database ---")
    for table, date in results.items():
        print(f"{table:20}: {date}")

if __name__ == "__main__":
    check_latest_dates()
