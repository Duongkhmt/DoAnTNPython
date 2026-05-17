from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_dashboard_data():
    db = DatabaseManager()
    engine = db.engine
    
    # Check if we have records for 2026-05-14 in views or summary tables
    queries = {
        "market_regime": "SELECT * FROM market_regime WHERE trading_date = '2026-05-14'",
        "trading_summary_count": "SELECT COUNT(*) FROM trading_summary WHERE trading_date = '2026-05-14'",
        "ml_predictions_count": "SELECT COUNT(*) FROM ml_predictions WHERE predict_date = '2026-05-14'"
    }
    
    for name, sql in queries.items():
        print(f"\n--- Checking {name} ---")
        try:
            df = pd.read_sql(text(sql), engine)
            print(df)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    check_dashboard_data()
