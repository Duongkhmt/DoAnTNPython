from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_quote_counts():
    db = DatabaseManager()
    query = text("SELECT trading_date, COUNT(*) FROM quote_history WHERE trading_date >= '2026-05-10' GROUP BY trading_date ORDER BY trading_date DESC")
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_quote_counts()
