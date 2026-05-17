from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_indicators():
    db = DatabaseManager()
    query = text("SELECT trading_date, rsi_14 FROM technical_indicators WHERE symbol = 'VNINDEX' AND trading_date >= '2026-05-12' ORDER BY trading_date DESC")
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_indicators()
