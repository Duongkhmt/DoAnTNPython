from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_v_symbols():
    db = DatabaseManager()
    query = text("SELECT symbol, MAX(trading_date) FROM technical_indicators WHERE symbol LIKE 'V%' GROUP BY symbol")
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_v_symbols()
