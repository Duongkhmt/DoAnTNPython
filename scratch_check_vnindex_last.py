from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_vnindex_quote():
    db = DatabaseManager()
    query = text("SELECT * FROM quote_history WHERE symbol = 'VNINDEX' ORDER BY trading_date DESC LIMIT 5")
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_vnindex_quote()
