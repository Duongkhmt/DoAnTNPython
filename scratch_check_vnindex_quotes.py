from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_vnindex_quotes():
    db = DatabaseManager()
    engine = db.engine
    
    query = text("""
        SELECT trading_date, close, volume 
        FROM quote_history 
        WHERE symbol = 'VNINDEX' AND trading_date >= '2026-05-10' 
        ORDER BY trading_date DESC
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_vnindex_quotes()
