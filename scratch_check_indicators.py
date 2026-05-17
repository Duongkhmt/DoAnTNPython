from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def check_indicators_status():
    db = DatabaseManager()
    engine = db.engine
    
    query = text("""
        SELECT 
            trading_date, 
            COUNT(*) as total_count,
            COUNT(CASE WHEN volume_sma_20 > 100000 THEN 1 END) as filtered_count,
            EXISTS(SELECT 1 FROM technical_indicators t2 WHERE t2.symbol = 'VNINDEX' AND t2.trading_date = t1.trading_date) as has_vnindex
        FROM technical_indicators t1
        WHERE trading_date >= '2026-05-10'
        GROUP BY trading_date
        ORDER BY trading_date DESC
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_indicators_status()
