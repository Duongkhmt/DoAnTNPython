import pandas as pd
from sqlalchemy import text
from timescale_utils import DatabaseManager

def main():
    db = DatabaseManager()
    engine = db.engine
    
    # Check what columns exist and if return_5d has values
    sql = """
    SELECT trading_date, return_5d
    FROM technical_indicators
    WHERE symbol = 'VNINDEX'
    ORDER BY trading_date DESC
    LIMIT 20
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    print("Technical indicators for VNINDEX (latest 20):")
    print(df)
    
    # Count non-null return_5d
    sql_count = """
    SELECT COUNT(*) AS total,
           COUNT(return_5d) AS non_null_returns
    FROM technical_indicators
    WHERE symbol = 'VNINDEX'
    """
    with engine.begin() as conn:
        df_count = pd.read_sql(text(sql_count), conn)
    print("\nCounts:")
    print(df_count)

if __name__ == "__main__":
    main()
