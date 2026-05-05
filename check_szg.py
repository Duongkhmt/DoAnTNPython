from timescale_utils import DatabaseManager
import pandas as pd

db = DatabaseManager()
sql = """
    SELECT
        q.symbol,
        q.trading_date,
        q.open,
        q.high,
        q.low,
        q.close,
        q.volume,
        t.sma_20,
        t.sma_50,
        t.bb_upper,
        t.bb_lower
    FROM quote_history q
    LEFT JOIN technical_indicators t
      ON t.symbol = q.symbol
     AND t.trading_date = q.trading_date
    WHERE q.symbol = 'SZG'
    ORDER BY q.trading_date DESC
    LIMIT 60
"""
prices = pd.read_sql(sql, db.engine).sort_values("trading_date")
print(prices)
