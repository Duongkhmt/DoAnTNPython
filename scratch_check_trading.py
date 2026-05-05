import pandas as pd
from sqlalchemy import text
from timescale_utils import DatabaseManager

db = DatabaseManager()
with db.engine.connect() as conn:
    df = pd.read_sql(text("SELECT COUNT(*) as cnt FROM trading WHERE trading_date = '2026-04-28'"), conn)
    print("Count trading 2026-04-28:", df['cnt'].iloc[0])
    
    df_all = pd.read_sql(text("SELECT MAX(trading_date) as max_dt FROM trading"), conn)
    print("Max trading date in DB:", df_all['max_dt'].iloc[0])
