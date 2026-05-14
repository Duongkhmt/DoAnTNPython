import pandas as pd
from timescale_utils import DatabaseManager

db = DatabaseManager()
df = pd.read_sql("SELECT COUNT(*) FROM quote_history WHERE symbol='VNINDEX'", db.engine)
print("VNINDEX COUNT:", df.iloc[0,0])

df_listing = pd.read_sql("SELECT COUNT(*) FROM listing WHERE symbol='VNINDEX'", db.engine)
print("VNINDEX IN LISTING:", df_listing.iloc[0,0])
