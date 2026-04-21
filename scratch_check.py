import sys
sys.path.append(r'venv\Lib\site-packages')
from timescale_utils import DatabaseManager
import pandas as pd

try:
    db = DatabaseManager()
    df = pd.read_sql("SELECT report_period, item_name, value FROM finance WHERE symbol='TCB' AND item_name IN ('P/E', 'P/B') ORDER BY report_period DESC LIMIT 10", db.engine)
    print("FINANCE P/E P/B:")
    print(df)
except Exception as e:
    print(e)
