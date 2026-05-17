from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def get_predicted_symbols():
    db = DatabaseManager()
    query = text("SELECT DISTINCT symbol FROM ml_predictions WHERE predict_date = '2026-05-14'")
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        symbols = df['symbol'].tolist()
        print(" ".join(symbols))

if __name__ == "__main__":
    get_predicted_symbols()
