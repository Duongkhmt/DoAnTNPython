from timescale_utils import DatabaseManager
from sqlalchemy import text
import pandas as pd

def list_tables():
    db = DatabaseManager()
    query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    with db.engine.connect() as conn:
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    list_tables()
