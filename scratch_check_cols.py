import psycopg2

def check_columns():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM technical_indicators LIMIT 0")
        colnames = [desc[0] for desc in cur.description]
        print(f"Technical indicators columns: {colnames}")
        
        cur.execute("SELECT * FROM quote_history LIMIT 0")
        colnames = [desc[0] for desc in cur.description]
        print(f"Quote history columns: {colnames}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_columns()
