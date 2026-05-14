import psycopg2

def check_dates():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        cur.execute("SELECT DISTINCT trading_date FROM quote_history ORDER BY trading_date DESC LIMIT 5")
        dates = cur.fetchall()
        print(f"Latest dates in quote_history: {dates}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_dates()
