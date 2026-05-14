import psycopg2

def check_signals():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        cur.execute("SELECT DISTINCT ai_signal FROM ml_predictions")
        signals = cur.fetchall()
        print("Signals found in ml_predictions:")
        for s in signals:
            print(s)
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_signals()
