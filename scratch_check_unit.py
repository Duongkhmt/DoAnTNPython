import psycopg2

def check_trading_values():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        cur.execute("SELECT fr_buy_value FROM trading WHERE fr_buy_value > 0 LIMIT 1")
        val = cur.fetchone()[0]
        print(f"Sample fr_buy_value from trading: {val}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_trading_values()
