import psycopg2

def check_valuation_type():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dashboard_valuation'")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        
        cur.execute("SELECT trading_date FROM dashboard_valuation LIMIT 1")
        val = cur.fetchone()[0]
        print(f"Sample trading_date value: {val}, Type: {type(val)}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_valuation_type()
