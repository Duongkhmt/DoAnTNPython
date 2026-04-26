import psycopg2

def check_valuation():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        cur.execute("SELECT symbol, trading_date, pe, pb FROM dashboard_valuation WHERE symbol = 'SHB' ORDER BY trading_date DESC LIMIT 5")
        rows = cur.fetchall()
        print("Valuation Data for SHB:")
        for row in rows:
            print(row)
        
        cur.execute("SELECT COUNT(*) FROM dashboard_valuation")
        count = cur.fetchone()[0]
        print(f"Total rows in dashboard_valuation: {count}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_valuation()
