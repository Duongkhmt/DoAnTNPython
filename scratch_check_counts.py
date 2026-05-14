import psycopg2

def check_ai_counts():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        cur.execute("""
            SELECT predict_date, COUNT(*) 
            FROM ml_predictions 
            GROUP BY predict_date 
            ORDER BY predict_date DESC 
            LIMIT 5
        """)
        rows = cur.fetchall()
        print("Predictions count per date:")
        for row in rows:
            print(row)
            
        cur.execute("""
            SELECT symbol, predict_date 
            FROM ml_predictions 
            WHERE predict_date = (SELECT MAX(predict_date) FROM ml_predictions)
        """)
        latest_symbols = cur.fetchall()
        print(f"\nSymbols on latest date ({rows[0][0]}):")
        for sym in latest_symbols:
            print(sym)
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_ai_counts()
