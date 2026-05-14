import psycopg2

def check_ai_data():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="vnstock_ts",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()
        
        # Check ml_predictions
        print("--- Checking ml_predictions ---")
        cur.execute("SELECT COUNT(*) FROM ml_predictions")
        count = cur.fetchone()[0]
        print(f"Total rows in ml_predictions: {count}")
        
        if count > 0:
            cur.execute("SELECT MAX(predict_date) FROM ml_predictions")
            max_date = cur.fetchone()[0]
            print(f"Latest predict_date: {max_date}")
            
            cur.execute("SELECT symbol, predict_date, ai_score, ai_signal FROM ml_predictions ORDER BY predict_date DESC LIMIT 5")
            rows = cur.fetchall()
            for row in rows:
                print(row)
        
        # Check technical_indicators (for joining)
        print("\n--- Checking technical_indicators ---")
        cur.execute("SELECT COUNT(*) FROM technical_indicators")
        print(f"Total rows in technical_indicators: {cur.fetchone()[0]}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_ai_data()
