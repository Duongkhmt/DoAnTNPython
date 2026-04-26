from sqlalchemy import text
from timescale_utils import DatabaseManager


def create_predictions_table():
    db = DatabaseManager()
    engine = db.engine
    
    if engine is None:
        print("[ERROR] Khong the ket noi Database PostgreSQL!")
        return
        
    sql = """
    CREATE TABLE IF NOT EXISTS ml_predictions (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        predict_date DATE NOT NULL,
        target_date DATE NOT NULL,
        predicted_close FLOAT NOT NULL,
        trend VARCHAR(20),
        model_used VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, target_date)
    );
    
    -- Tạo Index để phục vụ việc truy vấn Backend (Web) được mượt mà hơn
    CREATE INDEX IF NOT EXISTS idx_ml_pred_symbol ON ml_predictions (symbol);
    CREATE INDEX IF NOT EXISTS idx_ml_pred_target_date ON ml_predictions (target_date);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        print("[OK] Da tao thanh cong bang `ml_predictions` trong Database!")
    except Exception as e:
        print(f"[ERROR] Co loi khi tao bang: {e}")

if __name__ == "__main__":
    create_predictions_table()
