from sqlalchemy import text

from timescale_utils import DatabaseManager


CREATE_ML_PREDICTIONS_SQL = """
CREATE TABLE IF NOT EXISTS ml_predictions (
    symbol VARCHAR(10) NOT NULL,
    predict_date DATE NOT NULL,
    target_date DATE,
    ai_score FLOAT,
    ai_signal VARCHAR(20),
    model_name VARCHAR(100),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    predicted_close FLOAT,
    trend VARCHAR(20),
    model_used VARCHAR(100)
);

ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS symbol VARCHAR(10);
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS predict_date DATE;
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS target_date DATE;
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS ai_score FLOAT;
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS ai_signal VARCHAR(20);
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS model_name VARCHAR(100);
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS model_version VARCHAR(50);
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS predicted_close FLOAT;
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS trend VARCHAR(20);
ALTER TABLE ml_predictions ADD COLUMN IF NOT EXISTS model_used VARCHAR(100);
ALTER TABLE ml_predictions ALTER COLUMN predicted_close DROP NOT NULL;
ALTER TABLE ml_predictions ALTER COLUMN trend DROP NOT NULL;
ALTER TABLE ml_predictions ALTER COLUMN model_used DROP NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_ml_predictions_symbol_predict_date
    ON ml_predictions (symbol, predict_date);
CREATE INDEX IF NOT EXISTS idx_ml_pred_predict_date
    ON ml_predictions (predict_date);
CREATE INDEX IF NOT EXISTS idx_ml_pred_signal
    ON ml_predictions (ai_signal);
CREATE INDEX IF NOT EXISTS idx_ml_pred_score
    ON ml_predictions (predict_date, ai_score DESC);
"""


def create_predictions_table():
    db = DatabaseManager()
    engine = db.engine

    if engine is None:
        print("[ERROR] Khong the ket noi Database PostgreSQL!")
        return

    try:
        with engine.begin() as conn:
            for stmt in CREATE_ML_PREDICTIONS_SQL.strip().split(";"):
                sql = stmt.strip()
                if sql:
                    conn.execute(text(sql))
        print("[OK] Bang `ml_predictions` da san sang cho XGBoost daily prediction.")
    except Exception as exc:
        print(f"[ERROR] Co loi khi tao/cap nhat bang ml_predictions: {exc}")


def clear_predictions_table():
    db = DatabaseManager()
    engine = db.engine
    if engine is None:
        print("[ERROR] Khong the ket noi Database PostgreSQL!")
        return

    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM ml_predictions"))
        print("[OK] Da xoa toan bo du lieu cu trong ml_predictions.")
    except Exception as exc:
        print(f"[ERROR] Khong the xoa du lieu ml_predictions: {exc}")


if __name__ == "__main__":
    create_predictions_table()
