import os
from typing import Any

import numpy as np
import pandas as pd
import tensorflow as tf
import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import text

from create_ml_table import create_predictions_table
from timescale_utils import DatabaseManager

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

app = FastAPI(title="Stock AI Prediction Service API")

DEFAULT_FEATURES = ["close", "volume", "sma_10", "sma_20", "macd", "bb_upper", "bb_lower"]


def _get_model_path() -> str:
    candidates = [
        os.getenv("AI_MODEL_PATH"),
        "global_stock_model.h5",
        "hpg_lstm_model.h5",
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("No .h5 model file found. Set AI_MODEL_PATH or mount a model file.")


def _get_symbol_list(model_path: str) -> list[str] | None:
    env_symbols = os.getenv("AI_SYMBOLS")
    if env_symbols:
        if env_symbols.upper() == "ALL":
            return None
        return [symbol.strip().upper() for symbol in env_symbols.split(",") if symbol.strip()]
    
    # Mặc định trả về None để hệ thống tự động lấy toàn bộ mã từ Database
    return None


def _get_all_symbols_from_db(engine: Any) -> list[str]:
    """Lấy danh sách tất cả mã cổ phiếu có trong bảng quote_history."""
    query = text("SELECT DISTINCT symbol FROM quote_history ORDER BY symbol")
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)
    return df["symbol"].tolist()


def ensure_prediction_table() -> None:
    create_predictions_table()


def fetch_prediction_input(engine: Any, symbol: str) -> pd.DataFrame:
    query = text(
        """
        SELECT
            q.close,
            q.volume,
            t.sma_10,
            t.sma_20,
            t.macd,
            t.bb_upper,
            t.bb_lower,
            q.trading_date
        FROM quote_history q
        JOIN technical_indicators t
          ON q.symbol = t.symbol
         AND q.trading_date = t.trading_date
        WHERE q.symbol = :sym
        ORDER BY q.trading_date ASC
        """
    )
    with engine.begin() as conn:
        return pd.read_sql(query, conn, params={"sym": symbol}).dropna()


def upsert_prediction(engine: Any, symbol: str, predicted_close: float, trend: str, target_date, model_path: str) -> None:
    insert_sql = text(
        """
        INSERT INTO ml_predictions (
            symbol, predict_date, target_date, predicted_close, trend, model_used
        )
        VALUES (:sym, CURRENT_DATE, :tdate, :pclose, :trend, :model)
        ON CONFLICT (symbol, target_date)
        DO UPDATE SET
            predicted_close = EXCLUDED.predicted_close,
            trend = EXCLUDED.trend,
            model_used = EXCLUDED.model_used,
            created_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "sym": symbol,
                "tdate": target_date,
                "pclose": predicted_close,
                "trend": trend,
                "model": os.path.basename(model_path),
            },
        )


def run_predictions(symbols: list[str] | None = None, batch_size: int = 50) -> dict[str, Any]:
    print(f"\n[AI-Service] Bắt đầu quy trình dự đoán (Batch Size: {batch_size})...")
    ensure_prediction_table()

    db = DatabaseManager()
    engine = db.engine
    if engine is None:
        raise RuntimeError("Không thể kết nối TimescaleDB/PostgreSQL.")

    model_path = _get_model_path()
    model = tf.keras.models.load_model(model_path)
    print(f"[AI-Service] Model loaded: {model_path}")

    # Xác định danh sách mã
    if symbols is None:
        symbols = _get_symbol_list(model_path)
    
    if symbols is None:
        print("[AI-Service] Đang tải danh sách mã từ Database...")
        symbols = _get_all_symbols_from_db(engine)

    print(f"[AI-Service] Tổng cộng có {len(symbols)} mã cần xử lý.")
    results = []

    # Chia nhỏ danh sách mã thành các lô (Chunks)
    for i in range(0, len(symbols), batch_size):
        current_batch = symbols[i : i + batch_size]
        print(f"\n[*] Đang xử lý lô {i//batch_size + 1} ({len(current_batch)} mã)...")
        
        batch_inputs = []
        batch_metadata = []

        for symbol in current_batch:
            try:
                df = fetch_prediction_input(engine, symbol)
                if len(df) < 60:
                    print(f"  - Skip {symbol}: khong du 60 dong du lieu.")
                    results.append({"symbol": symbol, "status": "skipped", "reason": "not_enough_rows"})
                    continue

                dates = df["trading_date"].values
                data = df[DEFAULT_FEATURES].values

                # Scaling riêng cho từng mã (Quan trọng!)
                scaler = MinMaxScaler(feature_range=(0, 1))
                scaled_data = scaler.fit_transform(data)
                
                # Lấy 60 ngày cuối
                x_input = scaled_data[-60:]
                
                batch_inputs.append(x_input)
                batch_metadata.append({
                    "symbol": symbol,
                    "scaler": scaler,
                    "last_actual_price": float(data[-1, 0]),
                    "last_date": pd.to_datetime(dates[-1])
                })
            except Exception as e:
                print(f"  [ERROR] Loi khi chuan bi du lieu cho {symbol}: {e}")
                results.append({"symbol": symbol, "status": "error", "message": str(e)})

        if not batch_inputs:
            continue

        # Dự đoán hàng loạt cho bước đầu tiên (t+1)
        X_array = np.array(batch_inputs)
        
        # Chúng ta sẽ thực hiện dự báo 7 ngày liên tiếp (Recursive Multi-step)
        FORECAST_DAYS = 7
        
        for step in range(FORECAST_DAYS):
            # Dự đoán cho cả batch hiện tại
            X_array = np.array(batch_inputs)
            all_preds_scaled = model.predict(X_array, verbose=0)
            
            new_batch_inputs = []
            
            for idx, pred_scaled in enumerate(all_preds_scaled):
                meta = batch_metadata[idx]
                
                # 1. Giải mã giá dự báo
                dummy_matrix = np.zeros((1, len(DEFAULT_FEATURES)))
                dummy_matrix[0, 0] = pred_scaled[0]
                predicted_close = float(meta["scaler"].inverse_transform(dummy_matrix)[0, 0])
                
                # 2. Tính ngày mục tiêu (Bỏ qua cuối tuần)
                target_date = meta["last_date"] + pd.Timedelta(days=1)
                while target_date.weekday() >= 5:
                    target_date += pd.Timedelta(days=1)
                
                # 3. Xác định xu hướng so với giá thực tế cuối cùng
                trend = "TANG" if predicted_close > meta["last_actual_price"] else "GIAM"
                
                # 4. Lưu vào database
                try:
                    upsert_prediction(engine, meta["symbol"], predicted_close, trend, target_date.date(), model_path)
                    if step == 0:
                         print(f"  [OK] {meta['symbol']} step {step+1}: {predicted_close:,.0f} | {target_date.date()}")
                except Exception as e:
                    print(f"  [ERROR] Loi khi luu {meta['symbol']} buoc {step+1}: {e}")

                # 5. Chuẩn bị cho bước dự báo tiếp theo (Recursive)
                # Cập nhật meta cho vòng lặp tiếp theo
                meta["last_date"] = target_date
                
                # Cập nhật window (X_input)
                # Lấy window hiện tại của mã này
                current_window = batch_inputs[idx] # (60, 7)
                
                # Tạo dòng mới cho window
                # [close, volume, sma_10, sma_20, macd, bb_upper, bb_lower]
                # Giả định các chỉ số kỹ thuật giữ nguyên hoặc biến động nhẹ để duy trì window
                new_row = current_window[-1].copy()
                new_row[0] = pred_scaled[0] # Cập nhật Close dự báo (đã scale)
                
                # Cập nhật SMA đơn giản (nếu muốn ngoằn ngoèo hơn)
                # Ở đây chúng ta giữ các features khác để tránh nhiễu quá mức trong 7 ngày
                
                # Slide window
                next_window = np.append(current_window[1:], [new_row], axis=0)
                new_batch_inputs.append(next_window)
                
                if step == 0:
                    results.append({
                        "symbol": meta["symbol"],
                        "status": "predicted",
                        "steps": FORECAST_DAYS
                    })
            
            # Cập nhật batch_inputs cho ngày tiếp theo
            batch_inputs = new_batch_inputs

    return {
        "status": "completed",
        "forecast_days": 7,
        "model_used": os.path.basename(model_path),
        "total_symbols": len(symbols)
    }


@app.on_event("startup")
def on_startup() -> None:
    ensure_prediction_table()


@app.post("/daily_predict_all")
def trigger_daily_prediction(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_predictions)
    return {"status": "accepted", "message": "Prediction task started in background."}


@app.post("/predict_now")
def predict_now(symbols: list[str] | None = None):
    try:
        return run_predictions(symbols=symbols)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/predictions/latest")
def latest_predictions(limit: int = 20):
    ensure_prediction_table()
    db = DatabaseManager()
    engine = db.engine
    if engine is None:
        raise HTTPException(status_code=500, detail="Cannot connect to database.")

    query = text(
        """
        SELECT symbol, predict_date, target_date, predicted_close, trend, model_used, created_at
        FROM ml_predictions
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as conn:
        rows = pd.read_sql(query, conn, params={"limit": limit})
    return rows.to_dict(orient="records")


@app.get("/")
def health_check():
    model_path = os.getenv("AI_MODEL_PATH", "global_stock_model.h5")
    return {"status": "ok", "service": "ai-service", "model_path": model_path}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
