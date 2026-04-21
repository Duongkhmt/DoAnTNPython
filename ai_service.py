from fastapi import FastAPI, BackgroundTasks
import uvicorn
import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from timescale_utils import DatabaseManager
import os
import datetime

# Ẩn bớt các cảnh báo log không quan trọng của TensorFlow
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

app = FastAPI(title="Stock AI Prediction Service API")

def run_predictions():
    print("\n[AI-Service] Bắt đầu kích hoạt quy trình dự đoán hàng loạt...")
    db = DatabaseManager()
    engine = db.engine
    
    if engine is None:
        print("❌ [AI-Service] Lỗi: Không thể kết nối Database PostgreSQL cục bộ!")
        return
        
    model_path = "global_stock_model.h5"
    if not os.path.exists(model_path):
        # Nếu chưa Train xong Global Model, dùng tạm model HPG đã chạy thành công hồi nãy
        print("[AI-Service] Không tìm thấy Siêu Mô Hình (Global), sử dụng tạm phân hệ độc lấp HPG...")
        model_path = "hpg_lstm_model.h5"
        if not os.path.exists(model_path):
            print("❌ [AI-Service] Hoàn toàn không tìm thấy file h5 nào để chạy thuật toán.")
            return

    try:
        model = tf.keras.models.load_model(model_path)
        print(f"✅ [AI-Service] Hạch tâm nạp thành công: {model_path}")
    except Exception as e:
        print(f"❌ [AI-Service] Lỗi tải thư mục lõi: {e}")
        return

    # Xác định danh sách tính toán (Gom thành cụm top List để làm đại diện)
    symbol_list = ["HPG"] 
    if "global" in model_path:
        symbol_list = ["HPG", "VNM", "FPT", "TCB", "SSI"] # Tránh treo máy, ban đầu chạy top 5 mã này
        
    features = ['close', 'volume', 'sma_10', 'sma_20', 'macd', 'bb_upper', 'bb_lower']
    
    with engine.begin() as conn:
        for symbol in symbol_list:
            query = text("""
                SELECT 
                    q.close, q.volume, t.sma_10, t.sma_20, t.macd, t.bb_upper, t.bb_lower, q.trading_date
                FROM quote_history q
                JOIN technical_indicators t 
                  ON q.symbol = t.symbol AND q.trading_date = t.trading_date
                WHERE q.symbol = :sym
                ORDER BY q.trading_date ASC
            """)
            df = pd.read_sql(query, conn, params={"sym": symbol}).dropna()
            
            if len(df) < 60:
                print(f"⚠️ [AI-Service] Bỏ lỡ {symbol} vì niêm yết thiếu phiên (Chưa đủ 60).")
                continue
                
            dates = df['trading_date'].values
            data = df[features].values
            
            # Quá trình đồng bộ mảng tỉ lệ
            scaler = MinMaxScaler(feature_range=(0, 1))
            scaled_data = scaler.fit_transform(data)
            
            last_60_days = scaled_data[-60:]
            X_input = np.array([last_60_days])
            
            # Predict
            pred_scaled = model.predict(X_input, verbose=0)
            
            dummy_matrix = np.zeros((1, len(features)))
            dummy_matrix[0, 0] = pred_scaled[0, 0]
            pred_price = scaler.inverse_transform(dummy_matrix)[0, 0]
            
            last_actual_price = data[-1, 0]
            diff = pred_price - last_actual_price
            trend = "TĂNG" if diff > 0 else "GIẢM"
            
            # Lịch phiên tương lai (Tìm ngày giao dịch phi thứ 7/CN tiếp theo)
            last_date = pd.to_datetime(dates[-1])
            target_date = last_date + pd.Timedelta(days=1)
            if target_date.weekday() >= 5: # 5: Sat, 6: Sun
                target_date += pd.Timedelta(days=(7 - target_date.weekday()))
                
            # Đẩy kết quả lưu vào Database (Bảng ml_predictions ta vừa tạo ở Bước 1)
            insert_sql = text("""
                INSERT INTO ml_predictions (symbol, predict_date, target_date, predicted_close, trend, model_used)
                VALUES (:sym, CURRENT_DATE, :tdate, :pclose, :trend, :model)
                ON CONFLICT (symbol, target_date) 
                DO UPDATE SET predicted_close = EXCLUDED.predicted_close, trend = EXCLUDED.trend, created_at = CURRENT_TIMESTAMP
            """)
            
            conn.execute(insert_sql, {
                "sym": symbol,
                "tdate": target_date.date(),
                "pclose": float(pred_price),
                "trend": trend,
                "model": model_path
            })
            print(f"  👉 Đoán {symbol}: {pred_price:,.0f} VND (Hướng: {trend}) | Chờ kiểm định ngày: {target_date.date()}")

    print("🏁 [AI-Service] Mọi tính toán AI đã được ghi tệp thành công!")

@app.post("/daily_predict_all")
def trigger_daily_prediction(background_tasks: BackgroundTasks):
    """
    Airflow ping (chọc) Endpoint này sau khi cập nhật VNSTOCK xong.
    API trả lời ngay OK, và thiết lập luồng run_predictions chạy ngầm. 
    Không bao giờ báo lỗi Timeout.
    """
    background_tasks.add_task(run_predictions)
    return {"status": "success", "message": "Tiến trình AI đã được gọi dậy và đang chạy ngầm trong máy tính của bạn."}

@app.get("/")
def health_check():
    return {"status": "ok", "service": "LSTM AI Microservice Đang Chạy Mượt Mà"}

if __name__ == "__main__":
    # Để API có thể nghe mọi liên kết trong docker gửi ra, cần chạy host 0.0.0.0
    uvicorn.run(app, host="0.0.0.0", port=8000)
