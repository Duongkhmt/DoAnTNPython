import os
import argparse
import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from timescale_utils import DatabaseManager

# Ẩn bớt các cảnh báo log không quan trọng của TensorFlow
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

def predict_next_day(symbol="HPG", model_path="hpg_lstm_model.h5"):
    print(f"🔍 Bắt đầu khởi động chạy kiểm tra thuật toán cho mã {symbol}...")
    
    if not os.path.exists(model_path):
        print(f"❌ Lỗi: Không tìm thấy file mô hình '{model_path}'.")
        print("💡 Hướng dẫn:")
        print("  1. Hãy lên Google Drive, tải file hpg_lstm_model.h5 về máy tính.")
        print("  2. Bỏ file vừa tải vào thư mục chứa code: e:\\DoAnPython\\POSTGRESQL_GUIDE\\")
        print("  3. Sau đó chạy lại lệnh này.")
        return

    db = DatabaseManager()
    engine = db.engine
    
    if engine is None:
        print("❌ Lỗi: Không thể kết nối Database PostgreSQL cục bộ!")
        return

    # Lấy lại toàn bộ lịch sử để build lại MinMaxScaler trùng với lúc Train trên Colab
    # Lưu ý: Các cột phải được chọn đúng theo thứ tự đã Train ở Colab
    query = text("""
        SELECT 
            q.close, q.volume, t.sma_10, t.sma_20, t.macd, t.bb_upper, t.bb_lower, q.trading_date
        FROM quote_history q
        JOIN technical_indicators t 
          ON q.symbol = t.symbol AND q.trading_date = t.trading_date
        WHERE q.symbol = :sym
        ORDER BY q.trading_date ASC
    """)
    
    df = pd.read_sql(query, engine, params={"sym": symbol})
    if df.empty or len(df) < 60:
        print(f"❌ Lỗi: Không đủ dữ liệu (ít nhất 60 ngày) cho mã {symbol} trong Database.")
        return

    df = df.dropna()
    dates = df['trading_date'].values
    features = ['close', 'volume', 'sma_10', 'sma_20', 'macd', 'bb_upper', 'bb_lower']
    data = df[features].values

    # Chuẩn hóa (Phải giống y hệt như trên Colab thì AI mới hiểu giá trị thực tế)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(data)

    # Lấy 60 ngày giao dịch gần nhất làm Input cho AI
    last_60_days = scaled_data[-60:]
    
    # Reshape thành mảng 3D chuẩn xác cho đầu vào của LSTM: (1 mẫu dự đoán, 60 time-steps, 7 features)
    X_input = np.array([last_60_days])

    print(f"[*] Đang nạp bộ não Model AI ('{model_path}')... (Mất cỡ vài giây tới vài chục giây)")
    model = tf.keras.models.load_model(model_path)

    # Dự đoán!
    predicted_scaled = model.predict(X_input, verbose=0)

    # Đảo ngược chuẩn hóa (Inverse Scaling) về Giá Trị thực tế (Tiền VND)
    # Vì lúc chuẩn hóa mình dùng 7 cột (với Close nằm ở cột index 0), nên lúc đảo ngược cũng phải làm y hệt
    dummy_matrix = np.zeros((1, len(features)))
    dummy_matrix[0, 0] = predicted_scaled[0, 0]
    predicted_price = scaler.inverse_transform(dummy_matrix)[0, 0]

    last_actual_price = data[-1, 0]
    last_date = dates[-1]

    # In kết quả Dashboard giả lập lên Terminal
    print("\n" + "=".join(["="]*50))
    print("🎯 KẾT QUẢ DỰ ĐOÁN TỪ MÔ HÌNH NHÂN TẠO (LSTM)")
    print("=".join(["="]*50))
    print(f"Mã cổ phiếu     : {symbol}")
    print(f"Ngày giao dịch cuối: {last_date} (Giá đóng cửa thực tế: {last_actual_price:,.0f} đ)")
    print("-" * 50)
    
    diff = predicted_price - last_actual_price
    pct = (diff / last_actual_price) * 100
    trend = "📈 TĂNG CAO" if diff > 0 else "📉 GIẢM SÂU" # AI nghĩ ra vậy
    
    print(f"🔮 Dự báo phiên kết tiếp : {predicted_price:,.0f} đ")
    print(f"   Xu hướng mô hình phán : {trend} ({diff:+,.0f} đ | {pct:+.2f}%)")
    print("=".join(["="]*50))
    print("LƯU Ý: Đây là kết quả dự báo toán học, chỉ mang giá trị tham khảo ứng dụng công nghệ!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy bộ máy dự đoán LSTM tại máy tính")
    parser.add_argument("--symbol", type=str, default="HPG", help="Mã cổ phiếu (vd: HPG)")
    parser.add_argument("--model", type=str, default="hpg_lstm_model.h5", help="Tên file h5 AI")
    args = parser.parse_args()
    
    predict_next_day(args.symbol, args.model)
