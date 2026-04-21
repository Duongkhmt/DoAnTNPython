import argparse
import pandas as pd
from sqlalchemy import text
from timescale_utils import DatabaseManager

def export_all_data(output_file="all_stocks_lstm_data.csv"):
    db = DatabaseManager()
    engine = db.engine
    
    if engine is None:
        print("❌ Không thể kết nối Database. Vui lòng kiểm tra Docker!")
        return
        
    query = text("""
        SELECT 
            q.symbol, 
            q.trading_date, 
            q.open, 
            q.high, 
            q.low, 
            q.close, 
            q.volume,
            t.sma_10,
            t.sma_20,
            t.ema_12,
            t.macd,
            t.macd_signal,
            t.macd_hist,
            t.adx_14,
            t.bb_upper,
            t.bb_mid,
            t.bb_lower,
            t.atr_14,
            t.obv,
            t.vwap,
            t.cmf_20,
            t.candle_body_size
        FROM quote_history q
        JOIN technical_indicators t 
          ON q.symbol = t.symbol 
         AND q.trading_date = t.trading_date
        ORDER BY q.symbol, q.trading_date ASC
    """)
    
    print(f"[*] Đang trích xuất dữ liệu của TOÀN BỘ thị trường (Có thể mất 1-2 phút)....")
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print(f"❌ Không tìm thấy dữ liệu. Đảm bảo bạn đã chạy module cập nhật dữ liệu hàng ngày.")
        return
        
    # Loại bỏ các dòng có giá trị NaN do tính toán đường trung bình
    df = df.dropna()
    
    print(f"[*] Đang lưu ra file CSV...")
    df.to_csv(output_file, index=False)
    
    print(f"✅ Đã trích xuất thành công {len(df):,} dòng dữ liệu!")
    print(f"✅ Đã bao gồm {df['symbol'].nunique()} mã cổ phiếu khác nhau.")
    print(f"👉 Hãy tải file '{output_file}' này lên Google Drive để nạp vào Colab.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trích xuất TOÀN BỘ chứng khoán cho LSTM Model")
    parser.add_argument("--output", type=str, default="all_stocks_lstm_data.csv", help="Tên file lưu lại")
    args = parser.parse_args()
    
    export_all_data(args.output)
