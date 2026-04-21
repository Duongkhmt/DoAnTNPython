import argparse
import pandas as pd
from sqlalchemy import text
from timescale_utils import DatabaseManager

def export_data_for_lstm(symbol="HPG", output_file="lstm_data.csv"):
    db = DatabaseManager()
    engine = db.engine
    
    if engine is None:
        print("❌ Không thể kết nối Database. Vui lòng kiểm tra Docker!")
        return
        
    # Join quote_history and technical_indicators
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
        WHERE q.symbol = :sym
        ORDER BY q.trading_date ASC
    """)
    
    print(f"[*] Đang trích xuất dữ liệu cho {symbol}...")
    df = pd.read_sql(query, engine, params={"sym": symbol})
    
    if df.empty:
        print(f"❌ Không tìm thấy dữ liệu cho mã {symbol}. Đảm bảo bạn đã chạy compute_indicators.py cho mã này.")
        return
        
    # Xử lý các giá trị NaN có thể xảy ra trong những ngày đầu chưa đủ mốc tính SMA
    df = df.dropna()
    
    df.to_csv(output_file, index=False)
    print(f"✅ Đã trích xuất {len(df)} ngày giao dịch hợp lệ sang file {output_file}")
    print("👉 Bạn có thể tải file này lên Google Drive để nạp vào Colab.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trích xuất dữ liệu chuẩn bị cho LSTM trên Colab")
    parser.add_argument("--symbol", type=str, default="HPG", help="Mã cổ phiếu cần xuất (vd: HPG, VNM, TCB)")
    parser.add_argument("--output", type=str, default="lstm_data.csv", help="Tên file CSV muốn lưu")
    
    args = parser.parse_args()
    export_data_for_lstm(args.symbol, args.output)
