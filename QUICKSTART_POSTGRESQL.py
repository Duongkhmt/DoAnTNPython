"""
QUICK START - LƯU DỮ LIỆU NGAY VÀO POSTGRESQL (5 PHÚT)
================================================================================
"""

from vnstock_data import Quote, Trading, Listing, Company
from postgres_utils import PostgreSQLManager
import pandas as pd
from sqlalchemy import text

print("=" * 80)
print("QUICK START - 5 PHÚT ĐỂ LƯU DỮ LIỆU VÀO POSTGRESQL")
print("=" * 80)

# ============================================================================
# BƯỚC 1: KẾT NỐI POSTGRESQL (30 giây)
# ============================================================================
print("\n1️⃣  BƯỚC 1: KẾT NỐI POSTGRESQL (30 giây)")
print("-" * 80)

db = PostgreSQLManager(
    host='localhost',
    port=5432,
    database='vnstock',
    user='postgres',
    password='postgres'
)

if not db.test_connection():
    print("❌ Không kết nối được. Hãy kiểm tra:")
    print("  1. PostgreSQL đã chạy chưa?")
    print("  2. Database 'vnstock' đã tạo chưa?")
    print("  3. Username/password đúng chưa?")
    exit()

print("✓ Kết nối PostgreSQL thành công")

# ============================================================================
# BƯỚC 2: TẠO TABLE (30 giây)
# ============================================================================
print("\n2️⃣  BƯỚC 2: TẠO TABLE (30 giây)")
print("-" * 80)

db.create_all_tables()

# ============================================================================
# BƯỚC 3: FETCH DỮ LIỆU (2 phút)
# ============================================================================
print("\n3️⃣  BƯỚC 3: FETCH DỮ LIỆU (2 phút)")
print("-" * 80)

# Listing
print("📥 Đang lấy danh sách cổ phiếu...")
lst = Listing(source='VCI', show_log=False)
df_symbols = lst.all_symbols()
print(f"  ✓ {len(df_symbols)} cổ phiếu")

# Price history cho 3 cổ phiếu mẫu
print("📥 Đang lấy lịch sử giá...")
symbols = ['TCB', 'VNM', 'SHB']
df_histories = []

for sym in symbols:
    quote = Quote(symbol=sym, source='VCI')
    df = quote.history(start='2026-03-01', end='2026-03-30')
    df['symbol'] = sym
    df_histories.append(df)
    print(f"  ✓ {sym}: {len(df)} ngày")

df_all_history = pd.concat(df_histories, ignore_index=True)

# ============================================================================
# BƯỚC 4: SAVE VÀO DATABASE (1 phút)
# ============================================================================
print("\n4️⃣  BƯỚC 4: SAVE VÀO DATABASE (1 phút)")
print("-" * 80)

# Lưu listing - xóa dữ liệu cũ trước để tránh duplicate
print("💾 Lưu listing...")
try:
    # Xóa dữ liệu cũ
    with db.engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE listing CASCADE"))
        conn.commit()
        count = db.query("SELECT COUNT(*) as cnt FROM listing")['cnt'].values[0]
        print("🔥 After truncate:", count)
except Exception as e:
    # pass  # Table chưa tồn tại, bỏ qua
    print("❌ TRUNCATE ERROR:", e)
    raise

# df_symbols_clean = df_symbols[['symbol']].copy()
df_symbols_clean = df_symbols[['symbol']].drop_duplicates()
if 'organ_name' in df_symbols.columns:
    df_symbols_clean['organ_name'] = df_symbols['organ_name']
db.save_dataframe(df_symbols_clean, 'listing', if_exists='append')
print("  ✓ Listing saved")

# Lưu quote history
print("💾 Lưu lịch sử giá...")
# Sửa tên cột: 'time' -> 'trading_date' (giữ 'volume' không đổi tên)
df_history_clean = df_all_history[['symbol', 'time', 'open', 'high', 'low', 'close', 'volume']].copy()
df_history_clean = df_history_clean.rename(columns={'time': 'trading_date'})
db.save_dataframe(df_history_clean, 'quote_history', if_exists='append')
print("  ✓ Quote history saved")

# ============================================================================
# BƯỚC 5: KIỂM TRA DỮ LIỆU (30 giây)
# ============================================================================
print("\n5️⃣  BƯỚC 5: KIỂM TRA DỮ LIỆU (30 giây)")
print("-" * 80)

# Đếm
count_listing = db.query("SELECT COUNT(*) as cnt FROM listing")['cnt'].values[0]
count_history = db.query("SELECT COUNT(*) as cnt FROM quote_history")['cnt'].values[0]

print(f"✓ Listing: {count_listing} mục")
print(f"✓ History: {count_history} mục")

# Xem mẫu
print("\n📊 DỮ LIỆU MẪU:")
df_sample = db.query("SELECT * FROM quote_history LIMIT 5")
print(df_sample)

# ============================================================================
# BƯỚC 6: QUERY DỮ LIỆU (30 giây)
# ============================================================================
print("\n6️⃣  BƯỚC 6: QUERY DỮ LIỆU (30 giây)")
print("-" * 80)

print("\n📈 GIÁ ĐÓNG CỬA MỚI NHẤT:")
df_latest = db.query("""
    SELECT DISTINCT ON (symbol) symbol, trading_date, close 
    FROM quote_history 
    ORDER BY symbol, trading_date DESC
""")
print(df_latest)

print("\n📊 KHỐI LƯỢNG TRUNG BÌNH:")
df_avg = db.query("""
    SELECT symbol, 
           ROUND(AVG(volume)::numeric, 0) as avg_vol,
           MAX(volume) as max_vol
    FROM quote_history
    GROUP BY symbol
""")
print(df_avg)

# ============================================================================
# BƯỚC 7: HƯỚNG DẪN TIẾP THEO
# ============================================================================
print("\n7️⃣  BƯỚC 7: HƯỚNG DẪN TIẾP THEO")
print("-" * 80)

print("""
✅ BẠN ĐÃ HOÀN THÀNH! Dữ liệu đã lưu vào PostgreSQL.

🔥 TIẾP THEO, BẠN CÓ THỂ:

1. Lưu thêm dữ liệu giao dịch:
   -----------
   trading = Trading(symbol='TCB', source='VCI')
   df_trade = trading.price_history(start='2026-03-01', end='2026-03-30')
   db.save_dataframe(df_trade, 'trading', if_exists='append')

2. Lưu intraday chi tiết:
   -----------
   quote = Quote(symbol='TCB', source='VCI')
   df_intraday = quote.intraday(page_size=5000)
   db.save_dataframe(df_intraday, 'intraday', if_exists='append')

3. Query dữ liệu phức tạp:
   -----------
   result = db.query('''
       SELECT symbol, trading_date, close,
              LAG(close) OVER (PARTITION BY symbol ORDER BY trading_date) as prev_close,
              (close - LAG(close) OVER (PARTITION BY symbol ORDER BY trading_date)) as change
       FROM quote_history
       WHERE trading_date >= '2026-03-20'
       ORDER BY symbol, trading_date DESC
   ''')

4. Cập nhật dữ liệu tự động mỗi ngày:
   -----------
   from schedule import every
   import time
   
   def update_daily():
       for sym in ['TCB', 'VNM', 'SHB']:
           quote = Quote(symbol=sym, source='VCI')
           df = quote.history(length='1D')
           db.save_dataframe(df, 'quote_history', if_exists='append')
   
   every().day.at("17:00").do(update_daily)
   while True:
       run_pending()
       time.sleep(60)

5. Export dữ liệu ra Excel để phân tích:
   -----------
   df = db.query("SELECT * FROM quote_history WHERE symbol='TCB'")
   df.to_excel('TCB_data.xlsx', index=False)

📖 CHI TIẾT: Xem file POSTGRESQL_GUIDE.py hoặc 07_postgresql_examples.py
""")

print("\n" + "=" * 80)
print("✅ HOÀN THÀNH - DỮ LIỆU ĐÃ LƯU TRONG POSTGRESQL")
print("=" * 80)
