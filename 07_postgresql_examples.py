"""
POSTGRESQL EXAMPLES - Lưu dữ liệu vnstock vào PostgreSQL
Thêm dòng này trước khi chạy:
    pip install psycopg2-binary sqlalchemy
"""

from vnstock_data import Listing, Quote, Company, Finance, Trading
from postgres_utils import PostgreSQLManager, create_listing_table, create_quote_history, create_trading_data, create_intraday_data
import pandas as pd

print("=" * 80)
print("POSTGRESQL - LƯU DỮ LIỆU VNSTOCK VÀO DATABASE")
print("=" * 80)

# ============================================================================
# 1. KHỞI TẠO KẾT NỐI POSTGRESQL
# ============================================================================
print("\n1. KHỞI TẠO KẾT NỐI POSTGRESQL")
print("-" * 80)

# ⚠️ THAY ĐỔI THÔNG TIN KẾT NỐI PHÙ HỢP
db = PostgreSQLManager(
    host='localhost',        # Thay thành IP server nếu truy cập từ xa
    port=5432,              # Port mặc định PostgreSQL
    database='vnstock',     # Database name
    user='postgres',        # Username
    password='postgres'     # Password
)

# Kiểm tra kết nối
if not db.test_connection():
    print("⚠️  Không thể kết nối, vui lòng kiểm tra thông tin kết nối")
    exit()

# ============================================================================
# 2. TẠO CÁC TABLE
# ============================================================================
print("\n2. TẠO CÁC TABLE")
print("-" * 80)

db.create_all_tables()

# Xem các table đã tạo
tables = db.get_tables()
print(f"\n📋 Các table hiện có: {tables}")

# ============================================================================
# 3. LƯU DỮ LIỆU LISTING
# ============================================================================
print("\n3. LƯU DỮ LIỆU LISTING (DANH SÁCH CỔ PHIẾU)")
print("-" * 80)

lst = Listing(source='VCI', show_log=False)
df_all_symbols = lst.all_symbols()
print(f"Đang lưu {len(df_all_symbols)} cổ phiếu...")

create_listing_table(db, df_all_symbols)

# Kiểm tra dữ liệu
result = db.query("SELECT COUNT(*) as total FROM listing")
print(f"✓ Tổng cổ phiếu trong database: {result['total'].values[0]}")

# ============================================================================
# 4. LƯU DỮ LIỆU LỊCH SỬ GIÁ
# ============================================================================
print("\n4. LƯU DỮ LIỆU LỊCH SỬ GIÁ")
print("-" * 80)

symbols = ['TCB', 'VNM', 'HPG', 'SHB', 'BID']
start_date = '2026-03-01'
end_date = '2026-03-30'

for symbol in symbols:
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df_history = quote.history(start=start_date, end=end_date)
        
        if len(df_history) > 0:
            create_quote_history(db, symbol, df_history)
            print(f"  ✓ {symbol}: {len(df_history)} ngày giao dịch")
    except Exception as e:
        print(f"  ✗ {symbol}: Lỗi - {e}")

# Kiểm tra
result = db.query("SELECT COUNT(*) as total FROM quote_history")
print(f"\n✓ Tổng bản ghi lịch sử giá: {result['total'].values[0]}")

# Query mẫu
print("\nDữ liệu mẫu:")
df_sample = db.query("SELECT * FROM quote_history LIMIT 5")
print(df_sample)

# ============================================================================
# 5. LƯU DỮ LIỆU GIAO DỊCH
# ============================================================================
print("\n5. LƯU DỮ LIỆU GIAO DỊCH")
print("-" * 80)

for symbol in symbols:
    try:
        trading = Trading(symbol=symbol, source='VCI')
        df_trading = trading.price_history(start=start_date, end=end_date)
        
        if len(df_trading) > 0:
            create_trading_data(db, symbol, df_trading)
            print(f"  ✓ {symbol}: {len(df_trading)} ngày")
    except Exception as e:
        print(f"  ✗ {symbol}: Lỗi - {e}")

# Kiểm tra
result = db.query("SELECT COUNT(*) as total FROM trading")
print(f"\n✓ Tổng bản ghi giao dịch: {result['total'].values[0]}")

# ============================================================================
# 6. LƯU DỮ LIỆU INTRADAY
# ============================================================================
print("\n6. LƯU DỮ LIỆU INTRADAY")
print("-" * 80)

symbols_intraday = ['TCB', 'VNM', 'SHB']

for symbol in symbols_intraday:
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df_intraday = quote.intraday(page_size=5000)
        
        if len(df_intraday) > 0:
            create_intraday_data(db, symbol, df_intraday)
            print(f"  ✓ {symbol}: {len(df_intraday)} lệnh khớp")
    except Exception as e:
        print(f"  ✗ {symbol}: Lỗi - {e}")

# Kiểm tra
result = db.query("SELECT COUNT(*) as total FROM intraday")
print(f"\n✓ Tổng lệnh khớp intraday: {result['total'].values[0]}")

# ============================================================================
# 7. LƯU DỮ LIỆU CÔNG TY
# ============================================================================
print("\n7. LƯU DỮ LIỆU THÔNG TIN CÔNG TY")
print("-" * 80)

company_data = []

for symbol in symbols:
    try:
        company = Company(symbol=symbol, source='VCI')
        overview = company.overview()
        
        if len(overview) > 0:
            company_data.append({
                'symbol': symbol,
                'name': overview['org_name'].values[0] if 'org_name' in overview.columns else '',
                'sector': overview.get('sector_name', pd.Series(['']))[0] if 'sector_name' in overview.columns else '',
                'industry': overview.get('industry_name', pd.Series(['']))[0] if 'industry_name' in overview.columns else '',
                'exchange': overview.get('exchange', pd.Series(['']))[0] if 'exchange' in overview.columns else '',
            })
        print(f"  ✓ {symbol}")
    except Exception as e:
        print(f"  ✗ {symbol}: {e}")

if company_data:
    df_company = pd.DataFrame(company_data)
    db.save_dataframe(df_company, 'company', if_exists='replace')
    print(f"\n✓ Lưu {len(df_company)} công ty")

# ============================================================================
# 8. QUERY & PHÂN TÍCH DỮ LIỆU
# ============================================================================
print("\n8. QUERY & PHÂN TÍCH DỮ LIỆU")
print("-" * 80)

# Giá đóng cửa mới nhất
print("\n📈 GIÁ ĐÓNG CỬA MỚI NHẤT:")
df_latest = db.query("""
    SELECT symbol, trading_date, close 
    FROM quote_history 
    WHERE trading_date = (SELECT MAX(trading_date) FROM quote_history)
    ORDER BY symbol
""")
print(df_latest)

# Khối lượng trung bình
print("\n📊 KHỐI LƯỢNG TRUNG BÌNH:")
df_avg_vol = db.query("""
    SELECT symbol, 
           COUNT(*) as days,
           AVG(matched_volume) as avg_volume,
           MAX(matched_volume) as max_volume
    FROM trading
    GROUP BY symbol
    ORDER BY avg_volume DESC
""")
print(df_avg_vol)

# Top lệnh khớp
print("\n⭐ TOP LỆNH KHỚP (INTRADAY):")
df_top_trade = db.query("""
    SELECT symbol, 
           COUNT(*) as total_orders,
           SUM(volume) as total_volume,
           AVG(price) as avg_price
    FROM intraday
    GROUP BY symbol
    ORDER BY total_volume DESC
""")
print(df_top_trade)

# Mua vs Bán
print("\n🔄 LỆNH MUA VS BÁN:")
df_buy_sell = db.query("""
    SELECT symbol,
           match_type,
           COUNT(*) as order_count,
           SUM(volume) as total_volume
    FROM intraday
    GROUP BY symbol, match_type
    ORDER BY symbol, match_type
""")
print(df_buy_sell)

# ============================================================================
# 9. CÁCH SỬ DỤNG KHI CẦN THÊM DỮ LIỆU
# ============================================================================
print("\n9. CÁCH SỬ DỤNG KHI CẦN THÊM DỮ LIỆU")
print("-" * 80)

print("""
# Thêm dữ liệu mới cho một cổ phiếu
symbol = 'VJC'
quote = Quote(symbol=symbol, source='VCI')
df = quote.history(start='2026-04-01', end='2026-04-02')
create_quote_history(db, symbol, df)

# Query dữ liệu
result = db.query("SELECT * FROM quote_history WHERE symbol='VJC' ORDER BY trading_date")

# Xóa dữ liệu cũ nếu cần
db.delete_all_data('quote_history')

# Kiểm tra table
print(db.get_tables())

# Đóng kết nối (tự động khi kết thúc)
db.engine.dispose()
""")

# ============================================================================
# 10. BACKUP & RESTORE
# ============================================================================
print("\n10. BACKUP & RESTORE POSTGRESQL")
print("-" * 80)

print("""
# BACKUP (chạy trong cmd/terminal, không phải Python):
pg_dump -U postgres -h localhost vnstock > vnstock_backup.sql

# RESTORE:
psql -U postgres -h localhost vnstock < vnstock_backup.sql

# Export table thành CSV:
COPY quote_history TO '/tmp/quote_history.csv' CSV HEADER;

# Import CSV:
COPY quote_history FROM '/tmp/quote_history.csv' CSV HEADER;
""")

print("\n" + "=" * 80)
print("✅ HOÀN THÀNH - DỮ LIỆU ĐÃ ĐƯỢC LƯU VÀO POSTGRESQL")
print("=" * 80)
