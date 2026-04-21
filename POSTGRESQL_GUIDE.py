"""
HƯỚNG DẪN LƯU DỮ LIỆU VÀO POSTGRESQL
================================================================================
"""

print("=" * 80)
print("HƯỚNG DẪN SỬ DỤNG POSTGRESQL VỚI VNSTOCK_DATA")
print("=" * 80)

print("""
## 1. CÀI ĐẶT CẤN THIẾT

### Windows:
1. Tải PostgreSQL từ: https://www.postgresql.org/download/windows/
2. Cài đặt (ghi nhớ password user 'postgres')
3. Sau cài đặt, mở pgAdmin 4 để quản lý

### macOS:
brew install postgresql
brew services start postgresql

### Linux (Ubuntu/Debian):
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql

## 2. CÀI ĐẶT PYTHON PACKAGES

pip install psycopg2-binary sqlalchemy pandas

## 3. TẠO DATABASE TRONG POSTGRESQL

# Mở terminal/cmd (Windows: runas /user:Administrator cmd)

# Linux/Mac:
psql -U postgres

# Nhập password khi yêu cầu

# SQL Commands:
CREATE DATABASE vnstock;
CREATE USER vnstock_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE vnstock TO vnstock_user;
\\\\q  # Thoát

## 4. CẤU HÌNH KẾT NỐI

# Sửa thông tin trong code:
from postgres_utils import PostgreSQLManager

db = PostgreSQLManager(
    host='localhost',           # hoặc địa chỉ IP server
    port=5432,                 # port mặc định PostgreSQL
    database='vnstock',        # tên database
    user='postgres',           # hoặc 'vnstock_user'
    password='postgres'        # password bạn đặt
)

## 5. CÂU LỆNH CỌC BẦN POSTGRESQL

### Danh sách cơ sở dữ liệu
\\\\l

### Chuyển sang database vnstock
\\\\c vnstock

### Danh sách các bảng
\\\\dt

### Xem cấu trúc bảng
\\\\d quote_history

### Query dữ liệu
SELECT * FROM quote_history LIMIT 5;

### Đếm số dòng
SELECT COUNT(*) FROM quote_history;

### Xóa dữ liệu
DELETE FROM quote_history;

### Xóa bảng
DROP TABLE quote_history;

### Xóa tất cả dữ liệu và reset ID
TRUNCATE TABLE quote_history RESTART IDENTITY;

### Sở hữu dữ liệu
ALTER TABLE listing OWNER TO vnstock_user;

## 6. CHẠY CÁC EXAMPLES

# Ví dụ cơ bản (lưu vào PostgreSQL):
python 07_postgresql_examples.py

# Ví dụ nâng cao (lưu từ các file khác):
from postgres_utils import PostgreSQLManager
from vnstock_data import Listing

db = PostgreSQLManager()
lst = Listing(source='VCI')
df = lst.all_symbols()
db.save_dataframe(df, 'listing', if_exists='replace')

## 7. QUERY EXAMPLES

# Tất cả cổ phiếu niêm yết
SELECT * FROM listing ORDER BY symbol;

# Giá đóng cửa mới nhất
SELECT symbol, MAX(trading_date) as latest_date, close 
FROM quote_history 
WHERE trading_date = (SELECT MAX(trading_date) FROM quote_history)
GROUP BY symbol;

# Lịch sử giá của 1 cổ phiếu
SELECT * FROM quote_history WHERE symbol='TCB' ORDER BY trading_date DESC;

# Khối lượng giao dịch theo ngày
SELECT trading_date, SUM(matched_volume) as total_volume
FROM trading
GROUP BY trading_date
ORDER BY trading_date DESC;

# Phân tích lệnh khớp
SELECT symbol, match_type, COUNT(*) as count, SUM(volume) as total
FROM intraday
GROUP BY symbol, match_type
ORDER BY symbol;

# Giao dịch của 1 cổ phiếu trong range ngày
SELECT * FROM quote_history 
WHERE symbol='VNM' AND trading_date BETWEEN '2026-03-01' AND '2026-03-31'
ORDER BY trading_date;

## 8. BACKUP & RESTORE

# Backup toàn bộ database
pg_dump -U postgres vnstock > vnstock_backup.sql

# Backup một table
pg_dump -U postgres -t quote_history vnstock > quote_history_backup.sql

# Restore database
psql -U postgres vnstock < vnstock_backup.sql

# Export dữ liệu ra CSV (SQL):
COPY (SELECT * FROM quote_history) TO '/tmp/quote_history.csv' CSV HEADER;

# Import từ CSV:
COPY quote_history(symbol,trading_date,open,high,low,close,volume) 
FROM '/tmp/quote_history.csv' CSV HEADER;

## 9. PERFORMANCE OPTIMIZATION

# Tạo index: tăng tốc độ query
CREATE INDEX idx_symbol ON quote_history(symbol);
CREATE INDEX idx_date ON quote_history(trading_date);
CREATE INDEX idx_symbol_date ON quote_history(symbol, trading_date);

# Xem các index
SELECT * FROM pg_indexes WHERE tablename = 'quote_history';

# Xóa index nếu cần
DROP INDEX idx_symbol;

# Vacuum: dọn dẹp không gian
VACUUM ANALYZE quote_history;

## 10. MỘT SỐ LỖI THƯỜNG GẶP

### Lỗi: role "postgres" does not exist
→ Cài đặt lại PostgreSQL hoặc kiểm tra tên user

### Lỗi: password authentication failed
→ Kiểm tra lại password hoặc reset: ALTER USER postgres PASSWORD 'new_password';

### Lỗi: could not connect to server
→ PostgreSQL chưa chạy: systemctl start postgresql (Linux) hoặc khởi động từ pgAdmin (Windows)

### Lỗi: UNIQUE constraint failed
→ Dữ liệu đã tồn tại: dùng if_exists='append' hoặc 'replace'

## 11. TƯƠNG LAI: THÊM TÍNH NĂNG

# Lên lịch tự động cập nhật dữ liệu (mỗi ngày):
from schedule import every, run_pending
import time

every().day.at("17:00").do(update_stock_data)

while True:
    run_pending()
    time.sleep(1)

# Hoặc dùng APScheduler:
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
scheduler.add_job(update_stock_data, 'cron', hour=17, minute=0)
scheduler.start()

## 12. CÔNG CỤ HỖ TRỢ

# DBeaver: GUI để quản lý PostgreSQL
# PgAdmin 4: Web interface sẵn có khi cài PostgreSQL
# SQLAlchemy Alchemy: ORM, giúp viết query dễ hơn

""")

print("\n" + "=" * 80)
print("✅ HOÀN THÀNH HỌC")
print("=" * 80)
