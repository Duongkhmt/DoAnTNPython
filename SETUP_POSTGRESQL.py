"""
SETUP_POSTGRESQL.py - Hướng dẫn thiết lập PostgreSQL cho vnstock-data
"""

print("""
╔════════════════════════════════════════════════════════════════════════════╗
║          HƯỚNG DẪN SETUP POSTGRESQL CHO VNSTOCK-DATA                      ║
╚════════════════════════════════════════════════════════════════════════════╝

📋 CÁC BƯỚC SETUP:

1️⃣  CÀI ĐẶT POSTGRESQL
   Link: https://www.postgresql.org/download/
   
   Trên Windows:
   - Tải PostgreSQL Installer (version 12+)
   - Chọn các thành phần: PostgreSQL Server, pgAdmin 4
   - Đặt password cho user 'postgres'
   - Port mặc định: 5432

2️⃣  TẠO DATABASE
   Mở pgAdmin 4 hoặc Command Prompt:
   
   $ psql -U postgres
   postgres=# CREATE DATABASE vnstock ENCODING 'UTF8';
   postgres=# \\c vnstock
   vnstock=# \\q

3️⃣  CẤU HÌNH PYTHON
   Mở file: postgres_utils.py (dòng 19-21)
   
   Sửa password nếu cần:
   password='postgress'  # PostgreSQL password
   
   Sửa các thông số khác nếu cần:
   - host='localhost'      # Địa chỉ server PostgreSQL
   - port=5432             # Port
   - database='vnstock'    # Tên database
   - user='postgress'       # Username

4️⃣  KIỂM TRA KẾT NỐI
   $ python -c "
   from postgres_utils import PostgreSQLManager
   db = PostgreSQLManager()
   db.test_connection()
   "
   
   ✅ Kết nối thành công thì sẽ in: PostgreSQL connection test: OK

5️⃣  CHẠY QUICK START
   $ python QUICKSTART_POSTGRESQL.py
   
   Nếu muốn reset database:
   $ python RESET_DATABASE.py

═══════════════════════════════════════════════════════════════════════════════

🐛 TROUBLESHOOTING:

Lỗi: "psycopg2.OperationalError: could not connect to server"
→ Kiểm tra PostgreSQL đang chạy không? Nếu không, restart PostgreSQL service

Lỗi: "psycopg2.ProgrammingError: database 'vnstock' does not exist"
→ Tạo database: psql -U postgres -c "CREATE DATABASE vnstock ENCODING 'UTF8';"

Lỗi: "ERROR character with byte sequence 0xe1 0xbb 0x95 ... WIN1252 has no equivalent"
→ Encoding issue - File postgres_utils.py đã fix, dùng version mới

Lỗi: "UndefinedColumn: column matched_volume does not exist"
→ Table dùng 'volume' không phải 'matched_volume' - File QUICKSTART_POSTGRESQL.py đã fix

═══════════════════════════════════════════════════════════════════════════════

📊 SAU KHI SETUP XON:

1. Lấy danh sách cổ phiếu, chỉ số, ngành:
   $ python 01_listing_examples.py

2. Lấy lịch sử giá và intraday:
   $ python 02_quote_examples.py

3. Lấy thông tin công ty:
   $ python 03_company_examples.py

4. Lấy báo cáo tài chính:
   $ python 04_finance_examples.py

5. Lấy dữ liệu giao dịch:
   $ python 05_trading_examples.py

6. Lấy dữ liệu nâng cao (analytics, insights, macro):
   $ python 06_market_insights_macro_commodity_fund.py

7. Query dữ liệu từ database:
   $ python POSTGRESQL_GUIDE.py

═══════════════════════════════════════════════════════════════════════════════

💡 TIPS:

• Để query dữ liệu trực tiếp:
  from postgres_utils import PostgreSQLManager
  db = PostgreSQLManager()
  df = db.query("SELECT * FROM listing LIMIT 10")
  print(df)

• Để lưu dữ liệu thêm:
  df.to_sql('my_table', db.engine, if_exists='append')

• Backup database:
  pg_dump -U postgres vnstock > vnstock_backup.sql

• Restore database:
  psql -U postgres vnstock < vnstock_backup.sql

═══════════════════════════════════════════════════════════════════════════════
""")
