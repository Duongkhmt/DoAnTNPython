"""
INSTALLATION & SETUP - HƯỚNG DẪN CÀI ĐẶT HOÀN CHỈNH
================================================================================
"""

print("""
🚀 HƯỚNG DẪN CÀI ĐẶT & CHẠY POSTGRESQL EXAMPLES
================================================================================

## BƯỚC 1: CÀI ĐẶT POSTGRESQL (TỪng OS)

### 🪟 WINDOWS:
  1. Tải từ: https://www.postgresql.org/download/windows/
  2. Chọn installer mới nhất (ví dụ: 16.x)
  3. Install (default options, ghi nhớ password cho user 'postgres')
  4. Chọn "Install Stack Builder" khi hỏi
  5. Hoàn thành, PostgreSQL sẽ auto-run

### 🍎 MACOS:
  brew install postgresql
  brew services start postgresql
  
  # Hoặc cài từ: https://www.postgresql.org/download/macosx/

### 🐧 LINUX (Ubuntu/Debian):
  sudo apt-get update
  sudo apt-get install postgresql postgresql-contrib
  sudo systemctl start postgresql
  sudo systemctl enable postgresql  # Auto-start

### 🐧 LINUX (CentOS/RHEL):
  sudo yum install postgresql-server postgresql-contrib
  sudo systemctl start postgresql
  sudo systemctl enable postgresql

---

## BƯỚC 2: KIỂM TRA CÀI ĐẶT

psql --version

→ Kết quả: psql (PostgreSQL) 16.x ...

---

## BƯỚC 3: TẠO DATABASE

# Terminal/Command Prompt:
psql -U postgres

# SQL commands:
CREATE DATABASE vnstock;
CREATE USER vnstock_user WITH PASSWORD 'vnstock123';
GRANT ALL PRIVILEGES ON DATABASE vnstock TO vnstock_user;
\\q  (để thoát)

---

## BƯỚC 4: CÀI ĐẶT PYTHON PACKAGES

# Cách 1: Cài từ file requirements.txt (KHUYẾN NGHỊ)
pip install -r requirements.txt

# Cách 2: Cài từng package
pip install vnstock_data>=0.2.0
pip install pandas numpy
pip install psycopg2-binary sqlalchemy
pip install schedule APScheduler  # (Optional)

---

## BƯỚC 5: CẤU HÌNH THÔNG TIN KẾT NỐI

Mở file: QUICKSTART_POSTGRESQL.py

Tìm dòng:
db = PostgreSQLManager(
    host='localhost',
    port=5432,
    database='vnstock',
    user='postgres',
    password='postgres'  # PostgreSQL password
)

Thay 'password' thành password bạn đã tạo ở Bước 3

---

## BƯỚC 6: CHẠY QUICK START

cd /path/to/examples
python QUICKSTART_POSTGRESQL.py

→ Chạy thành công nếu không có error!

---

## BƯỚC 7: KIỂM TRA DỮ LIỆU

# Mở PostgreSQL:
psql -U postgres -d vnstock

# Query:
SELECT * FROM listing LIMIT 5;
SELECT COUNT(*) FROM quote_history;
\\q

---

## OPTIONAL: TOOLS GUI

### 🖥️  PgAdmin (Web Interface - Có sẵn sau khi cài PostgreSQL)
  1. Mở trình duyệt: http://localhost:5050
  2. Login với email/password (được hỏi lúc cài)
  3. Add server: localhost:5432
  4. Connect vào database 'vnstock'
  5. Browse tables & data

### 🖥️  DBeaver (Desktop Application)
  1. Tải từ: https://dbeaver.io/download/
  2. Cài đặt
  3. Tạo connection đến PostgreSQL
  4. Browse & query

### 📊 Visual Studio Code
  1. Cài extension: "SQLTools" + "SQLTools PostgreSQL/Cockroach Driver"
  2. Tạo connection
  3. Query trực tiếp trong VS Code

---

## TROUBLESHOOT

### ❌ Lỗi: "could not connect to server"
→ PostgreSQL chưa chạy
   Windows: Mở Services, tìm "postgresql-x64-XX", start
   Mac: brew services start postgresql
   Linux: sudo systemctl start postgresql

### ❌ Lỗi: "password authentication failed"
→ Password sai hoặc user không đúng
   Reset password: ALTER USER postgres PASSWORD 'new_password';

### ❌ Lỗi: "database vnstock does not exist"
→ Cần tạo database trước (Bước 3)
   psql -U postgres
   CREATE DATABASE vnstock;
   \\q

### ❌ Lỗi: "ModuleNotFoundError: No module named 'psycopg2'"
→ Cần cài psycopg2
   pip install psycopg2-binary

### ❌ Lỗi: "vnstock_data not found"
→ Chưa cài vnstock_data
   pip install vnstock_data

### ❌ Lỗi: "port 5432 already in use"
→ Port 5432 đang bị chiếm (có thể PostgreSQL chạy 2 lần)
   Windows: netstat -ano | findstr :5432
   Mac/Linux: lsof -i :5432
   Kill process & restart

---

## CHẠY EXAMPLES KHÁC

### 1. Chạy full examples (có tất cả tính năng):
python 07_postgresql_examples.py

### 2. Đọc hướng dẫn chi tiết:
python POSTGRESQL_GUIDE.py

### 3. Đọc các examples ban đầu (lưu CSV):
python 01_listing_examples.py
python 02_quote_examples.py
python 05_trading_examples.py

---

## INTEGRATION EXAMPLES

### Thêm vào file khác:
from postgres_utils import PostgreSQLManager
from vnstock_data import Quote

db = PostgreSQLManager()
quote = Quote(symbol='TCB', source='VCI')
df = quote.history(start='2026-01-01', end='2026-03-31')
db.save_dataframe(df, 'quote_history', if_exists='append')

### Query dữ liệu trong script:
df = db.query("SELECT * FROM quote_history WHERE symbol='TCB'")
print(df)

### Cập nhật tự động (mỗi ngày lúc 17:00):
from schedule import every, run_pending
import time

def update():
    # code update
    pass

every().day.at("17:00").do(update)
while True:
    run_pending()
    time.sleep(1)

---

## DOCKER (OPTIONAL - Nếu muốn deploy)

### docker-compose.yml:
version: '3.8'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: vnstock
      POSTGRES_USER: vnstock_user
      POSTGRES_PASSWORD: vnstock123
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:

### Chạy:
docker-compose up -d

---

## BACKUP & RESTORE

### Backup database:
pg_dump -U postgres -h localhost vnstock > vnstock_backup.sql

### Restore:
psql -U postgres -h localhost vnstock < vnstock_backup.sql

### Scheduled backup (Linux cron):
0 2 * * * pg_dump -U postgres vnstock > /backup/vnstock_$(date +\\%Y\\%m\\%d).sql

---

## NEXT STEPS

✅ PostgreSQL đã cài & chạy
✅ Dữ liệu đã lưu vào database
✅ Có thể query & phân tích

🔥 Tiếp theo:

1️⃣  Xây dựng API (Flask/FastAPI)
2️⃣  Tạo Dashboard (Streamlit/Plotly)
3️⃣  Machine Learning model
4️⃣  Tự động hóa cập nhật dữ liệu
5️⃣  Sharing data với team

---

## HỖ TRỢ

📖 Đọc file:
  - QUICKSTART_POSTGRESQL.py: Bắt đầu nhanh
  - 07_postgresql_examples.py: Đầy đủ tất cả
  - POSTGRESQL_GUIDE.py: Chi tiết & FAQ
  - postgres_utils.py: Code source

📞 Q&A:
  - ChatGPT: Hỏi về SQL, PostgreSQL
  - Stack Overflow: Hỏi lỗi technical
  - PostgreSQL Docs: https://www.postgresql.org/docs/

✅ OK! LET'S GO!
""")
