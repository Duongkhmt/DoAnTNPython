"""
📂 CẤU TRÚC THƯ MỤC - POSTGRESQL EXAMPLES
================================================================================

FILE STRUCTURE:
"""

print("""
examples/
├── 00_README_GUIDE.py                    # Hướng dẫn tổng quát (bắt đầu đây)
├── 01_listing_examples.py               # (Cũ) Listing examples
├── 02_quote_examples.py                 # (Cũ) Quote examples
├── 03_company_examples.py               # (Cũ) Company examples
├── 04_finance_examples.py               # (Cũ) Finance examples
├── 05_trading_examples.py               # (Cũ) Trading examples
├── 06_market_insights_macro_commodity_fund.py  # (Cũ) Other modules
│
├── postgres_utils.py                    # 🆕 PostgreSQL utilities (CORE)
├── 07_postgresql_examples.py            # 🆕 PostgreSQL examples (ĐẦY ĐỦ)
├── QUICKSTART_POSTGRESQL.py             # 🆕 Quick start 5 phút (DÙNG NGAY)
├── POSTGRESQL_GUIDE.py                  # 🆕 Hướng dẫn chi tiết (TỰ HỌC)
└── README_POSTGRESQL.py                 # 🆕 File này - Hướng dẫn cấu trúc

================================================================================
🚀 BẮTĐẦU (CHỌN MỘT TRONG BỒI):

1️⃣  CẾP NHẤT (KHUYÊN SỬ DỤNG):
   python QUICKSTART_POSTGRESQL.py
   
   → Chạy 5 phút, lưu dữ liệu vào PostgreSQL
   → Code đơn giản, dễ hiểu

2️⃣  ĐẦY ĐỦ (BẠN MUỐN CHI TIẾT):
   python 07_postgresql_examples.py
   
   → Tất cả tính năng
   → Query examples
   → Phân tích dữ liệu

3️⃣  TỰ HỌC (BẠN MUỐN HIỂU SÃU):
   python POSTGRESQL_GUIDE.py
   
   → Hướng dẫn cài đặt
   → SQL commands
   → FAQ
   → Performance tips

================================================================================
📋 CHI TIẾT CÁC FILE:

🔹 postgres_utils.py (QUAN TRỌNG)
   ├─ Class: PostgreSQLManager
   │  ├─ __init__(): Kết nối database
   │  ├─ test_connection(): Kiểm tra kết nối
   │  ├─ create_all_tables(): Tạo tất cả table
   │  ├─ save_dataframe(): Lưu DataFrame
   │  ├─ query(): Thực hiện query
   │  └─ delete_all_data(): Xóa dữ liệu
   │
   └─ Functions: create_listing_table(), create_quote_history(), ...

🔹 QUICKSTART_POSTGRESQL.py (DÙNG NGAY)
   ├─ Bước 1: Kết nốp PostgreSQL
   ├─ Bước 2: Tạo table
   ├─ Bước 3: Lấy dữ liệu từ API
   ├─ Bước 4: Lưu vào database
   ├─ Bước 5: Kiểm tra dữ liệu
   └─ Bước 6 & 7: Hướng dẫn tiếp theo

🔹 07_postgresql_examples.py (ĐẦY ĐỦ)
   ├─ 1. Khởi tạo kết nối
   ├─ 2. Tạo table
   ├─ 3. Lưu dữ liệu Listing
   ├─ 4. Lưu lịch sử giá
   ├─ 5. Lưu giao dịch
   ├─ 6. Lưu intraday
   ├─ 7. Lưu thông tin công ty
   ├─ 8. Query & phân tích dữ liệu
   ├─ 9. Hướng dẫn tiếp theo
   └─ 10. Backup & Restore

🔹 POSTGRESQL_GUIDE.py (TỰ HỌC)
   ├─ Cài đặt PostgreSQL (Windows/Mac/Linux)
   ├─ Tạo database
   ├─ Câu lệnh cơ bản PostgreSQL
   ├─ Query examples
   ├─ Backup & Restore
   ├─ Performance optimization
   ├─ Lỗi thường gặp
   └─ Công cụ hỗ trợ

================================================================================
❓ CÂU HỎI THƯỜNG GẶP:

Q: Làm sao để bắt đầu?
A: Chạy file QUICKSTART_POSTGRESQL.py (chỉ 5 phút)

Q: PostgreSQL chưa cài đặt?
A: 
   Windows: https://www.postgresql.org/download/windows/
   Mac: brew install postgresql
   Linux: sudo apt-get install postgresql

Q: Chưa biết SQL?
A: Không sao, code đã viết sẵn. Chặp POSTGRESQL_GUIDE.py để học

Q: Muốn backend API?
A: Dùng Flask/FastAPI + postgres_utils.py để tạo API

Q: Muốn dashboard?
A: Dùng Streamlit hoặc Plotly với PostgreSQL

Q: Lỗi "role does not exist"?
A: Kiểm tra username/password trong PostgreSQLManager()

Q: Muốn tự động cập nhật hàng ngày?
A: Dùng APScheduler hoặc cron job (xem POSTGRESQL_GUIDE.py)

================================================================================
💡 GỢI Ý SỬ DỤNG:

Scenario 1: NHÂN VIÊN PHÂN TÍCH (Phân tích stock)
├─ Bước 1: Chạy QUICKSTART_POSTGRESQL.py
├─ Bước 2: Sửa code lấy dữ liệu cho cổ phiếu của bạn
├─ Bước 3: Query dữ liệu để phân tích
└─ Bước 4: Export Excel (df.to_excel())

Scenario 2: DEVELOPER (Xây dựng API/Dashboard)
├─ Bước 1: Hiểu cấu trúc database (07_postgresql_examples.py)
├─ Bước 2: Dùng postgres_utils.py để tích hợp
├─ Bước 3: Xây dựng API với Flask/FastAPI
└─ Bước 4: Deploy lên server

Scenario 3: DATA SCIENTIST (Machine Learning)
├─ Bước 1: Lưu dữ liệu vào PostgreSQL
├─ Bước 2: Load dữ liệu thành DataFrame
├─ Bước 3: Xử lý và training
├─ Bước 4: Lưu kết quả về PostgreSQL
└─ Bước 5: Visualize với Plotly/Matplotlib

Scenario 4: STARTUP/TEAM (Quản lý chung dữ liệu)
├─ Bước 1: Cài PostgreSQL trên server
├─ Bước 2: Cấu hình backup tự động
├─ Bước 3: Share database connection cho team
├─ Bước 4: Tạo API để frontend dùng
└─ Bước 5: Monitor & maintain

================================================================================
🔗 CHUYỂN TIẾP (NEXT STEPS):

Sau khi dữ liệu đã lưu vào PostgreSQL:

1️⃣  Xây dựng API (Flask/FastAPI)
   from flask import Flask, jsonify
   from postgres_utils import PostgreSQLManager
   
   app = Flask(__name__)
   db = PostgreSQLManager()
   
   @app.route('/api/quote/<symbol>')
   def get_quote(symbol):
       df = db.query(f"SELECT * FROM quote_history WHERE symbol='{symbol}' LIMIT 10")
       return jsonify(df.to_dict('records'))

2️⃣  Tạo Dashboard (Streamlit)
   import streamlit as st
   from postgres_utils import PostgreSQLManager
   
   db = PostgreSQLManager()
   symbol = st.selectbox('Chọn cổ phiếu', ['TCB', 'VNM', 'SHB'])
   df = db.query(f"SELECT * FROM quote_history WHERE symbol='{symbol}'")
   st.line_chart(df.set_index('trading_date')['close'])

3️⃣  Tự động hóa (APScheduler)
   from apscheduler.schedulers.background import BackgroundScheduler
   from vnstock_data import Quote
   from postgres_utils import PostgreSQLManager
   
   def update_data():
       db = PostgreSQLManager()
       quote = Quote(symbol='TCB', source='VCI')
       df = quote.history(length='1D')
       db.save_dataframe(df, 'quote_history', if_exists='append')
   
   scheduler = BackgroundScheduler()
   scheduler.add_job(update_data, 'cron', hour=17, minute=0)
   scheduler.start()

4️⃣  Machine Learning
   from postgres_utils import PostgreSQLManager
   from sklearn.ensemble import RandomForestClassifier
   
   db = PostgreSQLManager()
   df = db.query("SELECT * FROM quote_history WHERE symbol='TCB'")
   # ... training
   # ... predict
   # ... save kết quả

================================================================================
📞 SUPPORT:

- Lỗi PostgreSQL: Xem POSTGRESQL_GUIDE.py (phần "Lỗi thường gặp")
- Muốn thêm column: Sửa CREATE TABLE trong postgres_utils.py
- Muốn có báo cáo: Dùng db.query() + pandas để xử lý
- Muốn deploy: Dùng docker-compose để chạy PostgreSQL + app

================================================================================
✅ TỔNG KẾT:

✓ Bạn đã có:
  - Thư viện vnstock_data để lấy dữ liệu
  - PostgreSQL utilities để lưu dữ liệu
  - Examples để reference
  - Hướng dẫn để tự học

✓ Bạn có thể:
  - Lưu dữ liệu stock khổng lộ vào PostgreSQL
  - Query dữ liệu bất kỳ lúc nào
  - Xây dựng ứng dụng trên nền dữ liệu này
  - Chia sẻ dữ liệu với team

✓ Tiếp theo:
  - Chạy QUICKSTART_POSTGRESQL.py (5 phút)
  - Đọc POSTGRESQL_GUIDE.py nếu cần chi tiết
  - Tích hợp vào project của bạn
  - Chia sẻ với team

🚀 OK, LET'S GO!
""")
