# Hướng dẫn thiết lập PostgreSQL cho vnstock-data
# ===================================================

# 1. CÀI ĐẶT POSTGRESQL
# ----------------------
# Windows: Tải từ https://www.postgresql.org/download/windows/
# macOS: brew install postgresql
# Ubuntu: sudo apt-get install postgresql postgresql-contrib

# 2. KHỞI ĐỘNG POSTGRESQL SERVICE
# -------------------------------
# Windows: Services -> PostgreSQL -> Start
# macOS: brew services start postgresql
# Ubuntu: sudo systemctl start postgresql

# 3. TẠO USER VÀ DATABASE
# -----------------------
# Mở pgAdmin hoặc command line:
# createdb vnstock_data
# createuser --createdb --encrypted --pwprompt vnstock_user
# (Nhập password khi được hỏi)

# 4. CẤU HÌNH KẾT NỐI
# -------------------
# Trong file 06_market_insights_macro_commodity_fund.py,
# sửa đổi phần DB_CONFIG:

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'vnstock_data',
    'user': 'postgres',  # hoặc tên user bạn tạo
    'password': 'your_password_here'  # thay bằng password thật
}

# 5. CHẠY SCRIPT
# --------------
# python 06_market_insights_macro_commodity_fund.py

# 6. XEM DỮ LIỆU
# -------------
# Sử dụng pgAdmin hoặc command line:
# psql -d vnstock_data -U postgres
# \dt  # xem danh sách bảng
# SELECT * FROM market_pe_ratio LIMIT 5;  # xem dữ liệu mẫu