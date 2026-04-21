"""
RESET_DATABASE.py - Xóa toàn bộ dữ liệu cũ để reset database
Dùng file này nếu muốn chạy QUICKSTART_POSTGRESQL.py lại từ đầu
"""

from postgres_utils import PostgreSQLManager
import sys

print("=" * 80)
print("RESET DATABASE - XÓA TOÀN BỘ DỮ LIỆU CŨ")
print("=" * 80)

# Kết nối
db = PostgreSQLManager(
    host='localhost',
    port=5432,
    database='vnstock',
    user='postgress',
    password='postgress'
)

if not db.test_connection():
    print("❌ Không kết nối được PostgreSQL")
    sys.exit(1)

# Xác nhận
print("\n⚠️  CẢNH BÁO: Thao tác này sẽ XÓA TOÀN BỘ DỮ LIỆU trong database 'vnstock'")
confirm = input("\nBạn chắc chắn muốn tiếp tục? (yes/no): ").strip().lower()

if confirm != 'yes':
    print("❌ Đã hủy")
    sys.exit(0)

# Reset tables
print("\n🔄 Đang reset database...")

try:
    # Drop all tables với CASCADE để tránh foreign key constraint
    with db.engine.connect() as conn:
        from sqlalchemy import inspect, text
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        
        for table in tables:
            print(f"  Xóa table: {table}")
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        
        conn.commit()
    
    print("\n✅ Đã reset database thành công!")
    print("\nBây giờ bạn có thể chạy:")
    print("  python QUICKSTART_POSTGRESQL.py")
    
except Exception as e:
    print(f"\n❌ Lỗi khi reset: {e}")
    sys.exit(1)
