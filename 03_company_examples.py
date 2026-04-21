"""
03-COMPANY - Thông tin công ty chi tiết
Tham khảo: 04-company.md
"""

from vnstock_data import Company
import pandas as pd
from postgres_utils import PostgreSQLManager

print("=" * 80)
print("MODULE 3: COMPANY - THÔNG TIN CÔNG TY CHI TIẾT")
print("=" * 80)

# Khởi tạo kết nối PostgreSQL
db = PostgreSQLManager(database='vnstock')

# ============================================================================
# 1. Thông tin tổng quan công ty
# ============================================================================
print("\n1. THÔNG TIN TỔNG QUAN CÔNG TY")
print("-" * 80)

company = Company(symbol='TCB', source='VCI')

df_overview = company.overview()
print(f"Các cột: {df_overview.columns.tolist()}")
print(df_overview.head())

# Lưu vào PostgreSQL
db.save_dataframe(df_overview, 'company_overview_tcb_vci')

# Thông tin chi tiết
for col in df_overview.columns:
    print(f"{col}: {df_overview[col].values[0]}")

# ============================================================================
# 2. Cổ đông lớn (Major Shareholders)
# ============================================================================
print("\n2. CỔ ĐÔNG LỚN")
print("-" * 80)

df_shareholders = company.shareholders()
print(f"Tổng số cổ đông lớn: {len(df_shareholders)}")
print(f"Các cột: {df_shareholders.columns.tolist()}")
print(df_shareholders.head(10))

# Lưu vào PostgreSQL
db.save_dataframe(df_shareholders, 'company_shareholders_tcb_vci')

# Tính tỷ lệ sở hữu
if 'shareholding_percentage' in df_shareholders.columns:
    top_shareholder = df_shareholders.iloc[0]
    print(f"\nCổ đông lớn nhất: {top_shareholder['org_name']} - {top_shareholder['shareholding_percentage']:.2f}%")

# ============================================================================
# 3. Ban lãnh đạo (Officers)
# ============================================================================
print("\n3. BAN LÃNH ĐẠO (OFFICERS)")
print("-" * 80)

df_officers = company.officers()
print(f"Tổng số thành viên: {len(df_officers)}")
print(f"Các cột: {df_officers.columns.tolist()}")
print(df_officers)

# Lưu vào PostgreSQL
db.save_dataframe(df_officers, 'company_officers_tcb_vci')

# ============================================================================
# 4. Công ty con & Công ty liên kết (Subsidiaries)
# ============================================================================
print("\n4. CÔNG TY CON & CÔNG TY LIÊN KẾT")
print("-" * 80)

try:
    df_subsidiaries = company.subsidiaries()
    print(f"Tổng số công ty con/liên kết: {len(df_subsidiaries)}")
    print(f"Các cột: {df_subsidiaries.columns.tolist()}")
    print(df_subsidiaries.head(10))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_subsidiaries, 'company_subsidiaries_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 5. Tin tức công ty
# ============================================================================
print("\n5. TIN TỨC CÔNG TY")
print("-" * 80)

try:
    df_news = company.news(limit=10)
    print(f"Tổng tin tức: {len(df_news)}")
    print(f"Các cột: {df_news.columns.tolist()}")
    print(df_news.head())
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_news, 'company_news_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 6. Sự kiện công ty
# ============================================================================
print("\n6. SỰ KIỆN CÔNG TY (CORPORATE ACTION)")
print("-" * 80)

try:
    df_events = company.events()
    print(f"Tổng sự kiện: {len(df_events)}")
    print(f"Các cột: {df_events.columns.tolist()}")
    print(df_events.head(10))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_events, 'company_events_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 7. Thống kê giao dịch công ty
# ============================================================================
print("\n7. THỐNG KÊ GIAO DỊCH CÔNG TY")
print("-" * 80)

try:
    df_trading_stats = company.trading_stats()
    print(f"Các cột: {df_trading_stats.columns.tolist()}")
    print(df_trading_stats)
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_trading_stats, 'company_trading_stats_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 8. Tóm tắt chỉ số tài chính
# ============================================================================
print("\n8. TÓM TẮT CHỈ SỐ TÀI CHÍNH")
print("-" * 80)

try:
    df_ratio_summary = company.ratio_summary()
    print(f"Các cột: {df_ratio_summary.columns.tolist()}")
    print(df_ratio_summary)
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_ratio_summary, 'company_ratio_summary_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 9. Báo cáo từ các công ty chứng khoán
# ============================================================================
print("\n9. BÁO CÁO TỪ CÁC CÔNG TY CHỨNG KHOÁN")
print("-" * 80)

try:
    df_reports = company.reports()
    print(f"Tổng báo cáo: {len(df_reports)}")
    print(f"Các cột: {df_reports.columns.tolist()}")
    print(df_reports.head(10))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_reports, 'company_reports_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 10. Giao dịch nội bộ (Insider trading)
# ============================================================================
print("\n10. GIAO DỊCH NỘI BỘ (INSIDER TRADING)")
print("-" * 80)

try:
    df_insider = company.insider_trading()
    print(f"Tổng giao dịch nộ bộ: {len(df_insider)}")
    print(f"Các cột: {df_insider.columns.tolist()}")
    print(df_insider.head(10))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_insider, 'company_insider_trading_tcb_vci')
except Exception as e:
    print(f"Lỗi hoặc không có dữ liệu: {e}")

# ============================================================================
# 11. Lấy thông tin công ty cho nhiều cổ phiếu
# ============================================================================
print("\n11. LẤY THÔNG TIN CÔNG TY CHO NHIỀU CỔ PHIẾU")
print("-" * 80)

symbols = ['TCB', 'VNM', 'HPG', 'SHB']
company_info = []

for symbol in symbols:
    try:
        c = Company(symbol=symbol, source='VCI')
        overview = c.overview()
        
        info = {
            'Symbol': symbol,
            'Tên công ty': overview['org_name'].values[0] if 'org_name' in overview.columns else 'N/A',
            'Ngành': overview['sector_name'].values[0] if 'sector_name' in overview.columns else 'N/A',
            'Sàn giao dịch': overview['exchange'].values[0] if 'exchange' in overview.columns else 'N/A',
        }
        company_info.append(info)
        print(f"✓ {symbol}")
    except Exception as e:
        print(f"✗ {symbol}: {e}")

df_company = pd.DataFrame(company_info)
print(f"\nTổng hợp thông tin công ty:")
print(df_company)

# Lưu file
df_company.to_csv('company_overview.csv', index=False, encoding='utf-8-sig')
print("\n✅ Dữ liệu đã lưu vào: company_overview.csv")

# ============================================================================
# 12. Tổng hợp - Portfolio Information
# ============================================================================
print("\n12. TỔNG HỢP - PORTFOLIO INFORMATION")
print("-" * 80)

portfolio = {
    'symbols': ['TCB', 'VNM', 'HPG'],
    'details': {}
}

for symbol in portfolio['symbols']:
    try:
        c = Company(symbol=symbol, source='VCI')
        overview = c.overview()
        shareholders = c.shareholders()
        officers = c.officers()
        
        portfolio['details'][symbol] = {
            'name': overview['org_name'].values[0] if 'org_name' in overview.columns else 'N/A',
            'top_shareholder': shareholders.iloc[0]['org_name'] if len(shareholders) > 0 else 'N/A',
            'ceo': officers[officers['position'] == 'Tổng Giám Đốc']['name'].values[0] if len(officers) > 0 else 'N/A',
        }
    except Exception as e:
        print(f"Lỗi {symbol}: {e}")

print("\nThông tin portfolio:")
for symbol, details in portfolio['details'].items():
    print(f"\n{symbol}:")
    for key, value in details.items():
        print(f"  {key}: {value}")
