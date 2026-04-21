"""
04-FINANCE - Báo cáo tài chính chi tiết
Tham khảo: 05-finance.md
"""

from vnstock_data import Finance
import pandas as pd
from postgres_utils import PostgreSQLManager

print("=" * 80)
print("MODULE 4: FINANCE - BÁO CÁO TÀI CHÍNH CHI TIẾT")
print("=" * 80)

# Khởi tạo kết nối PostgreSQL
db = PostgreSQLManager(database='vnstock')

# ============================================================================
# 1. Bảng cân đối kế toán (Balance Sheet)
# ============================================================================
print("\n1. BẢNG CÂN ĐỐI KẾ TOÁN (BALANCE SHEET)")
print("-" * 80)

finance = Finance(symbol='TCB', source='VCI')

df_balance = finance.balance_sheet(lang='vi')
print(f"Số mục: {len(df_balance)}")
print(f"Các cột: {df_balance.columns.tolist()}")
print(df_balance.head(20))

# Lưu vào PostgreSQL
db.save_dataframe(df_balance, 'finance_balance_sheet_tcb_vci')

# Lấy từng phần
if 'title_name' in df_balance.columns:
    assets = df_balance[df_balance['title_name'].str.contains('TỔNG CỘNG TÀI SẢN|TỔNG TÀI SẢN', case=False, na=False)]
    print(f"\nTổng tài sản:")
    print(assets)

# ============================================================================
# 2. Báo cáo kết quả kinh doanh (Income Statement)
# ============================================================================
print("\n2. BÁO CÁO KẾT QUẢ KINH DOANH (INCOME STATEMENT)")
print("-" * 80)

df_income = finance.income_statement()
print(f"Số mục: {len(df_income)}")
print(f"Các cột: {df_income.columns.tolist()}")
print(df_income.head(20))

# Lưu vào PostgreSQL
db.save_dataframe(df_income, 'finance_income_statement_tcb_vci')

# Lấy doanh thu
if 'title_name' in df_income.columns:
    revenue = df_income[df_income['title_name'].str.contains('DOANH THU|REVENUE', case=False, na=False)]
    print(f"\nDoanh thu:")
    print(revenue)

# ============================================================================
# 3. Báo cáo lưu chuyển tiền tệ (Cash Flow)
# ============================================================================
print("\n3. BÁO CÁO LƯU CHUYỂN TIỀN TỆ (CASH FLOW)")
print("-" * 80)

df_cashflow = finance.cash_flow()
print(f"Số mục: {len(df_cashflow)}")
print(f"Các cột: {df_cashflow.columns.tolist()}")
print(df_cashflow.head(20))

# Lưu vào PostgreSQL
db.save_dataframe(df_cashflow, 'finance_cashflow_tcb_vci')

# ============================================================================
# 4. Chỉ số tài chính (Financial Ratios)
# ============================================================================
print("\n4. CHỈ SỐ TÀI CHÍNH (FINANCIAL RATIOS)")
print("-" * 80)

df_ratio = finance.ratio()
print(f"Số chỉ số: {len(df_ratio)}")
print(f"Các cột: {df_ratio.columns.tolist()}")
print(df_ratio)

# Lưu vào PostgreSQL
db.save_dataframe(df_ratio, 'finance_ratios_tcb_vci')

# ============================================================================
# 5. Thuyết minh BCTC (Notes)
# ============================================================================
print("\n5. THUYẾT MINH BCTC (NOTES)")
print("-" * 80)

try:
    df_notes = finance.note()
    print(f"Số ghi chú: {len(df_notes)}")
    print(f"Các cột: {df_notes.columns.tolist()}")
    print(df_notes.head(10))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_notes, 'finance_notes_tcb_vci')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 6. Kế hoạch năm (Annual Plan) - Chỉ MAS có
# ============================================================================
print("\n6. KẾ HOẠCH NĂM (ANNUAL PLAN) - CHỈ MAS")
print("-" * 80)

try:
    finance_mas = Finance(symbol='TCB', source='MAS')
    df_plan = finance_mas.annual_plan(lang='vi')
    print(f"Số mục: {len(df_plan)}")
    print(f"Các cột: {df_plan.columns.tolist()}")
    print(df_plan.head(20))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_plan, 'finance_annual_plan_tcb_mas')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 7. Đổi nguồn dữ liệu (MAS - Chi tiết hơn)
# ============================================================================
print("\n7. ĐỔI NGUỒN DỮ LIỆU - MAS (CHI TIẾT HƠN)")
print("-" * 80)

try:
    finance_mas = Finance(symbol='TCB', source='MAS')
    
    df_balance_mas = finance_mas.balance_sheet(lang='vi')
    print(f"\nBalance Sheet MAS ({len(df_balance_mas)} mục - chi tiết phân cấp):")
    print(df_balance_mas.head(20))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_balance_mas, 'finance_balance_sheet_tcb_mas')
    
    df_income_mas = finance_mas.income_statement(lang='vi')
    print(f"\nIncome Statement MAS ({len(df_income_mas)} mục):")
    print(df_income_mas.head(20))
    
    # Lưu vào PostgreSQL
    db.save_dataframe(df_income_mas, 'finance_income_statement_tcb_mas')
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 8. Lấy BCTC cho nhiều năm
# ============================================================================
print("\n8. LẤY BCTC CHO NHIỀU NĂM")
print("-" * 80)

# VCI
finance_vci = Finance(symbol='VNM', source='VCI')

df_income = finance_vci.income_statement(lang='vi')
print(f"Báo cáo kết quả kinh doanh VNM:")
print(f"Các cột (năm): {[col for col in df_income.columns if col.isdigit() or 'year' in col.lower()]}")
print(df_income.head(10))

# Lưu vào PostgreSQL
db.save_dataframe(df_income, 'finance_income_statement_vnm_vci')

# ============================================================================
# 9. Tính toán các chỉ báo tài chính
# ============================================================================
print("\n9. TÍNH TOÁN CÁC CHỈ BÁO TÀI CHÍNH")
print("-" * 80)

finance = Finance(symbol='TCB', source='VCI')

balance = finance.balance_sheet(lang='vi')
income = finance.income_statement(lang='vi')
cashflow = finance.cash_flow(lang='vi')

# Lưu vào PostgreSQL
db.save_dataframe(balance, 'finance_balance_calc_tcb_vci')
db.save_dataframe(income, 'finance_income_calc_tcb_vci')
db.save_dataframe(cashflow, 'finance_cashflow_calc_tcb_vci')

# Tìm các dòng quan trọng
def find_row(df, keyword):
    """Tìm dòng chứa keyword"""
    if 'title_name' in df.columns:
        rows = df[df['title_name'].str.contains(keyword, case=False, na=False)]
        return rows
    return pd.DataFrame()

# Tổng tài sản
total_assets = find_row(balance, 'TỔNG')
print("Tổng tài sản:")
print(total_assets)

# Doanh thu
revenue = find_row(income, 'DOANH THU|Doanh thu')
print("\nDoanh thu:")
print(revenue)

# Lãi ròng
net_income = find_row(income, 'LỢI NHUẬN RÒNG|Net profit|Lợi nhuận ròng')
print("\nLợi nhuận ròng:")
print(net_income)

# ============================================================================
# 10. So sánh BCTC giữa các công ty
# ============================================================================
print("\n10. SO SÁNH BCTC GIỮA CÁC CÔNG TY")
print("-" * 80)

symbols = ['TCB', 'BID', 'SHB']
comparison_data = []

for symbol in symbols:
    try:
        f = Finance(symbol=symbol, source='VCI')
        ratio = f.ratio(lang='vi')
        
        if len(ratio) > 0:
            # Lấy các chỉ số quan trọng
            roe = ratio[ratio['name'].str.contains('ROE|Return on Equity', case=False, na=False)]
            roa = ratio[ratio['name'].str.contains('ROA|Return on Assets', case=False, na=False)]
            
            comparison_data.append({
                'Symbol': symbol,
                'ROE': roe['value'].values[0] if len(roe) > 0 else 'N/A',
                'ROA': roa['value'].values[0] if len(roa) > 0 else 'N/A',
            })
            print(f"✓ {symbol}")
    except Exception as e:
        print(f"✗ {symbol}: {e}")

df_comparison = pd.DataFrame(comparison_data)
print(f"\nSo sánh chỉ số:")
print(df_comparison)

# Lưu vào PostgreSQL
db.save_dataframe(df_comparison, 'finance_comparison_tcb_bid_shb')

# Lưu file
df_comparison.to_csv('finance_comparison.csv', index=False, encoding='utf-8-sig')
print("\n✅ Dữ liệu đã lưu vào: finance_comparison.csv")

# ============================================================================
# 11. Phân tích - Tính Growth
# ============================================================================
print("\n11. PHÂN TÍCH - TÍNH GROWTH")
print("-" * 80)

finance = Finance(symbol='VNM', source='VCI')
income = finance.income_statement(lang='vi')

# Lưu vào PostgreSQL
db.save_dataframe(income, 'finance_income_growth_vnm_vci')

if 'title_name' in income.columns:
    revenue_rows = find_row(income, 'DOANH THU|Revenue')
    if len(revenue_rows) > 0:
        print("Doanh thu qua các năm:")
        print(revenue_rows)
        
        # Tính growth rate nếu có dữ liệu nhiều năm
        numeric_cols = [col for col in revenue_rows.columns if col not in ['title_name', 'name']]
        if len(numeric_cols) >= 2:
            print(f"\nCác năm: {numeric_cols}")

# ============================================================================
# 12. Export dữ liệu hoàn chỉnh
# ============================================================================
print("\n12. EXPORT DỮ LIỆU HOÀN CHỈNH")
print("-" * 80)

finance = Finance(symbol='TCB', source='VCI')

df_balance = finance.balance_sheet(lang='vi')
df_income = finance.income_statement(lang='vi')
df_cashflow = finance.cash_flow(lang='vi')
df_ratio = finance.ratio(lang='vi')

# Lưu vào PostgreSQL
db.save_dataframe(df_balance, 'finance_balance_export_tcb_vci')
db.save_dataframe(df_income, 'finance_income_export_tcb_vci')
db.save_dataframe(df_cashflow, 'finance_cashflow_export_tcb_vci')
db.save_dataframe(df_ratio, 'finance_ratios_export_tcb_vci')

# Export to Excel files
df_balance.to_csv('finance_balance_sheet.csv', index=False, encoding='utf-8-sig')
df_income.to_csv('finance_income_statement.csv', index=False, encoding='utf-8-sig')
df_cashflow.to_csv('finance_cashflow.csv', index=False, encoding='utf-8-sig')
df_ratio.to_csv('finance_ratio.csv', index=False, encoding='utf-8-sig')

print("✅ Dữ liệu tài chính đã export:")
print("  - finance_balance_sheet.csv")
print("  - finance_income_statement.csv")
print("  - finance_cashflow.csv")
print("  - finance_ratio.csv")
