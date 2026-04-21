"""
01-LISTING - Danh sách & Phân loại cổ phiếu
Tham khảo: 02-listing.md
Lưu dữ liệu vào PostgreSQL
"""

from vnstock_data import Listing
import pandas as pd
from postgres_utils import PostgreSQLManager

# Khởi tạo PostgreSQL
db = PostgreSQLManager(database='vnstock')

print("=" * 80)
print("MODULE 1: LISTING - DANH SÁCH & PHÂN LOẠI CỔ PHIẾU")
print("=" * 80)

print("=" * 80)
print("MODULE 1: LISTING - DANH SÁCH & PHÂN LOẠI CỔ PHIẾU")
print("=" * 80)

# ============================================================================
# 1. Danh sách toàn bộ chỉ số thị trường
# ============================================================================
print("\n1. DANH SÁCH TOÀN BỘ CHỈ SỐ THỊ TRƯỜNG")
print("-" * 80)

lst = Listing(show_log=False)
df_indices = lst.all_indices()
print(f"Tổng số chỉ số: {len(df_indices)}")
print(df_indices.head(10))
print(f"\nCác cột: {df_indices.columns.tolist()}")
db.save_dataframe(df_indices, 'listing_indices')

# ============================================================================
# 2. Lọc chỉ số theo nhóm
# ============================================================================
print("\n2. LỌC CHỈ SỐ THEO NHÓM")
print("-" * 80)

df_hose = lst.indices_by_group(group='HOSE')
print(f"Chỉ số sàn HOSE: {len(df_hose)}")
print(df_hose)
db.save_dataframe(df_hose, 'listing_indices_hose')

# ============================================================================
# 3. Danh sách toàn bộ cổ phiếu (VCI)
# ============================================================================
print("\n3. DANH SÁCH TOÀN BỘ CỔ PHIẾU")
print("-" * 80)

lst_vci = Listing(source='VCI', show_log=False)
df_symbols = lst_vci.all_symbols()
print(f"Tổng số cổ phiếu: {len(df_symbols)}")
print(df_symbols.head(10))
print(f"\nCác cột: {df_symbols.columns.tolist()}")
db.save_dataframe(df_symbols, 'listing_symbols')

# ============================================================================
# 4. Danh sách ngành ICB
# ============================================================================
print("\n4. DANH SÁCH NGÀNH ICB")
print("-" * 80)

df_industries = lst_vci.industries_icb()
print(f"Tổng số ngành: {len(df_industries)}")
print(df_industries.head(20))
db.save_dataframe(df_industries, 'listing_industries')

# ============================================================================
# 5. Cổ phiếu theo ngành (VD: Ngân hàng)
# ============================================================================
print("\n5. CỔ PHIẾU THEO NGÀNH - NGÂN HÀNG")
print("-" * 80)

df_bank = lst_vci.symbols_by_industries(industry="Ngân hàng")
print(f"Tổng cổ phiếu ngân hàng: {len(df_bank)}")
print(df_bank.head(10))
db.save_dataframe(df_bank, 'listing_bank_stocks')

# ============================================================================
# 6. Cổ phiếu theo sàn giao dịch
# ============================================================================
print("\n6. CỔ PHIẾU THEO SÀN GIAO DỊCH")
print("-" * 80)

df_hose_stocks = lst_vci.symbols_by_exchange(exchange="HOSE")
print(f"Tổng cổ phiếu HOSE: {len(df_hose_stocks)}")
print(df_hose_stocks.head(10))
db.save_dataframe(df_hose_stocks, 'listing_hose_stocks')

# ============================================================================
# 7. Cổ phiếu theo nhóm chỉ số
# ============================================================================
print("\n7. CỔ PHIẾU THEO NHÓM CHỈ SỐ")
print("-" * 80)

# VN30
df_vn30 = lst_vci.symbols_by_group(group="VN30")
print(f"Cổ phiếu VN30: {len(df_vn30)}")
print(df_vn30.head(10))
db.save_dataframe(df_vn30, 'listing_vn30')

# HNX30
df_hnx30 = lst_vci.symbols_by_group(group="HNX30")
print(f"\nCổ phiếu HNX30: {len(df_hnx30)}")
print(df_hnx30)
db.save_dataframe(df_hnx30, 'listing_hnx30')

# ============================================================================
# 8. Danh sách trái phiếu chính phủ
# ============================================================================
print("\n8. DANH SÁCH TRÁI PHIẾU CHÍNH PHỦ")
print("-" * 80)

try:
    df_bonds = lst_vci.all_government_bonds()
    print(f"Tổng số trái phiếu: {len(df_bonds)}")
    print(df_bonds.head(10))
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 9. Danh sách chứng quyền (Covered Warrant)
# ============================================================================
print("\n9. DANH SÁCH CHỨNG QUYỀN")
print("-" * 80)

try:
    df_cw = lst_vci.all_covered_warrant()
    print(f"Tổng số chứng quyền: {len(df_cw)}")
    print(df_cw.head(10))
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 10. Danh sách ETF
# ============================================================================
print("\n10. DANH SÁCH ETF")
print("-" * 80)

try:
    df_etf = lst_vci.all_etf()
    print(f"Tổng số ETF: {len(df_etf)}")
    print(df_etf.head(10))
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 11. Danh sách hợp đồng tương lai
# ============================================================================
print("\n11. DANH SÁCH HỢP ĐỒNG TƯƠNG LAI")
print("-" * 80)

try:
    df_futures = lst_vci.all_future_indices()
    print(f"Tổng số hợp đồng tương lai: {len(df_futures)}")
    print(df_futures.head(10))
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 12. Tổng hợp - Lọc cổ phiếu theo tiêu chí
# ============================================================================
print("\n12. TỔNG HỢP - LỌC CỔ PHIẾU THEO TIÊU CHÍ")
print("-" * 80)

# Lấy tất cả cổ phiếu
all_symbols = lst_vci.all_symbols()

# Lọc cổ phiếu bắt đầu với 'V'
vn_stocks = all_symbols[all_symbols['symbol'].str.startswith('V')]
print(f"Cổ phiếu bắt đầu với 'V': {len(vn_stocks)}")
print(vn_stocks.head())

# Lưu file
output_file = 'listing_data.csv'
all_symbols.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\n✅ Dữ liệu đã lưu vào: {output_file}")
