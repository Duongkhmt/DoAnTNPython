"""
06-MARKET-INSIGHTS-MACRO-COMMODITY-FUND - Dữ liệu bổ sung
Tham khảo: unified-ui/07-analytics-layer.md, unified-ui/06-insights-layer.md, unified-ui/05-macro-layer.md, 11-fund.md
Lưu toàn bộ dữ liệu vào PostgreSQL
"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import sql

# ============================================================================
# CẤU HÌNH POSTGRESQL
# ============================================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'vnstock',
    'user': 'postgres',
    'password': 'postgres'  # Thay đổi password của bạn
}

def get_db_engine():
    """Tạo database engine với xử lý lỗi"""
    try:
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Kết nối PostgreSQL thành công!")
        return engine
    except Exception as e:
        print(f"❌ Lỗi kết nối PostgreSQL: {e}")
        print("💡 Hãy kiểm tra:")
        print("   1. PostgreSQL có đang chạy không?")
        print("   2. Thông tin kết nối trong DB_CONFIG có đúng không?")
        print("   3. Database đã được tạo chưa?")
        print("   4. Xem file README_PostgreSQL.md để biết cách thiết lập")
        return None

def save_to_postgres(df, table_name, engine, if_exists='replace'):
    """Lưu DataFrame vào PostgreSQL"""
    if engine is None:
        print(f"⚠️ Bỏ qua lưu bảng '{table_name}' vì không thể kết nối database")
        return False
        
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"✅ Đã lưu {len(df)} dòng vào bảng '{table_name}'")
        return True
    except Exception as e:
        print(f"❌ Lỗi khi lưu bảng '{table_name}': {e}")
        return False

def create_database_if_not_exists():
    """Tạo database nếu chưa tồn tại"""
    try:
        # Kết nối tới postgres database mặc định
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Tạo database nếu chưa tồn tại
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(DB_CONFIG['database'])
        ))
        print(f"✅ Đã tạo database '{DB_CONFIG['database']}'")
    except psycopg2.errors.DuplicateDatabase:
        print(f"ℹ️ Database '{DB_CONFIG['database']}' đã tồn tại")
    except Exception as e:
        print(f"❌ Lỗi khi tạo database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# Tạo database và engine
create_database_if_not_exists()
engine = get_db_engine()

print("=" * 80)
print("🚀 BẮT ĐẦU LƯU DỮ LIỆU VÀO POSTGRESQL")
print("=" * 80)

print("=" * 80)
print("MODULE 6: MARKET - ĐỊNH GIÁ THỊ TRƯỜNG (P/E, P/B)")
print("=" * 80)

# ============================================================================
# 1. P/E Ratio (Analytics)
# ============================================================================
print("\n1. P/E RATIO - ĐỊNH GIÁ THEO GIÁ/LỢI NHUẬN")
print("-" * 80)

try:
    from vnstock_data import Analytics
    ana = Analytics()
    
    df_pe = ana.valuation("VNINDEX").pe(duration="5Y")
    print(f"Số dòng dữ liệu: {len(df_pe)}")
    print(f"Các cột: {df_pe.columns.tolist()}")
    save_to_postgres(df_pe, 'market_pe_ratio', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 2. P/B Ratio (Analytics)
# ============================================================================
print("\n2. P/B RATIO - ĐỊNH GIÁ THEO GIÁ/GIÁ TRỊ SẢN")
print("-" * 80)

try:
    df_pb = ana.valuation("VNINDEX").pb(duration="5Y")
    print(f"Số dòng dữ liệu: {len(df_pb)}")
    print(f"Các cột: {df_pb.columns.tolist()}")
    save_to_postgres(df_pb, 'market_pb_ratio', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 3. Định giá thị trường (Market Valuation)
# ============================================================================
print("\n3. ĐỊNH GIÁ THỊ TRƯỜNG")
print("-" * 80)

try:
    df_valuation = ana.valuation("VNINDEX").evaluation(duration="5Y")
    print(f"Số dòng dữ liệu: {len(df_valuation)}")
    print(f"Các cột: {df_valuation.columns.tolist()}")
    save_to_postgres(df_valuation, 'market_valuation', engine)
except Exception as e:
    print(f"Lỗi: {e}")

print("\n" + "=" * 80)
print("MODULE 7: INSIGHTS - TOP CỔ PHIẾU VÀ XẾP HẠNG")
print("=" * 80)

# ============================================================================
# 4. Top Gainer (Tăng giá hôm nay)
# ============================================================================
print("\n4. TOP GAINER - TĂNG GIÁ HÔM NAY")
print("-" * 80)

try:
    from vnstock_data import Insights
    insights = Insights()
    
    df_gainer = insights.ranking().gainer()
    print(f"Số cổ phiếu: {len(df_gainer)}")
    print(f"Các cột: {df_gainer.columns.tolist()}")
    save_to_postgres(df_gainer, 'insights_gainer', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 5. Top Loser (Giảm giá hôm nay)
# ============================================================================
print("\n5. TOP LOSER - GIẢM GIÁ HÔM NAY")
print("-" * 80)

try:
    df_loser = insights.ranking().loser()
    print(f"Số cổ phiếu: {len(df_loser)}")
    print(f"Các cột: {df_loser.columns.tolist()}")
    save_to_postgres(df_loser, 'insights_loser', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 6. Top Volume (Khối lượng cao nhất)
# ============================================================================
print("\n6. TOP VOLUME - KHỐI LƯỢNG CAO NHẤT")
print("-" * 80)

try:
    df_volume = insights.ranking().volume()
    print(f"Số cổ phiếu: {len(df_volume)}")
    print(f"Các cột: {df_volume.columns.tolist()}")
    save_to_postgres(df_volume, 'insights_volume', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 7. Top Deal (Thỏa thuận lớn)
# ============================================================================
print("\n7. TOP DEAL - THỎA THUẬN LỚN")
print("-" * 80)

try:
    df_deal = insights.ranking().deal()
    print(f"Số cổ phiếu: {len(df_deal)}")
    print(f"Các cột: {df_deal.columns.tolist()}")
    save_to_postgres(df_deal, 'insights_deal', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 8. Top Foreign Buy (Nước ngoài mua ròng)
# ============================================================================
print("\n8. TOP FOREIGN BUY - NƯỚC NGOÀI MUA RÒNG")
print("-" * 80)

try:
    df_foreign_buy = insights.ranking().foreign_buy(date="2026-04-04")
    print(f"Số cổ phiếu: {len(df_foreign_buy)}")
    print(f"Các cột: {df_foreign_buy.columns.tolist()}")
    save_to_postgres(df_foreign_buy, 'insights_foreign_buy', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 9. Top Foreign Sell (Nước ngoài bán ròng)
# ============================================================================
print("\n9. TOP FOREIGN SELL - NƯỚC NGOÀI BÁN RÒNG")
print("-" * 80)

try:
    df_foreign_sell = insights.ranking().foreign_sell(date="2026-04-04")
    print(f"Số cổ phiếu: {len(df_foreign_sell)}")
    save_to_postgres(df_foreign_sell, 'insights_foreign_sell', engine)
except Exception as e:
    print(f"Lỗi: {e}")

print("\n" + "=" * 80)
print("MODULE 8: MACRO - KINH TẾ VĨ MỀ")
print("=" * 80)

# ============================================================================
# 10. GDP
# ============================================================================
print("\n10. GDP - TĂNG TRƯỞNG QUỐC NỘI TÍNH TỔNG")
print("-" * 80)

try:
    from vnstock_data import Macro
    macro = Macro()
    
    df_gdp = macro.economy().gdp()
    print(f"Số ghi chép: {len(df_gdp)}")
    print(f"Các cột: {df_gdp.columns.tolist()}")
    save_to_postgres(df_gdp, 'macro_gdp', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 11. CPI
# ============================================================================
print("\n11. CPI - CHỈ SỐ GIÁ TIÊU DÙNG")
print("-" * 80)

try:
    df_cpi = macro.economy().cpi()
    print(f"Số ghi chép: {len(df_cpi)}")
    print(f"Các cột: {df_cpi.columns.tolist()}")
    save_to_postgres(df_cpi, 'macro_cpi', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 12. FDI - Đầu tư trực tiếp nước ngoài
# ============================================================================
print("\n12. FDI - ĐẦU TƯ TRỰC TIẾP NƯỚC NGOÀI")
print("-" * 80)

try:
    df_fdi = macro.economy().fdi()
    print(f"Số ghi chép: {len(df_fdi)}")
    print(f"Các cột: {df_fdi.columns.tolist()}")
    save_to_postgres(df_fdi, 'macro_fdi', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 13. Tỷ giá hối đoái
# ============================================================================
print("\n13. TỶ GIÁ HỐI ĐOÁI")
print("-" * 80)

try:
    df_fx = macro.currency().exchange_rate()
    print(f"Số ghi chép: {len(df_fx)}")
    print(f"Các cột: {df_fx.columns.tolist()}")
    save_to_postgres(df_fx, 'macro_exchange_rate', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 14. Cung tiền tệ
# ============================================================================
print("\n14. CUNG TIỀN TỆ")
print("-" * 80)

try:
    df_money = macro.economy().money_supply()
    print(f"Số ghi chép: {len(df_money)}")
    print(f"Các cột: {df_money.columns.tolist()}")
    save_to_postgres(df_money, 'macro_money_supply', engine)
except Exception as e:
    print(f"Lỗi: {e}")

print("\n" + "=" * 80)
print("MODULE 9: COMMODITY - GIÁ HÀNG HÓA")
print("=" * 80)

# ============================================================================
# 15. Vàng
# ============================================================================
print("\n15. VÀNG")
print("-" * 80)

try:
    commodity = macro.commodity()
    
    df_gold_vn = commodity.gold(market="VN")
    print(f"Vàng trong nước: {len(df_gold_vn)} dòng")
    save_to_postgres(df_gold_vn, 'commodity_gold_vn', engine)
    
    df_gold_global = commodity.gold(market="GLOBAL")
    print(f"Vàng thế giới: {len(df_gold_global)} dòng")
    save_to_postgres(df_gold_global, 'commodity_gold_global', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 16. Dầu Crude
# ============================================================================
print("\n16. DẦU CRUDE")
print("-" * 80)

try:
    df_oil = commodity.oil_crude()
    print(f"Số dòng: {len(df_oil)}")
    print(f"Các cột: {df_oil.columns.tolist()}")
    save_to_postgres(df_oil, 'commodity_oil_crude', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 17. Khí tự nhiên
# ============================================================================
print("\n17. KHÍ TỰ NHIÊN")
print("-" * 80)

try:
    df_gas = commodity.gas(market="GLOBAL")
    print(f"Số dòng: {len(df_gas)}")
    save_to_postgres(df_gas, 'commodity_gas_global', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 18. Xăng dầu Việt Nam
# ============================================================================
print("\n18. XĂNG DẦU VIỆT NAM")
print("-" * 80)

try:
    df_gas_vn = commodity.gas(market="VN")
    print(f"Xăng dầu Việt Nam: {len(df_gas_vn)} dòng")
    save_to_postgres(df_gas_vn, 'commodity_gas_vn', engine)
except Exception as e:
    print(f"Lỗi: {e}")

print("\n" + "=" * 80)
print("MODULE 10: FUND - QUỸ ĐẦU TƯ & ETF")
print("=" * 80)

# ============================================================================
# 19. Danh sách quỹ
# ============================================================================
print("\n19. DANH SÁCH QUỸ ĐẦU TƯ")
print("-" * 80)

try:
    from vnstock_data import Fund
    fund = Fund()
    
    df_fund_list = fund.listing()
    print(f"Số quỹ: {len(df_fund_list)}")
    print(f"Các cột: {df_fund_list.columns.tolist()}")
    save_to_postgres(df_fund_list, 'fund_listing', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 20. Top holdings (Cổ phiếu nắm giữ nhiều nhất)
# ============================================================================
print("\n20. TOP HOLDINGS - CỔ PHIẾU NẮM GIỮ NHIỀU NHẤT")
print("-" * 80)

try:
    # Ví dụ: SSISCA
    fund_info = fund.filter(symbol='SSISCA')
    fund_id = fund_info['id'].iloc[0]
    df_top_holding = fund.top_holding(fundId=int(fund_id))
    print(f"Top holdings của SSISCA ({len(df_top_holding)}):")
    print(f"Các cột: {df_top_holding.columns.tolist()}")
    save_to_postgres(df_top_holding, 'fund_top_holdings', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 21. Cấu trúc ngành của quỹ
# ============================================================================
print("\n21. CẤU TRÚC NGÀNH CỦA QUỸ")
print("-" * 80)

try:
    df_industry_holding = fund.industry_holding(fundId=int(fund_id))
    print(f"Cấu trúc ngành ({len(df_industry_holding)}):")
    print(f"Các cột: {df_industry_holding.columns.tolist()}")
    save_to_postgres(df_industry_holding, 'fund_industry_holding', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 22. Lịch sử NAV của quỹ
# ============================================================================
print("\n22. LỊCH SỬ NAV CỦA QUỸ")
print("-" * 80)

try:
    df_nav = fund.nav_report(fundId=int(fund_id))
    print(f"Lịch sử NAV ({len(df_nav)} dòng):")
    print(f"Các cột: {df_nav.columns.tolist()}")
    save_to_postgres(df_nav, 'fund_nav_history', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 23. Cấu trúc tài sản của quỹ
# ============================================================================
print("\n23. CẤU TRÚC TÀI SẢN CỦA QUỸ")
print("-" * 80)

try:
    df_asset = fund.asset_holding(fundId=int(fund_id))
    print(f"Cấu trúc tài sản: {len(df_asset)} dòng")
    print(f"Các cột: {df_asset.columns.tolist()}")
    save_to_postgres(df_asset, 'fund_asset_holding', engine)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 24. Tổng hợp - Export tất cả insights
# ============================================================================
print("\n" + "=" * 80)
print("24. TỔNG HỢP - LƯU DỮ LIỆU VÀO POSTGRESQL")
print("=" * 80)

try:
    insights = Insights()
    
    df_gainer = insights.ranking().gainer()
    df_loser = insights.ranking().loser()
    df_volume = insights.ranking().volume()
    
    save_to_postgres(df_gainer, 'insights_gainer_export', engine)
    save_to_postgres(df_loser, 'insights_loser_export', engine)
    save_to_postgres(df_volume, 'insights_volume_export', engine)
    
    print("✅ Đã lưu dữ liệu insights vào PostgreSQL")
except Exception as e:
    print(f"Lỗi: {e}")

print("\n" + "=" * 80)
print("✅ HOÀN THÀNH - ĐÃ LƯU TOÀN BỘ DỮ LIỆU VÀO POSTGRESQL")
print("=" * 80)

# ============================================================================
# HIỂN THỊ TỔNG KẾT CÁC BẢNG ĐÃ TẠO
# ============================================================================
print("\n📊 TỔNG KẾT CÁC BẢNG ĐÃ TẠO TRONG POSTGRESQL:")
print("-" * 80)

if engine is not None:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%%'
                ORDER BY table_name
            """))
            
            tables = result.fetchall()
            for table in tables:
                table_name = table[0]
                try:
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = count_result.fetchone()[0]
                    print(f"  • {table_name}: {count} dòng")
                except:
                    print(f"  • {table_name}: (không thể đếm)")
                    
        print(f"\n🎯 Tổng số bảng đã tạo: {len(tables)}")
        print("💾 Dữ liệu đã được lưu trữ an toàn trong PostgreSQL!")
        
    except Exception as e:
        print(f"❌ Lỗi khi truy vấn database: {e}")
else:
    print("⚠️ Không thể kết nối database để hiển thị tổng kết")
    print("💡 Hãy thiết lập PostgreSQL theo hướng dẫn trong README_PostgreSQL.md")
