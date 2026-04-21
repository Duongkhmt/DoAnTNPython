"""
05-TRADING - Dữ liệu giao dịch chi tiết
Tham khảo: 06-trading.md
"""

from vnstock_data import Trading, Quote
import pandas as pd
from postgres_utils import PostgreSQLManager

print("=" * 80)
print("MODULE 5: TRADING - DỮ LIỆU GIAO DỊCH CHI TIẾT")
print("=" * 80)

# Khởi tạo kết nối PostgreSQL
db = PostgreSQLManager(database='vnstock')

# ============================================================================
# 1. Bảng giá realtime (Price Board)
# ============================================================================
print("\n1. BẢNG GIÁ REALTIME (PRICE BOARD)")
print("-" * 80)

trading = Trading(symbol='SHB', source='VCI')

df_board = trading.price_board(['SHB', 'TCB', 'VNM'])
print(f"Bảng giá realtime ({len(df_board)} cổ phiếu):")
print(f"Các cột: {df_board.columns.tolist()[:10]}...")
print(df_board.head())

# Lưu vào PostgreSQL
db.save_dataframe(df_board, 'trading_price_board_shb_tcb_vnm')

# ============================================================================
# 2. Lịch sử giá & Khối lượng khớp
# ============================================================================
print("\n2. LỊCH SỬ GIÁ & KHỐI LƯỢNG KHỚP")
print("-" * 80)

df_history = trading.price_history(start='2026-03-20', end='2026-03-30')
print(f"Số ngày giao dịch: {len(df_history)}")
print(f"Các cột: {df_history.columns.tolist()[:10]}...")
print(df_history[['trading_date', 'open', 'high', 'low', 'close', 'matched_volume']].head())

# Lưu vào PostgreSQL
db.save_dataframe(df_history, 'trading_price_history_shb_vci')

# ============================================================================
# 3. Khối lượng khớp mua/bán (từ Intraday)
# ============================================================================
print("\n3. KHỐI LƯỢNG KHỚP MUA/BÁN (TỪ INTRADAY)")
print("-" * 80)

quote = Quote(symbol='SHB', source='VCI')
df_intraday = quote.intraday(page_size=10000)
print(f"Tổng lệnh khớp trong ngày: {len(df_intraday)}")
print(f"Các cột: {df_intraday.columns.tolist()}")

# Lưu vào PostgreSQL
db.save_dataframe(df_intraday, 'trading_intraday_shb_vci')

# Tính toán
matched_buy_vol = df_intraday[df_intraday['match_type'] == 'Buy']['volume'].sum()
matched_sell_vol = df_intraday[df_intraday['match_type'] == 'Sell']['volume'].sum()
matched_buy_count = len(df_intraday[df_intraday['match_type'] == 'Buy'])
matched_sell_count = len(df_intraday[df_intraday['match_type'] == 'Sell'])

print(f"\nKhớp Mua:")
print(f"  - Lệnh: {matched_buy_count}")
print(f"  - Khối lượng: {matched_buy_vol:,}")
print(f"\nKhớp Bán:")
print(f"  - Lệnh: {matched_sell_count}")
print(f"  - Khối lượng: {matched_sell_vol:,}")

# ============================================================================
# 4. Thống kê đặt lệnh & Tính lệnh hủy (CafeF)
# ============================================================================
print("\n4. THỐNG KÊ ĐẶT LỆNH & TÍNH LỆNH HỦY")
print("-" * 80)

trading_cafe = Trading(symbol='SHB', source='CafeF')
df_order = trading_cafe.order_stats(start='2026-03-20', end='2026-03-30')
print(f"Số ngày: {len(df_order)}")
print(f"Các cột: {df_order.columns.tolist()}")

# Lưu vào PostgreSQL
db.save_dataframe(df_order, 'trading_order_stats_shb_cafef')

# Gộp với intraday để tính hủy
result_list = []

for idx, row_order in df_order.iterrows():
    # Lấy dữ liệu intraday của ngày đó
    trading_date = idx if hasattr(idx, 'date') else pd.to_datetime(idx).date()
    
    # Nếu cần, lấy intraday của ngày cụ thể
    # df_intraday_day = quote.intraday() # có thể lọc theo ngày
    
    buy_orders = row_order['buy_orders']
    sell_orders = row_order['sell_orders']
    buy_volume = row_order['buy_volume']
    sell_volume = row_order['sell_volume']
    avg_buy = row_order['avg_buy_order_volume']
    avg_sell = row_order['avg_sell_order_volume']
    
    # Tính hủy (gần đúng)
    cancelled_buy = buy_volume - matched_buy_vol if buy_volume > matched_buy_vol else 0
    cancelled_sell = sell_volume - matched_sell_vol if sell_volume > matched_sell_vol else 0
    
    result_list.append({
        'Ngày': trading_date,
        'Đặt Mua (Lệnh)': buy_orders,
        'Đặt Mua (KL)': int(buy_volume),
        'Khớp Mua (KL)': int(matched_buy_vol),
        'Hủy Mua (KL)': int(cancelled_buy),
        'Đặt Bán (Lệnh)': sell_orders,
        'Đặt Bán (KL)': int(sell_volume),
        'Khớp Bán (KL)': int(matched_sell_vol),
        'Hủy Bán (KL)': int(cancelled_sell),
    })

df_combined = pd.DataFrame(result_list)
print(f"\nDữ liệu gộp (Đặt lệnh + Khớp + Hủy):")
print(df_combined.head())

# ============================================================================
# 5. Khối ngoại (Foreign Trade)
# ============================================================================
print("\n5. KHỐI NGOẠI (FOREIGN TRADE)")
print("-" * 80)

df_foreign = trading.foreign_trade(start='2026-03-20', end='2026-03-30')
print(f"Số ngày: {len(df_foreign)}")
print(f"Các cột: {df_foreign.columns.tolist()}")
print(df_foreign[['trading_date', 'fr_buy_volume_matched', 'fr_sell_volume_matched', 'fr_owned']].head())

# Tính net volume
df_foreign['fr_net_volume'] = df_foreign['fr_buy_volume_matched'] - df_foreign['fr_sell_volume_matched']
print("\nKhối ngoại ròng (mua - bán):")
print(df_foreign[['trading_date', 'fr_buy_volume_matched', 'fr_sell_volume_matched', 'fr_net_volume']].head())

# ============================================================================
# 6. Tự doanh (Proprietary Trade)
# ============================================================================
print("\n6. TỰ DOANH (PROPRIETARY TRADE)")
print("-" * 80)

df_prop = trading.prop_trade(start='2026-03-20', end='2026-03-30')
print(f"Số ngày: {len(df_prop)}")
print(f"Các cột: {df_prop.columns.tolist()}")
print(df_prop[['trading_date', 'total_buy_trade_volume', 'total_sell_trade_volume']].head())

# Tính net volume
df_prop['prop_net_volume'] = df_prop['total_buy_trade_volume'] - df_prop['total_sell_trade_volume']
print("\nTự doanh ròng (mua - bán):")
print(df_prop[['trading_date', 'total_buy_trade_volume', 'total_sell_trade_volume', 'prop_net_volume']].head())

# ============================================================================
# 7. Giao dịch nội bộ (Insider Deal)
# ============================================================================
print("\n7. GIAO DỊCH NỘI BỘ (INSIDER DEAL)")
print("-" * 80)

try:
    df_insider = trading.insider_deal(limit=10)
    print(f"Số giao dịch: {len(df_insider)}")
    print(f"Các cột: {df_insider.columns.tolist()}")
    print(df_insider.head())
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 8. Order book depth (Bid/Ask)
# ============================================================================
print("\n8. ORDER BOOK DEPTH (BID/ASK)")
print("-" * 80)

quote = Quote(symbol='SHB', source='VCI')
df_depth = quote.price_depth()
print(f"Số mức giá: {len(df_depth)}")
print(f"Các cột: {df_depth.columns.tolist()}")
print(df_depth.head(10))

# Tính total bid/ask
total_bid = pd.to_numeric(df_depth['buy_volume'], errors='coerce').sum()
total_ask = pd.to_numeric(df_depth['sell_volume'], errors='coerce').sum()
print(f"\nTổng mua (Bid): {total_bid:,.0f}")
print(f"Tổng bán (Ask): {total_ask:,.0f}")
print(f"Dư mua/bán: {total_bid - total_ask:,.0f}")

# ============================================================================
# 9. Summary - Thống kê tổng hợp (VCI)
# ============================================================================
print("\n9. SUMMARY - THỐNG KÊ TỔNG HỢP")
print("-" * 80)

try:
    df_summary = trading.summary(start='2026-03-20', end='2026-03-30')
    print(f"Các cột: {df_summary.columns.tolist()}")
    print(df_summary)
except Exception as e:
    print(f"Lỗi: {e}")

# ============================================================================
# 10. Lấy dữ liệu giao dịch cho nhiều cổ phiếu
# ============================================================================
print("\n10. LẤY DỮ LIỆU GIAO DỊCH CHO NHIỀU CỔ PHIẾU")
print("-" * 80)

symbols = ['TCB', 'VNM', 'HPG', 'SHB']
trading_data = []

for symbol in symbols:
    try:
        tr = Trading(symbol=symbol, source='VCI')
        df = tr.price_history(start='2026-03-30', end='2026-03-30')
        
        if len(df) > 0:
            row = df.iloc[0]
            trading_data.append({
                'Symbol': symbol,
                'Ngày': row['trading_date'],
                'Close': row['close'],
                'Khối lượng': int(row['matched_volume']),
                'Thay đổi': row.get('price_change', 'N/A'),
            })
            print(f"✓ {symbol}")
    except Exception as e:
        print(f"✗ {symbol}: {e}")

df_trading = pd.DataFrame(trading_data)
print(f"\nDữ liệu giao dịch các cổ phiếu (30/03/2026):")
print(df_trading)

# ============================================================================
# 11. Phân tích - So sánh khối ngoại vs tự doanh
# ============================================================================
print("\n11. PHÂN TÍCH - SO SÁNH KHỐI NGOẠI VS TỰ DOANH")
print("-" * 80)

symbol = 'SHB'
trading = Trading(symbol=symbol, source='VCI')

df_foreign = trading.foreign_trade(start='2026-03-01', end='2026-03-30')
df_prop = trading.prop_trade(start='2026-03-01', end='2026-03-30')

# Gộp
df_analysis = df_foreign[['trading_date']].copy()
df_analysis['fr_net'] = df_foreign['fr_buy_volume_matched'] - df_foreign['fr_sell_volume_matched']
df_analysis['prop_net'] = df_prop['total_buy_trade_volume'] - df_prop['total_sell_trade_volume']

print(f"So sánh khối ngoại vs tự doanh:")
print(df_analysis.head(10))

# Chart
print(f"\nTổng cộng 1 tháng:")
print(f"  - Khối ngoại ròng: {df_analysis['fr_net'].sum():,}")
print(f"  - Tự doanh ròng: {df_analysis['prop_net'].sum():,}")

# ============================================================================
# 12. Export dữ liệu hoàn chỉnh
# ============================================================================
print("\n12. EXPORT DỮ LIỆU HOÀN CHỈNH")
print("-" * 80)

trading = Trading(symbol='SHB', source='VCI')

df_history = trading.price_history(start='2026-03-20', end='2026-03-30')
df_foreign = trading.foreign_trade(start='2026-03-20', end='2026-03-30')
df_prop = trading.prop_trade(start='2026-03-20', end='2026-03-30')

df_history.to_csv('trading_price_history.csv', index=False, encoding='utf-8-sig')
df_foreign.to_csv('trading_foreign.csv', index=False, encoding='utf-8-sig')
df_prop.to_csv('trading_proprietary.csv', index=False, encoding='utf-8-sig')

print("✅ Dữ liệu giao dịch đã export:")
print("  - trading_price_history.csv")
print("  - trading_foreign.csv")
print("  - trading_proprietary.csv")
