"""
02-QUOTE - Lịch sử giá & Dữ liệu khớp lệnh
Tham khảo: 03-quote.md
Lưu dữ liệu vào PostgreSQL
"""

from vnstock_data import Quote
import pandas as pd
from postgres_utils import PostgreSQLManager

# Khởi tạo PostgreSQL
db = PostgreSQLManager(database='vnstock')

print("=" * 80)
print("MODULE 2: QUOTE - LỊCH SỬ GIÁ & DỮ LIỆU KHỚP LỆNH")
print("=" * 80)

print("=" * 80)
print("MODULE 2: QUOTE - LỊCH SỬ GIÁ & DỮ LIỆU KHỚP LỆNH")
print("=" * 80)

# ============================================================================
# 1. Lịch sử giá OHLCV - Khung ngày (Daily)
# ============================================================================
print("\n1. LỊCH SỬ GIÁ OHLCV - KHUNG NGÀY (DAILY)")
print("-" * 80)

quote = Quote(symbol='TCB', source='VCI')

# Cách 1: Dùng start/end
df_daily = quote.history(start='2026-03-01', end='2026-03-31', interval='1D')
print(f"Số ngày giao dịch: {len(df_daily)}")
print(f"\nCác cột: {df_daily.columns.tolist()}")
print(df_daily.head(10))
db.save_dataframe(df_daily, 'quote_daily_tcb')

# ============================================================================
# 2. Lịch sử giá - Dùng length (tiện lợi)
# ============================================================================
print("\n2. LỊCH SỬ GIÁ - DÙNG LENGTH (KHỐI LƯỢNG 3 THÁNG)")
print("-" * 80)

df_3m = quote.history(length='3M', interval='1D')
print(f"Số ngày giao dịch (3 tháng gần nhất): {len(df_3m)}")
print(df_3m.head(10))
db.save_dataframe(df_3m, 'quote_3m_tcb')

# ============================================================================
# 3. Dữ liệu phút (1 minute)
# ============================================================================
print("\n3. DỮ LIỆU PHÚT (1 MINUTE)")
print("-" * 80)

df_1m = quote.history(length='1D', interval='1m')
print(f"Số bar 1 phút: {len(df_1m)}")
print(f"Các cột: {df_1m.columns.tolist()}")
print(df_1m.head(10))
db.save_dataframe(df_1m, 'quote_1m_tcb')

# ============================================================================
# 4. Dữ liệu giờ (1 hour)
# ============================================================================
print("\n4. DỮ LIỆU GIỜ (1 HOUR)")
print("-" * 80)

# Thử với nguồn MAS vì VCI có thể không ổn định cho hourly data
quote_hourly = Quote(symbol='TCB', source='MAS')
df_1h = quote_hourly.history(length='1W', interval='1H')
print(f"Số bar 1 giờ (1 tuần, nguồn MAS): {len(df_1h)}")
print(df_1h.head(10))
db.save_dataframe(df_1h, 'quote_1h_tcb')

# ============================================================================
# 5. Dữ liệu 5 phút
# ============================================================================
print("\n5. DỮ LIỆU 5 PHÚT")
print("-" * 80)

df_5m = quote.history(length='1D', interval='5m')
print(f"Số bar 5 phút: {len(df_5m)}")
print(df_5m.head())
db.save_dataframe(df_5m, 'quote_5m_tcb')

# ============================================================================
# 6. Khớp lệnh chi tiết trong ngày (Intraday)
# ============================================================================
print("\n6. KHỚP LỆNH CHI TIẾT TRONG NGÀY (INTRADAY)")
print("-" * 80)

print("[SOURCE] Sử dụng quote.intraday() với VCI để lấy lệnh khớp chi tiết trong phiên")
df_intraday = quote.intraday(page_size=10000)
print(f"Tổng lệnh khớp: {len(df_intraday)}")
print(f"Các cột: {df_intraday.columns.tolist()}")
print(df_intraday.head(10))
db.save_dataframe(df_intraday, 'quote_intraday_tcb')

# Phân loại lệnh khớp
print(f"\nPhân loại match_type:")
match_types = df_intraday['match_type'].value_counts()
print(match_types)

# Lưu ý: ATC/ATO là lệnh theo phiên mở/đóng, không nhất thiết là Buy/Sell thông thường
special_types = df_intraday[df_intraday['match_type'].isin(['ATC', 'ATO'])]
if len(special_types) > 0:
    print("\nLưu ý: Dữ liệu có ATC/ATO. Những lệnh này là khớp theo phiên mở/đóng, nên nếu cần phân tích mua/bán chính xác bạn nên tách riêng chúng.")
    print(special_types.head(10))

buy_orders = len(df_intraday[df_intraday['match_type'] == 'Buy'])
sell_orders = len(df_intraday[df_intraday['match_type'] == 'Sell'])
buy_volume = df_intraday[df_intraday['match_type'] == 'Buy']['volume'].sum()
sell_volume = df_intraday[df_intraday['match_type'] == 'Sell']['volume'].sum()

print(f"\nTính toán (chỉ Buy/Sell):")
print(f"  - Lệnh mua: {buy_orders}, Khối lượng: {buy_volume:,}")
print(f"  - Lệnh bán: {sell_orders}, Khối lượng: {sell_volume:,}")

# Tổng tất cả lệnh khớp trong intraday
print(f"  - Tổng lệnh khớp: {len(df_intraday)}")
print(f"  - Tổng khối lượng: {df_intraday['volume'].sum():,}")

# Nếu cần, bạn có thể lọc ATC/ATO ra để phân tích riêng
print("\nGhi chú: ATC/ATO nên được xử lý riêng nếu bạn phân tích khớp lệnh Mua/Bán trong phiên giao dịch chính.")

# ============================================================================
# 7. Độ sâu thị trường (Order Book / Price Depth)
# ============================================================================
print("\n7. ĐỘ SÂU THỊ TRƯỜNG (ORDER BOOK / PRICE DEPTH)")
print("-" * 80)

print("[SOURCE] quote.price_depth() trả về bid/ask depth, tức dữ liệu order book hiện tại")
df_depth = quote.price_depth()
print(f"Số mức giá trong order book: {len(df_depth)}")
print(f"Các cột: {df_depth.columns.tolist()}")
print(df_depth.head(10))

# Tổng hợp bid/ask volume
if 'buy_volume' in df_depth.columns and 'sell_volume' in df_depth.columns:
    buy_tot = pd.to_numeric(df_depth['buy_volume'], errors='coerce').sum()
    sell_tot = pd.to_numeric(df_depth['sell_volume'], errors='coerce').sum()
    print(f"\nTổng dư mua: {buy_tot:,.0f}")
    print(f"Tổng dư bán: {sell_tot:,.0f}")
    if 'undefined_volume' in df_depth.columns:
        undef_tot = pd.to_numeric(df_depth['undefined_volume'], errors='coerce').sum()
        print(f"Tổng undefined_volume: {undef_tot:,.0f}")
        print("Lưu ý: undefined_volume là lượng chưa được phân loại rõ Buy/Sell trong nguồn dữ liệu.")
else:
    print("\nLưu ý: Cột buy_volume/sell_volume không tồn tại trong nguồn hiện tại. Kiểm tra lại cấu trúc df_depth.")

# ============================================================================
# 8. Đổi nguồn dữ liệu (KBS)
# ============================================================================
print("\n8. ĐỔI NGUỒN DỮ LIỆU - KBS")
print("-" * 80)

quote_kbs = Quote(symbol='TCB', source='KBS')
df_daily_kbs = quote_kbs.history(start='2026-03-01', end='2026-03-31', interval='1D')
print(f"Lịch sử giá KBS ({len(df_daily_kbs)} ngày):")
print(f"Các cột: {df_daily_kbs.columns.tolist()}")
print(df_daily_kbs.head())

# ============================================================================
# 9. Đổi nguồn dữ liệu (MAS)
# ============================================================================
print("\n9. ĐỔI NGUỒN DỮ LIỆU - MAS")
print("-" * 80)

quote_mas = Quote(symbol='TCB', source='MAS')
df_daily_mas = quote_mas.history(start='2026-03-01', end='2026-03-31', interval='1D')
print(f"Lịch sử giá MAS ({len(df_daily_mas)} ngày):")
print(df_daily_mas.head())

# ============================================================================
# 10. Lấy dữ liệu cho nhiều mã cổ phiếu
# ============================================================================
print("\n10. LẤY DỮ LIỆU CHO NHIỀU MÃ CỔ PHIẾU")
print("-" * 80)

symbols = ['TCB', 'VNM', 'HPG', 'SHB']
data = {}

for symbol in symbols:
    try:
        q = Quote(symbol=symbol, source='VCI')
        df = q.history(start='2026-03-01', end='2026-03-31', interval='1D')
        data[symbol] = df
        print(f"✓ {symbol}: {len(df)} ngày")
    except Exception as e:
        print(f"✗ {symbol}: {e}")

# Gộp dữ liệu (lấy close price)
if data:
    df_combined = pd.DataFrame()
    for symbol, df in data.items():
        df_combined[symbol] = df['close']
    print(f"\nDữ liệu gộp ({len(df_combined)} ngày):")
    print(df_combined.head(10))
    
    # Lưu file
    df_combined.to_csv('quote_combined.csv', encoding='utf-8-sig')
    print("\n✅ Dữ liệu đã lưu vào: quote_combined.csv")

# ============================================================================
# 11. Phân tích - Tính các chỉ báo đơn giản
# ============================================================================
print("\n11. PHÂN TÍCH - TÍNH CÁC CHỈ BÁO ĐƠN GIẢN")
print("-" * 80)

quote = Quote(symbol='VNM', source='VCI')
df = quote.history(start='2026-01-01', end='2026-03-31', interval='1D')

# SMA (Simple Moving Average)
df['SMA_5'] = df['close'].rolling(window=5).mean()
df['SMA_20'] = df['close'].rolling(window=20).mean()

# Volatility
df['Daily_Return'] = df['close'].pct_change()
df['Volatility'] = df['Daily_Return'].rolling(window=20).std()

# Thay đổi giá
df['Price_Change'] = df['close'].diff()
df['Price_Change_Pct'] = df['close'].pct_change() * 100

print(df[['time', 'close', 'SMA_5', 'SMA_20', 'Price_Change_Pct', 'Volatility']].head(20))

# Lưu file
df.to_csv('quote_analysis.csv', index=False, encoding='utf-8-sig')
print("\n✅ Dữ liệu phân tích đã lưu vào: quote_analysis.csv")
