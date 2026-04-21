"""
HƯỚNG DẪN SỬ DỤNG VNSTOCK_DATA - KHAI THÁC TỐI ĐA DỮ LIỆU
================================================================================

Tài liệu này cung cấp hướng dẫn toàn diện cách sử dụng thư viện vnstock_data
để lấy dữ liệu chứng khoán từ nhiều nguồn khác nhau.

CẤU TRÚC HƯỚNG DẪN:
- Các file example được tổ chức theo module
- Mỗi file chứa các ví dụ thực tế
- Code có sẵn và có thể chạy trực tiếp

================================================================================
"""

import os

print("=" * 80)
print("VNSTOCK_DATA - HƯỚNG DẪN SỬ DỤNG TOÀN DIỆN")
print("=" * 80)

print("\n📁 DANH SÁCH FILE EXAMPLES:")
print("-" * 80)

examples = [
    ("01_listing_examples.py", "LISTING - Danh sách cổ phiếu, ngành, chỉ số"),
    ("02_quote_examples.py", "QUOTE - Lịch sử giá, khớp lệnh, order book"),
    ("03_company_examples.py", "COMPANY - Thông tin công ty, cổ đông, ban lãnh đạo"),
    ("04_finance_examples.py", "FINANCE - Báo cáo tài chính, chỉ số"),
    ("05_trading_examples.py", "TRADING - Giao dịch, khối ngoại, tự doanh"),
    ("06_market_insights_macro_commodity_fund.py", "MARKET, INSIGHTS, MACRO, COMMODITY, FUND"),
]

for i, (filename, description) in enumerate(examples, 1):
    print(f"{i}. {filename}")
    print(f"   └─ {description}")

print("\n" + "=" * 80)
print("HƯỚNG DẪN SỬ DỤNG:")
print("=" * 80)

print("""
1. CHẠY TỪNG FILE EXAMPLE:
   python 01_listing_examples.py
   python 02_quote_examples.py
   ... v.v

2. LẤY DỮ LIỆU CỤ THỂ:
   
   a) Danh sách cổ phiếu:
      from vnstock_data import Listing
      lst = Listing(source='VCI')
      df = lst.all_symbols()
   
   b) Lịch sử giá:
      from vnstock_data import Quote
      quote = Quote(symbol='TCB', source='VCI')
      df = quote.history(start='2026-01-01', end='2026-03-31')
   
   c) Thông tin công ty:
      from vnstock_data import Company
      company = Company(symbol='VNM', source='VCI')
      df = company.overview()
      df = company.shareholders()
   
   d) Báo cáo tài chính:
      from vnstock_data import Finance
      finance = Finance(symbol='TCB', source='VCI')
      df = finance.balance_sheet(lang='vi')
      df = finance.income_statement(lang='vi')
   
   e) Dữ liệu giao dịch:
      from vnstock_data import Trading
      trading = Trading(symbol='SHB', source='VCI')
      df = trading.price_history(start='2026-03-01', end='2026-03-30')
      df = trading.foreign_trade(start='2026-03-01', end='2026-03-30')
   
   f) Định giá thị trường:
      from vnstock_data import Market
      market = Market(source='VND')
      df = market.pe()
      df = market.pb()
   
   g) Top cổ phiếu:
      from vnstock_data import Insights
      insights = Insights(source='VND')
      df = insights.gainer()
      df = insights.volume()
   
   h) Dữ liệu kinh tế vĩ mô:
      from vnstock_data import Macro
      macro = Macro(source='MBK')
      df = macro.gdp()
      df = macro.cpi()
   
   i) Giá hàng hóa:
      from vnstock_data import Commodity
      commodity = Commodity(source='SPL')
      df = commodity.gold_global()
      df = commodity.oil_crude()
   
   j) Quỹ ETF:
      from vnstock_data import Fund
      fund = Fund(source='Fmarket')
      df = fund.listing()
      df = fund.nav_report(symbol='SSISCA')

3. CHỌN NGUỒN DỮ LIỆU PHÙ HỢP:
   
   ┌─────────────────────────────────────────────────────────┐
   │ LOẠI DỮ LIỆU            │ NGUỒN KHUYẾN NGHỊ │ LƯU Ý    │
   ├─────────────────────────────────────────────────────────┤
   │ Danh sách cổ phiếu      │ VCI, VND          │ VCI đầy đủ│
   │ Lịch sử giá (Daily)     │ VCI, VND, KBS     │ VND nhanh │
   │ Lịch sử giá (Intraday)  │ VCI, KBS          │ Chi tiết  │
   │ Thông tin công ty       │ VCI (duy nhất)    │ Đầy đủ   │
   │ BCTC đơn giản           │ VCI               │ Dễ dùng  │
   │ BCTC chi tiết           │ MAS               │ Phân cấp │
   │ Giao dịch chi tiết      │ CafeF             │ Đầy đủ   │
   │ Định giá thị trường     │ VND               │ Duy nhất  │
   │ Top cổ phiếu           │ VND               │ Duy nhất  │
   │ Kinh tế vĩ mô          │ MBK               │ Duy nhất  │
   │ Hàng hóa               │ SPL               │ Duy nhất  │
   │ Quỹ ETF                │ Fmarket           │ Duy nhất  │
   └─────────────────────────────────────────────────────────┘

4. GỢI Ý WORKFLOW:

   a) Phân tích kỹ thuật (Technical Analysis):
      - Quote.history (VND) + Intraday
      - Tính SMA, RSI, MACD
   
   b) Phân tích cơ bản (Fundamental):
      - Company.overview, shareholders
      - Finance.balance_sheet, income_statement
      - Market.evaluation (so sánh P/E, P/B)
   
   c) Phân tích thị trường:
      - Listing.all_symbols (phân loại)
      - Insights (top gainer, loser, volume)
      - Market (định giá toàn thị trường)
   
   d) Phân tích khối ngoại:
      - Trading.foreign_trade (VCI)
      - Insights.foreign_buy, foreign_sell (VND)
   
   e) Phân tích macro:
      - Macro.gdp, cpi, fdi, exchange_rate (MBK)
      - Commodity.gold_global, oil_crude (SPL)
      - So sánh correlation với nhóm ngành

5. XỬ LỚ DỮ LIỆU VÀ TÍNH TOÁN:
   
   # Tính KHỐI LƯỢNG KHỚP MUA/BÁN từ intraday
   quote = Quote(symbol='SHB', source='VCI')
   df_intraday = quote.intraday(page_size=10000)
   buy_vol = df_intraday[df_intraday['match_type']=='Buy']['volume'].sum()
   sell_vol = df_intraday[df_intraday['match_type']=='Sell']['volume'].sum()
   
   # Tính LỆNH HỦY
   trading = Trading(symbol='SHB', source='CafeF')
   df_order = trading.order_stats(...)
   cancelled = df_order['buy_volume'] - buy_vol
   
   # Tính CÁC CHỈ BÁO ĐƠN GIẢN
   df['SMA_20'] = df['close'].rolling(window=20).mean()
   df['ROE'] = (net_income / equity) * 100
   
   # GỘP DỮ LIỆU TỪNG NGÀY
   df_combined = df_price.merge(df_foreign, on='trading_date')

6. LƯU DỮ LIỆU:
   
   # Export CSV
   df.to_csv('data.csv', index=False, encoding='utf-8-sig')
   
   # Export Excel
   df.to_excel('data.xlsx', index=False)
   
   # Export JSON
   df.to_json('data.json', orient='records')

7. XỬ LÝ LỖI:
   
   try:
       quote = Quote(symbol='TCB', source='VCI')
       df = quote.history(start='2026-01-01', end='2026-03-31')
   except Exception as e:
       print(f"Lỗi: {e}")
       # Thử fallback với VND hoặc KBS
       quote = Quote(symbol='TCB', source='VND')
       df = quote.history(start='2026-01-01', end='2026-03-31')

8. DỮ LIỆU THƯỜNG DÙNG:

   # Top 100 cổ phiếu theo khối lượng hôm nay
   insights = Insights(source='VND')
   df_top = insights.volume().head(100)
   
   # Nhóm VN30
   lst = Listing(source='VCI')
   df_vn30 = lst.symbols_by_group(group='VN30')
   
   # BCTC ngân hàng
   companies = ['TCB', 'BID', 'MBB', 'ACB']
   for symbol in companies:
       finance = Finance(symbol=symbol, source='VCI')
       df = finance.income_statement()
       # xử lý dữ liệu

9. BEST PRACTICES:

   ✓ Sử dụng 1 nguồn chính tránh không nhất quán dữ liệu
   ✓ Cache dữ liệu nếu gọi API cùng lúc nhiều lần
   ✓ Kiểm tra dữ liệu (OHLC logic: high >= low)
   ✓ Luôn có fallback strategy
   ✓ Đọc error messages cẩn thận
   ✓ Log dữ liệu bất thường để debug

10. THAM KHẢO THÊM:
    
    - 12-data-sources.md: Ma trận hỗ trợ của từng nguồn
    - 13-best-practices.md: Các mẹo và patterns
    - 14-unified-ui.md: Giao diện hợp nhất mới
""")

print("\n" + "=" * 80)
print("✅ ĐỤC CHẠY FILE EXAMPLES ĐỂ THẤY KẾT QUẢ THỰC TẾ")
print("=" * 80)
