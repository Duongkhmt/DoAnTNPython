# from vnstock import Trading
# import pandas as pd

# # Thiết lập hiển thị hết các cột trong Pandas (không bị ẩn ...)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 1000)

# def check_single_day(symbol: str, target_date: str):
#     print("="*60)
#     print(f"🕵️ ĐANG ĐIỀU TRA MÃ {symbol} NGÀY {target_date}")
#     print("="*60)

#     # ---------------------------------------------------------
#     # 1. KIỂM TRA LỆNH CAFEF (Bảng trading_order_stats)
#     # ---------------------------------------------------------
#     print(f"\n[1] GỌI API CAFEF - Lấy thống kê đặt lệnh...")
#     try:
#         tr_cafe = Trading(symbol=symbol, source='CAFEF')
#         df_order = tr_cafe.order_stats(start=target_date, end=target_date)
        
#         if df_order.empty:
#             print("  ❌ API chạy thành công nhưng TRẢ VỀ BẢNG TRỐNG (Không có dữ liệu).")
#             print("  👉 Kết luận: CafeF không cấp dữ liệu ngày này (hoặc chặn bot).")
#         else:
#             print("  ✅ API TRẢ VỀ CÓ DỮ LIỆU:")
#             print(df_order)
#     except Exception as e:
#         print(f"  ❌ LỖI VĂNG APP KHI GỌI API: {e}")
#         print("  👉 Kết luận: API CafeF bị lỗi hoặc đổi cấu trúc.")

#     # ---------------------------------------------------------
#     # 2. KIỂM TRA SUMMARY VCI (Bảng trading_summary)
#     # ---------------------------------------------------------
#     print(f"\n{'-'*60}")
#     print(f"[2] GỌI API VCI - Lấy Summary (Kiểm tra 4 cột giá)...")
#     try:
#         tr_vci = Trading(symbol=symbol, source='VCI')
#         df_sum = tr_vci.summary(start=target_date, end=target_date)
        
#         if df_sum.empty:
#             print("  ❌ API TRẢ VỀ BẢNG TRỐNG.")
#         else:
#             print("  ✅ API TRẢ VỀ DỮ LIỆU THÔ NHƯ SAU:")
#             print(df_sum)
            
#             # Kiểm tra xem có cột giá không
#             missing_price = [c for c in ['open', 'high', 'low', 'close', 'open_price', 'close_price'] if c in df_sum.columns]
#             if not missing_price:
#                 print("\n  🚨 CẢNH BÁO: Nhìn bảng trên xem, KHÔNG HỀ CÓ cột open/high/low/close!")
#                 print("  👉 Kết luận: Lỗi do VCI không cung cấp giá ở API này, code của bạn chèn [null] là xử lý đúng!")
#             else:
#                 print(f"\n  ✅ Đã tìm thấy các cột giá: {missing_price}")

#     except Exception as e:
#         print(f"  ❌ LỖI VĂNG APP: {e}")

#     print("\n" + "="*60)

# # ==========================================
# # CHẠY THỬ NGHIỆM TẠI ĐÂY
# # ==========================================
# if __name__ == "__main__":
#     # Điền mã cổ phiếu và 1 ngày làm việc bất kỳ (Thứ 2 - Thứ 6)
#     MA_CO_PHIEU = "FPT"
#     NGAY_KIEM_TRA = "2024-04-12" 
    
#     check_single_day(MA_CO_PHIEU, NGAY_KIEM_TRA)
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from io import StringIO
import time

def lay_bang_lenh_cafef_bang_selenium(symbol: str):
    print("="*60)
    print(f"🚀 ĐANG MỞ CHROME ẢO ĐỂ CÀO MÃ {symbol}...")
    print("="*60)

    # Cấu hình Chrome (Không ẩn cửa sổ để bạn xem nó chạy trực tiếp)
    options = Options()
    options.add_argument("--start-maximized")
    
    # Tự động tải ChromeDriver phù hợp với máy bạn
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    url = f"https://s.cafef.vn/Lich-su-giao-dich-{symbol}-3.chn"
    
    try:
        print("⏳ Đang truy cập trang web và chờ dữ liệu load...")
        driver.get(url)
        
        # Chờ 5 giây để JavaScript của CafeF kịp vẽ cái bảng ra màn hình
        time.sleep(5) 
        
        # Lấy toàn bộ mã HTML sau khi trang đã load xong 100%
        html = driver.page_source
        
        print("⚙️ Đang bóc tách bảng dữ liệu...")
        dfs = pd.read_html(StringIO(html))
        
        # Lấy bảng bự nhất trên web (chắc chắn là bảng dữ liệu)
        df = max(dfs, key=len)
        
        if df.empty or len(df) < 5:
            print("❌ Vẫn không thấy dữ liệu. Có thể web bị lỗi.")
        else:
            print("✅ ĐÃ LẤY THÀNH CÔNG! Xem 5 dòng đầu:")
            print(df.head())
            
            # Lưu ra file CSV
            filename = f"{symbol}_lenh_dat_selenium.csv"
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"\n🎉 ĐÃ LƯU KẾT QUẢ RA FILE: {filename}")
            print("Bạn hãy mở file này lên, xem tên cột của nó là gì để map vào Database nhé!")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        # Xong việc thì tự động đóng Chrome
        driver.quit()

# ==========================================
if __name__ == "__main__":
    # Điền mã cổ phiếu bạn muốn cào
    lay_bang_lenh_cafef_bang_selenium("FPT")