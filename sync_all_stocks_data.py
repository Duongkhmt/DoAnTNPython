import pandas as pd
from vnstock_data import Listing, Quote, Finance, Company, Trading
from postgres_utils import PostgreSQLManager
import math
import time
from datetime import datetime, timedelta
import sys

print("=" * 80)
print("MASTER SYNC CHỨNG KHOÁN (VNSTOCK DATA)")
print("=" * 80)

# Khởi tạo Postgres và tự động tạo bảng nếu chưa có
db = PostgreSQLManager(database='vnstock')
db.create_all_tables()

if '--daily' in sys.argv:
    # Lấy lùi lại 7 ngày để quét bù dữ liệu cuối tuần/ngày lễ bị hụt
    START_DATE = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    print(f"[*] CHẾ ĐỘ ĐỒNG BỘ HẰNG NGÀY: {START_DATE} đến nay")
else:
    START_DATE = '2024-01-01'
    print(f"[*] CHẾ ĐỘ ĐỒNG BỘ LỊCH SỬ: {START_DATE} đến nay")

TODAY = datetime.now().strftime('%Y-%m-%d')

# def sync_listing():
#     """Đồng bộ danh sách toàn bộ các mã chứng khoán"""
#     print("\n[1] ĐANG ĐỒNG BỘ DANH SÁCH MÃ CHỨNG KHOÁN (LISTING)...")
#     lst = Listing(source='VND') # VCI có lượng mã lớn, hoặc VND tùy gói.
#     df_listing = lst.all_symbols()

#     # THÊM DÒNG NÀY ĐỂ DEBUG TÊN CỘT:
#     print("Các cột API trả về là:", df_listing.columns.tolist())
    
#     if len(df_listing) == 0:
#         print("Không kéo được danh sách mã. Thoát!")
#         return []
    
#     # Chuẩn hóa cột theo đúng chuẩn Table 'listing' (symbol, organ_name, exchange...)
#     df_clean = pd.DataFrame()
#     df_clean['symbol'] = df_listing['symbol']
#     df_clean['organ_name'] = df_listing.get('organ_name', None)
#     df_clean['exchange'] = df_listing.get('exchange', None)
#     df_clean['industry'] = df_listing.get('industry', None)
#     df_clean['sector'] = df_listing.get('sector', None)
#     df_clean['company_type'] = df_listing.get('company_type', None)

    
#     # Lưu vào PostgreSQL với UPSERT (cập nhật mới nếu trùng conflict 'symbol')
#     db.upsert_dataframe(df_clean, 'listing', conflict_cols=['symbol'])
#     return df_clean['symbol'].tolist()

def sync_listing():
    """Đồng bộ danh sách toàn bộ các mã chứng khoán"""
    print("\n[1] ĐANG ĐỒNG BỘ DANH SÁCH MÃ CHỨNG KHOÁN (LISTING)...")
    
    # Bắt buộc dùng VND theo giới hạn của thư viện
    lst = Listing(source='VND') 
    df_listing = lst.all_symbols()

    # Chỉ giữ lại những mã là Cổ phiếu (type = 'STOCK')
    # Tùy thuộc vào cột 'type' của VND trả về, có thể là 'STOCK', 'Cổ phiếu', v.v.
    if 'type' in df_listing.columns:
        # Bạn có thể in print(df_listing['type'].unique()) để xem các loại hình
        df_listing = df_listing[df_listing['type'].isin(['STOCK', 'Cổ phiếu', 'CP'])]
    
    if len(df_listing) == 0:
        print("Không kéo được danh sách mã. Thoát!")
        return []
    
    # Chuẩn hóa cột theo đúng chuẩn Table 'listing'
    df_clean = pd.DataFrame()
    df_clean['symbol'] = df_listing['symbol']
    df_clean['organ_name'] = df_listing.get('company_name', None)
    df_clean['exchange'] = df_listing.get('exchange', None)
    df_clean['company_type'] = df_listing.get('type', None)
    
    # Chấp nhận để None, chúng ta sẽ xử lý Ngành ở bảng Company
    df_clean['industry'] = None
    df_clean['sector'] = None
    
    # Lưu vào PostgreSQL
    db.upsert_dataframe(df_clean, 'listing', conflict_cols=['symbol'])
    return df_clean['symbol'].tolist()

def sync_quotes(symbols):
    """Đồng bộ Lịch sử Giá (Quote) cho tất cả mã từ 2024-01-01"""
    print(f"\n[2] ĐANG ĐỒNG BỘ LỊCH SỬ GIÁ QUOTE CHO {len(symbols)} MÃ...")
    print(f"Từ {START_DATE} đến nay...")
    
    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"[{idx+1}/{len(symbols)}] Đang tải Quote: {symbol}...")
        try:
            q = Quote(symbol=symbol, source='VND') # Dùng VND cho daily lịch sử tốt.
            df = q.history(start=START_DATE, end=TODAY, interval='1D')
            
            if len(df) > 0:
                df_clean = df.copy()
                if 'time' in df_clean.columns and 'trading_date' not in df_clean.columns:
                    df_clean = df_clean.rename(columns={'time': 'trading_date'})
                
                if 'symbol' not in df_clean.columns:
                    df_clean['symbol'] = symbol
                expected_cols = ['symbol', 'trading_date', 'open', 'high', 'low', 'close', 'volume']
                for col in expected_cols:
                    if col not in df_clean.columns: df_clean[col] = None
                
                df_clean = df_clean[expected_cols]
                db.upsert_dataframe(df_clean, 'quote_history', conflict_cols=['symbol', 'trading_date'])
                success += 1
            
            time.sleep(0.1) # Tránh bị rate limited
        except Exception:
            pass
        
    print(f"\n✅ Hoàn thành đồng bộ Giá! Thành công: {success}/{len(symbols)}.")

def sync_company(symbols):
    """Đồng bộ Hồ sơ Công ty (Overview)"""
    print(f"\n[3] ĐANG ĐỒNG BỘ THÔNG TIN COMPANY CHO {len(symbols)} MÃ...")
    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"[{idx+1}/{len(symbols)}] Đang tải Company: {symbol}...")
        try:
            # Dùng nguồn VCI đã test thành công
            c = Company(symbol=symbol, source='VCI')
            df = c.overview()
            
            if len(df) > 0:
                df_clean = pd.DataFrame()
                df_clean['symbol'] = [symbol]
                
                # Map chính xác cột Ngành và Lĩnh vực từ VCI
                df_clean['industry'] = df.get('icb_name2', None)
                df_clean['sector'] = df.get('icb_name3', None)
                
                # Lưu ý: VCI không có 'name' hay 'exchange' ở API này, 
                # ta chỉ Upsert 3 cột trên. Hàm Upsert của bạn sẽ tự động 
                # điền thông tin này vào Database mà không báo lỗi.
                db.upsert_dataframe(df_clean, 'company', conflict_cols=['symbol'])
                success += 1
                
        except Exception as e:
            # In ra lỗi để dễ theo dõi nếu gặp cổ phiếu rác
            print(f"   -> Bỏ qua Company {symbol}: {e}")
            pass
            
        time.sleep(0.1) # Giãn cách request tránh block IP
        
    print(f"\n✅ Hoàn thành đồng bộ Company! Thành công: {success}/{len(symbols)}.")

# def sync_finance(symbols):
    """Đồng bộ Thông tin Tài chính (Dùng Ratio để lấy chỉ số sinh lời)"""
    print(f"\n[4] ĐANG ĐỒNG BỘ TÀI CHÍNH (FINANCE) CHO {len(symbols)} MÃ...")
    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"[{idx+1}/{len(symbols)}] Đang tải Finance: {symbol}...")
        try:
            f = Finance(symbol=symbol, source='VCI')
            df = f.ratio(lang='vi')
            if len(df) > 0:
                records = []
                for _, row in df.iterrows():
                    records.append({
                        'symbol': symbol,
                        'report_type': 'ratio',
                        'report_period': 'Latest',
                        'item_name': row.get('name', 'N/A'),
                        'value': row.get('value', 0.0)
                    })
                df_clean = pd.DataFrame(records)
                df_clean = df_clean.drop_duplicates(subset=['symbol', 'report_type', 'report_period', 'item_name'], keep='last')
                db.upsert_dataframe(df_clean, 'finance', conflict_cols=['symbol', 'report_type', 'report_period', 'item_name'])
                success += 1
            time.sleep(0.1)
        except Exception:
            pass
    print(f"\n✅ Hoàn thành đồng bộ Finance! Thành công: {success}/{len(symbols)}.")

def sync_finance(symbols):
    """Đồng bộ Thông tin Tài chính (Dùng Ratio để lấy chỉ số sinh lời)"""
    print(f"\n[4] ĐANG ĐỒNG BỘ TÀI CHÍNH (FINANCE) CHO {len(symbols)} MÃ...")
    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"[{idx+1}/{len(symbols)}] Đang tải Finance: {symbol}...")
        try:
            # Dùng VCI theo đúng yêu cầu của vnstock
            f = Finance(symbol=symbol, source='VCI')
            df = f.ratio()
            
            if len(df) > 0:
                # 1. Kéo kỳ báo cáo (2018-Q1) từ Index ra thành một cột thật sự
                df = df.reset_index() 
                
                # Sau khi reset, cột index thường mang tên 'period' (hoặc 'index')
                # Ta lấy luôn tên của cột đầu tiên để làm mỏ neo
                period_col = df.columns[0] 
                
                # 2. Lọc bỏ các cột kỹ thuật không phải là chỉ số tài chính
                cols_to_drop = [period_col, 'report_period', 'Ratio TTM Id', 'Ratio Type', 'Ratio Year Id', 'ticker', 'symbol']
                val_cols = [col for col in df.columns if col not in cols_to_drop]
                
                # 3. DÙNG MELT ĐỂ XOAY BẢNG TỪ NGANG SANG DỌC
                df_melted = pd.melt(df, 
                                    id_vars=[period_col],   # Giữ nguyên cột thời gian (VD: 2018-Q1)
                                    value_vars=val_cols,    # Xoay toàn bộ cột chỉ số (P/E, P/B...) thành dòng
                                    var_name='item_name',   # Đặt tên cột mới là item_name
                                    value_name='value')     # Đặt tên cột giá trị là value
                
                # 4. Chuẩn hóa để Map vào Database
                df_melted = df_melted.rename(columns={period_col: 'report_period'})
                df_melted['symbol'] = symbol
                df_melted['report_type'] = 'ratio'
                
                # Ép kiểu giá trị về dạng số (float). Nếu là N/A hoặc rỗng, Pandas sẽ biến thành NaN
                df_melted['value'] = pd.to_numeric(df_melted['value'], errors='coerce')
                
                # Xóa những dòng NaN (những quý mà công ty không có chỉ số đó)
                df_melted = df_melted.dropna(subset=['value'])
                
                # Sắp xếp đúng thứ tự cột của Table
                df_clean = df_melted[['symbol', 'report_type', 'report_period', 'item_name', 'value']]
                
                # Đề phòng API nhả dòng trùng lặp
                df_clean = df_clean.drop_duplicates(subset=['symbol', 'report_type', 'report_period', 'item_name'], keep='last')
                
                # Upsert vào Postgres
                db.upsert_dataframe(df_clean, 'finance', conflict_cols=['symbol', 'report_type', 'report_period', 'item_name'])
                success += 1
                
        except Exception as e:
            print(f"   -> Bỏ qua Finance {symbol}: {e}")
            pass
            
        time.sleep(0.1) # Tránh bị rate limit
        
    print(f"\n✅ Hoàn thành đồng bộ Finance! Thành công: {success}/{len(symbols)}.")

def sync_trading(symbols):
    """Đồng bộ Giao dịch Nước ngoài, Tự doanh, Lệnh & Summary..."""
    print(f"\n[5] ĐANG ĐỒNG BỘ TRADING (KHỐI NGOẠI, TỰ DOANH, LỆNH, SUMMARY) {len(symbols)} MÃ...")
    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"[{idx+1}/{len(symbols)}] Đang tải Trading: {symbol}...")
        try:
            tr = Trading(symbol=symbol, source='VCI')
            df_foreign = tr.foreign_trade(start=START_DATE, end=TODAY)
            df_prop = tr.prop_trade(start=START_DATE, end=TODAY)
            
            # --- 1. Lưu trading history (Ngoại & Tự doanh) ---
            if len(df_foreign) > 0 or len(df_prop) > 0:
                alldates = set()
                if len(df_foreign) > 0:
                    alldates.update(df_foreign['trading_date'].tolist())
                if len(df_prop) > 0:
                    alldates.update(df_prop['trading_date'].tolist())
                
                fr_map = df_foreign.set_index('trading_date').to_dict('index') if len(df_foreign) > 0 else {}
                prop_map = df_prop.set_index('trading_date').to_dict('index') if len(df_prop) > 0 else {}
                
                records = []
                for d in alldates:
                    f_row = fr_map.get(d, {})
                    p_row = prop_map.get(d, {})
                    records.append({
                        'symbol': symbol,
                        'trading_date': d,
                        'fr_buy_volume': float(f_row.get('fr_buy_volume_matched', 0) or 0),
                        'fr_sell_volume': float(f_row.get('fr_sell_volume_matched', 0) or 0),
                        'prop_buy_volume': float(p_row.get('total_buy_trade_volume', 0) or 0),
                        'prop_sell_volume': float(p_row.get('total_sell_trade_volume', 0) or 0)
                    })
                
                df_clean = pd.DataFrame(records)
                db.upsert_dataframe(df_clean, 'trading', conflict_cols=['symbol', 'trading_date'])

            # --- 2. Thống kê đặt lệnh/hủy lệnh (CafeF) ---
            try:
                tr_cafe = Trading(symbol=symbol, source='CAFEF')
                df_order = tr_cafe.order_stats(start=START_DATE, end=TODAY)
                if len(df_order) > 0:
                    df_clean = df_order.copy()
                    if 'symbol' not in df_clean.columns: df_clean['symbol'] = symbol
                    # order_stats indices are usually dates
                    if 'trading_date' not in df_clean.columns:
                        if df_clean.index.name == 'date' or type(df_clean.index) == pd.DatetimeIndex:
                             df_clean['trading_date'] = df_clean.index.date
                    if 'trading_date' in df_clean.columns:
                        db.upsert_dataframe(df_clean, 'trading_order_stats', conflict_cols=['symbol', 'trading_date'])
            except Exception as e: pass
            
            # # --- 3. Summary (Tổng hợp nhanh) ---
            # try:
            #     df_sum = tr.summary(start=START_DATE, end=TODAY)
            #     if len(df_sum) > 0:
            #         df_clean = df_sum.copy()
            #         if 'symbol' not in df_clean.columns: df_clean['symbol'] = symbol
            #         db.upsert_dataframe(df_clean, 'trading_summary', conflict_cols=['symbol', 'trading_date'])
            # except Exception as e: pass
            
            # # --- 4. Order Book Depth (Bid/Ask) - Hiện tại ---
            # try:
            #     q = Quote(symbol=symbol, source='VCI') # Price depth requires Quote
            #     df_depth = q.price_depth()
            #     if len(df_depth) > 0:
            #         df_clean = df_depth.copy()
            #         df_clean['symbol'] = symbol
            #         # Prepare matching columns
            #         expected_cols = ['symbol', 'buy_price', 'buy_volume', 'sell_price', 'sell_volume']
            #         for col in expected_cols:
            #             if col not in df_clean.columns: df_clean[col] = None 
                    
            #         db.upsert_dataframe(df_clean, 'trading_price_depth', conflict_cols=['symbol', 'buy_price', 'sell_price'])
            # except Exception as e: pass

            # --- 3. Summary (Tổng hợp nhanh) ---
            # --- 3. Summary (Tổng hợp nhanh) ---
            try:
                df_sum = tr.summary(start=START_DATE, end=TODAY)
                if len(df_sum) > 0:
                    df_clean = df_sum.copy()
                    df_clean['symbol'] = symbol
                    
                    if 'trading_date' not in df_clean.columns: 
                        df_clean['trading_date'] = TODAY
                        
                    if 'total_volume' in df_clean.columns: df_clean['total_trading_vol'] = df_clean['total_volume']
                    if 'total_value' in df_clean.columns: df_clean['total_trading_val'] = df_clean['total_value']

                    expected_cols = ['symbol', 'trading_date', 'total_trading_vol', 'total_trading_val', 'open_price', 'highest_price', 'lowest_price', 'close_price']
                    
                    for col in expected_cols:
                        if col not in df_clean.columns: df_clean[col] = None
                        
                    df_clean = df_clean[expected_cols]
                    
                    # 🔥 FIX: ÉP KIỂU DỮ LIỆU CHO BẢNG SUMMARY
                    # Ép cột ngày tháng
                    df_clean['trading_date'] = pd.to_datetime(df_clean['trading_date']).dt.date
                    # Ép các cột số lượng, giá tiền về dạng số (nếu lỗi hoặc rỗng thì gán là NaN)
                    num_cols_sum = ['total_trading_vol', 'total_trading_val', 'open_price', 'highest_price', 'lowest_price', 'close_price']
                    for col in num_cols_sum:
                        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                        
                    db.upsert_dataframe(df_clean, 'trading_summary', conflict_cols=['symbol', 'trading_date'])
            except Exception as e: 
                pass
            
            # --- 4. Order Book Depth ---
            try:
                q = Quote(symbol=symbol, source='VCI')
                df_depth = q.price_depth()
                if len(df_depth) > 0:
                    df_clean = df_depth.copy()
                    df_clean['symbol'] = symbol
                    
                    expected_cols = ['symbol', 'buy_price', 'buy_volume', 'sell_price', 'sell_volume']
                    for col in expected_cols:
                        if col not in df_clean.columns: df_clean[col] = None 
                        
                    df_clean = df_clean[expected_cols] 
                    
                    # 🔥 FIX: ÉP KIỂU DỮ LIỆU CHO BẢNG PRICE DEPTH
                    # Ép tất cả các cột giá và khối lượng về dạng số
                    num_cols_depth = ['buy_price', 'buy_volume', 'sell_price', 'sell_volume']
                    for col in num_cols_depth:
                        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                        
                    db.upsert_dataframe(df_clean, 'trading_price_depth', conflict_cols=['symbol', 'buy_price', 'sell_price'])
            except Exception as e: 
                pass
            
            success += 1
            time.sleep(0.1)
        except Exception:
            pass
    print(f"\n✅ Hoàn thành đồng bộ Trading! Thành công: {success}/{len(symbols)}.")

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def get_done_symbols():
    try:
        # Dùng pd.read_sql để đọc thẳng dữ liệu từ Postgres ra DataFrame
        df = pd.read_sql("SELECT DISTINCT symbol FROM quote_history", db.engine)
        return set(df['symbol'].tolist())
    except Exception as e:
        print(f"⚠️ Chưa có bảng quote_history hoặc lỗi lấy lịch sử: {e}")
        return set() # Nếu chưa có bảng thì trả về set rỗng (coi như chưa chạy mã nào)

def main():
    # Cấu hình biến DEMO_MODE. Đặt là False để chạy cho toàn thị trường (>1600 mã)
    DEMO_MODE = False
    
    # Bước 1: Lấy danh sách mã
    symbols = sync_listing()
    
    if DEMO_MODE:
        symbols = ['TCB', 'FPT', 'VNM', 'SHB', 'ACB']
        print(f"\n[DEMO MODE] Chỉ chạy đồng bộ cho {len(symbols)} mã để thử nghiệm nhanh: {symbols}")
    
    # if symbols:
    #     # Bước 2: Đồng bộ Quote History
    #     sync_quotes(symbols)
    #     # Bước 3: Đồng bộ Company (Profile)
    #     sync_company(symbols)
    #     # Bước 4: Đồng bộ Finance (Ratio sinh lời / chỉ số)
    #     sync_finance(symbols)
    #     # Bước 5: Đồng bộ Trading (Khối ngoại, Tự doanh...)
    #     sync_trading(symbols)
        
    # print("\n✅ TOÀN BỘ QUÁ TRÌNH SYNC ĐÃ HOÀN TẤT!")
    # ===== RESUME (🔥 QUAN TRỌNG) =====
    done_symbols = get_done_symbols()
    symbols = [s for s in symbols if s not in done_symbols]

    print(f"\n📊 Tổng: {len(symbols) + len(done_symbols)}")
    print(f"✅ Đã có: {len(done_symbols)}")
    print(f"🚀 Còn crawl: {len(symbols)}")

    if not symbols:
        print("🎉 Không còn gì để crawl!")
        return

    # ===== BATCH =====
    BATCH_SIZE = 50
    total_batches = math.ceil(len(symbols) / BATCH_SIZE)

    print(f"\n🚀 Chạy {total_batches} batch...")

    for i, batch in enumerate(chunk_list(symbols, BATCH_SIZE)):
        print("\n" + "="*50)
        print(f"🚀 BATCH {i+1}/{total_batches}")
        print(f"📌 {len(batch)} mã | Ví dụ: {batch[:5]}")
        print("="*50)

        # ===== CHẠY TỪNG SYMBOL (AN TOÀN HƠN) =====
        for symbol in batch:
            try:
                sync_quotes([symbol])
                sync_company([symbol])
                sync_finance([symbol])
                sync_trading([symbol])

                print(f"✔ DONE {symbol}")

            except Exception as e:
                print(f"❌ Lỗi {symbol}: {e}")

            time.sleep(0.1)  # tránh spam API

        # ===== PROGRESS =====
        done_now = len(done_symbols) + (i+1)*BATCH_SIZE
        total = len(symbols) + len(done_symbols)
        percent = min(100, round(done_now * 100 / total, 2))

        print(f"\n📊 Progress: ~{percent}%")

        print("\n⏳ Nghỉ 5s...")
        time.sleep(5)

    print("\n🎉 DONE ALL!")
if __name__ == '__main__':
    main()

