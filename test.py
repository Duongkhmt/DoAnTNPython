import sys

# Thêm đường dẫn thư viện ảo (venv) trên Windows được móc vào Docker
docker_venv_path = "/opt/airflow/dags/venv/Lib/site-packages"
if docker_venv_path not in sys.path:
    sys.path.append(docker_venv_path)

import pandas as pd
from vnstock_data import Listing, Quote, Finance, Company, Trading
from timescale_utils import DatabaseManager   # ← thay postgres_utils
import math
import time
from datetime import datetime, timedelta

print("=" * 80)
print("MASTER SYNC CHỨNG KHOÁN (VNSTOCK DATA)")
print("=" * 80)

# Khởi tạo DatabaseManager duy nhất — lo trọn metadata + time-series
db = DatabaseManager(database='vnstock_ts')
db.create_all_tables()

if '--daily' in sys.argv:
    START_DATE = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    print(f"[*] CHẾ ĐỘ ĐỒNG BỘ HẰNG NGÀY: {START_DATE} đến nay")
else:
    START_DATE = '2024-01-01'
    print(f"[*] CHẾ ĐỘ ĐỒNG BỘ LỊCH SỬ: {START_DATE} đến nay")

TODAY = datetime.now().strftime('%Y-%m-%d')


# ======================================================================
# [1] LISTING
# ======================================================================
def sync_listing():
    """Đồng bộ danh sách toàn bộ các mã chứng khoán"""
    print("\n[1] ĐANG ĐỒNG BỘ DANH SÁCH MÃ CHỨNG KHOÁN (LISTING)...")

    lst = Listing(source='VND')
    df_listing = lst.all_symbols()

    if 'type' in df_listing.columns:
        df_listing = df_listing[df_listing['type'].isin(['STOCK', 'Cổ phiếu', 'CP'])]

    if len(df_listing) == 0:
        print("Không kéo được danh sách mã. Thoát!")
        return []

    df_clean = pd.DataFrame()
    df_clean['symbol']       = df_listing['symbol']
    df_clean['organ_name']   = df_listing.get('company_name', None)
    df_clean['exchange']     = df_listing.get('exchange', None)
    df_clean['company_type'] = df_listing.get('type', None)
    df_clean['industry']     = None
    df_clean['sector']       = None

    db.upsert_dataframe(df_clean, 'listing', conflict_cols=['symbol'])
    return df_clean['symbol'].tolist()


# ======================================================================
# [2] QUOTE HISTORY
# ======================================================================
def sync_quotes(symbols):
    """Đồng bộ lịch sử giá (Quote) cho danh sách mã"""
    print(f"\n[2] ĐANG ĐỒNG BỘ LỊCH SỬ GIÁ QUOTE CHO {len(symbols)} MÃ...")
    print(f"    Từ {START_DATE} đến {TODAY}")

    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"  [{idx+1}/{len(symbols)}] Quote: {symbol}")
        try:
            q = Quote(symbol=symbol, source='VND')
            df = q.history(start=START_DATE, end=TODAY, interval='1D')

            if len(df) > 0:
                df_clean = df.copy()
                if 'time' in df_clean.columns and 'trading_date' not in df_clean.columns:
                    df_clean = df_clean.rename(columns={'time': 'trading_date'})
                if 'symbol' not in df_clean.columns:
                    df_clean['symbol'] = symbol

                expected_cols = ['symbol', 'trading_date', 'open', 'high', 'low', 'close', 'volume']
                for col in expected_cols:
                    if col not in df_clean.columns:
                        df_clean[col] = None
                df_clean = df_clean[expected_cols]

                # Đảm bảo trading_date là kiểu DATE — tránh lệch timezone
                df_clean['trading_date'] = pd.to_datetime(df_clean['trading_date']).dt.date

                db.upsert_dataframe(df_clean, 'quote_history',
                                    conflict_cols=['symbol', 'trading_date'])
                success += 1

            time.sleep(0.1)
        except Exception:
            pass

    print(f"\n  ✅ Quote xong! Thành công: {success}/{len(symbols)}.")


# ======================================================================
# [3] COMPANY
# ======================================================================
def sync_company(symbols):
    """Đồng bộ hồ sơ công ty (overview)"""
    print(f"\n[3] ĐANG ĐỒNG BỘ COMPANY CHO {len(symbols)} MÃ...")

    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"  [{idx+1}/{len(symbols)}] Company: {symbol}")
        try:
            c = Company(symbol=symbol, source='VCI')
            df = c.overview()

            if len(df) > 0:
                df_clean = pd.DataFrame()
                df_clean['symbol']   = [symbol]
                df_clean['industry'] = df.get('icb_name2', None)
                df_clean['sector']   = df.get('icb_name3', None)

                db.upsert_dataframe(df_clean, 'company', conflict_cols=['symbol'])
                success += 1

        except Exception as e:
            print(f"    -> Bỏ qua Company {symbol}: {e}")

        time.sleep(0.1)

    print(f"\n  ✅ Company xong! Thành công: {success}/{len(symbols)}.")


# ======================================================================
# [4] FINANCE
# ======================================================================
def sync_finance(symbols):
    """Đồng bộ chỉ số tài chính (ratio) — melt từ wide sang long"""
    print(f"\n[4] ĐANG ĐỒNG BỘ FINANCE CHO {len(symbols)} MÃ...")

    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"  [{idx+1}/{len(symbols)}] Finance: {symbol}")
        try:
            f = Finance(symbol=symbol, source='VCI')
            df = f.ratio()

            if len(df) > 0:
                df = df.reset_index()
                period_col = df.columns[0]

                cols_to_drop = [
                    period_col, 'report_period', 'Ratio TTM Id',
                    'Ratio Type', 'Ratio Year Id', 'ticker', 'symbol'
                ]
                val_cols = [c for c in df.columns if c not in cols_to_drop]

                df_melted = pd.melt(
                    df,
                    id_vars=[period_col],
                    value_vars=val_cols,
                    var_name='item_name',
                    value_name='value'
                )
                df_melted = df_melted.rename(columns={period_col: 'report_period'})
                df_melted['symbol']      = symbol
                df_melted['report_type'] = 'ratio'
                df_melted['value']       = pd.to_numeric(df_melted['value'], errors='coerce')
                df_melted = df_melted.dropna(subset=['value'])

                df_clean = df_melted[['symbol', 'report_type', 'report_period',
                                      'item_name', 'value']]
                df_clean = df_clean.drop_duplicates(
                    subset=['symbol', 'report_type', 'report_period', 'item_name'],
                    keep='last'
                )

                db.upsert_dataframe(df_clean, 'finance',
                                    conflict_cols=['symbol', 'report_type',
                                                   'report_period', 'item_name'])
                success += 1

        except Exception as e:
            print(f"    -> Bỏ qua Finance {symbol}: {e}")

        time.sleep(0.1)

    print(f"\n  ✅ Finance xong! Thành công: {success}/{len(symbols)}.")


# ======================================================================
# [5] TRADING
# ======================================================================
def sync_trading(symbols):
    """Đồng bộ giao dịch: khối ngoại, tự doanh, lệnh, summary"""
    print(f"\n[5] ĐANG ĐỒNG BỘ TRADING CHO {len(symbols)} MÃ...")

    success = 0
    for idx, symbol in enumerate(symbols):
        print(f"  [{idx+1}/{len(symbols)}] Trading: {symbol}")
        try:
            tr = Trading(symbol=symbol, source='VCI')
            df_foreign = tr.foreign_trade(start=START_DATE, end=TODAY)
            df_prop    = tr.prop_trade(start=START_DATE, end=TODAY)

            # --- 5a. Khối ngoại + Tự doanh → bảng trading ---
            if len(df_foreign) > 0 or len(df_prop) > 0:
                alldates = set()
                if len(df_foreign) > 0:
                    alldates.update(df_foreign['trading_date'].tolist())
                if len(df_prop) > 0:
                    alldates.update(df_prop['trading_date'].tolist())

                fr_map   = df_foreign.set_index('trading_date').to_dict('index') if len(df_foreign) > 0 else {}
                prop_map = df_prop.set_index('trading_date').to_dict('index')    if len(df_prop)    > 0 else {}

                records = []
                for d in alldates:
                    f_row = fr_map.get(d, {})
                    p_row = prop_map.get(d, {})
                    records.append({
                        'symbol':           symbol,
                        'trading_date':     d,
                        'fr_buy_volume':    float(f_row.get('fr_buy_volume_matched',   0) or 0),
                        'fr_sell_volume':   float(f_row.get('fr_sell_volume_matched',  0) or 0),
                        'prop_buy_volume':  float(p_row.get('total_buy_trade_volume',  0) or 0),
                        'prop_sell_volume': float(p_row.get('total_sell_trade_volume', 0) or 0),
                    })

                df_clean = pd.DataFrame(records)
                df_clean['trading_date'] = pd.to_datetime(df_clean['trading_date']).dt.date
                db.upsert_dataframe(df_clean, 'trading',
                                    conflict_cols=['symbol', 'trading_date'])

            # --- 5b. Thống kê lệnh (CAFEF) → bảng trading_order_stats ---
            try:
                tr_cafe  = Trading(symbol=symbol, source='CAFEF')
                df_order = tr_cafe.order_stats(start=START_DATE, end=TODAY)
                if len(df_order) > 0:
                    df_clean = df_order.copy()
                    if 'symbol' not in df_clean.columns:
                        df_clean['symbol'] = symbol
                    if 'trading_date' not in df_clean.columns:
                        if isinstance(df_clean.index, pd.DatetimeIndex) \
                                or df_clean.index.name == 'date':
                            df_clean['trading_date'] = df_clean.index.date
                    if 'trading_date' in df_clean.columns:
                        df_clean['trading_date'] = pd.to_datetime(
                            df_clean['trading_date']).dt.date
                        db.upsert_dataframe(df_clean, 'trading_order_stats',
                                            conflict_cols=['symbol', 'trading_date'])
            except Exception:
                pass

            # --- 5c. Summary → bảng trading_summary ---
            try:
                df_sum = tr.summary(start=START_DATE, end=TODAY)
                if len(df_sum) > 0:
                    df_clean = df_sum.copy()
                    df_clean['symbol'] = symbol

                    if 'trading_date' not in df_clean.columns:
                        df_clean['trading_date'] = TODAY
                    if 'total_volume' in df_clean.columns:
                        df_clean['total_trading_vol'] = df_clean['total_volume']
                    if 'total_value' in df_clean.columns:
                        df_clean['total_trading_val'] = df_clean['total_value']

                    expected_cols = [
                        'symbol', 'trading_date',
                        'total_trading_vol', 'total_trading_val',
                        'open_price', 'highest_price', 'lowest_price', 'close_price'
                    ]
                    for col in expected_cols:
                        if col not in df_clean.columns:
                            df_clean[col] = None
                    df_clean = df_clean[expected_cols]

                    df_clean['trading_date'] = pd.to_datetime(
                        df_clean['trading_date']).dt.date
                    for col in ['total_trading_vol', 'total_trading_val',
                                'open_price', 'highest_price', 'lowest_price', 'close_price']:
                        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

                    db.upsert_dataframe(df_clean, 'trading_summary',
                                        conflict_cols=['symbol', 'trading_date'])
            except Exception:
                pass

            success += 1
            time.sleep(0.1)

        except Exception:
            pass

    print(f"\n  ✅ Trading xong! Thành công: {success}/{len(symbols)}.")


# ======================================================================
# HELPERS
# ======================================================================
def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def get_done_symbols():
    """
    Lấy danh sách mã đã có dữ liệu ĐỦ MỚI trong quote_history để resume crawl.

    Điều kiện "đã xong" = mã có ít nhất 1 bản ghi trading_date >= START_DATE.
    Nếu chỉ dùng DISTINCT symbol (không có WHERE), mã cào hôm qua nhưng
    chưa có dữ liệu hôm nay vẫn bị coi là "done" và bị bỏ qua sai.
    """
    try:
        sql = """
        SELECT DISTINCT symbol
        FROM quote_history
        WHERE trading_date = CURRENT_DATE
        """
        df = pd.read_sql(sql, db.engine)
        return set(df['symbol'].tolist())
    except Exception as e:
        print(f"⚠️  Lỗi get_done_symbols: {e}")
        return set()


# ======================================================================
# MAIN
# ======================================================================
def main():
    DEMO_MODE = False

    # Bước 1: Lấy danh sách mã
    symbols = sync_listing()

    if DEMO_MODE:
        symbols = ['TCB', 'FPT', 'VNM', 'SHB', 'ACB']
        print(f"\n[DEMO] Chạy thử {len(symbols)} mã: {symbols}")

    # Resume — bỏ qua mã đã có quote_history
    done_symbols = get_done_symbols()
    symbols = [s for s in symbols if s not in done_symbols]

    print(f"\n📊 Tổng mã: {len(symbols) + len(done_symbols)}")
    print(f"  ✅ Đã có : {len(done_symbols)}")
    print(f"  🚀 Còn crawl: {len(symbols)}")

    if not symbols:
        print("\n🎉 Không còn gì để crawl!")

        # Backfill Continuous Aggregates nếu chạy với flag --backfill
        if '--backfill' in sys.argv:
            db.refresh_historical_aggregates(start=START_DATE)
        return

    # Batch loop
    BATCH_SIZE   = 50
    total_batches = math.ceil(len(symbols) / BATCH_SIZE)
    print(f"\n🚀 Bắt đầu {total_batches} batch (mỗi batch {BATCH_SIZE} mã)...")

    for i, batch in enumerate(chunk_list(symbols, BATCH_SIZE)):
        print("\n" + "=" * 55)
        print(f"  BATCH {i+1}/{total_batches} — {len(batch)} mã | Ví dụ: {batch[:5]}")
        print("=" * 55)

        for symbol in batch:
            try:
                sync_quotes([symbol])
                sync_company([symbol])
                sync_finance([symbol])
                sync_trading([symbol])
                print(f"  ✔ DONE {symbol}")
            except Exception as e:
                print(f"  ❌ Lỗi {symbol}: {e}")
            time.sleep(0.1)

        done_now = len(done_symbols) + (i + 1) * BATCH_SIZE
        total    = len(symbols) + len(done_symbols)
        percent  = min(100, round(done_now * 100 / total, 2))
        print(f"\n  📊 Tiến độ: ~{percent}%")
        print("  ⏳ Nghỉ 5s...")
        time.sleep(5)

    print("\n🎉 DONE ALL!")

    # Sau khi cào xong lần đầu, backfill aggregate
    print("\n⏳ Backfill Continuous Aggregates...")
    db.refresh_historical_aggregates(start=START_DATE)


if __name__ == '__main__':
    main()