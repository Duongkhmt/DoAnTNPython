"""
DatabaseManager — Trái tim duy nhất quản lý toàn bộ DB (thay thế postgres_utils.py)
Kết nối một TimescaleDB instance duy nhất, lo trọn gói:
  • Metadata  : listing, company, finance
  • Time-series: quote_history, trading, trading_summary, trading_order_stats
  • Aggregates : quote_weekly, quote_monthly (Continuous Aggregate)

FIXES so với phiên bản cũ:
  [1] Bỏ ::timestamptz trong Continuous Aggregates — tránh lệch ngày do timezone UTC
  [2] Dùng PRIMARY KEY thay UNIQUE — chuẩn schema, tương thích ORM
  [3] Thêm hàm refresh_historical_aggregates() — chạy 1 lần sau khi cào đủ lịch sử
  [4] Gộp metadata tables (listing, company, finance) vào 1 file duy nhất
"""

from sqlalchemy import create_engine, text
import pandas as pd
import uuid


class DatabaseManager:
    """
    Quản lý toàn bộ DB: metadata (listing/company/finance) + time-series (quote/trading).
    Thay thế hoàn toàn postgres_utils.py — chỉ cần 1 instance duy nhất trong master_sync.py.

    Ví dụ dùng:
        db = DatabaseManager()
        db.create_all_tables()
        db.upsert_dataframe(df, 'listing', ['symbol'])
        db.upsert_dataframe(df, 'quote_history', ['symbol', 'trading_date'])
    """

    def __init__(self, host=None, port=5433, database='vnstock_ts',
                 user='postgres', password='postgres'):
        import os
        if host is None:
            # Tự động nhảy sang localhost nếu đang chạy trên Windows host
            host = 'host.docker.internal' if os.path.exists('/.dockerenv') else 'localhost'

        self.connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
            f"?client_encoding=utf8"
        )
        try:
            self.engine = create_engine(
                self.connection_string, echo=False,
                connect_args={'client_encoding': 'utf8'}
            )
            with self.engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                if hasattr(conn, 'commit'): conn.commit()
            print(f"✓ Kết nối Database thành công: {database}@{host}:{port}")
        except Exception as e:
            print(f"✗ Lỗi kết nối TimescaleDB: {e}")
            self.engine = None

    # ------------------------------------------------------------------ #
    #  INTERNAL HELPER                                                     #
    # ------------------------------------------------------------------ #
    def _exec(self, sql: str, label: str):
        """Thực thi một khối SQL nhiều lệnh, mỗi lệnh cách nhau bởi dấu ';'"""
        try:
            with self.engine.connect() as conn:
                for stmt in sql.strip().split(';'):
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(text(stmt))
                if hasattr(conn, 'commit'): conn.commit()
            print(f"  ✓ {label}")
        except Exception as e:
            print(f"  ✗ {label}: {e}")

    # ------------------------------------------------------------------ #
    #  UPSERT qua Temp Table                                               #
    # ------------------------------------------------------------------ #
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str,
                         conflict_cols: list) -> bool:
        if len(df) == 0:
            return True
        temp = f"{table_name}_tmp_{uuid.uuid4().hex[:8]}"
        try:
            df.to_sql(temp, self.engine, if_exists='replace', index=False)
            columns = ", ".join(df.columns)
            update_cols = [c for c in df.columns if c not in conflict_cols]

            if update_cols:
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
                sql = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp}
                ON CONFLICT ({", ".join(conflict_cols)})
                DO UPDATE SET {updates};
                """
            else:
                sql = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp}
                ON CONFLICT ({", ".join(conflict_cols)}) DO NOTHING;
                """
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {temp}"))
                if hasattr(conn, 'commit'): conn.commit()
            print(f"  ✓ UPSERT {len(df)} dòng → '{table_name}'")
            return True
        except Exception as e:
            print(f"  ✗ Lỗi upsert '{table_name}': {e}")
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp}"))
                    if hasattr(conn, 'commit'): conn.commit()
            except Exception:
                pass
            return False

    # ------------------------------------------------------------------ #
    #  BẢNG METADATA                                                       #
    # ------------------------------------------------------------------ #
    def create_table_listing(self):
        print("\n[listing]")
        self._exec("""
        CREATE TABLE IF NOT EXISTS listing (
            symbol       VARCHAR(20)   PRIMARY KEY,
            organ_name   VARCHAR(1000),
            exchange     VARCHAR(50),
            industry     VARCHAR(255),
            sector       VARCHAR(255),
            company_type VARCHAR(100),
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """, "Table listing")
        # Index tra cứu theo sàn (HOSE / HNX / UPCOM)
        self._exec(
            "CREATE INDEX IF NOT EXISTS idx_listing_exchange ON listing(exchange)",
            "Index listing.exchange"
        )

    def create_table_company(self):
        print("\n[company]")
        self._exec("""
        CREATE TABLE IF NOT EXISTS company (
            symbol     VARCHAR(20)  PRIMARY KEY,
            name       VARCHAR(255),
            sector     VARCHAR(100),
            industry   VARCHAR(100),
            exchange   VARCHAR(20),
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """, "Table company")

    def create_table_finance(self):
        print("\n[finance]")
        self._exec("""
        CREATE TABLE IF NOT EXISTS finance (
            symbol        VARCHAR(20)  NOT NULL,
            report_type   VARCHAR(50),
            report_period VARCHAR(20),
            item_name     VARCHAR(255),
            value         FLOAT,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (symbol, report_type, report_period, item_name)
        )
        """, "Table finance")
        # Index thường dùng khi filter theo kỳ báo cáo
        self._exec(
            "CREATE INDEX IF NOT EXISTS idx_finance_period ON finance(report_period)",
            "Index finance.report_period"
        )

    # ------------------------------------------------------------------ #
    #  TẠO CÁC HYPERTABLE                                                 #
    # ------------------------------------------------------------------ #
    def create_table_quote_history(self):
        print("\n[quote_history]")

        # FIX [2]: PRIMARY KEY thay vì UNIQUE
        # Timescale yêu cầu partition key (trading_date) phải nằm trong PK
        self._exec("""
        CREATE TABLE IF NOT EXISTS quote_history (
            symbol       VARCHAR(20) NOT NULL,
            trading_date DATE        NOT NULL,
            open         FLOAT,
            high         FLOAT,
            low          FLOAT,
            close        FLOAT,
            volume       BIGINT,
            turnover     FLOAT,
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (symbol, trading_date)
        )
        """, "Table quote_history")

        self._exec("""
        SELECT create_hypertable(
            'quote_history', 'trading_date',
            if_not_exists => TRUE,
            migrate_data   => TRUE
        )
        """, "Hypertable quote_history")

        self._exec("""
        ALTER TABLE quote_history SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol',
            timescaledb.compress_orderby   = 'trading_date DESC'
        )
        """, "Compression options")

        self._exec("""
        SELECT add_compression_policy(
            'quote_history',
            INTERVAL '7 days',
            if_not_exists => TRUE
        )
        """, "Compression policy (7 days)")

    def create_table_trading(self):
        print("\n[trading]")

        self._exec("""
        CREATE TABLE IF NOT EXISTS trading (
            symbol           VARCHAR(20) NOT NULL,
            trading_date     DATE        NOT NULL,
            fr_buy_volume    FLOAT,
            fr_sell_volume   FLOAT,
            prop_buy_volume  FLOAT,
            prop_sell_volume FLOAT,
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (symbol, trading_date)
        )
        """, "Table trading")

        self._exec("""
        SELECT create_hypertable(
            'trading', 'trading_date',
            if_not_exists => TRUE,
            migrate_data   => TRUE
        )
        """, "Hypertable trading")

        self._exec("""
        ALTER TABLE trading SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol',
            timescaledb.compress_orderby   = 'trading_date DESC'
        )
        """, "Compression options")

        self._exec("""
        SELECT add_compression_policy(
            'trading',
            INTERVAL '7 days',
            if_not_exists => TRUE
        )
        """, "Compression policy (7 days)")

    def create_table_trading_summary(self):
        print("\n[trading_summary]")

        self._exec("""
        CREATE TABLE IF NOT EXISTS trading_summary (
            symbol            VARCHAR(20) NOT NULL,
            trading_date      DATE        NOT NULL,
            total_trading_vol BIGINT,
            total_trading_val FLOAT,
            open_price        FLOAT,
            highest_price     FLOAT,
            lowest_price      FLOAT,
            close_price       FLOAT,
            created_at        TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (symbol, trading_date)
        )
        """, "Table trading_summary")

        self._exec("""
        SELECT create_hypertable(
            'trading_summary', 'trading_date',
            if_not_exists => TRUE,
            migrate_data   => TRUE
        )
        """, "Hypertable trading_summary")

    def create_table_order_stats(self):
        print("\n[trading_order_stats]")

        self._exec("""
        CREATE TABLE IF NOT EXISTS trading_order_stats (
            symbol                VARCHAR(20) NOT NULL,
            trading_date          DATE        NOT NULL,
            buy_orders            BIGINT,
            sell_orders           BIGINT,
            buy_volume            BIGINT,
            sell_volume           BIGINT,
            avg_buy_order_volume  FLOAT,
            avg_sell_order_volume FLOAT,
            created_at            TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (symbol, trading_date)
        )
        """, "Table trading_order_stats")

        self._exec("""
        SELECT create_hypertable(
            'trading_order_stats', 'trading_date',
            if_not_exists => TRUE,
            migrate_data   => TRUE
        )
        """, "Hypertable trading_order_stats")

    def create_table_trading_price_depth(self):
        print("\n[trading_price_depth]")

        self._exec("""
        CREATE TABLE IF NOT EXISTS trading_price_depth (
            symbol        VARCHAR(20) NOT NULL,
            trading_date  DATE        NOT NULL,
            buy_price     FLOAT       NOT NULL,
            sell_price    FLOAT       NOT NULL,
            buy_volume    BIGINT,
            sell_volume   BIGINT,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (symbol, trading_date, buy_price, sell_price)
        )
        """, "Table trading_price_depth")

        self._exec("""
        SELECT create_hypertable(
            'trading_price_depth', 'trading_date',
            if_not_exists => TRUE,
            migrate_data   => TRUE
        )
        """, "Hypertable trading_price_depth")
    # ------------------------------------------------------------------ #
    #  CONTINUOUS AGGREGATES                                               #
    # ------------------------------------------------------------------ #
    def create_continuous_aggregates(self):
        """
        Tạo Continuous Aggregate view OHLCV theo tuần và tháng.

        FIX [1]: Bỏ ::timestamptz — dùng trực tiếp kiểu DATE để tránh
        timezone shift UTC vs Asia/Ho_Chi_Minh gây lệch ngày.
        TimescaleDB >= 2.x hỗ trợ time_bucket() trên DATE natively.

        LƯU Ý: Cả hai view được tạo với WITH NO DATA để tránh block DB
        khi có lịch sử lớn. Sau khi cào xong toàn bộ dữ liệu, chạy:
            tsdb.refresh_historical_aggregates()
        đúng 1 lần để backfill toàn bộ lịch sử.
        """
        print("\n[continuous aggregates]")

        # -- Weekly --
        self._exec("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS quote_weekly
        WITH (timescaledb.continuous) AS
        SELECT
            symbol,
            time_bucket('1 week'::INTERVAL, trading_date) AS week,
            first(open,  trading_date)                     AS open,
            max(high)                                       AS high,
            min(low)                                        AS low,
            last(close,  trading_date)                     AS close,
            sum(volume)                                     AS volume
        FROM quote_history
        GROUP BY symbol, week
        WITH NO DATA
        """, "View quote_weekly (WITH NO DATA)")

        self._exec("""
        SELECT add_continuous_aggregate_policy(
            'quote_weekly',
            start_offset      => INTERVAL '3 months',
            end_offset        => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 day',
            if_not_exists     => TRUE
        )
        """, "Policy quote_weekly")

        # -- Monthly --
        self._exec("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS quote_monthly
        WITH (timescaledb.continuous) AS
        SELECT
            symbol,
            time_bucket('1 month'::INTERVAL, trading_date) AS month,
            first(open,  trading_date)                      AS open,
            max(high)                                        AS high,
            min(low)                                         AS low,
            last(close,  trading_date)                      AS close,
            sum(volume)                                      AS volume
        FROM quote_history
        GROUP BY symbol, month
        WITH NO DATA
        """, "View quote_monthly (WITH NO DATA)")

        self._exec("""
        SELECT add_continuous_aggregate_policy(
            'quote_monthly',
            start_offset      => INTERVAL '10 years',
            end_offset        => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 day',
            if_not_exists     => TRUE
        )
        """, "Policy quote_monthly")

    # ------------------------------------------------------------------ #
    #  BACKFILL — chạy đúng 1 lần sau khi cào xong toàn bộ lịch sử      #
    # ------------------------------------------------------------------ #
    def refresh_historical_aggregates(self, start: str = '2000-01-01',
                                      end: str = None):
        """
        Backfill toàn bộ lịch sử cho quote_weekly và quote_monthly.

        Gọi hàm này ĐÚNG 1 LẦN sau khi master_sync.py đã cào xong
        toàn bộ dữ liệu vào quote_history.

        Tương đương với lệnh thủ công trong DBeaver/pgAdmin:
            CALL refresh_continuous_aggregate('quote_weekly',  '2000-01-01', NULL);
            CALL refresh_continuous_aggregate('quote_monthly', '2000-01-01', NULL);

        Args:
            start: Ngày bắt đầu backfill, mặc định '2000-01-01'
            end:   Ngày kết thúc, mặc định NULL (= đến hôm nay)
        """
        end_val = f"'{end}'" if end else "NULL"
        print(f"\n⏳ Đang backfill Continuous Aggregates từ {start} đến {end_val}...")
        print("   (Có thể mất vài phút nếu có nhiều năm dữ liệu)\n")

        self._exec(
            f"CALL refresh_continuous_aggregate('quote_weekly', '{start}', {end_val})",
            "Backfill quote_weekly hoàn tất"
        )
        self._exec(
            f"CALL refresh_continuous_aggregate('quote_monthly', '{start}', {end_val})",
            "Backfill quote_monthly hoàn tất"
        )
        print("\n✅ Backfill xong! Từ nay policy sẽ tự cập nhật hằng ngày.")

    # ------------------------------------------------------------------ #
    #  RETENTION POLICY                                                    #
    # ------------------------------------------------------------------ #
    def create_retention_policies(self):
        print("\n[retention policies]")
        self._exec("""
        SELECT add_retention_policy(
            'quote_history', INTERVAL '5 years', if_not_exists => TRUE
        )
        """, "Retention quote_history (5 năm)")

        self._exec("""
        SELECT add_retention_policy(
            'trading', INTERVAL '5 years', if_not_exists => TRUE
        )
        """, "Retention trading (5 năm)")

    # ------------------------------------------------------------------ #
    #  SETUP TỔNG                                                          #
    # ------------------------------------------------------------------ #
    def create_all_tables(self):
        print("\n" + "=" * 55)
        print("  DATABASE SETUP — TimescaleDB (all-in-one)")
        print("=" * 55)

        print("\n── METADATA ──────────────────────────────────────────")
        self.create_table_listing()
        self.create_table_company()
        self.create_table_finance()

        print("\n── TIME-SERIES (HYPERTABLE) ──────────────────────────")
        self.create_table_quote_history()
        self.create_table_trading()
        self.create_table_trading_summary()
        self.create_table_order_stats()

        print("\n── CONTINUOUS AGGREGATES ─────────────────────────────")
        self.create_continuous_aggregates()

        print("\n── RETENTION POLICIES ────────────────────────────────")
        self.create_retention_policies()

        print("\n" + "=" * 55)
        print("✅ Setup hoàn tất — tất cả 7 bảng sẵn sàng")
        print()
        print("⚠️  SAU KHI CÀO XONG LỊCH SỬ, chạy 1 lần duy nhất:")
        print("   db.refresh_historical_aggregates(start='2024-01-01')")
        print("=" * 55 + "\n")

    # ------------------------------------------------------------------ #
    #  QUERY HELPERS                                                       #
    # ------------------------------------------------------------------ #
    def query_ohlcv(self, symbol: str, start: str, end: str,
                    bucket: str = '1D') -> pd.DataFrame:
        """
        Lấy OHLCV theo bucket: '1D' | '1W' | '1M'

        '1W' và '1M' đọc từ Continuous Aggregate (cực nhanh, không cần
        GROUP BY lúc runtime). '1D' đọc thẳng từ quote_history.

        Ví dụ:
            df = tsdb.query_ohlcv('TCB', '2024-01-01', '2024-12-31', '1W')
        """
        if bucket == '1W':
            sql = """
            SELECT symbol, week AS period, open, high, low, close, volume
            FROM quote_weekly
            WHERE symbol = :symbol
              AND week BETWEEN :start AND :end
            ORDER BY period
            """
        elif bucket == '1M':
            sql = """
            SELECT symbol, month AS period, open, high, low, close, volume
            FROM quote_monthly
            WHERE symbol = :symbol
              AND month BETWEEN :start AND :end
            ORDER BY period
            """
        else:
            sql = """
            SELECT symbol, trading_date AS period, open, high, low, close, volume
            FROM quote_history
            WHERE symbol = :symbol
              AND trading_date BETWEEN :start AND :end
            ORDER BY period
            """
        with self.engine.connect() as conn:
            return pd.read_sql(
                text(sql), conn,
                params={'symbol': symbol, 'start': start, 'end': end}
            )

    def get_compression_stats(self) -> pd.DataFrame:
        """Xem tỉ lệ nén của từng hypertable"""
        sql = """
        SELECT
            hypertable_name,
            pg_size_pretty(before_compression_total_bytes) AS before,
            pg_size_pretty(after_compression_total_bytes)  AS after,
            ROUND(
                (1 - after_compression_total_bytes::FLOAT
                       / NULLIF(before_compression_total_bytes, 0)
                ) * 100, 1
            ) AS saved_pct
        FROM chunk_compression_stats(NULL)
        GROUP BY hypertable_name, before_compression_total_bytes,
                 after_compression_total_bytes
        ORDER BY hypertable_name
        """
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn)