"""
PostgreSQL Utilities - Nâng cấp để hỗ trợ nhiều field và UPSERT
Tham khảo: sqlalchemy + psycopg2
"""

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import uuid

class PostgreSQLManager:
    """Quản lý kết nối và lưu dữ liệu vào PostgreSQL (Upsert support)"""
    
    def __init__(self, host='localhost', port=5432, database='vnstock', 
                 user='postgress', password='postgress'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}?client_encoding=utf8"
        try:
            self.engine = create_engine(self.connection_string, echo=False, 
                                       connect_args={'client_encoding': 'utf8'})
            print(f"✓ Kết nối PostgreSQL thành công: {database}")
        except Exception as e:
            print(f"✗ Lỗi kết nối PostgreSQL: {e}")
            self.engine = None
    
    def test_connection(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            return False

    def save_dataframe(self, df, table_name, if_exists='append', index=False):
        """Lưu truyền thống"""
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=index)
            print(f"✓ Lưu {len(df)} dòng vào '{table_name}'")
            return True
        except Exception as e:
            print(f"✗ Lỗi lưu dữ liệu: {e}")
            return False
            
    def upsert_dataframe(self, df, table_name, conflict_cols):
        """Lưu theo cơ chế UPSERT thông qua Temp Table"""
        if len(df) == 0: return True
        temp_table = f"{table_name}_temp_{uuid.uuid4().hex[:8]}"
        try:
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            columns = ", ".join(df.columns)
            updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in conflict_cols])
            
            if updates:
                sql = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ({", ".join(conflict_cols)}) 
                DO UPDATE SET {updates};
                """
            else:
                sql = f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ({", ".join(conflict_cols)}) 
                DO NOTHING;
                """
                
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                conn.commit()
            print(f"✓ UPSERT {len(df)} dòng vào '{table_name}' (Conflict keys: {conflict_cols})")
            return True
        except Exception as e:
            print(f"✗ Lỗi upsert '{table_name}': {e}")
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
                    conn.commit()
            except:
                pass
            return False
    
    def create_table_listing(self):
        sql = """
        CREATE TABLE IF NOT EXISTS listing (
            symbol VARCHAR(20) PRIMARY KEY,
            organ_name VARCHAR(1000),
            exchange VARCHAR(50),
            industry VARCHAR(255),
            sector VARCHAR(255),
            company_type VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_exchange ON listing(exchange);
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            print("✓ Table 'listing' created")
        except Exception as e:
            print(f"✗ Lỗi tạo table listing: {e}")
    
    def create_table_quote_history(self):
        sql = """
        CREATE TABLE IF NOT EXISTS quote_history (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            trading_date DATE NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            turnover FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, trading_date)
            -- Bỏ FOREIGN KEY cứng để tránh phụ thuộc nếu symbol chưa được insert
        );
        CREATE INDEX IF NOT EXISTS idx_symbol_date ON quote_history(symbol, trading_date);
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            print("✓ Table 'quote_history' created")
        except Exception as e:
            print(f"✗ Lỗi tạo table quote_history: {e}")
            
    def create_table_finance(self):
        sql = """
        CREATE TABLE IF NOT EXISTS finance (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            report_type VARCHAR(50),
            report_period VARCHAR(20),
            item_name VARCHAR(255),
            value FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, report_type, report_period, item_name)
        );
        CREATE INDEX IF NOT EXISTS idx_finance_symbol ON finance(symbol);
        CREATE INDEX IF NOT EXISTS idx_finance_period ON finance(report_period);
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            print("✓ Table 'finance' created")
        except Exception as e: pass

    def create_table_company(self):
        sql = """
        CREATE TABLE IF NOT EXISTS company (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) UNIQUE NOT NULL,
            name VARCHAR(255),
            sector VARCHAR(100),
            industry VARCHAR(100),
            exchange VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except: pass

    def create_table_trading(self):
        sql = """
        CREATE TABLE IF NOT EXISTS trading (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            trading_date DATE NOT NULL,
            matched_volume BIGINT,
            fr_buy_volume FLOAT,
            fr_sell_volume FLOAT,
            prop_buy_volume FLOAT,
            prop_sell_volume FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, trading_date)
        );
        """
        try:
             with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except: pass

    def create_table_intraday(self):
        sql = """
        CREATE TABLE IF NOT EXISTS intraday (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            time TIMESTAMP NOT NULL,
            price FLOAT,
            volume BIGINT,
            match_type VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_intraday_symbol_time ON intraday(symbol, time);
        """
        try:
             with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
        except: pass
        
    def create_table_trading_extras(self):
        sqls = [
            """
            CREATE TABLE IF NOT EXISTS trading_order_stats (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                trading_date DATE NOT NULL,
                buy_orders BIGINT,
                sell_orders BIGINT,
                buy_volume BIGINT,
                sell_volume BIGINT,
                avg_buy_order_volume FLOAT,
                avg_sell_order_volume FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, trading_date)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS trading_price_depth (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                buy_price FLOAT,
                buy_volume BIGINT,
                sell_price FLOAT,
                sell_volume BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, buy_price, sell_price)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS trading_summary (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                trading_date DATE NOT NULL,
                total_trading_vol BIGINT,
                total_trading_val FLOAT,
                open_price FLOAT,
                highest_price FLOAT,
                lowest_price FLOAT,
                close_price FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, trading_date)
            );
            """
        ]
        for sql in sqls:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(sql))
                    conn.commit()
            except: pass
            
            
    def create_all_tables(self):
        print("\n📊 TẠO CÁC TABLE POSTGRESQL")
        self.create_table_listing()
        self.create_table_quote_history()
        self.create_table_trading()
        self.create_table_company()
        self.create_table_finance()
        self.create_table_intraday()
        self.create_table_trading_extras()
        print("\n✅ Tất cả table đã được tạo/upgrade")

# Helper functions can be adapted here optionally, but now DB wrapper directly supports UPSERT.
