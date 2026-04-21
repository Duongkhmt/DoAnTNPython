import sys
sys.path.append(r'venv\Lib\site-packages')
from timescale_utils import DatabaseManager
from sqlalchemy import text

def create_dashboard_views():
    db = DatabaseManager()
    if db.engine is None:
        print("Lỗi: Không kết nối được Database.")
        return

    print("Đang tạo các Views (bảng ảo) cho Dashboard...")

    try:
        with db.engine.connect() as conn:
            # 1. VIEW CHO BIỂU ĐỒ ĐỊNH GIÁ (Dashboard Valuation)
            print("  -> Creating dashboard_valuation view...")
            conn.execute(text("""
                CREATE OR REPLACE VIEW dashboard_valuation AS
                WITH latest_valuation AS (
                    SELECT symbol,
                           MAX(report_period) AS report_period,
                           MAX(value) FILTER (WHERE item_name = 'P/E') AS last_pe,
                           MAX(value) FILTER (WHERE item_name = 'P/B') AS last_pb
                    FROM finance
                    WHERE item_name IN ('P/E', 'P/B')
                    GROUP BY symbol
                ),
                recent_quote AS (
                    SELECT symbol, close
                    FROM (
                        SELECT symbol, close, row_number() OVER (PARTITION BY symbol ORDER BY trading_date DESC) as rn
                        FROM quote_history
                    ) t WHERE rn = 1
                )
                SELECT q.symbol,
                       q.trading_date,
                       q.close as close_price,
                       CASE WHEN rq.close > 0 AND lv.last_pe IS NOT NULL 
                            THEN CAST((q.close / rq.close) * lv.last_pe AS NUMERIC(10, 2)) 
                            ELSE NULL END AS pe,
                       CASE WHEN rq.close > 0 AND lv.last_pb IS NOT NULL 
                            THEN CAST((q.close / rq.close) * lv.last_pb AS NUMERIC(10, 2))
                            ELSE NULL END AS pb
                FROM quote_history q
                LEFT JOIN latest_valuation lv ON q.symbol = lv.symbol
                LEFT JOIN recent_quote rq ON q.symbol = rq.symbol
            """))

            # 2. VIEW CHO PHÂN TÍCH KHỚP LỆNH / ĐẶT LỆNH (Dashboard Order Stats)
            print("  -> Creating dashboard_order_stats view...")
            conn.execute(text("""
                CREATE OR REPLACE VIEW dashboard_order_stats AS
                WITH base AS (
                    SELECT 
                        q.symbol,
                        q.trading_date,
                        q.volume AS matched_volume,
                        COALESCE(tos.buy_volume, 0) AS buy_volume,
                        COALESCE(tos.sell_volume, 0) AS sell_volume,
                        tos.avg_buy_order_volume AS avg_buy_order,
                        tos.avg_sell_order_volume AS avg_sell_order,
                        t.fr_buy_volume AS foreign_buy_volume,
                        t.fr_sell_volume AS foreign_sell_volume
                    FROM quote_history q
                    LEFT JOIN trading_order_stats tos 
                        ON q.symbol = tos.symbol AND q.trading_date = tos.trading_date
                    LEFT JOIN trading t 
                        ON q.symbol = t.symbol AND q.trading_date = t.trading_date
                )
                SELECT 
                    symbol,
                    trading_date,
                    matched_volume,
                    buy_volume,
                    sell_volume,
                    avg_buy_order,
                    avg_sell_order,
                    CASE WHEN avg_buy_order > 0 THEN CAST(avg_sell_order / avg_buy_order AS NUMERIC(10, 2)) ELSE NULL END AS ratio_sell_buy_order,
                    
                    -- Tính toán KL khớp mua và bán (xấp xỉ theo tỉ lệ đặt mua/bán)
                    CASE WHEN (buy_volume + sell_volume) > 0 THEN
                        ROUND(matched_volume * (buy_volume::numeric / (buy_volume + sell_volume)))
                    ELSE 0 END AS matched_buy_volume,
                    
                    CASE WHEN (buy_volume + sell_volume) > 0 THEN
                        ROUND(matched_volume * (sell_volume::numeric / (buy_volume + sell_volume)))
                    ELSE 0 END AS matched_sell_volume,
                    
                    -- Khối lượng hủy = Đặt - Khớp
                    GREATEST(0, buy_volume - 
                        CASE WHEN (buy_volume + sell_volume) > 0 THEN
                            ROUND(matched_volume * (buy_volume::numeric / (buy_volume + sell_volume)))
                        ELSE 0 END
                    ) AS cancel_buy_volume,
                    
                    GREATEST(0, sell_volume - 
                        CASE WHEN (buy_volume + sell_volume) > 0 THEN
                            ROUND(matched_volume * (sell_volume::numeric / (buy_volume + sell_volume)))
                        ELSE 0 END
                    ) AS cancel_sell_volume,
                    
                    foreign_buy_volume,
                    foreign_sell_volume
                FROM base
            """))

            # 3. VIEW CHO THEO DÕI DÒNG TIỀN NGÀNH (Dashboard Industry Flow)
            print("  -> Creating dashboard_industry_flow view...")
            conn.execute(text("""
                CREATE OR REPLACE VIEW dashboard_industry_flow AS
                WITH stock_flow AS (
                    SELECT 
                        q.trading_date,
                        c.industry,
                        COALESCE(q.turnover, q.close * q.volume) AS turnover,
                        CASE WHEN (COALESCE(tos.buy_volume, 0) + COALESCE(tos.sell_volume, 0)) > 0 THEN
                            tos.buy_volume::numeric / (tos.buy_volume + tos.sell_volume)
                        ELSE 0.5 END AS buy_ratio
                    FROM quote_history q
                    LEFT JOIN company c ON q.symbol = c.symbol
                    LEFT JOIN trading_order_stats tos ON q.symbol = tos.symbol AND q.trading_date = tos.trading_date
                ),
                industry_agg AS (
                    SELECT 
                        trading_date,
                        industry,
                        SUM(turnover) AS val_total,
                        SUM(turnover * buy_ratio) AS val_buy,
                        SUM(turnover * (1 - buy_ratio)) AS val_sell
                    FROM stock_flow
                    GROUP BY trading_date, industry
                )
                SELECT 
                    trading_date,
                    SUM(val_total) AS market_total_val,

                    -- NGÂN HÀNG
                    SUM(val_total) FILTER (WHERE industry ILIKE '%Ngân hàng%') AS bank_total,
                    SUM(val_buy) FILTER (WHERE industry ILIKE '%Ngân hàng%') AS bank_buy,
                    SUM(val_sell) FILTER (WHERE industry ILIKE '%Ngân hàng%') AS bank_sell,
                    CASE WHEN SUM(val_total) > 0 THEN 
                        SUM(val_total) FILTER (WHERE industry ILIKE '%Ngân hàng%') / SUM(val_total) * 100 
                    ELSE 0 END AS bank_ratio_pct,

                    -- CHỨNG KHOÁN
                    SUM(val_total) FILTER (WHERE industry ILIKE '%Chứng khoán%' OR industry ILIKE '%tài chính%') AS sec_total,
                    SUM(val_buy) FILTER (WHERE industry ILIKE '%Chứng khoán%' OR industry ILIKE '%tài chính%') AS sec_buy,
                    SUM(val_sell) FILTER (WHERE industry ILIKE '%Chứng khoán%' OR industry ILIKE '%tài chính%') AS sec_sell,
                    CASE WHEN SUM(val_total) > 0 THEN 
                        SUM(val_total) FILTER (WHERE industry ILIKE '%Chứng khoán%' OR industry ILIKE '%tài chính%') / SUM(val_total) * 100 
                    ELSE 0 END AS sec_ratio_pct,

                    -- BẤT ĐỘNG SẢN
                    SUM(val_total) FILTER (WHERE industry ILIKE '%Bất động sản%') AS re_total,
                    SUM(val_buy) FILTER (WHERE industry ILIKE '%Bất động sản%') AS re_buy,
                    SUM(val_sell) FILTER (WHERE industry ILIKE '%Bất động sản%') AS re_sell,
                    CASE WHEN SUM(val_total) > 0 THEN 
                        SUM(val_total) FILTER (WHERE industry ILIKE '%Bất động sản%') / SUM(val_total) * 100 
                    ELSE 0 END AS re_ratio_pct,

                    -- THÉP
                    SUM(val_total) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Thép - Sản phẩm thép%') AS steel_total,
                    SUM(val_buy) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Thép - Sản phẩm thép%') AS steel_buy,
                    SUM(val_sell) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Thép - Sản phẩm thép%') AS steel_sell,
                    CASE WHEN SUM(val_total) > 0 THEN 
                        SUM(val_total) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Thép - Sản phẩm thép%') / SUM(val_total) * 100 
                    ELSE 0 END AS steel_ratio_pct

                FROM industry_agg
                GROUP BY trading_date
            """))

            if hasattr(conn, 'commit'):
                conn.commit()

        print("✅ Đã tạo thành công 3 Views cho Dashboard trong PostgreSQL!")

    except Exception as e:
        print(f"Lỗi tạo Views: {e}")

if __name__ == "__main__":
    create_dashboard_views()
