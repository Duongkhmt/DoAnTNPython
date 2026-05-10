import sys
sys.path.append(r'venv\Lib\site-packages')

from timescale_utils import DatabaseManager
from sqlalchemy import text


def create_dashboard_views():
    db = DatabaseManager()
    if db.engine is None:
        print("❌ Không kết nối được DB")
        return False

    print("🚀 Creating optimized views (MV + wrapper)...")

    try:
        with db.engine.connect() as conn:

            # =========================================================
            # 1. DASHBOARD VALUATION
            # =========================================================
            print("  -> dashboard_valuation")

            conn.execute(text("DROP VIEW IF EXISTS dashboard_valuation CASCADE"))
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS dashboard_valuation_mv CASCADE"))

            conn.execute(text("""
                CREATE MATERIALIZED VIEW dashboard_valuation_mv AS
                WITH latest_valuation AS (
                    SELECT symbol,
                           MAX(value) FILTER (WHERE item_name = 'P/E') AS last_pe,
                           MAX(value) FILTER (WHERE item_name = 'P/B') AS last_pb
                    FROM finance
                    WHERE item_name IN ('P/E', 'P/B')
                    GROUP BY symbol
                ),
                recent_quote AS (
                    SELECT DISTINCT ON (symbol)
                        symbol, close
                    FROM quote_history
                    ORDER BY symbol, trading_date DESC
                )
                SELECT 
                    q.symbol,
                    q.trading_date,
                    q.close AS close_price,

                    CASE 
                        WHEN rq.close > 0 AND lv.last_pe IS NOT NULL
                        THEN ROUND(((q.close / rq.close) * lv.last_pe)::numeric, 2)
                    END AS pe,

                    CASE 
                        WHEN rq.close > 0 AND lv.last_pb IS NOT NULL
                        THEN ROUND(((q.close / rq.close) * lv.last_pb)::numeric, 2)
                    END AS pb

                FROM quote_history q
                LEFT JOIN latest_valuation lv ON q.symbol = lv.symbol
                LEFT JOIN recent_quote rq ON q.symbol = rq.symbol
            """))

            conn.execute(text("""
                CREATE INDEX idx_val_symbol_date
                ON dashboard_valuation_mv(symbol, trading_date DESC)
            """))

            conn.execute(text("""
                CREATE VIEW dashboard_valuation AS
                SELECT * FROM dashboard_valuation_mv
            """))

            # =========================================================
            # 2. DASHBOARD ORDER STATS
            # =========================================================
            print("  -> dashboard_order_stats")

            conn.execute(text("DROP VIEW IF EXISTS dashboard_order_stats CASCADE"))
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS dashboard_order_stats_mv CASCADE"))

            conn.execute(text("""
                CREATE MATERIALIZED VIEW dashboard_order_stats_mv AS
                SELECT 
                    q.symbol,
                    q.trading_date,
                    q.volume AS matched_volume,

                    COALESCE(tos.buy_volume, 0) AS buy_volume,
                    COALESCE(tos.sell_volume, 0) AS sell_volume,

                    tos.avg_buy_order_volume AS avg_buy_order,
                    tos.avg_sell_order_volume AS avg_sell_order,

                    CASE 
                        WHEN tos.avg_buy_order_volume > 0 
                        THEN ROUND((tos.avg_sell_order_volume / tos.avg_buy_order_volume)::numeric, 2)
                    END AS ratio_sell_buy_order,

                    CASE 
                        WHEN (COALESCE(tos.buy_volume,0)+COALESCE(tos.sell_volume,0)) > 0
                        THEN ROUND((q.volume * tos.buy_volume::numeric / (tos.buy_volume + tos.sell_volume))::numeric,0)
                        ELSE 0
                    END AS matched_buy_volume,

                    CASE 
                        WHEN (COALESCE(tos.buy_volume,0)+COALESCE(tos.sell_volume,0)) > 0
                        THEN ROUND((q.volume * tos.sell_volume::numeric / (tos.buy_volume + tos.sell_volume))::numeric,0)
                        ELSE 0
                    END AS matched_sell_volume,

                    t.fr_buy_volume AS foreign_buy_volume,
                    t.fr_sell_volume AS foreign_sell_volume,
                    
                    0 AS cancel_buy_volume,
                    0 AS cancel_sell_volume

                FROM quote_history q
                LEFT JOIN trading_order_stats tos 
                    ON q.symbol = tos.symbol AND q.trading_date = tos.trading_date
                LEFT JOIN trading t 
                    ON q.symbol = t.symbol AND q.trading_date = t.trading_date
            """))

            conn.execute(text("""
                CREATE INDEX idx_dos_symbol_date
                ON dashboard_order_stats_mv(symbol, trading_date DESC)
            """))

            conn.execute(text("""
                CREATE VIEW dashboard_order_stats AS
                SELECT * FROM dashboard_order_stats_mv
            """))

            # =========================================================
            # 3. DASHBOARD INDUSTRY FLOW
            # =========================================================
            print("  -> dashboard_industry_flow")

            conn.execute(text("DROP VIEW IF EXISTS dashboard_industry_flow CASCADE"))
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS dashboard_industry_flow_mv CASCADE"))

            conn.execute(text("""
                CREATE MATERIALIZED VIEW dashboard_industry_flow_mv AS
                WITH stock_flow AS (
                    SELECT 
                        q.trading_date,
                        c.industry,
                        COALESCE(q.turnover, q.close * q.volume) AS turnover,
                        CASE 
                            WHEN (COALESCE(tos.buy_volume,0)+COALESCE(tos.sell_volume,0)) > 0
                            THEN tos.buy_volume::numeric / (tos.buy_volume + tos.sell_volume)
                            ELSE 0.5
                        END AS buy_ratio
                    FROM quote_history q
                    LEFT JOIN company c ON q.symbol = c.symbol
                    LEFT JOIN trading_order_stats tos 
                        ON q.symbol = tos.symbol AND q.trading_date = tos.trading_date
                )
                SELECT 
                    trading_date,
                    SUM(turnover) AS market_total_val,

                    SUM(turnover) FILTER (WHERE industry ILIKE '%Ngân hàng%') AS bank_total,
                    SUM(turnover * buy_ratio) FILTER (WHERE industry ILIKE '%Ngân hàng%') AS bank_buy,
                    SUM(turnover * (1-buy_ratio)) FILTER (WHERE industry ILIKE '%Ngân hàng%') AS bank_sell,

                    SUM(turnover) FILTER (WHERE industry ILIKE '%Dịch vụ tài chính%' OR industry ILIKE '%chứng khoán%') AS sec_total,
                    SUM(turnover * buy_ratio) FILTER (WHERE industry ILIKE '%Dịch vụ tài chính%' OR industry ILIKE '%chứng khoán%') AS sec_buy,
                    SUM(turnover * (1-buy_ratio)) FILTER (WHERE industry ILIKE '%Dịch vụ tài chính%' OR industry ILIKE '%chứng khoán%') AS sec_sell,

                    SUM(turnover) FILTER (WHERE industry ILIKE '%Bất động sản%') AS re_total,
                    SUM(turnover * buy_ratio) FILTER (WHERE industry ILIKE '%Bất động sản%') AS re_buy,
                    SUM(turnover * (1-buy_ratio)) FILTER (WHERE industry ILIKE '%Bất động sản%') AS re_sell,

                    SUM(turnover) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Tài nguyên cơ bản%') AS steel_total,
                    SUM(turnover * buy_ratio) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Tài nguyên cơ bản%') AS steel_buy,
                    SUM(turnover * (1-buy_ratio)) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Tài nguyên cơ bản%') AS steel_sell,

                    ROUND((SUM(turnover) FILTER (WHERE industry ILIKE '%Ngân hàng%') / NULLIF(SUM(turnover), 0) * 100)::numeric, 2) AS bank_ratio_pct,
                    ROUND((SUM(turnover) FILTER (WHERE industry ILIKE '%Dịch vụ tài chính%' OR industry ILIKE '%chứng khoán%') / NULLIF(SUM(turnover), 0) * 100)::numeric, 2) AS sec_ratio_pct,
                    ROUND((SUM(turnover) FILTER (WHERE industry ILIKE '%Bất động sản%') / NULLIF(SUM(turnover), 0) * 100)::numeric, 2) AS re_ratio_pct,
                    ROUND((SUM(turnover) FILTER (WHERE industry ILIKE '%Thép%' OR industry ILIKE '%Tài nguyên cơ bản%') / NULLIF(SUM(turnover), 0) * 100)::numeric, 2) AS steel_ratio_pct

                FROM stock_flow
                GROUP BY trading_date
            """))

            conn.execute(text("""
                CREATE INDEX idx_industry_date
                ON dashboard_industry_flow_mv(trading_date DESC)
            """))

            conn.execute(text("""
                CREATE VIEW dashboard_industry_flow AS
                SELECT * FROM dashboard_industry_flow_mv
            """))

            conn.commit()

        print("✅ CREATE SUCCESS")
        return True

    except Exception as e:
        print("❌ ERROR:", e)
        return False


# =========================================================
# REFRESH
# =========================================================
def refresh_views():
    db = DatabaseManager()
    if db.engine is None:
        return

    print("🔄 Refreshing materialized views...")

    try:
        with db.engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW dashboard_valuation_mv"))
            conn.execute(text("REFRESH MATERIALIZED VIEW dashboard_order_stats_mv"))
            conn.execute(text("REFRESH MATERIALIZED VIEW dashboard_industry_flow_mv"))
            conn.commit()

        print("✅ REFRESH DONE")

    except Exception as e:
        print("❌ REFRESH ERROR:", e)


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    ok = create_dashboard_views()

    if ok:
        refresh_views()
    else:
        print("⚠️ Skip refresh vì create lỗi")