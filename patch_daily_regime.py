import os

filepath = r"e:\DoAnPython\POSTGRESQL_GUIDE\daily_predict.py"
with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

target = """def fetch_latest_indicator_frame(engine: Any) -> pd.DataFrame:
    query = text(
        \"\"\"
        WITH latest_date AS (
            SELECT MAX(trading_date) AS trading_date
            FROM technical_indicators
        )
        SELECT
            t.*,
            q.open, q.high, q.low, q.close, q.volume,
            l.exchange, l.industry, l.sector
        FROM technical_indicators t
        JOIN latest_date d ON t.trading_date = d.trading_date
        LEFT JOIN quote_history q ON q.symbol = t.symbol AND q.trading_date = t.trading_date
        LEFT JOIN listing l ON l.symbol = t.symbol
        WHERE t.volume_sma_20 > 100000 
          AND l.exchange IN ('HOSE', 'HNX')
        ORDER BY t.symbol
        \"\"\"
    )"""

replace = """def fetch_latest_indicator_frame(engine: Any) -> pd.DataFrame:
    query = text(
        \"\"\"
        WITH latest_date AS (
            SELECT MAX(trading_date) AS trading_date
            FROM technical_indicators
        ),
        vnindex_data AS (
            SELECT 
                price_momentum_5 AS vnindex_momentum_5,
                price_momentum_20 AS vnindex_momentum_20,
                rsi_14 AS vnindex_rsi
            FROM technical_indicators
            WHERE symbol = 'VNINDEX' 
              AND trading_date = (SELECT trading_date FROM latest_date)
        )
        SELECT
            t.*,
            q.open, q.high, q.low, q.close, q.volume,
            l.exchange, l.industry, l.sector,
            
            v.vnindex_momentum_5,
            v.vnindex_momentum_20,
            v.vnindex_rsi,
            
            (t.price_momentum_20 * v.vnindex_momentum_20) AS regime_momentum,
            (t.price_vs_sma20 * v.vnindex_momentum_5) AS regime_sma,
            (t.rsi_14 - v.vnindex_rsi) AS rsi_vs_vnindex
            
        FROM technical_indicators t
        CROSS JOIN vnindex_data v
        JOIN latest_date d ON t.trading_date = d.trading_date
        LEFT JOIN quote_history q ON q.symbol = t.symbol AND q.trading_date = t.trading_date
        LEFT JOIN listing l ON l.symbol = t.symbol
        WHERE t.volume_sma_20 > 100000 
          AND l.exchange IN ('HOSE', 'HNX')
        ORDER BY t.symbol
        \"\"\"
    )"""

content = content.replace(target, replace)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied to daily_predict.py for Regime Features")
