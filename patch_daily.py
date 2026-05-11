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
        ),
        target_symbols AS (
            SELECT t.symbol, t.trading_date
            FROM technical_indicators t
            JOIN latest_date d ON t.trading_date = d.trading_date
            LEFT JOIN listing l ON l.symbol = t.symbol
            WHERE t.volume_sma_20 > 100000 AND l.exchange IN ('HOSE', 'HNX')
        ),
        historical_quotes AS (
            SELECT 
                symbol, 
                trading_date, 
                close, 
                volume,
                LAG(close, 5) OVER (PARTITION BY symbol ORDER BY trading_date) AS close_lag_5,
                LAG(close, 10) OVER (PARTITION BY symbol ORDER BY trading_date) AS close_lag_10,
                LAG(close, 20) OVER (PARTITION BY symbol ORDER BY trading_date) AS close_lag_20,
                LAG(volume, 5) OVER (PARTITION BY symbol ORDER BY trading_date) AS vol_lag_5
            FROM quote_history
            WHERE symbol IN (SELECT symbol FROM target_symbols)
              AND trading_date >= (SELECT trading_date - INTERVAL '60 days' FROM latest_date)
        ),
        latest_historical AS (
            SELECT * FROM historical_quotes 
            WHERE trading_date = (SELECT trading_date FROM latest_date)
        )
        SELECT
            t.*,
            q.open, q.high, q.low, q.close, q.volume,
            l.exchange, l.industry, l.sector,
            
            -- Normalized Features
            (q.close - t.sma_20) / NULLIF(t.sma_20, 0) AS price_vs_sma20,
            (q.close - t.sma_50) / NULLIF(t.sma_50, 0) AS price_vs_sma50,
            (t.sma_20 - t.sma_50) / NULLIF(t.sma_50, 0) AS sma20_vs_sma50,
            
            t.macd / NULLIF(q.close + 1e-6, 0) AS macd_norm,
            t.macd_hist / NULLIF(q.close + 1e-6, 0) AS macd_hist_norm,
            t.atr_14 / NULLIF(q.close + 1e-6, 0) AS atr_pct,
            t.bb_width / NULLIF(t.sma_20 + 1e-6, 0) AS bb_width_norm,
            
            t.candle_body_size / NULLIF(q.open + 1e-6, 0) AS candle_body_pct,
            t.candle_upper_wick / NULLIF(q.open + 1e-6, 0) AS candle_upper_pct,
            t.candle_lower_wick / NULLIF(q.open + 1e-6, 0) AS candle_lower_pct,
            
            -- Momentum Features
            (lh.close - lh.close_lag_5) / NULLIF(lh.close_lag_5, 0) AS price_momentum_5,
            (lh.close - lh.close_lag_10) / NULLIF(lh.close_lag_10, 0) AS price_momentum_10,
            (lh.close - lh.close_lag_20) / NULLIF(lh.close_lag_20, 0) AS price_momentum_20,
            (lh.volume - lh.vol_lag_5) / NULLIF(lh.vol_lag_5, 0) AS volume_momentum_5
            
        FROM technical_indicators t
        JOIN target_symbols ts ON t.symbol = ts.symbol AND t.trading_date = ts.trading_date
        LEFT JOIN quote_history q ON q.symbol = t.symbol AND q.trading_date = t.trading_date
        LEFT JOIN latest_historical lh ON lh.symbol = t.symbol
        LEFT JOIN listing l ON l.symbol = t.symbol
        ORDER BY t.symbol
        \"\"\"
    )
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)
        
    # Clip extreme values for momentum and normalized features
    momentum_cols = [
        'price_momentum_5', 'price_momentum_10', 'price_momentum_20', 'volume_momentum_5',
        'price_vs_sma20', 'price_vs_sma50', 'sma20_vs_sma50',
        'macd_norm', 'macd_hist_norm', 'atr_pct', 'bb_width_norm',
        'candle_body_pct', 'candle_upper_pct', 'candle_lower_pct'
    ]
    for col in momentum_cols:
        if col in df.columns:
            df[col] = df[col].astype(float).clip(lower=-0.5, upper=0.5)
            
    return df"""

replace = """def fetch_latest_indicator_frame(engine: Any) -> pd.DataFrame:
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
    )
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)
            
    return df"""

content = content.replace(target, replace)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied to daily_predict.py")
