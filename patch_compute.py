import os

filepath = r"e:\DoAnPython\POSTGRESQL_GUIDE\compute_indicators.py"
with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

target1 = """    -- Target labels (ML)
    direction_5d    SMALLINT,   -- 1 tăng / 0 giảm sau 5 phiên"""
replace1 = """    -- Normalized Features
    price_vs_sma20      FLOAT,
    price_vs_sma50      FLOAT,
    sma20_vs_sma50      FLOAT,
    price_momentum_5    FLOAT,
    price_momentum_10   FLOAT,
    price_momentum_20   FLOAT,
    volume_momentum_5   FLOAT,
    macd_norm           FLOAT,
    macd_hist_norm      FLOAT,
    atr_pct             FLOAT,
    bb_width_norm       FLOAT,
    candle_body_pct     FLOAT,
    candle_upper_pct    FLOAT,
    candle_lower_pct    FLOAT,

    -- Target labels (ML)
    direction_5d    SMALLINT,   -- 1 tăng / 0 giảm sau 5 phiên"""

target2 = """"is_morning_star", "is_evening_star",
    "direction_5d", "direction_10d", "return_5d","""
replace2 = """"is_morning_star", "is_evening_star",
    "price_vs_sma20", "price_vs_sma50", "sma20_vs_sma50",
    "price_momentum_5", "price_momentum_10", "price_momentum_20",
    "volume_momentum_5", "macd_norm", "macd_hist_norm", "atr_pct", "bb_width_norm",
    "candle_body_pct", "candle_upper_pct", "candle_lower_pct",
    "direction_5d", "direction_10d", "return_5d","""

target3 = """df = compute_candles(df)\n    df = compute_target(df)"""
replace3 = """df = compute_candles(df)\n    df = compute_normalized_features(df)\n    df = compute_target(df)"""

content = content.replace(target1, replace1)
content = content.replace(target2, replace2)
content = content.replace(target3, replace3)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied")
