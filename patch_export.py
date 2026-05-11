import os

filepath = r"e:\DoAnPython\POSTGRESQL_GUIDE\export_data.py"
with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

target = """    t.is_morning_star,
    t.is_evening_star,
    t.direction_5d,"""

replace = """    t.is_morning_star,
    t.is_evening_star,
    t.price_vs_sma20,
    t.price_vs_sma50,
    t.sma20_vs_sma50,
    t.price_momentum_5,
    t.price_momentum_10,
    t.price_momentum_20,
    t.volume_momentum_5,
    t.macd_norm,
    t.macd_hist_norm,
    t.atr_pct,
    t.bb_width_norm,
    t.candle_body_pct,
    t.candle_upper_pct,
    t.candle_lower_pct,
    t.direction_5d,"""

content = content.replace(target, replace)

with open(filepath, "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied to export_data.py")
