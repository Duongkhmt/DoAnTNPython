# explain_model.py
import pickle, json
import pandas as pd
import numpy as np
from sqlalchemy import text
from timescale_utils import DatabaseManager

model    = pickle.load(open("xgb_direction_5d.pkl", "rb"))
features = json.load(open("features.json"))
if isinstance(features, dict):
    features = features["features"]

db  = DatabaseManager()
eng = db.engine

# Lấy data của PLX và MCH
sql = text("""
SELECT t.symbol, t.trading_date, t.rsi_14, t.macd, 
       t.volume_ratio, t.bb_pct_b, t.adx_14,
       t.stoch_k, t.williams_r, t.sma_10, t.sma_20, t.sma_50
FROM technical_indicators t
WHERE t.symbol IN ('PLX', 'MCH')
  AND t.trading_date = (SELECT MAX(trading_date) FROM technical_indicators)
""")
df_show = pd.read_sql(sql, eng)
print("Chỉ báo hiện tại:")
print(df_show.to_string())

# Lấy đủ features để predict
sql2 = text("""
SELECT t.*
FROM technical_indicators t
WHERE t.symbol IN ('PLX', 'MCH')
  AND t.trading_date = (SELECT MAX(trading_date) FROM technical_indicators)
""")
df = pd.read_sql(sql2, eng)
available = [f for f in features if f in df.columns]
df_clean  = df.dropna(subset=available)

scores = model.predict_proba(df_clean[available])[:, 1]
df_clean = df_clean.copy()
df_clean['ai_score'] = scores

print("\nAI Score:")
print(df_clean[['symbol', 'ai_score']].to_string())

# Feature importance — tại sao model cho điểm cao
import xgboost as xgb
fi = pd.DataFrame({
    'feature': available,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\nTop 10 feature model dựa vào nhiều nhất:")
print(fi.head(10).to_string(index=False))

# Giá trị của top features với PLX và MCH
top_features = fi.head(10)['feature'].tolist()
print("\nGiá trị top features của PLX và MCH:")
print(df_clean[['symbol'] + top_features].to_string())