import pandas as pd
import numpy as np
import json
import lightgbm as lgb
import xgboost as xgb
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
import pickle

def load_feature_config(features_path="lgbm_alpha_5d_features.json"):
    with open(features_path, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    return config["features"], config["target"]

def preprocess_data(df, features):
    print("Preprocessing raw data...")
    df['trading_date'] = pd.to_datetime(df['trading_date'])
    df = df.drop_duplicates(subset=['symbol', 'trading_date'], keep='last')
    df = df.sort_values(['symbol', 'trading_date']).reset_index(drop=True)

    # Compute VNINDEX indicators
    vnindex = (
        df[df['symbol'] == 'VNINDEX'][['trading_date', 'close', 'return_5d']]
        .drop_duplicates(subset=['trading_date'], keep='last')
        .sort_values('trading_date')
        .reset_index(drop=True)
    )
    vnindex['vnindex_momentum_5'] = vnindex['close'].pct_change(5).clip(-0.3, 0.3)
    vnindex['vnindex_momentum_20'] = vnindex['close'].pct_change(20).clip(-0.3, 0.3)
    vnindex_diff = vnindex['close'].diff()
    gain = vnindex_diff.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-vnindex_diff.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    vnindex['vnindex_rsi'] = 100 - (100 / (1 + rs))
    vnindex = vnindex.rename(columns={'return_5d': 'vnindex_return_5d'})

    # Preprocess stock rows
    stocks = df[df['symbol'] != 'VNINDEX'].copy()
    stocks = stocks[stocks['exchange'].isin(['HOSE', 'HNX'])]
    stocks = stocks[stocks['volume_sma_20'] > 100000]
    stocks = stocks[stocks['trading_date'] >= '2021-01-01']

    # Merge
    stocks = stocks.merge(vnindex, on='trading_date', how='left', validate='many_to_one')
    stocks['alpha_5d'] = stocks['return_5d'] - stocks['vnindex_return_5d']

    # Compute relative features
    stocks['rel_momentum_5'] = (stocks['price_momentum_5'] - stocks['vnindex_momentum_5']).clip(-0.5, 0.5)
    stocks['rel_momentum_20'] = (stocks['price_momentum_20'] - stocks['vnindex_momentum_20']).clip(-0.5, 0.5)
    stocks['rsi_vs_vnindex'] = (stocks['rsi_14'] - stocks['vnindex_rsi']).clip(-100, 100)

    # Compute target
    stocks = stocks.dropna(subset=['alpha_5d'])
    stocks['alpha_rank_pct'] = stocks.groupby('trading_date')['alpha_5d'].rank(pct=True, method='first')

    # Handle NaNs for features
    stocks[features] = stocks.groupby('symbol')[features].ffill()
    stocks = stocks.dropna(subset=features)
    
    print(f"Preprocessing complete. Row count: {len(stocks):,}")
    return stocks

def train_ensemble(df, features, target):
    # Preprocess to calculate relative features and rank target
    df = preprocess_data(df, features)
    
    # Sort by date for time-series split
    df = df.sort_values('trading_date')
    
    # We will do a simple train/val split for the meta-learner
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    val_df = df.iloc[split_idx:]
    
    X_train, y_train = train_df[features], train_df[target]
    X_val, y_val = val_df[features], val_df[target]
    
    print("Training LightGBM...")
    lgb_model = lgb.LGBMRegressor(n_estimators=100, random_state=42)
    lgb_model.fit(X_train, y_train)
    
    print("Training XGBoost...")
    xgb_model = xgb.XGBRegressor(n_estimators=100, random_state=42)
    xgb_model.fit(X_train, y_train)
    
    # Meta-learner (Ridge)
    print("Training Meta-Learner (Ridge)...")
    lgb_val_preds = lgb_model.predict(X_val)
    xgb_val_preds = xgb_model.predict(X_val)
    
    meta_X = np.column_stack((lgb_val_preds, xgb_val_preds))
    meta_model = Ridge(alpha=1.0)
    meta_model.fit(meta_X, y_val)
    
    print(f"Meta-learner weights: LGBM={meta_model.coef_[0]:.4f}, XGB={meta_model.coef_[1]:.4f}")
    
    # Save ensemble
    with open("ensemble_model.pkl", "wb") as f:
        pickle.dump({
            "lgbm": lgb_model,
            "xgb": xgb_model,
            "meta": meta_model
        }, f)
    
    print("Ensemble model saved to ensemble_model.pkl")

if __name__ == "__main__":
    print("Starting Stacking Ensemble training locally...")
    df = pd.read_csv("all_stocks_train.csv")
    features, target = load_feature_config()
    train_ensemble(df, features, target)

