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

def train_ensemble(df, features, target):
    # Sort by date for time-series split
    df = df.sort_values('trading_date').dropna(subset=features + [target])
    
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
    print("Please run this on Colab or locally with the full dataset.")
    # Example usage:
    # df = pd.read_csv("all_stocks_train.csv")
    # features, target = load_feature_config()
    # train_ensemble(df, features, target)
