import json
import os
import pickle
import sys
import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.metrics import roc_auc_score, classification_report, accuracy_score, confusion_matrix
from timescale_utils import DatabaseManager

# Configure standard output to use UTF-8 to prevent encoding errors on Windows terminal
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Import LightGBM
try:
    import lightgbm as lgb
except ImportError:
    print("[ERROR] Vui long cai dat lightgbm: pip install lightgbm")
    sys.exit(1)

def load_features_config(config_path="vnindex_features.json"):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Khong tim thay cau hinh features tai: {config_path}")
    with open(config_path, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    return config["features"], config.get("target", "direction_5d")

def fetch_vnindex_data():
    print("[INFO] Dang lay du lieu VNINDEX tu database...")
    db = DatabaseManager()
    engine = db.engine
    if engine is None:
        raise RuntimeError("Khong the ket noi den co so du lieu TimescaleDB.")
    
    query = text("""
        SELECT *
        FROM technical_indicators
        WHERE symbol = 'VNINDEX'
        ORDER BY trading_date
    """)
    with engine.begin() as conn:
        df = pd.read_sql(query, conn)
    
    print(f"[INFO] Da tai {len(df)} dong du lieu VNINDEX tu database.")
    return df


def preprocess_and_split(df, features, target_col):
    df = df.copy()
    df['trading_date'] = pd.to_datetime(df['trading_date'])
    df = df.sort_values('trading_date').reset_index(drop=True)
    
    # Tính toán lại target sạch sẽ để đảm bảo độ chính xác
    # Nhãn target thực tế: direction_5d = 1 nếu return_5d > 0, ngược lại 0
    # direction_5d trong DB có sẵn hoặc ta tự tính lại
    df['target'] = (df['return_5d'] > 0).astype(int)
    
    # Loại bỏ các dòng cuối cùng chưa có kết quả (return_5d bị NaN)
    df = df.dropna(subset=['return_5d'])
    
    # Loại bỏ các dòng bị khuyết đặc trưng
    df = df.dropna(subset=features)
    
    if len(df) < 100:
        raise ValueError(f"Du lieu VNINDEX qua it de huan luyen (chi co {len(df)} dong sau tien xu ly).")
        
    print(f"[INFO] So dong du lieu sau tien xu ly (loai bo NaN): {len(df)}")
    
    # Phân chia dữ liệu theo chuỗi thời gian (Chronological Time-Series Split 80/20)
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]
    val_df = df.iloc[split_idx:]
    
    print(f"[INFO] Khoang thoi gian tap Train: {train_df['trading_date'].min().strftime('%Y-%m-%d')} -> {train_df['trading_date'].max().strftime('%Y-%m-%d')} ({len(train_df)} dong)")
    print(f"[INFO] Khoang thoi gian tap Val  : {val_df['trading_date'].min().strftime('%Y-%m-%d')} -> {val_df['trading_date'].max().strftime('%Y-%m-%d')} ({len(val_df)} dong)")
    
    X_train = train_df[features]
    y_train = train_df['target']
    X_val = val_df[features]
    y_val = val_df['target']
    
    return X_train, y_train, X_val, y_val

def train_and_evaluate():
    features, target_col = load_features_config()
    df = fetch_vnindex_data()
    X_train, y_train, X_val, y_val = preprocess_and_split(df, features, target_col)
    
    # Phân phối nhãn trong tập Train
    train_counts = y_train.value_counts()
    print(f"[INFO] Phan phoi nhan tap Train: Giam/Di ngang={train_counts.get(0, 0)} ({train_counts.get(0, 0)/len(y_train)*100:.1f}%), Tang={train_counts.get(1, 0)} ({train_counts.get(1, 0)/len(y_train)*100:.1f}%)")
    
    # Huấn luyện LGBMClassifier sử dụng class_weight='balanced'
    print("[INFO] Dang huan luyen LightGBM Classifier...")
    model = lgb.LGBMClassifier(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=5,
        num_leaves=31,
        class_weight='balanced',
        random_state=42,
        verbosity=-1
    )
    
    model.fit(X_train, y_train)
    
    # Dự đoán trên tập Validation
    y_val_pred = model.predict(X_val)
    y_val_proba = model.predict_proba(X_val)[:, 1]
    
    # Đánh giá các chỉ số quan trọng
    auc = roc_auc_score(y_val, y_val_proba)
    acc = accuracy_score(y_val, y_val_pred)
    cm = confusion_matrix(y_val, y_val_pred)
    
    print("\n" + "=" * 50)
    print("KET QUA DANH GIA MO HINH VNINDEX TREND")
    print("=" * 50)
    print(f"ROC-AUC Score  : {auc:.4f} (Chi so danh gia chinh)")
    print(f"Accuracy Score : {acc:.4f}")
    print("\nMa tran nham lan (Confusion Matrix):")
    print(f"   Du doan GIAM  Du doan TANG")
    print(f"Thuc te GIAM:  {cm[0][0]:<12} {cm[0][1]}")
    print(f"Thuc te TĂNG:  {cm[1][0]:<12} {cm[1][1]}")
    
    print("\nBao cao phan loai chi tiet (Classification Report):")
    print(classification_report(y_val, y_val_pred, target_names=["GIAM/NEUTRAL", "TANG"]))
    print("=" * 50)
    
    # Lưu mô hình vào file vnindex_model.pkl
    model_path = "vnindex_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"[SUCCESS] Mo hinh da duoc luu thanh cong tai {model_path}")

if __name__ == "__main__":
    train_and_evaluate()
