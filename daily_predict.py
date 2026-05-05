import argparse
import json
import os
import pickle
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from create_ml_table import create_predictions_table
from timescale_utils import DatabaseManager


DEFAULT_MODEL_PATH = "xgb_direction_5d.pkl"
DEFAULT_FEATURES_PATH = "features.json"
DEFAULT_MODEL_VERSION = "xgb_direction_5d_v1"


def load_model(model_path: str):
    with open(model_path, "rb") as fh:
        return pickle.load(fh)


def load_features(features_path: str) -> list[str]:
    with open(features_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        features = data.get("features", [])
    else:
        features = data
    if not isinstance(features, list) or not features:
        raise ValueError("features.json khong hop le hoac khong co danh sach feature.")
    return features


def get_db_engine():
    db = DatabaseManager()
    if db.engine is None:
        raise RuntimeError("Khong the ket noi TimescaleDB/PostgreSQL.")
    return db.engine


def compute_target_date(predict_date: pd.Timestamp, horizon_days: int = 5) -> pd.Timestamp:
    current = pd.Timestamp(predict_date)
    remaining = horizon_days
    while remaining > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def classify_signal(score: float) -> str:
    if score >= 0.70:
        return "MUA_MANH"
    if score >= 0.60:
        return "MUA"
    if score <= 0.30:
        return "BAN_MANH"
    if score <= 0.40:
        return "BAN"
    return "TRUNG_TINH"


def fetch_latest_indicator_frame(engine: Any) -> pd.DataFrame:
    query = text(
        """
        WITH latest_date AS (
            SELECT MAX(trading_date) AS trading_date
            FROM technical_indicators
        )
        SELECT
            t.symbol,
            t.trading_date,
            l.exchange,
            l.industry,
            l.sector,
            t.sma_10,
            t.sma_20,
            t.sma_50,
            t.ema_12,
            t.ema_26,
            t.macd,
            t.macd_signal,
            t.macd_hist,
            t.adx_14,
            t.di_plus,
            t.di_minus,
            t.rsi_14,
            t.stoch_k,
            t.stoch_d,
            t.williams_r,
            t.bb_upper,
            t.bb_mid,
            t.bb_lower,
            t.bb_pct_b,
            t.bb_width,
            t.atr_14,
            t.obv,
            t.vwap,
            t.cmf_20,
            t.volume_sma_20,
            t.volume_ratio,
            t.candle_body_size,
            t.candle_upper_wick,
            t.candle_lower_wick,
            t.is_doji,
            t.is_hammer,
            t.is_bull_engulfing,
            t.is_bear_engulfing,
            t.is_morning_star,
            t.is_evening_star
        FROM technical_indicators t
        JOIN latest_date d
          ON t.trading_date = d.trading_date
        LEFT JOIN listing l
          ON l.symbol = t.symbol
        ORDER BY t.symbol
        """
    )
    with engine.begin() as conn:
        return pd.read_sql(query, conn)


def score_frame(model: Any, df: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing_features = [feature for feature in features if feature not in df.columns]
    if missing_features:
        raise ValueError(f"Thieu feature trong technical_indicators: {missing_features}")

    usable = df.dropna(subset=features).copy()
    skipped = df[df[features].isna().any(axis=1)].copy()

    if usable.empty:
        usable["ai_score"] = pd.Series(dtype=float)
        usable["ai_signal"] = pd.Series(dtype=str)
        return usable, skipped

    x = usable[features]
    if hasattr(model, "predict_proba"):
        scores = model.predict_proba(x)[:, 1]
    else:
        scores = model.predict(x)
    usable["ai_score"] = scores.astype(float)
    usable["ai_signal"] = usable["ai_score"].apply(classify_signal)
    usable["target_date"] = usable["trading_date"].apply(compute_target_date)
    return usable, skipped


def upsert_predictions(
    engine: Any,
    predictions: pd.DataFrame,
    model_name: str,
    model_version: str,
) -> None:
    sql = text(
        """
        INSERT INTO ml_predictions (
            symbol,
            predict_date,
            target_date,
            ai_score,
            ai_signal,
            model_name,
            model_version,
            model_used,
            trend,
            created_at,
            updated_at
        )
        VALUES (
            :symbol,
            :predict_date,
            :target_date,
            :ai_score,
            :ai_signal,
            :model_name,
            :model_version,
            :model_used,
            :trend,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (symbol, predict_date)
        DO UPDATE SET
            target_date = EXCLUDED.target_date,
            ai_score = EXCLUDED.ai_score,
            ai_signal = EXCLUDED.ai_signal,
            model_name = EXCLUDED.model_name,
            model_version = EXCLUDED.model_version,
            model_used = EXCLUDED.model_used,
            trend = EXCLUDED.trend,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    rows = []
    for row in predictions.itertuples(index=False):
        rows.append(
            {
                "symbol": row.symbol,
                "predict_date": pd.Timestamp(row.trading_date).date(),
                "target_date": pd.Timestamp(row.target_date).date(),
                "ai_score": float(row.ai_score),
                "ai_signal": row.ai_signal,
                "model_name": model_name,
                "model_version": model_version,
                "model_used": model_name,
                "trend": "TANG" if row.ai_score >= 0.5 else "GIAM",
            }
        )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def fetch_previous_signal_changes(engine: Any, predict_date: pd.Timestamp) -> pd.DataFrame:
    query = text(
        """
        WITH prev_day AS (
            SELECT MAX(predict_date) AS predict_date
            FROM ml_predictions
            WHERE predict_date < :predict_date
        )
        SELECT
            cur.symbol,
            prev.ai_signal AS yesterday_signal,
            cur.ai_signal AS today_signal,
            prev.ai_score AS yesterday_score,
            cur.ai_score AS today_score
        FROM ml_predictions cur
        JOIN prev_day p
          ON 1 = 1
        JOIN ml_predictions prev
          ON prev.symbol = cur.symbol
         AND prev.predict_date = p.predict_date
        WHERE cur.predict_date = :predict_date
          AND prev.ai_signal IN ('MUA', 'MUA_MANH')
          AND cur.ai_signal IN ('BAN', 'BAN_MANH')
        ORDER BY cur.ai_score ASC, cur.symbol
        """
    )
    with engine.begin() as conn:
        return pd.read_sql(query, conn, params={"predict_date": pd.Timestamp(predict_date).date()})


def build_report(predictions: pd.DataFrame, skipped: pd.DataFrame, engine: Any) -> dict[str, Any]:
    latest_date = pd.Timestamp(predictions["trading_date"].iloc[0]).date() if not predictions.empty else None
    signal_counts = Counter(predictions["ai_signal"].tolist())
    top_buy = predictions[predictions["ai_signal"] == "MUA_MANH"].sort_values("ai_score", ascending=False).head(20)
    top_sell = predictions[predictions["ai_signal"] == "BAN_MANH"].sort_values("ai_score", ascending=True).head(10)
    changes = fetch_previous_signal_changes(engine, latest_date) if latest_date else pd.DataFrame()

    return {
        "predict_date": str(latest_date) if latest_date else None,
        "total_symbols": int(len(predictions)),
        "skipped_symbols": int(len(skipped)),
        "signal_counts": dict(signal_counts),
        "top_buy_strong": top_buy[["symbol", "ai_score", "exchange", "industry"]].to_dict(orient="records"),
        "top_sell_strong": top_sell[["symbol", "ai_score", "exchange", "industry"]].to_dict(orient="records"),
        "mua_to_ban_changes": changes.to_dict(orient="records"),
    }


def print_report(report: dict[str, Any]) -> None:
    print("\n" + "=" * 72)
    print("DAILY PREDICT REPORT")
    print("=" * 72)
    print(f"Ngay du doan: {report['predict_date']}")
    print(f"Tong ma du doan: {report['total_symbols']}")
    print(f"So ma bi bo qua do thieu feature: {report['skipped_symbols']}")
    print("\nPhan bo tin hieu:")
    for signal, count in sorted(report["signal_counts"].items()):
        print(f"  {signal:12s}: {count}")

    print("\nTop 20 MUA_MANH:")
    for item in report["top_buy_strong"]:
        print(f"  {item['symbol']:8s} score={item['ai_score']:.4f} | {item.get('exchange') or '-'} | {item.get('industry') or '-'}")

    print("\nTop 10 BAN_MANH:")
    for item in report["top_sell_strong"]:
        print(f"  {item['symbol']:8s} score={item['ai_score']:.4f} | {item.get('exchange') or '-'} | {item.get('industry') or '-'}")

    print("\nMa chuyen tu MUA sang BAN so voi hom qua:")
    if report["mua_to_ban_changes"]:
        for item in report["mua_to_ban_changes"]:
            print(
                f"  {item['symbol']:8s} {item['yesterday_signal']}({item['yesterday_score']:.4f})"
                f" -> {item['today_signal']}({item['today_score']:.4f})"
            )
    else:
        print("  Khong co.")


def run_daily_prediction(
    model_path: str = DEFAULT_MODEL_PATH,
    features_path: str = DEFAULT_FEATURES_PATH,
    model_version: str = DEFAULT_MODEL_VERSION,
) -> dict[str, Any]:
    if not Path(model_path).exists():
        raise FileNotFoundError(f"Khong tim thay model: {model_path}")
    if not Path(features_path).exists():
        raise FileNotFoundError(f"Khong tim thay feature list: {features_path}")

    create_predictions_table()
    engine = get_db_engine()
    model = load_model(model_path)
    features = load_features(features_path)
    source = fetch_latest_indicator_frame(engine)
    predictions, skipped = score_frame(model, source, features)
    if not predictions.empty:
        upsert_predictions(
            engine,
            predictions,
            model_name=os.path.basename(model_path),
            model_version=model_version,
        )
    report = build_report(predictions, skipped, engine)
    print_report(report)
    return report


def main():
    parser = argparse.ArgumentParser(description="Daily XGBoost prediction cho co phieu Viet Nam")
    parser.add_argument("--model", default=DEFAULT_MODEL_PATH, help="Duong dan file xgb_direction_5d.pkl")
    parser.add_argument("--features", default=DEFAULT_FEATURES_PATH, help="Duong dan features.json")
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION, help="Model version de luu vao DB")
    args = parser.parse_args()
    run_daily_prediction(args.model, args.features, args.model_version)


if __name__ == "__main__":
    main()
