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


DEFAULT_MODEL_PATH = "lgbm_alpha_5d_ranker.txt"
DEFAULT_FEATURES_PATH = "lgbm_alpha_5d_features.json"
DEFAULT_MODEL_VERSION = "lgbm_alpha_5d_ranker_v1"
MIN_UNIVERSE_SIZE = 50


def load_model(model_path: str):
    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise RuntimeError("Can cai dat lightgbm de chay model ranker: pip install lightgbm") from exc
    return lgb.Booster(model_file=str(Path(model_path)))


def load_feature_config(features_path: str) -> dict[str, Any]:
    with open(features_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        config = data
    else:
        config = {"features": data}
    features = config.get("features", [])
    if not isinstance(features, list) or not features:
        raise ValueError("Feature config khong hop le hoac khong co danh sach feature.")
    return config


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


def assign_ranker_signals(frame: pd.DataFrame) -> pd.DataFrame:
    ranked = frame.sort_values(["trading_date", "ai_score"], ascending=[True, False]).copy()
    ranked["score_rank_pct"] = ranked.groupby("trading_date")["ai_score"].rank(pct=True, ascending=False, method="first")

    ranked["ai_signal"] = "NEUTRAL"
    ranked.loc[ranked["score_rank_pct"] <= 0.10, "ai_signal"] = "TOP_STRONG"
    ranked.loc[(ranked["score_rank_pct"] > 0.10) & (ranked["score_rank_pct"] <= 0.30), "ai_signal"] = "TOP"
    ranked.loc[ranked["score_rank_pct"] >= 0.90, "ai_signal"] = "WEAK_STRONG"
    ranked.loc[(ranked["score_rank_pct"] < 0.90) & (ranked["score_rank_pct"] >= 0.70), "ai_signal"] = "WEAK"
    return ranked


def fetch_latest_indicator_frame(engine: Any) -> pd.DataFrame:
    query = text(
        """
        WITH eligible_dates AS (
            SELECT
                t.trading_date,
                COUNT(*) AS universe_size
            FROM technical_indicators t
            LEFT JOIN listing l ON l.symbol = t.symbol
            WHERE t.volume_sma_20 > 100000
              AND l.exchange IN ('HOSE', 'HNX')
            GROUP BY t.trading_date
            HAVING COUNT(*) >= :min_universe_size
        ),
        latest_date AS (
            SELECT MAX(e.trading_date) AS trading_date
            FROM eligible_dates e
            WHERE EXISTS (
                SELECT 1
                FROM technical_indicators vi
                WHERE vi.symbol = 'VNINDEX'
                  AND vi.trading_date = e.trading_date
            )
        )
        SELECT
            t.*,
            q.open, q.high, q.low, q.close, q.volume,
            l.exchange, l.industry, l.sector,
            d.trading_date AS predict_date,
            vi.price_momentum_5 AS vnindex_momentum_5,
            vi.price_momentum_20 AS vnindex_momentum_20,
            vi.rsi_14 AS vnindex_rsi
        FROM technical_indicators t
        JOIN latest_date d ON t.trading_date = d.trading_date
        JOIN technical_indicators vi
          ON vi.symbol = 'VNINDEX'
         AND vi.trading_date = d.trading_date
        LEFT JOIN quote_history q ON q.symbol = t.symbol AND q.trading_date = t.trading_date
        LEFT JOIN listing l ON l.symbol = t.symbol
        WHERE t.volume_sma_20 > 100000 
          AND l.exchange IN ('HOSE', 'HNX')
          AND t.symbol <> 'VNINDEX'
        ORDER BY t.symbol
        """
    )
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"min_universe_size": MIN_UNIVERSE_SIZE})
            
    return df


def enrich_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    enriched["rel_momentum_5"] = (enriched["price_momentum_5"] - enriched["vnindex_momentum_5"]).clip(-0.5, 0.5)
    enriched["rel_momentum_20"] = (enriched["price_momentum_20"] - enriched["vnindex_momentum_20"]).clip(-0.5, 0.5)
    enriched["rsi_vs_vnindex"] = (enriched["rsi_14"] - enriched["vnindex_rsi"]).clip(-100, 100)
    enriched["regime_momentum"] = enriched["price_momentum_20"] * enriched["vnindex_momentum_20"]
    enriched["regime_sma"] = enriched["price_vs_sma20"] * enriched["vnindex_momentum_5"]

    rank_source_cols = [
        "price_momentum_5", "price_momentum_10", "price_momentum_20",
        "rsi_14", "stoch_k", "stoch_d", "williams_r",
        "bb_pct_b", "volume_ratio", "cmf_20",
        "rel_momentum_5", "rel_momentum_20", "rsi_vs_vnindex",
    ]
    for col in rank_source_cols:
        enriched[f"{col}_rank_pct"] = enriched.groupby("trading_date")[col].rank(pct=True)

    return enriched


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
    scores = model.predict(x)
    usable["ai_score"] = scores.astype(float)
    usable = assign_ranker_signals(usable)
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
                "trend": (
                    "TANG" if row.ai_signal in ("TOP", "TOP_STRONG")
                    else "GIAM" if row.ai_signal in ("WEAK", "WEAK_STRONG")
                    else "TRUNG_TINH"
                ),
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
          AND prev.ai_signal IN ('TOP', 'TOP_STRONG')
          AND cur.ai_signal IN ('WEAK', 'WEAK_STRONG')
        ORDER BY cur.ai_score ASC, cur.symbol
        """
    )
    with engine.begin() as conn:
        return pd.read_sql(query, conn, params={"predict_date": pd.Timestamp(predict_date).date()})


def build_report(predictions: pd.DataFrame, skipped: pd.DataFrame, engine: Any) -> dict[str, Any]:
    latest_date = pd.Timestamp(predictions["trading_date"].iloc[0]).date() if not predictions.empty else None
    signal_counts = Counter(predictions["ai_signal"].tolist())
    top_buy = predictions[predictions["ai_signal"] == "TOP_STRONG"].sort_values("ai_score", ascending=False).head(20)
    top_sell = predictions[predictions["ai_signal"] == "WEAK_STRONG"].sort_values("ai_score", ascending=True).head(10)
    changes = fetch_previous_signal_changes(engine, latest_date) if latest_date else pd.DataFrame()

    return {
        "predict_date": str(latest_date) if latest_date else None,
        "source_rows": int(len(predictions) + len(skipped)),
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
    print(f"So dong source: {report['source_rows']}")
    print(f"Tong ma du doan: {report['total_symbols']}")
    print(f"So ma bi bo qua do thieu feature: {report['skipped_symbols']}")
    print("\nPhan bo tin hieu:")
    for signal, count in sorted(report["signal_counts"].items()):
        print(f"  {signal:12s}: {count}")

    print("\nTop 20 TOP_STRONG:")
    for item in report["top_buy_strong"]:
        print(f"  {item['symbol']:8s} score={item['ai_score']:.4f} | {item.get('exchange') or '-'} | {item.get('industry') or '-'}")

    print("\nTop 10 WEAK_STRONG:")
    for item in report["top_sell_strong"]:
        print(f"  {item['symbol']:8s} score={item['ai_score']:.4f} | {item.get('exchange') or '-'} | {item.get('industry') or '-'}")

    print("\nMa chuyen tu TOP sang WEAK so voi hom qua:")
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
    feature_config = load_feature_config(features_path)
    model_type = feature_config.get("model_type")
    if model_type != "lightgbm_ranker":
        raise ValueError(f"Chi ho tro model_type='lightgbm_ranker', nhan duoc: {model_type}")
    model = load_model(model_path)
    features = feature_config["features"]
    source = fetch_latest_indicator_frame(engine)
    source = enrich_derived_features(source)
    predictions, skipped = score_frame(model, source, features)
    resolved_version = model_version
    if not predictions.empty:
        upsert_predictions(
            engine,
            predictions,
            model_name=os.path.basename(model_path),
            model_version=resolved_version,
        )
    report = build_report(predictions, skipped, engine)
    print_report(report)
    return report


def main():
    parser = argparse.ArgumentParser(description="Daily LightGBM ranker prediction cho co phieu Viet Nam")
    parser.add_argument("--model", default=DEFAULT_MODEL_PATH, help="Duong dan file LightGBM ranker .txt")
    parser.add_argument("--features", default=DEFAULT_FEATURES_PATH, help="Duong dan file feature config .json")
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION, help="Model version de luu vao DB")
    args = parser.parse_args()
    run_daily_prediction(args.model, args.features, args.model_version)


if __name__ == "__main__":
    main()
