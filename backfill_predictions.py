import os
import argparse
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from daily_predict import (
    get_db_engine,
    load_model,
    load_features,
    score_frame,
    upsert_predictions,
    DEFAULT_MODEL_PATH,
    DEFAULT_FEATURES_PATH,
    DEFAULT_MODEL_VERSION
)

def fetch_indicator_frame_by_date(engine, target_date: str) -> pd.DataFrame:
    query = text(
        """
        SELECT
            t.*,
            q.open,
            q.high,
            q.low,
            q.close,
            q.volume,
            l.exchange,
            l.industry,
            l.sector
        FROM technical_indicators t
        LEFT JOIN quote_history q
          ON q.symbol = t.symbol AND q.trading_date = t.trading_date
        LEFT JOIN listing l
          ON l.symbol = t.symbol
        WHERE t.trading_date = :target_date
          AND t.volume_sma_20 > 100000
          AND l.exchange IN ('HOSE', 'HNX')
        ORDER BY t.symbol
        """
    )
    with engine.begin() as conn:
        return pd.read_sql(query, conn, params={"target_date": target_date})

def get_missing_dates(engine, start_date: str) -> list[str]:
    query = text(
        """
        WITH all_dates AS (
            SELECT DISTINCT trading_date
            FROM technical_indicators
            WHERE trading_date >= :start_date
        ),
        predicted_dates AS (
            SELECT DISTINCT predict_date
            FROM ml_predictions
            WHERE predict_date >= :start_date
        )
        SELECT a.trading_date
        FROM all_dates a
        LEFT JOIN predicted_dates p ON a.trading_date = p.predict_date
        WHERE p.predict_date IS NULL
        ORDER BY a.trading_date ASC
        """
    )
    with engine.begin() as conn:
        df = pd.read_sql(query, conn, params={"start_date": start_date})
    return df['trading_date'].astype(str).tolist()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2025-10-01", help="Start date for backfill")
    args = parser.parse_args()

    engine = get_db_engine()
    model = load_model(DEFAULT_MODEL_PATH)
    features = load_features(DEFAULT_FEATURES_PATH)

    print(f"Checking missing prediction dates from {args.start_date}...")
    missing_dates = get_missing_dates(engine, args.start_date)
    
    if not missing_dates:
        print("No missing dates found. Everything is up-to-date!")
        return

    print(f"Found {len(missing_dates)} missing dates to backfill.")
    
    for date in tqdm(missing_dates, desc="Backfilling"):
        source = fetch_indicator_frame_by_date(engine, date)
        if source.empty:
            continue
            
        predictions, skipped = score_frame(model, source, features)
        if not predictions.empty:
            upsert_predictions(
                engine,
                predictions,
                model_name=os.path.basename(DEFAULT_MODEL_PATH),
                model_version=DEFAULT_MODEL_VERSION,
            )

    print("Backfill completed successfully!")

if __name__ == "__main__":
    main()
