import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from timescale_utils import DatabaseManager


OUTPUT_CSV = "all_stocks_train.csv"
MIN_ROWS = 200


EXPORT_SQL = """
WITH eligible_symbols AS (
    SELECT symbol
    FROM technical_indicators
    WHERE symbol <> 'VNINDEX'
    GROUP BY symbol
    HAVING COUNT(*) >= :min_rows
)
SELECT
    q.symbol,
    q.trading_date,
    q.open,
    q.high,
    q.low,
    q.close,
    q.volume,
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
    t.direction_5d,
    t.direction_10d,
    t.return_5d
FROM technical_indicators t
JOIN eligible_symbols e
  ON e.symbol = t.symbol
JOIN quote_history q
  ON q.symbol = t.symbol
 AND q.trading_date = t.trading_date
LEFT JOIN listing l
  ON l.symbol = t.symbol
ORDER BY q.symbol, q.trading_date
"""


def get_engine():
    db = DatabaseManager()
    if db.engine is None:
        raise RuntimeError("Khong the ket noi PostgreSQL/TimescaleDB.")
    return db.engine


def export_dataset(output_path: Path, min_rows: int) -> pd.DataFrame:
    engine = get_engine()
    
    # Export dữ liệu chính (cổ phiếu)
    with engine.begin() as conn:
        df = pd.read_sql(text(EXPORT_SQL), conn, params={"min_rows": min_rows})
    
    # Export thêm VNINDEX riêng, dùng đúng return_5d từ technical_indicators
    vnindex_sql = text("""
        SELECT 
            q.symbol,
            q.trading_date,
            q.open,
            q.high,
            q.low,
            q.close,
            q.volume,
            'INDEX' AS exchange,
            NULL AS industry,
            NULL AS sector,
            t.return_5d
        FROM quote_history q
        JOIN technical_indicators t
          ON t.symbol = q.symbol
         AND t.trading_date = q.trading_date
        WHERE q.symbol = 'VNINDEX'
        ORDER BY q.trading_date
    """)
    
    with engine.begin() as conn:
        df_vnindex = pd.read_sql(vnindex_sql, conn)
    
    print(f"VNINDEX: {len(df_vnindex)} dòng")
    
    # Merge lại và đảm bảo không có duplicate theo (symbol, trading_date)
    df_all = pd.concat([df, df_vnindex], ignore_index=True)
    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=["symbol", "trading_date"], keep="last")
    df_all = df_all.sort_values(['symbol', 'trading_date']).reset_index(drop=True)
    print(f"Drop duplicate rows: {before:,} -> {len(df_all):,}")
    
    df_all.to_csv(output_path, index=False, encoding="utf-8-sig")
    return df_all

def print_report(df: pd.DataFrame) -> None:
    total_symbols = df["symbol"].nunique()
    total_rows = len(df)
    start_date = df["trading_date"].min()
    end_date = df["trading_date"].max()
    nan_ratio = (df.isna().mean() * 100).sort_values(ascending=False)
    direction_ratio = (
        df["direction_5d"]
        .dropna()
        .map({1: "TANG", 0: "GIAM"})
        .value_counts(normalize=True)
        .mul(100)
        .round(2)
    )

    print("\n" + "=" * 72)
    print("EXPORT DATA REPORT")
    print("=" * 72)
    print(f"Tong ma: {total_symbols}")
    print(f"Tong dong: {total_rows:,}")
    print(f"Khoang thoi gian: {start_date} -> {end_date}")
    print("\nTy le NaN tung cot (%):")
    for col, pct in nan_ratio.items():
        print(f"  {col:20s} {pct:7.2f}%")

    print("\nTy le direction_5d:")
    for label in ["TANG", "GIAM"]:
        print(f"  {label:6s}: {direction_ratio.get(label, 0.0):6.2f}%")


def main():
    parser = argparse.ArgumentParser(description="Export du lieu train XGBoost tu PostgreSQL")
    parser.add_argument("--output", default=OUTPUT_CSV, help="Ten file CSV dau ra")
    parser.add_argument("--min-rows", type=int, default=MIN_ROWS, help="So ngay du lieu toi thieu moi ma")
    args = parser.parse_args()

    output_path = Path(args.output).resolve()
    df = export_dataset(output_path, args.min_rows)
    print(f"[OK] Da luu file CSV: {output_path}")
    print_report(df)


if __name__ == "__main__":
    main()
