import argparse
from pathlib import Path

import pandas as pd

from daily_predict import DEFAULT_FEATURES_PATH, fetch_latest_indicator_frame, get_db_engine, load_features


def summarize_skips(df: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing_mask = df[features].isna()
    skipped = df[missing_mask.any(axis=1)].copy()

    feature_summary = pd.DataFrame(
        {
            "missing_count": missing_mask.sum(),
            "missing_pct": (missing_mask.mean() * 100).round(2),
        }
    ).sort_values(["missing_count", "missing_pct"], ascending=False)

    if skipped.empty:
        symbol_summary = pd.DataFrame(columns=["symbol", "missing_features", "missing_count", "exchange", "industry"])
        return feature_summary, symbol_summary

    skipped["missing_features"] = missing_mask[missing_mask.any(axis=1)].apply(
        lambda row: [col for col, is_missing in row.items() if is_missing],
        axis=1,
    )
    skipped["missing_count"] = skipped["missing_features"].apply(len)

    symbol_summary = (
        skipped[["symbol", "exchange", "industry", "missing_features", "missing_count"]]
        .sort_values(["missing_count", "symbol"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return feature_summary, symbol_summary


def print_report(latest_date, total_symbols: int, skipped_symbols: int, feature_summary: pd.DataFrame, symbol_summary: pd.DataFrame, top_n: int):
    print("\n" + "=" * 72)
    print("CHECK SKIPPED PREDICTIONS REPORT")
    print("=" * 72)
    print(f"Ngay du lieu moi nhat: {latest_date}")
    print(f"Tong ma o ngay moi nhat: {total_symbols}")
    print(f"So ma bi skip: {skipped_symbols}")
    print(f"Ty le skip: {skipped_symbols / total_symbols * 100:.2f}%" if total_symbols else "Ty le skip: 0.00%")

    print("\nTop feature gay skip:")
    for feature, row in feature_summary[feature_summary["missing_count"] > 0].head(20).iterrows():
        print(f"  {feature:20s} missing={int(row['missing_count']):4d} | {row['missing_pct']:6.2f}%")

    print(f"\nTop {top_n} ma bi skip:")
    if symbol_summary.empty:
        print("  Khong co ma nao bi skip.")
        return

    for row in symbol_summary.head(top_n).itertuples(index=False):
        print(
            f"  {row.symbol:8s} missing={row.missing_count:2d} | "
            f"{row.exchange or '-':6s} | {row.industry or '-'} | "
            f"{', '.join(row.missing_features)}"
        )


def save_outputs(output_dir: Path, feature_summary: pd.DataFrame, symbol_summary: pd.DataFrame):
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_path = output_dir / "skip_feature_summary.csv"
    symbol_path = output_dir / "skipped_symbols_latest.csv"
    feature_summary.to_csv(feature_path, encoding="utf-8-sig")
    symbol_summary.to_csv(symbol_path, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Da luu: {feature_path}")
    print(f"[OK] Da luu: {symbol_path}")


def main():
    parser = argparse.ArgumentParser(description="Kiem tra vi sao ma bi skip trong daily_predict.py")
    parser.add_argument("--features", default=DEFAULT_FEATURES_PATH, help="Duong dan features.json")
    parser.add_argument("--top", type=int, default=50, help="So ma skip hien thi")
    parser.add_argument("--save-dir", default=".", help="Thu muc luu file CSV bao cao")
    args = parser.parse_args()

    features = load_features(args.features)
    engine = get_db_engine()
    latest_df = fetch_latest_indicator_frame(engine)

    missing_features = [feature for feature in features if feature not in latest_df.columns]
    if missing_features:
        raise ValueError(f"Thieu feature trong technical_indicators: {missing_features}")

    feature_summary, symbol_summary = summarize_skips(latest_df, features)
    latest_date = latest_df["trading_date"].max() if not latest_df.empty else None
    print_report(
        latest_date=latest_date,
        total_symbols=len(latest_df),
        skipped_symbols=len(symbol_summary),
        feature_summary=feature_summary,
        symbol_summary=symbol_summary,
        top_n=args.top,
    )
    save_outputs(Path(args.save_dir).resolve(), feature_summary, symbol_summary)


if __name__ == "__main__":
    main()
