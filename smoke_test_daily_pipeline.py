import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from timescale_utils import DatabaseManager


BASE_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Smoke test cho pipeline daily: quote -> indicators -> AI prediction"
    )
    parser.add_argument("--symbol", required=True, help="Ma co phieu can test, vi du HPG")
    parser.add_argument("--daily", action="store_true", help="Chay compute_indicators o che do daily")
    parser.add_argument("--skip-compute", action="store_true", help="Bo qua buoc compute indicators")
    parser.add_argument("--skip-ai", action="store_true", help="Bo qua buoc AI prediction")
    parser.add_argument("--min-quote-rows", type=int, default=30, help="So dong quote toi thieu")
    parser.add_argument("--min-feature-rows", type=int, default=1, help="So dong feature toi thieu cho AI")
    parser.add_argument("--json", action="store_true", help="In ket qua dang JSON o cuoi")
    return parser.parse_args()


def print_step(title: str):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def fail(message: str, code: int = 1):
    print(f"[FAIL] {message}")
    sys.exit(code)


def get_db():
    db = DatabaseManager()
    if db.engine is None:
        fail("Khong ket noi duoc TimescaleDB/PostgreSQL.")
    return db


def query_one_row(engine, sql: str, params: dict) -> pd.Series | None:
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    if df.empty:
        return None
    return df.iloc[0]


def check_quote_history(engine, symbol: str):
    print_step(f"[1] KIEM TRA QUOTE_HISTORY CHO {symbol}")
    row = query_one_row(
        engine,
        """
        SELECT symbol, COUNT(*) AS rows, MAX(trading_date) AS last_date
        FROM quote_history
        WHERE symbol = :sym
        GROUP BY symbol
        """,
        {"sym": symbol},
    )
    if row is None:
        fail(f"Khong tim thay du lieu quote_history cho {symbol}.")
    print(f"symbol={row['symbol']} | rows={int(row['rows'])} | last_date={row['last_date']}")
    return {"rows": int(row["rows"]), "last_date": str(row["last_date"])}


def run_compute(symbol: str, daily: bool):
    print_step(f"[2] CHAY COMPUTE INDICATORS CHO {symbol}")
    cmd = [sys.executable, str(BASE_DIR / "compute_indicators.py"), "--symbol", symbol]
    if daily:
        cmd.insert(2, "--daily")

    print("Command:", " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(BASE_DIR), check=False)
    if result.returncode != 0:
        fail(f"compute_indicators.py that bai voi exit code {result.returncode}.", result.returncode)


def check_indicators(engine, symbol: str):
    print_step(f"[3] KIEM TRA TECHNICAL_INDICATORS CHO {symbol}")
    row = query_one_row(
        engine,
        """
        SELECT
            symbol,
            COUNT(*) AS rows,
            MAX(trading_date) AS last_date,
            COUNT(macd) AS macd_rows,
            COUNT(sma_20) AS sma20_rows,
            COUNT(bb_upper) AS bb_rows
        FROM technical_indicators
        WHERE symbol = :sym
        GROUP BY symbol
        """,
        {"sym": symbol},
    )
    if row is None:
        fail(f"Khong tim thay technical_indicators cho {symbol}.")
    print(
        "symbol={symbol} | rows={rows} | last_date={last_date} | macd_rows={macd_rows} | sma20_rows={sma20_rows} | bb_rows={bb_rows}".format(
            symbol=row["symbol"],
            rows=int(row["rows"]),
            last_date=row["last_date"],
            macd_rows=int(row["macd_rows"]),
            sma20_rows=int(row["sma20_rows"]),
            bb_rows=int(row["bb_rows"]),
        )
    )
    return {
        "rows": int(row["rows"]),
        "last_date": str(row["last_date"]),
        "macd_rows": int(row["macd_rows"]),
        "sma20_rows": int(row["sma20_rows"]),
        "bb_rows": int(row["bb_rows"]),
    }


def check_ai_features(symbol: str):
    print_step(f"[4] KIEM TRA INPUT FEATURES CHO AI {symbol}")
    from daily_predict import fetch_latest_indicator_frame, get_db_engine

    df = fetch_latest_indicator_frame(get_db_engine())
    df = df[df["symbol"] == symbol]
    print(f"feature_rows={len(df)}")
    if len(df) > 0:
        print(df.tail(1).to_string(index=False))
    return len(df)


def run_ai_prediction(symbol: str):
    print_step(f"[5] CHAY AI PREDICTION CHO {symbol}")
    from daily_predict import run_daily_prediction

    result = run_daily_prediction()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return result


def check_latest_prediction(engine, symbol: str):
    print_step(f"[6] KIEM TRA ML_PREDICTIONS CHO {symbol}")
    row = query_one_row(
        engine,
        """
        SELECT symbol, predict_date, target_date, ai_score, ai_signal, model_name, created_at
        FROM ml_predictions
        WHERE symbol = :sym
        ORDER BY predict_date DESC, created_at DESC
        LIMIT 1
        """,
        {"sym": symbol},
    )
    if row is None:
        fail(f"Khong tim thay ml_predictions cho {symbol}.")
    result = {
        "symbol": row["symbol"],
        "predict_date": str(row["predict_date"]),
        "target_date": str(row["target_date"]),
        "ai_score": float(row["ai_score"]) if row["ai_score"] is not None else None,
        "ai_signal": row["ai_signal"],
        "model_name": row["model_name"],
        "created_at": str(row["created_at"]),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main():
    args = parse_args()
    symbol = args.symbol.upper().strip()

    summary = {"symbol": symbol}

    db = get_db()
    summary["quote_history"] = check_quote_history(db.engine, symbol)
    if summary["quote_history"]["rows"] < args.min_quote_rows:
        fail(
            f"Quote rows cua {symbol} chi co {summary['quote_history']['rows']}, nho hon min_quote_rows={args.min_quote_rows}."
        )

    if not args.skip_compute:
        run_compute(symbol, daily=args.daily)
        db = get_db()
        summary["technical_indicators"] = check_indicators(db.engine, symbol)
    else:
        print_step("[2-3] BO QUA COMPUTE INDICATORS")

    if not args.skip_ai:
        feature_rows = check_ai_features(symbol)
        summary["feature_rows"] = feature_rows
        if feature_rows < args.min_feature_rows:
            fail(
                f"Feature rows cua {symbol} chi co {feature_rows}, nho hon min_feature_rows={args.min_feature_rows}."
            )
        summary["ai_result"] = run_ai_prediction(symbol)
        db = get_db()
        summary["latest_prediction"] = check_latest_prediction(db.engine, symbol)
    else:
        print_step("[4-6] BO QUA AI PREDICTION")

    print_step("[DONE] SMOKE TEST THANH CONG")
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
