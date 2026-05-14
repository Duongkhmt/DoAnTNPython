from typing import Any
import pandas as pd
import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from sqlalchemy import text

from create_ml_table import create_predictions_table
from daily_predict import run_daily_prediction
from timescale_utils import DatabaseManager


app = FastAPI(title="VN Stock AI Ranking Service")


_db_manager = None

def get_engine():
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    if _db_manager.engine is None:
        raise HTTPException(status_code=500, detail="Cannot connect to PostgreSQL.")
    return _db_manager.engine


def _read_sql(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Thay thế NaN/NaT bằng None để tránh lỗi JSON Decode Error do FastAPI trả về NaN unquoted."""
    return df.astype(object).where(pd.notnull(df), None)


def _latest_predict_date() -> Any:
    df = _read_sql("SELECT MAX(predict_date) AS predict_date FROM ml_predictions WHERE symbol != 'VNINDEX'")
    if df.empty or pd.isna(df.loc[0, "predict_date"]):
        # Fallback to absolute max if no other symbols found
        df = _read_sql("SELECT MAX(predict_date) AS predict_date FROM ml_predictions")
        
    if df.empty or pd.isna(df.loc[0, "predict_date"]):
        raise HTTPException(status_code=404, detail="No AI predictions found.")
    return df.loc[0, "predict_date"]


def _signal_filter_clause(signal: str) -> tuple[str, dict[str, Any]]:
    normalized = (signal or "ALL").upper()
    if normalized == "ALL":
        return "1 = 1", {}
    if normalized == "TOP":
        return "p.ai_signal IN ('TOP', 'TOP_STRONG')", {}
    if normalized == "WEAK":
        return "p.ai_signal IN ('WEAK', 'WEAK_STRONG')", {}
    return "p.ai_signal = :signal", {"signal": normalized}


@app.on_event("startup")
def on_startup() -> None:
    create_predictions_table()


@app.post("/daily_predict_all")
def trigger_daily_prediction(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_daily_prediction)
    return {"status": "accepted", "message": "Daily LightGBM ranker prediction started in background."}


@app.post("/predict_now")
def predict_now():
    try:
        return run_daily_prediction()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/screening/today")
def screening_today(
    signal: str = Query(default="ALL"),
    exchange: str | None = Query(default=None),
    industry: str | None = Query(default=None),
):
    predict_date = _latest_predict_date()
    signal_clause, extra_params = _signal_filter_clause(signal)
    params: dict[str, Any] = {"predict_date": predict_date, **extra_params}
    filters = [
        signal_clause,
        "(:exchange IS NULL OR ls.exchange = :exchange)",
        "(:industry IS NULL OR l.industry = :industry)",
        "t.volume_sma_20 > 100000",
        "ls.exchange IN ('HOSE', 'HNX')"
    ]
    params["exchange"] = exchange
    params["industry"] = industry

    sql = f"""
    SELECT
        p.symbol,
        p.predict_date,
        p.ai_score,
        p.ai_signal,
        p.model_name,
        p.model_version,
        t.rsi_14,
        t.macd,
        t.volume_ratio,
        t.price_momentum_5,
        t.price_momentum_20,
        t.return_5d,
        ROUND(COALESCE(tr.fr_buy_value, 0)::numeric, 3) AS fr_buy_value,
        ROUND(COALESCE(tr.fr_sell_value, 0)::numeric, 3) AS fr_sell_value,
        ROUND(COALESCE(tr.prop_buy_value, 0)::numeric, 3) AS td_buy_value,
        ROUND(COALESCE(tr.prop_sell_value, 0)::numeric, 3) AS td_sell_value,
        ls.exchange,
        l.industry,
        l.sector
    FROM ml_predictions p
    JOIN technical_indicators t
      ON t.symbol = p.symbol
     AND t.trading_date = p.predict_date
    LEFT JOIN quote_history q
      ON q.symbol = p.symbol
     AND q.trading_date = p.predict_date
    LEFT JOIN LATERAL (
        SELECT fr_buy_value, fr_sell_value, prop_buy_value, prop_sell_value
        FROM trading
        WHERE symbol = p.symbol AND trading_date <= p.predict_date
        ORDER BY trading_date DESC
        LIMIT 1
    ) tr ON true
    LEFT JOIN listing ls
      ON ls.symbol = p.symbol
    LEFT JOIN company l
      ON l.symbol = p.symbol
    WHERE p.predict_date = :predict_date
      AND p.symbol != 'VNINDEX'
      AND {" AND ".join(filters)}
    ORDER BY p.ai_score DESC, p.symbol
    """
    df = _clean_df(_read_sql(sql, params))
    return {
        "predict_date": str(predict_date),
        "total": int(len(df)),
        "signal_filter": signal.upper(),
        "items": df.to_dict(orient="records"),
    }


@app.get("/stock/{symbol}/detail")
def stock_detail(symbol: str):
    sym = symbol.upper().strip()

    profile_sql = """
    SELECT l.symbol, l.organ_name, l.exchange, l.industry, l.sector
    FROM listing l
    WHERE l.symbol = :symbol
    """
    price_sql = """
    SELECT
        q.symbol,
        q.trading_date,
        q.open,
        q.high,
        q.low,
        q.close,
        q.volume,
        t.sma_20,
        t.sma_50,
        t.bb_upper,
        t.bb_lower
    FROM quote_history q
    LEFT JOIN technical_indicators t
      ON t.symbol = q.symbol
     AND t.trading_date = q.trading_date
    WHERE q.symbol = :symbol
    ORDER BY q.trading_date DESC
    LIMIT 100
    """
    indicator_sql = """
    SELECT *
    FROM technical_indicators
    WHERE symbol = :symbol
    ORDER BY trading_date DESC
    LIMIT 1
    """
    ai_sql = """
    SELECT predict_date, target_date, ai_score, ai_signal, model_name, model_version
    FROM ml_predictions
    WHERE symbol = :symbol
    ORDER BY predict_date DESC
    LIMIT 30
    """
    flow_sql = """
    SELECT
        tr.symbol,
        tr.trading_date,
        tr.fr_buy_volume,
        tr.fr_sell_volume,
        tr.prop_buy_volume,
        tr.prop_sell_volume,
        ROUND(COALESCE(tr.fr_buy_value, 0)::numeric, 3) AS fr_buy_value,
        ROUND(COALESCE(tr.fr_sell_value, 0)::numeric, 3) AS fr_sell_value,
        ROUND(COALESCE(tr.prop_buy_value, 0)::numeric, 3) AS prop_buy_value,
        ROUND(COALESCE(tr.prop_sell_value, 0)::numeric, 3) AS prop_sell_value
    FROM trading tr
    LEFT JOIN quote_history q
      ON q.symbol = tr.symbol
     AND q.trading_date = tr.trading_date
    WHERE tr.symbol = :symbol
    ORDER BY tr.trading_date DESC
    LIMIT 30
    """

    profile = _clean_df(_read_sql(profile_sql, {"symbol": sym}))
    if profile.empty:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} not found.")

    prices = _read_sql(price_sql, {"symbol": sym}).sort_values("trading_date")
    indicators = _clean_df(_read_sql(indicator_sql, {"symbol": sym}))
    ai_scores = _clean_df(_read_sql(ai_sql, {"symbol": sym}).sort_values("predict_date"))
    flows = _clean_df(_read_sql(flow_sql, {"symbol": sym}).sort_values("trading_date"))

    latest_price = None
    pct_change = None
    if len(prices) >= 1:
        val1 = prices.iloc[-1]["close"]
        if pd.notna(val1):
            latest_price = float(val1)
    if len(prices) >= 2:
        val2 = prices.iloc[-2]["close"]
        if pd.notna(val2) and val2 != 0 and latest_price is not None:
            prev_close = float(val2)
            pct_change = (latest_price - prev_close) / prev_close * 100

    prices = _clean_df(prices)

    return {
        "profile": profile.iloc[0].to_dict(),
        "latest_price": latest_price,
        "pct_change": pct_change,
        "price_history": prices.to_dict(orient="records"),
        "latest_indicators": indicators.iloc[0].to_dict() if not indicators.empty else None,
        "ai_score_history": ai_scores.to_dict(orient="records"),
        "money_flow_30d": flows.to_dict(orient="records"),
    }


@app.get("/screening/history")
def screening_history(days: int = Query(default=30, ge=1, le=365)):
    sql = """
    WITH vnindex_returns AS (
        SELECT trading_date, return_5d
        FROM technical_indicators
        WHERE symbol = 'VNINDEX'
    )
    SELECT
        p.symbol,
        p.predict_date,
        p.target_date,
        p.ai_score,
        p.ai_signal,
        p.model_name,
        p.model_version,
        t.return_5d,
        v.return_5d AS vnindex_return_5d,
        (t.return_5d - v.return_5d) AS alpha_5d,
        CASE WHEN p.ai_signal IN ('TOP', 'TOP_STRONG') THEN 1 ELSE 0 END AS is_top_signal,
        CASE
            WHEN t.return_5d IS NULL OR v.return_5d IS NULL THEN NULL
            WHEN p.ai_signal IN ('TOP', 'TOP_STRONG') AND (t.return_5d - v.return_5d) > 0 THEN 1
            WHEN p.ai_signal IN ('WEAK', 'WEAK_STRONG') AND (t.return_5d - v.return_5d) < 0 THEN 1
            ELSE 0
        END AS is_correct_relative
    FROM ml_predictions p
    LEFT JOIN technical_indicators t
      ON t.symbol = p.symbol
     AND t.trading_date = p.predict_date
    LEFT JOIN vnindex_returns v
      ON v.trading_date = p.predict_date
    WHERE p.predict_date >= CURRENT_DATE - :days
      AND p.symbol != 'VNINDEX'
    ORDER BY p.predict_date DESC, p.ai_score DESC
    """
    history = _read_sql(sql, {"days": days})
    if history.empty:
        return {"days": days, "items": [], "daily_win_rate": []}

    # Convert 1/0 to True/False for better Jackson mapping
    history["is_top_signal"] = history["is_top_signal"].astype(bool)
    history["is_correct_relative"] = history["is_correct_relative"].astype(bool)

    stats = (
        history.dropna(subset=["alpha_5d", "is_correct_relative"])
        .groupby("predict_date", as_index=False)
        .agg(
            total_predictions=("symbol", "count"),
            correct_predictions=("is_correct_relative", "sum"),
            avg_alpha=("alpha_5d", "mean"),
            top_signal_count=("is_top_signal", "sum"),
            relative_win_rate=("is_correct_relative", "mean"),
        )
    )
    stats["win_rate"] = stats["relative_win_rate"]
    stats["avg_alpha"] = pd.to_numeric(stats["avg_alpha"], errors="coerce").round(6)
    stats["relative_win_rate"] = pd.to_numeric(stats["relative_win_rate"], errors="coerce").round(4)
    stats["win_rate"] = pd.to_numeric(stats["win_rate"], errors="coerce").round(4)
    
    history = _clean_df(history)
    stats = _clean_df(stats)
    
    return {
        "days": days,
        "items": history.to_dict(orient="records"),
        "daily_win_rate": stats.sort_values("predict_date", ascending=False).to_dict(orient="records"),
    }


@app.get("/market/overview")
def market_overview():
    predict_date = _latest_predict_date()

    market_sql = """
    WITH latest_date AS (
        SELECT MAX(trading_date) FROM quote_history
    ),
    prev_date AS (
        SELECT MAX(trading_date) FROM quote_history WHERE trading_date < (SELECT * FROM latest_date)
    ),
    current_prices AS (
        SELECT symbol, close FROM quote_history WHERE trading_date = (SELECT * FROM latest_date)
    ),
    previous_prices AS (
        SELECT symbol, close FROM quote_history WHERE trading_date = (SELECT * FROM prev_date)
    )
    SELECT
        COUNT(*) FILTER (WHERE c.close > p.close) AS up_count,
        COUNT(*) FILTER (WHERE c.close < p.close) AS down_count,
        COUNT(*) FILTER (WHERE c.close = p.close) AS neutral_count,
        COUNT(*) AS total_count
    FROM current_prices c
    JOIN previous_prices p ON c.symbol = p.symbol
    """
    signal_sql = """
    SELECT 
        ai_signal, 
        COUNT(*) AS total,
        AVG(ai_score) AS avg_ai_score
    FROM ml_predictions
    WHERE predict_date = :predict_date
      AND symbol != 'VNINDEX'
    GROUP BY ai_signal
    ORDER BY total DESC, ai_signal
    """
    industry_sql = """
    SELECT
        l.industry,
        AVG(p.ai_score) AS avg_ai_score,
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE p.ai_signal IN ('TOP', 'TOP_STRONG')) AS top_count,
        COUNT(*) FILTER (WHERE p.ai_signal IN ('WEAK', 'WEAK_STRONG')) AS weak_count
    FROM ml_predictions p
    LEFT JOIN company l
      ON l.symbol = p.symbol
    WHERE p.predict_date = :predict_date
      AND p.symbol != 'VNINDEX'
      AND l.industry IS NOT NULL
    GROUP BY l.industry
    HAVING COUNT(*) >= 3
    ORDER BY avg_ai_score DESC
    """

    market_df = _read_sql(market_sql)
    signal_distribution = _clean_df(_read_sql(signal_sql, {"predict_date": predict_date}))
    industries = _clean_df(_read_sql(industry_sql, {"predict_date": predict_date}))

    market_breadth = {}
    if not market_df.empty:
        row = market_df.iloc[0]
        total = float(row["total_count"]) if row["total_count"] > 0 else 1
        market_breadth = {
            "up_count": int(row["up_count"]),
            "down_count": int(row["down_count"]),
            "neutral_count": int(row["neutral_count"]),
            "up_ratio": float(row["up_count"]) / total,
            "down_ratio": float(row["down_count"]) / total
        }

    return {
        "predict_date": str(predict_date),
        "market_breadth": market_breadth,
        "signal_distribution": signal_distribution.to_dict(orient="records"),
        "top_industries": industries.head(10).to_dict(orient="records"),
        "bottom_industries": industries.sort_values("avg_ai_score", ascending=True).head(10).to_dict(orient="records"),
    }


@app.get("/predictions/latest")
def latest_predictions(limit: int = 20):
    sql = """
    SELECT symbol, predict_date, target_date, ai_score, ai_signal, model_name, model_version, updated_at
    FROM ml_predictions
    WHERE symbol != 'VNINDEX'
    ORDER BY predict_date DESC, ai_score DESC
    LIMIT :limit
    """
    df = _clean_df(_read_sql(sql, {"limit": limit}))
    return df.to_dict(orient="records")


@app.get("/")
def health_check():
    return {"status": "ok", "service": "vn-stock-ai-ranking"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
