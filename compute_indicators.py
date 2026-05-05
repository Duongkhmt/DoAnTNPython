"""
compute_indicators.py
=====================
Tính chỉ báo kỹ thuật từ bảng quote_history và lưu vào bảng
technical_indicators trong PostgreSQL.

Chỉ báo được tính:
  Trend    : SMA(10,20,50), EMA(12,26), MACD + Signal + Hist, ADX(14)
  Volatility: Bollinger Bands(20), ATR(14), BB %B, BB Width
  Volume   : OBV, VWAP (daily approx), CMF(20), Volume SMA(20), Volume Ratio
  Candle   : Body size, Upper/Lower wick, Doji, Hammer, Bearish Engulfing,
             Bullish Engulfing, Morning Star, Evening Star

Target label (cho ML):
  direction_5d  : 1 nếu close tăng sau 5 nến, 0 nếu giảm/không đổi
  direction_10d : tương tự với 10 nến
  return_5d     : % thay đổi close sau 5 nến (raw, dùng khi cần regression)

Yêu cầu:
  pip install pandas numpy psycopg2-binary sqlalchemy pandas-ta

Dùng:
  python compute_indicators.py            # tính toàn bộ (mọi mã)
  python compute_indicators.py --daily    # chỉ cập nhật 7 ngày gần nhất
  python compute_indicators.py --symbol TCB FPT VNM   # chỉ tính cho mã chỉ định
"""

import sys
import time
import argparse
import math
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from timescale_utils import DatabaseManager

# ─────────────────────────────────────────────
# 1. KẾT NỐI DATABASE  (dùng lại postgres_utils nếu muốn)
# ─────────────────────────────────────────────
db = DatabaseManager()
engine = db.engine 

if engine is None:
    print("❌ Không thể kết nối Database. Vui lòng kiểm tra Docker!")
    sys.exit(1)

# ─────────────────────────────────────────────
# 2. TẠO BẢNG NẾU CHƯA CÓ
# ─────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS technical_indicators (
    symbol          VARCHAR(10)  NOT NULL,
    trading_date    DATE         NOT NULL,

    -- Trend
    sma_10          FLOAT,
    sma_20          FLOAT,
    sma_50          FLOAT,
    ema_12          FLOAT,
    ema_26          FLOAT,
    macd            FLOAT,
    macd_signal     FLOAT,
    macd_hist       FLOAT,
    adx_14          FLOAT,
    di_plus         FLOAT,
    di_minus        FLOAT,
    rsi_14          FLOAT,
    stoch_k         FLOAT,
    stoch_d         FLOAT,
    williams_r      FLOAT,

    -- Volatility
    bb_upper        FLOAT,
    bb_mid          FLOAT,
    bb_lower        FLOAT,
    bb_pct_b        FLOAT,
    bb_width        FLOAT,
    atr_14          FLOAT,

    -- Volume
    obv             FLOAT,
    vwap            FLOAT,
    cmf_20          FLOAT,
    volume_sma_20   FLOAT,
    volume_ratio    FLOAT,

    -- Candlestick patterns (1 = xuất hiện, 0 = không)
    candle_body_size    FLOAT,
    candle_upper_wick   FLOAT,
    candle_lower_wick   FLOAT,
    is_doji             SMALLINT,
    is_hammer           SMALLINT,
    is_bull_engulfing   SMALLINT,
    is_bear_engulfing   SMALLINT,
    is_morning_star     SMALLINT,
    is_evening_star     SMALLINT,

    -- Target labels (ML)
    direction_5d    SMALLINT,   -- 1 tăng / 0 giảm sau 5 phiên
    direction_10d   SMALLINT,
    return_5d       FLOAT,      -- % thay đổi close sau 5 phiên

    PRIMARY KEY (symbol, trading_date)
);
CREATE INDEX IF NOT EXISTS idx_ti_symbol ON technical_indicators (symbol);
CREATE INDEX IF NOT EXISTS idx_ti_date   ON technical_indicators (trading_date);
"""

def create_table():
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text("ALTER TABLE technical_indicators ADD COLUMN IF NOT EXISTS rsi_14 FLOAT"))
        conn.execute(text("ALTER TABLE technical_indicators ADD COLUMN IF NOT EXISTS stoch_k FLOAT"))
        conn.execute(text("ALTER TABLE technical_indicators ADD COLUMN IF NOT EXISTS stoch_d FLOAT"))
        conn.execute(text("ALTER TABLE technical_indicators ADD COLUMN IF NOT EXISTS williams_r FLOAT"))
    print("✅ Bảng technical_indicators đã sẵn sàng.")


# ─────────────────────────────────────────────
# 3. HÀM TÍNH CHỈ BÁO
# ─────────────────────────────────────────────

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def compute_trend(df: pd.DataFrame) -> pd.DataFrame:
    c = df["close"]
    hi, lo, cl = df["high"], df["low"], df["close"]
    df["sma_10"] = c.rolling(10).mean()
    df["sma_20"] = c.rolling(20).mean()
    df["sma_50"] = c.rolling(50).mean()
    df["ema_12"] = _ema(c, 12)
    df["ema_26"] = _ema(c, 26)
    df["macd"]   = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = _ema(df["macd"], 9)
    df["macd_hist"]   = df["macd"] - df["macd_signal"]

    # RSI(14)
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # Stochastic %K(14), %D(3)
    low_14 = lo.rolling(14).min()
    high_14 = hi.rolling(14).max()
    stoch_range = (high_14 - low_14).replace(0, np.nan)
    df["stoch_k"] = 100 * (c - low_14) / stoch_range
    df["stoch_d"] = df["stoch_k"].rolling(3).mean()

    # Williams %R(14)
    df["williams_r"] = -100 * (high_14 - c) / stoch_range

    # ADX(14)
    prev_cl = cl.shift(1)
    tr = pd.concat([
        hi - lo,
        (hi - prev_cl).abs(),
        (lo - prev_cl).abs()
    ], axis=1).max(axis=1)

    dm_plus  = np.where((hi - hi.shift(1)) > (lo.shift(1) - lo), np.maximum(hi - hi.shift(1), 0), 0)
    dm_minus = np.where((lo.shift(1) - lo) > (hi - hi.shift(1)), np.maximum(lo.shift(1) - lo, 0), 0)

    atr14  = tr.ewm(span=14, adjust=False).mean()
    dip14  = pd.Series(dm_plus,  index=df.index).ewm(span=14, adjust=False).mean()
    dim14  = pd.Series(dm_minus, index=df.index).ewm(span=14, adjust=False).mean()

    di_plus  = 100 * dip14 / atr14.replace(0, np.nan)
    di_minus = 100 * dim14 / atr14.replace(0, np.nan)
    dx = (100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan))

    df["adx_14"]   = dx.ewm(span=14, adjust=False).mean()
    df["di_plus"]  = di_plus
    df["di_minus"] = di_minus
    return df


def compute_volatility(df: pd.DataFrame) -> pd.DataFrame:
    c = df["close"]
    hi, lo = df["high"], df["low"]

    # Bollinger Bands (20, 2σ)
    mid   = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    upper = mid + 2 * std20
    lower = mid - 2 * std20

    df["bb_upper"] = upper
    df["bb_mid"]   = mid
    df["bb_lower"] = lower
    df["bb_pct_b"] = (c - lower) / (upper - lower).replace(0, np.nan)
    df["bb_width"] = (upper - lower) / mid.replace(0, np.nan)

    # ATR(14)
    prev_cl = c.shift(1)
    tr = pd.concat([
        hi - lo,
        (hi - prev_cl).abs(),
        (lo - prev_cl).abs()
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.ewm(span=14, adjust=False).mean()
    return df


def compute_volume(df: pd.DataFrame) -> pd.DataFrame:
    c, v = df["close"], df["volume"]
    hi, lo = df["high"], df["low"]

    # OBV
    obv = (np.sign(c.diff()) * v).fillna(0).cumsum()
    df["obv"] = obv

    # VWAP xấp xỉ hàng ngày (typical price × volume / cumulative volume)
    tp = (hi + lo + c) / 3
    df["vwap"] = (tp * v).cumsum() / v.cumsum().replace(0, np.nan)

    # CMF(20) — Chaikin Money Flow
    mfv = ((c - lo) - (hi - c)) / (hi - lo).replace(0, np.nan) * v
    df["cmf_20"] = mfv.rolling(20).sum() / v.rolling(20).sum().replace(0, np.nan)

    # Volume SMA(20) & Volume Ratio
    vol_ma = v.rolling(20).mean()
    df["volume_sma_20"] = vol_ma
    df["volume_ratio"]  = v / vol_ma.replace(0, np.nan)
    return df


def compute_candles(df: pd.DataFrame) -> pd.DataFrame:
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]

    body      = (c - o).abs()
    full_range = h - l
    upper_wick = h - c.where(c >= o, o)
    lower_wick = c.where(c >= o, o) - l

    df["candle_body_size"]  = body
    df["candle_upper_wick"] = upper_wick
    df["candle_lower_wick"] = lower_wick

    # Doji: body rất nhỏ so với full range
    df["is_doji"] = ((body / full_range.replace(0, np.nan)) < 0.1).astype(int)

    # Hammer: lower wick >= 2× body, upper wick nhỏ, downtrend (close < sma20)
    sma20 = c.rolling(20).mean()
    df["is_hammer"] = (
        (lower_wick >= 2 * body) &
        (upper_wick <= 0.3 * body) &
        (c < sma20)
    ).astype(int)

    # Bullish Engulfing: nến hôm nay dương bọc toàn bộ nến âm hôm qua
    prev_o, prev_c = o.shift(1), c.shift(1)
    df["is_bull_engulfing"] = (
        (c > o) & (prev_c < prev_o) &
        (o < prev_c) & (c > prev_o)
    ).astype(int)

    # Bearish Engulfing
    df["is_bear_engulfing"] = (
        (c < o) & (prev_c > prev_o) &
        (o > prev_c) & (c < prev_o)
    ).astype(int)

    # Morning Star (3 nến): nến -2 âm lớn, nến -1 body nhỏ (indecision), nến 0 dương lớn
    c2, o2 = c.shift(2), o.shift(2)   # 2 phiên trước
    c1, o1 = c.shift(1), o.shift(1)   # 1 phiên trước
    body2 = (c2 - o2).abs()
    body1 = (c1 - o1).abs()

    df["is_morning_star"] = (
        (c2 < o2) & (body2 > body2.rolling(20).mean()) &
        (body1 < 0.3 * body2) &
        (c > o) & (c > (o2 + c2) / 2)
    ).astype(int)

    # Evening Star (ngược lại morning star)
    df["is_evening_star"] = (
        (c2 > o2) & (body2 > body2.rolling(20).mean()) &
        (body1 < 0.3 * body2) &
        (c < o) & (c < (o2 + c2) / 2)
    ).astype(int)

    return df


def compute_target(df: pd.DataFrame) -> pd.DataFrame:
    future_close_5  = df["close"].shift(-5)
    future_close_10 = df["close"].shift(-10)

    df["return_5d"]    = (future_close_5 - df["close"]) / df["close"]
    df["direction_5d"] = (df["return_5d"] > 0).astype(float)
    df["direction_5d"] = df["direction_5d"].where(future_close_5.notna(), np.nan)

    ret10 = (future_close_10 - df["close"]) / df["close"]
    df["direction_10d"] = (ret10 > 0).astype(float)
    df["direction_10d"] = df["direction_10d"].where(future_close_10.notna(), np.nan)
    return df


# ─────────────────────────────────────────────
# 4. PIPELINE CHÍNH
# ─────────────────────────────────────────────

INDICATOR_COLS = [
    "symbol", "trading_date",
    "sma_10", "sma_20", "sma_50", "ema_12", "ema_26",
    "macd", "macd_signal", "macd_hist", "adx_14", "di_plus", "di_minus",
    "rsi_14", "stoch_k", "stoch_d", "williams_r",
    "bb_upper", "bb_mid", "bb_lower", "bb_pct_b", "bb_width", "atr_14",
    "obv", "vwap", "cmf_20", "volume_sma_20", "volume_ratio",
    "candle_body_size", "candle_upper_wick", "candle_lower_wick",
    "is_doji", "is_hammer", "is_bull_engulfing", "is_bear_engulfing",
    "is_morning_star", "is_evening_star",
    "direction_5d", "direction_10d", "return_5d",
]


def upsert_indicators(df_result: pd.DataFrame):
    """Upsert từng batch 500 dòng vào technical_indicators."""
    cols = [c for c in INDICATOR_COLS if c in df_result.columns]
    df_result = df_result[cols].copy()

    # Chuyển đổi NaN thành None (NULL trong SQL) để tránh lỗi SMALLINT out of range
    df_result = df_result.replace({np.nan: None})

    rows = df_result.to_dict(orient="records")
    col_names = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in ("symbol", "trading_date")])

    upsert_sql = text(f"""
        INSERT INTO technical_indicators ({col_names})
        VALUES ({placeholders})
        ON CONFLICT (symbol, trading_date)
        DO UPDATE SET {update_set}
    """)

    CHUNK = 500
    with engine.begin() as conn:
        for i in range(0, len(rows), CHUNK):
            conn.execute(upsert_sql, rows[i:i+CHUNK])


def process_symbol(symbol: str, daily_mode: bool = False) -> int:
    """Tải quote_history, tính chỉ báo, upsert. Trả về số dòng đã ghi."""
    # Cần ít nhất 60 nến lịch sử để tính SMA50 và các chỉ báo rolling khác
    if daily_mode:
        query = text("""
            SELECT symbol, trading_date, open, high, low, close, volume
            FROM quote_history
            WHERE symbol = :sym
              AND trading_date >= (
                  SELECT MAX(trading_date) - INTERVAL '90 days'
                  FROM quote_history WHERE symbol = :sym
              )
            ORDER BY trading_date
        """)
    else:
        query = text("""
            SELECT symbol, trading_date, open, high, low, close, volume
            FROM quote_history
            WHERE symbol = :sym
            ORDER BY trading_date
        """)

    df = pd.read_sql(query, engine, params={"sym": symbol})
    if len(df) < 30:
        return 0

    df = df.sort_values("trading_date").reset_index(drop=True)

    # Ép kiểu tránh lỗi tính toán
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close", "volume"])

    df = compute_trend(df)
    df = compute_volatility(df)
    df = compute_volume(df)
    df = compute_candles(df)
    df = compute_target(df)

    # Trong daily mode chỉ upsert phần dữ liệu gần đây (tránh rewrite toàn bộ)
    if daily_mode:
        cutoff = df["trading_date"].max() - pd.Timedelta(days=14)
        df = df[df["trading_date"] >= cutoff]

    upsert_indicators(df)
    return len(df)


def get_all_symbols() -> list[str]:
    df = pd.read_sql("SELECT DISTINCT symbol FROM quote_history ORDER BY symbol", engine)
    return df["symbol"].tolist()


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]


def main():
    parser = argparse.ArgumentParser(description="Tính chỉ báo kỹ thuật cho VNSTOCK data")
    parser.add_argument("--daily",  action="store_true", help="Chỉ cập nhật 14 ngày gần nhất")
    parser.add_argument("--symbol", nargs="*",           help="Chỉ tính cho các mã cụ thể")
    args = parser.parse_args()

    print("=" * 70)
    print("TÍNH CHỈ BÁO KỸ THUẬT — TECHNICAL INDICATORS")
    print("=" * 70)

    create_table()

    if args.symbol:
        symbols = args.symbol
        print(f"[*] Chế độ thủ công: {symbols}")
    elif args.daily:
        symbols = get_all_symbols()
        print(f"[*] Chế độ DAILY: cập nhật {len(symbols)} mã, 14 ngày gần nhất")
    else:
        symbols = get_all_symbols()
        print(f"[*] Chế độ FULL: tính toàn bộ {len(symbols)} mã")

    BATCH_SIZE = 100
    total = len(symbols)
    success = 0
    total_rows = 0

    for batch_idx, batch in enumerate(chunk_list(symbols, BATCH_SIZE)):
        print(f"\n{'='*50}")
        print(f"BATCH {batch_idx+1}/{math.ceil(total/BATCH_SIZE)} — {len(batch)} mã")
        print("="*50)

        for symbol in batch:
            try:
                n = process_symbol(symbol, daily_mode=args.daily)
                if n > 0:
                    success += 1
                    total_rows += n
                    print(f"  ✔ {symbol}: {n} dòng")
                else:
                    print(f"  — {symbol}: bỏ qua (không đủ dữ liệu)")
            except Exception as e:
                print(f"  ✗ {symbol}: {e}")

        pct = round((batch_idx + 1) * BATCH_SIZE / total * 100, 1)
        print(f"\n  📊 Progress: ~{min(pct, 100)}% | Thành công: {success} mã | {total_rows:,} dòng")
        time.sleep(0.5)

    print(f"\n{'='*70}")
    print(f"🎉 XONG! Đã tính chỉ báo cho {success}/{total} mã ({total_rows:,} dòng)")
    print(f"   Bảng lưu: technical_indicators")
    print("="*70)


if __name__ == "__main__":
    main()
