
import json
import logging
import argparse
import numpy as np
import pandas as pd
from dataclasses import dataclass, asdict, field
from typing import Optional
from sqlalchemy import text
from timescale_utils import DatabaseManager

LOG = logging.getLogger("wyckoff")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Vietnamese exchange price limits
HOSE_LIMIT = 0.07
HNX_LIMIT = 0.10
UPCOM_LIMIT = 0.15

@dataclass
class WyckoffEvent:
    date: str
    kind: str
    price: float
    volume: int
    confidence: float
    note: str = ""

@dataclass
class WyckoffAnalysis:
    symbol: str
    phase: str                         # "A" | "B" | "C" | "D" | "D/E" | "N/A"
    schematic: str                     # "accumulation" | "distribution" | "neutral"
    tr_low: Optional[float]
    tr_high: Optional[float]
    events: list = field(default_factory=list)
    entry_zone: Optional[dict] = None
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    risk_reward: Optional[float] = None
    narrative_vi: str = ""
    last_close: Optional[float] = None
    last_date: Optional[str] = None
    vsa_signals: list = field(default_factory=list)

def _to_date_str(val) -> str:
    if hasattr(val, "date") and callable(getattr(val, "date")):
        return str(val.date())
    return str(val)

# ---------- indicators ----------
def _atr(df: pd.DataFrame, n: int = 20) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"].shift(1)
    tr = pd.concat([(h - l).abs(), (h - c).abs(), (l - c).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean()

def _vol_percentile(v: pd.Series, n: int = 60) -> pd.Series:
    return v.rolling(n, min_periods=10).apply(
        lambda x: x.rank(pct=True).iloc[-1], raw=False
    )

def _spread(df: pd.DataFrame) -> pd.Series:
    return (df["high"] - df["low"]).clip(lower=1e-6)

def _close_pos(df: pd.DataFrame) -> pd.Series:
    rng = _spread(df)
    return ((df["close"] - df["low"]) / rng).clip(0, 1)

def _is_limit_down(df: pd.DataFrame, limit: float = HOSE_LIMIT) -> pd.Series:
    prev = df["close"].shift(1)
    return df["close"] <= prev * (1 - limit) * 1.002

def _is_limit_up(df: pd.DataFrame, limit: float = HOSE_LIMIT) -> pd.Series:
    prev = df["close"].shift(1)
    return df["close"] >= prev * (1 + limit) * 0.998

# ---------- VSA Detection ----------
def detect_vsa(df: pd.DataFrame) -> list:
    """Basic VSA signal detection."""
    signals = []
    if len(df) < 5:
        return signals
    
    df = df.copy()
    df["spread"] = _spread(df)
    df["vol_ma20"] = df["volume"].rolling(20).mean()
    df["close_pos"] = _close_pos(df)
    
    for i in range(2, len(df)):
        row = df.iloc[i]
        prev1 = df.iloc[i-1]
        prev2 = df.iloc[i-2]
        
        date_str = _to_date_str(row["trading_date"])
        price = float(row["close"])
        
        # 1. No Demand: Up bar, narrow spread, vol < prev 2
        if row["close"] > prev1["close"] and row["spread"] < df["spread"].rolling(20).mean().iloc[i] \
           and row["volume"] < prev1["volume"] and row["volume"] < prev2["volume"]:
            signals.append({
                "date": date_str, "signal": "NoDemand", "price": price,
                "desc_vi": "Cầu yếu: Giá tăng nhưng spread hẹp và vol thấp"
            })
            
        # 2. No Supply: Down bar, narrow spread, vol < prev 2
        elif row["close"] < prev1["close"] and row["spread"] < df["spread"].rolling(20).mean().iloc[i] \
             and row["volume"] < prev1["volume"] and row["volume"] < prev2["volume"]:
            signals.append({
                "date": date_str, "signal": "NoSupply", "price": price,
                "desc_vi": "Cung cạn: Giá giảm nhưng spread hẹp và vol rất thấp"
            })
            
        # 3. Stopping Volume: Down bar, wide spread, high vol, closes high
        elif row["close"] < prev1["close"] and row["spread"] > df["spread"].rolling(20).mean().iloc[i] \
             and row["volume"] > 1.5 * row["vol_ma20"] and row["close_pos"] > 0.6:
            signals.append({
                "date": date_str, "signal": "StoppingVolume", "price": price,
                "desc_vi": "Stopping Volume: Lực cầu hấp thụ mạnh ở vùng giá thấp"
            })
            
        # 4. Shakeout: Wide spread, low then close high, high vol
        elif row["low"] < prev1["low"] and row["close_pos"] > 0.8 and row["spread"] > 1.2 * df["spread"].rolling(20).mean().iloc[i]:
            signals.append({
                "date": date_str, "signal": "Shakeout", "price": price,
                "desc_vi": "Shakeout: Rũ bỏ nhà đầu tư yếu trước khi tăng"
            })
            
    return signals

# ---------- Wyckoff Detection ----------
def detect_wyckoff(df: pd.DataFrame, symbol: str = "", exchange: str = "HOSE") -> WyckoffAnalysis:
    if len(df) < 120:
        return WyckoffAnalysis(
            symbol=symbol, phase="N/A", schematic="neutral",
            tr_low=None, tr_high=None,
            narrative_vi="Không đủ dữ liệu (<120 phiên).",
            last_close=float(df["close"].iloc[-1]) if len(df) else None,
            last_date=str(df["trading_date"].iloc[-1].date()) if hasattr(df["trading_date"].iloc[-1], "date") else str(df["trading_date"].iloc[-1]),
        )

    df = df.copy().reset_index(drop=True)
    limit = {"HOSE": HOSE_LIMIT, "HSX": HOSE_LIMIT, "HNX": HNX_LIMIT, "UPCOM": UPCOM_LIMIT}.get(exchange, HOSE_LIMIT)

    df["atr20"] = _atr(df, 20)
    df["vol_pct60"] = _vol_percentile(df["volume"], 60)
    df["spread"] = _spread(df)
    df["close_pos"] = _close_pos(df)
    df["limit_down"] = _is_limit_down(df, limit)
    df["limit_up"] = _is_limit_up(df, limit)

    events: list[WyckoffEvent] = []
    n = len(df)

    # SC
    sc_idx = None
    search_start = max(20, n - 250)
    sc_mask = ((df["vol_pct60"] >= 0.95) & (df["spread"] >= 1.8 * df["atr20"]) & (df["close_pos"] <= 0.4)) | (df["limit_down"].astype(int).rolling(3).sum() >= 2)
    for i in range(search_start, n):
        if sc_mask.iloc[i]:
            if sc_idx is None or df["low"].iloc[i] < df["low"].iloc[sc_idx]: sc_idx = i
    if sc_idx is not None:
        events.append(WyckoffEvent(date=_to_date_str(df["trading_date"].iloc[sc_idx]), kind="SC", price=float(df["low"].iloc[sc_idx]), volume=int(df["volume"].iloc[sc_idx]), confidence=0.85, note="Selling Climax"))

    # AR
    ar_idx = None
    if sc_idx is not None and sc_idx + 3 < n:
        window = df.iloc[sc_idx + 1:min(sc_idx + 40, n)]
        if len(window) > 0:
            ar_idx = int(window["high"].idxmax())
            events.append(WyckoffEvent(date=_to_date_str(df["trading_date"].iloc[ar_idx]), kind="AR", price=float(df["high"].iloc[ar_idx]), volume=int(df["volume"].iloc[ar_idx]), confidence=0.8, note="Automatic Rally"))

    # ST
    st_idx = None
    if sc_idx is not None and ar_idx is not None and ar_idx + 3 < n:
        win = df.iloc[ar_idx + 1:min(ar_idx + 60, n)]
        cand = win[(win["low"] <= df["low"].iloc[sc_idx] * 1.04) & (win["low"] >= df["low"].iloc[sc_idx] * 0.96) & (win["volume"] < df["volume"].iloc[sc_idx] * 0.75)]
        if len(cand) > 0:
            st_idx = int(cand.index[0])
            events.append(WyckoffEvent(date=_to_date_str(df["trading_date"].iloc[st_idx]), kind="ST", price=float(df["low"].iloc[st_idx]), volume=int(df["volume"].iloc[st_idx]), confidence=0.75, note="Secondary Test"))

    tr_low = float(min(df["low"].iloc[sc_idx], df["low"].iloc[st_idx])) if (sc_idx and st_idx) else (float(df["low"].iloc[sc_idx]) if sc_idx else None)
    tr_high = float(df["high"].iloc[ar_idx]) if ar_idx else None

    # Spring
    spring_idx = None
    if tr_low and ar_idx:
        for i in range((st_idx or ar_idx) + 1, n):
            if df["low"].iloc[i] < tr_low - 0.3 * df["atr20"].iloc[i] and df["close"].iloc[i] > tr_low and df["vol_pct60"].iloc[i] <= 0.5:
                spring_idx = i
                events.append(WyckoffEvent(date=_to_date_str(df["trading_date"].iloc[i]), kind="Spring", price=float(df["low"].iloc[i]), volume=int(df["volume"].iloc[i]), confidence=0.9, note="Spring"))
                break

    # SOS
    sos_idx = None
    if tr_high:
        for i in range((spring_idx or st_idx or ar_idx or 0) + 1, n):
            if df["close"].iloc[i] > tr_high and df["vol_pct60"].iloc[i] >= 0.8 and df["spread"].iloc[i] >= 1.5 * df["atr20"].iloc[i]:
                sos_idx = i
                events.append(WyckoffEvent(date=_to_date_str(df["trading_date"].iloc[i]), kind="SOS", price=float(df["low"].iloc[i]), volume=int(df["volume"].iloc[i]), confidence=0.85, note="Sign of Strength"))
                break

    # Phase & Schematic
    if spring_idx and not sos_idx: phase = "C"
    elif sos_idx: phase = "D/E" if (sos_idx + 5 < n) else "D"
    elif st_idx: phase = "B"
    elif sc_idx: phase = "A"
    else: phase = "N/A"

    schematic = "accumulation" if (spring_idx or sos_idx or sc_idx) else "neutral"

    # Entry/Stop/Target
    entry_zone, stop, target, rr = None, None, None, None
    if spring_idx:
        entry_lo = float(df["close"].iloc[spring_idx])
        entry_hi = entry_lo * 1.02
        stop = float(df["low"].iloc[spring_idx]) * 0.97
        target = tr_high if tr_high else entry_lo * 1.15
        entry_zone = {"low": entry_lo, "high": entry_hi, "reason": "Spring"}
        rr = round((target - entry_lo) / max(entry_lo - stop, 1), 2)

    vsa_sigs = detect_vsa(df)
    
    analysis = WyckoffAnalysis(
        symbol=symbol, phase=phase, schematic=schematic,
        tr_low=tr_low, tr_high=tr_high,
        events=[asdict(e) for e in events],
        entry_zone=entry_zone, stop_loss=stop, target=target, risk_reward=rr,
        narrative_vi=f"Phân tích Wyckoff cho {symbol}. Phase: {phase}, Schematic: {schematic}.",
        last_close=float(df["close"].iloc[-1]),
        last_date=_to_date_str(df["trading_date"].iloc[-1]),
        vsa_signals=vsa_sigs
    )
    return analysis

# ---------- DB Integration ----------
class WyckoffService:
    def __init__(self):
        self.db = DatabaseManager()
    
    def process_all_symbols(self):
        with self.db.engine.connect() as conn:
            symbols_df = pd.read_sql("SELECT symbol, exchange FROM listing", conn)
        
        for _, row in symbols_df.iterrows():
            sym = row["symbol"]
            exc = row["exchange"]
            try:
                LOG.info(f"Processing {sym}...")
                df = self.db.query_ohlcv(sym, "2020-01-01", "2030-01-01")
                if len(df) < 120: continue
                
                # Fetch foreign net flow if exists
                with self.db.engine.connect() as conn:
                    tr_df = pd.read_sql(text("SELECT trading_date, fr_buy_volume - fr_sell_volume as foreign_net FROM trading WHERE symbol = :s"), conn, params={"s": sym})
                
                if not tr_df.empty:
                    # Merge and drop the redundant trading_date from tr_df
                    df = df.merge(tr_df, left_on="period", right_on="trading_date", how="left")
                    if "trading_date" in df.columns and "period" in df.columns:
                        df = df.drop(columns=["trading_date"])
                
                # Always ensure we have 'trading_date' column
                if "period" in df.columns:
                    df = df.rename(columns={"period": "trading_date"})
                
                df = df.fillna(0)
                if "foreign_net" not in df.columns:
                    df["foreign_net"] = 0

                analysis = detect_wyckoff(df, symbol=sym, exchange=exc)
                self.save_analysis(analysis)
            except Exception as e:
                LOG.error(f"Error processing {sym}: {e}")

    def save_analysis(self, a: WyckoffAnalysis):
        data = asdict(a)
        data_json = json.dumps({
            "events": data["events"],
            "entry_zone": data["entry_zone"],
            "narrative_vi": data["narrative_vi"],
            "vsa_signals": data["vsa_signals"]
        }, ensure_ascii=False)
        
        sql = text("""
            INSERT INTO wyckoff_analysis (symbol, phase, schematic, tr_low, tr_high, last_close, last_date, risk_reward, data_json, updated_at)
            VALUES (:sym, :phase, :schematic, :tr_low, :tr_high, :last_close, :last_date, :rr, :data, NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                phase = EXCLUDED.phase, schematic = EXCLUDED.schematic,
                tr_low = EXCLUDED.tr_low, tr_high = EXCLUDED.tr_high,
                last_close = EXCLUDED.last_close, last_date = EXCLUDED.last_date,
                risk_reward = EXCLUDED.risk_reward, data_json = EXCLUDED.data_json,
                updated_at = NOW()
        """)
        
        with self.db.engine.begin() as conn:
            conn.execute(sql, {
                "sym": a.symbol, "phase": a.phase, "schematic": a.schematic,
                "tr_low": a.tr_low, "tr_high": a.tr_high, "last_close": a.last_close,
                "last_date": a.last_date, "rr": a.risk_reward, "data": data_json
            })

if __name__ == "__main__":
    service = WyckoffService()
    service.process_all_symbols()
