# brains/qb1/core.py
from __future__ import annotations
from typing import List
from pydantic import BaseModel
import yfinance as yf
import numpy as np
from fia.config_loader import get_config

class QB1Output(BaseModel):
    ticker: str
    queries: List[str]

# Utility: default universe if config has none
DEFAULT_UNIVERSE = ["AAPL","SPY","QQQ","TSLA","MSFT"]

def load_universe() -> List[str]:
    cfg = get_config()
    u = cfg.paths.static_universe
    if u and isinstance(u, list) and len(u) > 0:
        return [str(x).upper() for x in u]
    return DEFAULT_UNIVERSE

def fetch_price_history(ticker: str, days: int = 14, interval: str = "1d"):
    try:
        t = yf.Ticker(ticker)
        df = t.history(period=f"{days}d", interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None

def compute_simple_z(df):
    if df is None or "Close" not in df.columns:
        return 0.0
    close = df["Close"].dropna().astype(float)
    if len(close) < 3:
        return 0.0
    series = close[-10:]
    mu = series.mean()
    sigma = series.std(ddof=0) if len(series) > 1 else 0.0
    if sigma == 0:
        return 0.0
    return float((series.iloc[-1] - mu) / sigma)

def build_queries_from_signal(ticker: str, z_score: float):
    base = [
        f"{ticker} price action explanation",
        f"{ticker} recent earnings reaction",
        f"{ticker} analyst upgrades downgrades",
        f"{ticker} upcoming catalysts",
    ]
    if abs(z_score) >= 2.0:
        base.insert(0, f"{ticker} abnormal price movement z={z_score:.2f}")
    return base

def run_qb1() -> List[QB1Output]:
    cfg = get_config()
    universe = load_universe()
    out = []
    for t in universe:
        df = fetch_price_history(t, days=int(cfg.api.yfinance.get("history_days",14)))
        z = compute_simple_z(df)
        queries = build_queries_from_signal(t, z)
        out.append(QB1Output(ticker=t, queries=queries))
    return out

if __name__ == "__main__":
    res = run_qb1()
    for r in res:
        print(r.model_dump_json(indent=2))
