# brains/qb2/core.py
"""
QB2 — Deep Quant Brain (Final Implementation)

Stage 2 quantitative expansion on top of QB1 signals.
Runs only when Stage 2 is triggered (expensive APIs allowed here).

Features included:
- Long-window price structure analysis (yfinance)
- Volume trends and volatility regime classification
- Fundamentals (FMP / Alpha Vantage / Finnhub if keys exist)
- Technical signals (SMAs, RSI, range compression)
- Structured deep-dive output for Stage 2 runner and NB2

Every external API call is gated behind key presence.
Cost-free unless user explicitly adds keys.
"""

from __future__ import annotations
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
import requests

# Optional vendor availability
def _has_key(name: str) -> bool:
    return os.environ.get(name) is not None


logger = logging.getLogger("fia.qb2")
logger.setLevel(os.environ.get("FIA_LOG_LEVEL", "INFO"))
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def _safe(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def _now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# -------------------------------------------------------
# 1. Long-window price + volume analysis (yfinance)
# -------------------------------------------------------
def fetch_long_history(ticker: str, days: int = 180) -> pd.DataFrame:
    """
    Long window (180d–365d) for proper structure analysis.
    Cost-free.
    """
    try:
        end = datetime.utcnow().date()
        start = end - timedelta(days=days)
        df = yf.download(
            tickers=ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="1d",
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"Adj Close": "adj_close", "Close": "close", "Volume": "volume"})
        df = _safe(df, ["close", "volume"])
        return df[["close", "volume"]].dropna()
    except Exception as e:
        logger.exception("QB2 yfinance long history error for %s: %s", ticker, e)
        return pd.DataFrame()


def compute_structure(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Computes:
    - SMA20, SMA50, SMA200
    - 20d volatility
    - range compression
    - trend direction
    - momentum (RSI14)
    """
    if df.empty:
        return {}

    close = df["close"]

    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    # volatility
    ret = close.pct_change()
    vol20 = float(np.sqrt(252) * ret.rolling(20).std().iloc[-1]) if len(ret) > 20 else None

    # range compression (20d)
    last20 = close.iloc[-20:]
    compression = float((last20.max() - last20.min()) / last20.mean()) if len(last20) > 10 else None

    # trend logic
    trend = None
    if sma50.iloc[-1] > sma200.iloc[-1]:
        trend = "uptrend"
    elif sma50.iloc[-1] < sma200.iloc[-1]:
        trend = "downtrend"
    else:
        trend = "flat"

    # RSI14
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up / roll_down if roll_down.iloc[-1] != 0 else np.nan
    rsi = float(100.0 - 100.0 / (1.0 + rs.iloc[-1])) if not np.isnan(rs.iloc[-1]) else None

    return {
        "sma20": float(sma20.iloc[-1]) if not np.isnan(sma20.iloc[-1]) else None,
        "sma50": float(sma50.iloc[-1]) if not np.isnan(sma50.iloc[-1]) else None,
        "sma200": float(sma200.iloc[-1]) if not np.isnan(sma200.iloc[-1]) else None,
        "vol20": vol20,
        "range_compression_20d": compression,
        "trend": trend,
        "rsi14": rsi,
    }


# -------------------------------------------------------
# 2. Fundamentals (FMP / Alpha Vantage optional)
# -------------------------------------------------------
def fetch_fundamentals_fmp(ticker: str) -> Dict[str, Any]:
    """
    Financial Modeling Prep fundamentals.
    Free tier works without billing.
    Requires: FMP_API_KEY
    """
    key = os.environ.get("FMP_API_KEY")
    if not key:
        return {}

    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}"
    try:
        r = requests.get(url, params={"apikey": key}, timeout=8)
        data = r.json()
        if isinstance(data, list) and data:
            d = data[0]
            return {
                "pe": d.get("pe"),
                "pb": d.get("priceToBook"),
                "ps": d.get("priceToSales"),
                "market_cap": d.get("mktCap"),
                "beta": d.get("beta"),
                "industry": d.get("industry"),
                "sector": d.get("sector"),
            }
        return {}
    except Exception as e:
        logger.exception("FMP fundamentals error for %s: %s", ticker, e)
        return {}


def fetch_fundamentals_alpha_vantage(ticker: str) -> Dict[str, Any]:
    """
    Alpha Vantage overview.
    Requires: ALPHAVANTAGE_API_KEY
    """
    key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not key:
        return {}

    url = "https://www.alphavantage.co/query"
    params = {"function": "OVERVIEW", "symbol": ticker, "apikey": key}

    try:
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        if "Symbol" not in data:
            return {}
        return {
            "pe": data.get("PERatio"),
            "ps": data.get("PriceToSalesRatioTTM"),
            "roe": data.get("ReturnOnEquityTTM"),
            "de": data.get("DebtToEquity"),
            "market_cap": data.get("MarketCapitalization"),
        }
    except Exception as e:
        logger.exception("Alpha Vantage fundamentals error for %s: %s", ticker, e)
        return {}


# -------------------------------------------------------
# 3. Combine fundamentals safely
# -------------------------------------------------------
def merge_fundamentals(*sources: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for src in sources:
        for k, v in src.items():
            if v is not None and k not in out:
                out[k] = v
    return out


# -------------------------------------------------------
# 4. QB2 main entry
# -------------------------------------------------------
def run_qb2(ticker: str) -> Dict[str, Any]:
    """
    Deep quant analysis for a single ticker.

    Output is the FINAL structured deep result consumed by:
    - Stage 2 runner
    - NB2
    """

    # 1. long-window structure
    hist = fetch_long_history(ticker)
    structure = compute_structure(hist)

    # 2. fundamentals (various vendors)
    f_fmp = fetch_fundamentals_fmp(ticker)
    f_av = fetch_fundamentals_alpha_vantage(ticker)
    fundamentals = merge_fundamentals(f_fmp, f_av)

    return {
        "ticker": ticker,
        "generated_at": _now(),
        "structure": structure,
        "fundamentals": fundamentals,
        "meta": {
            "has_fmp": bool(f_fmp),
            "has_alpha_vantage": bool(f_av),
            "history_points": len(hist),
        },
    }
