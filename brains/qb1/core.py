# brains/qb1/core.py
"""
QB1 core implementation for FIA Stage1.
Production-ready implementations for:
- load_universe_from_sheets
- fetch_price_history
- compute_price_anomaly
- fetch_realtime_price
- compute_macro_weight
- detect_news_presence
- build_stage1_signal
- run_qb1

Design & rules:
- No placeholders. Robust to missing keys.
- All external API usage is gated by presence of API keys in env or config.
- Uses fia.config_loader.get_config() for runtime configuration.
"""

from __future__ import annotations

import os
import json
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests
import yfinance as yf

# Optional imports (wrapped to fail-safe)
try:
    from fredapi import Fred
except Exception:
    Fred = None

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
except Exception:
    service_account = None
    build = None

# Import project helpers
try:
    from fia.config_loader import get_config
except Exception:
    # If import fails, provide a simple fallback to read defaults from env - but prefer to surface the import error.
    def get_config() -> Dict[str, Any]:
        logging.warning("Could not import fia.config_loader.get_config; using minimal defaults from environment.")
        return {
            "run_settings": {"live_mode": False},
            "paths": {},
            "trigger_thresholds": {"score": 2.0},
            "api": {},
            "secrets": {}
        }

# Module-level logger
logger = logging.getLogger("fia.qb1")
logger.setLevel(os.environ.get("FIA_LOG_LEVEL", "INFO"))
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)


# --------------------------
# Utility helpers
# --------------------------
def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# --------------------------
# Primary functions
# --------------------------
def load_universe_from_sheets(config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Load the universe from Google Sheets.
    Expects:
      - sheet_id at config['paths']['universe_sheet_id'] OR config['secrets']['GOOGLE_SHEETS_ID']
      - range at config['paths']['universe_range'] (default: 'Universe!A:C')
      - credentials JSON either as path or as JSON string in env 'GOOGLE_SHEETS_CREDENTIALS'
        or config['secrets']['GOOGLE_SHEETS_CREDENTIALS'].

    Returns: list of { "ticker": "...", "name": "...", "meta": {...} }
    """
    config = config or get_config()
    sheet_id = _safe_get(config, "paths", "universe_sheet_id") or _safe_get(config, "secrets", "GOOGLE_SHEETS_ID")
    sheet_range = _safe_get(config, "paths", "universe_range", default="Universe!A:C")
    creds_payload = os.environ.get("GOOGLE_SHEETS_CREDENTIALS") or _safe_get(config, "secrets", "GOOGLE_SHEETS_CREDENTIALS")

    if not sheet_id:
        logger.info("No universe sheet_id configured; falling back to config.static_universe if present.")
        static_universe = _safe_get(config, "paths", "static_universe") or _safe_get(config, "static_universe")
        if static_universe:
            return static_universe
        return []

    if not build or not service_account:
        logger.warning("googleapiclient not available in environment. Returning empty universe.")
        return []

    # Load credentials
    credentials = None
    try:
        if creds_payload:
            # If creds_payload looks like a file path and exists, use it; otherwise treat as JSON string
            if os.path.exists(creds_payload):
                credentials = service_account.Credentials.from_service_account_file(creds_payload, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
            else:
                # assume JSON string
                cred_dict = json.loads(creds_payload)
                credentials = service_account.Credentials.from_service_account_info(cred_dict, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
        else:
            logger.warning("No Google Sheets credentials provided; cannot read universe. Returning empty list.")
            return []
    except Exception as e:
        logger.exception("Failed to create Google Sheets credentials: %s", e)
        return []

    try:
        service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
        values = result.get("values", [])
        universe = []
        # Expect header row; try to parse gracefully
        if not values:
            return []
        headers = [h.strip().lower() for h in values[0]]
        for row in values[1:]:
            row_dict = {}
            for i, h in enumerate(headers):
                row_dict[h] = row[i] if i < len(row) else ""
            # Normalize ticker field
            ticker = row_dict.get("ticker") or row_dict.get("symbol") or row_dict.get("tick")
            name = row_dict.get("name") or row_dict.get("company") or ""
            if ticker:
                universe.append({"ticker": ticker.strip(), "name": name.strip(), "meta": row_dict})
        return universe
    except Exception as e:
        logger.exception("Failed to read Google Sheets universe: %s", e)
        return []


def fetch_price_history(ticker: str, config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Fetch price history using yfinance.
    Config options read:
      - config['api']['yfinance']['history_days'] (default 14)
      - config['api']['yfinance']['interval'] ('1d' or '60m' or '1m')
    Returns pandas DataFrame with DatetimeIndex and 'close' column (if available).
    """
    config = config or get_config()
    days = int(_safe_get(config, "api", "yfinance", "history_days", default=14))
    interval = _safe_get(config, "api", "yfinance", "interval", default="1d")

    end = datetime.utcnow().date()
    start = end - timedelta(days=days)
    try:
        # yfinance returns timezone-aware index; normalize
        ticker_obj = yf.Ticker(ticker)
        hist = ticker_obj.history(start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(), interval=interval, auto_adjust=False)
        if hist is None or hist.empty:
            logger.info("yfinance returned no history for %s", ticker)
            return pd.DataFrame(columns=["close"])
        df = hist.copy()
        # prefer 'Close' or 'close'
        if "Close" in df.columns:
            df = df.rename(columns={"Close": "close"})
        elif "close" in df.columns:
            pass
        else:
            # attempt to pick last numeric column as close
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            if numeric_cols:
                df = df.rename(columns={numeric_cols[-1]: "close"})
            else:
                df["close"] = np.nan
        df = df[["close"]].dropna()
        # ensure index is datetime
        df.index = pd.to_datetime(df.index)
        return df
    except Exception as e:
        logger.exception("Error fetching price history for %s: %s", ticker, e)
        return pd.DataFrame(columns=["close"])


def compute_price_anomaly(history_df: pd.DataFrame, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Compute a z-score style anomaly measure and volatility estimate.
    Returns dict: { "z_score": float, "volatility": float, "n_points": int }
    If insufficient data, returns zeros.
    """
    config = config or get_config()
    lookback = int(_safe_get(config, "trigger_thresholds", "anomaly_lookback", default=10))
    if history_df is None or history_df.empty:
        return {"z_score": 0.0, "volatility": 0.0, "n_points": 0}

    try:
        # use recent lookback window
        series = history_df["close"].dropna().astype(float)
        if series.empty:
            return {"z_score": 0.0, "volatility": 0.0, "n_points": 0}

        recent = series[-lookback:]
        mu = recent.mean()
        sigma = recent.std(ddof=0) if len(recent) > 1 else 0.0
        # compare latest to mean
        latest = recent.iloc[-1]
        z = 0.0
        if sigma and sigma > 0:
            z = (latest - mu) / sigma
        # volatility estimate: annualized simple based on std of returns
        returns = series.pct_change().dropna()
        vol = float(np.sqrt(252) * returns.std()) if not returns.empty else 0.0

        return {"z_score": float(z), "volatility": float(vol), "n_points": int(len(series))}
    except Exception as e:
        logger.exception("Error computing price anomaly: %s", e)
        return {"z_score": 0.0, "volatility": 0.0, "n_points": 0}


def fetch_realtime_price(ticker: str, config: Optional[Dict[str, Any]] = None) -> Optional[float]:
    """
    Fetch last price via Twelve Data (if API key available).
    Twelve Data price endpoint: https://api.twelvedata.com/price?symbol={symbol}&apikey={key}
    Returns float price or None if not available.
    """
    config = config or get_config()
    api_key = os.environ.get("TWELVE_DATA_KEY") or _safe_get(config, "secrets", "TWELVE_DATA_KEY")
    if not api_key:
        logger.debug("TWELVE_DATA_KEY missing; skipping realtime price for %s", ticker)
        return None

    url = "https://api.twelvedata.com/price"
    params = {"symbol": ticker, "apikey": api_key}
    try:
        resp = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            logger.warning("Twelve Data non-200 for %s: %s %s", ticker, resp.status_code, resp.text[:200])
            return None
        data = resp.json()
        # expected key 'price'
        price = data.get("price")
        if price is None:
            logger.debug("Twelve Data response missing price for %s: %s", ticker, data)
            return None
        return float(price)
    except Exception as e:
        logger.exception("Error fetching realtime price for %s: %s", ticker, e)
        return None


def compute_macro_weight(config: Optional[Dict[str, Any]] = None) -> float:
    """
    Compute a compact macro weight from FRED indicators (CPI and 10Y yield).
    Returns a float between -2 and +2 (not strictly normalized).
    Positive weight suggests macro tailwinds for risk assets; negative suggests headwinds.
    Gates: only runs if FRED_API_KEY present and fredapi is importable.
    """
    config = config or get_config()
    fred_key = os.environ.get("FRED_API_KEY") or _safe_get(config, "secrets", "FRED_API_KEY")
    if not fred_key or Fred is None:
        logger.debug("FRED key or fredapi not available; returning macro weight 0.0")
        return 0.0

    try:
        fred = Fred(api_key=fred_key)
        # CPI (CPIAUCSL) and 10-year (DGS10)
        end = datetime.utcnow().date()
        start = end - timedelta(days=90)  # short-term window for Stage1
        cpi = fred.get_series("CPIAUCSL", observation_start=start.isoformat(), observation_end=end.isoformat())
        d10 = fred.get_series("DGS10", observation_start=start.isoformat(), observation_end=end.isoformat())

        # Compute simple metrics
        cpi = cpi.dropna()
        d10 = d10.dropna()

        cpi_weight = 0.0
        yield_weight = 0.0
        if not cpi.empty and len(cpi) >= 2:
            # percent change over window
            pct = (cpi.iloc[-1] - cpi.iloc[0]) / float(cpi.iloc[0]) if cpi.iloc[0] != 0 else 0.0
            # higher CPI (rising inflation) -> negative for equities
            cpi_weight = -pct * 100  # scale
        if not d10.empty and len(d10) >= 2:
            # slope of yield (last - first)
            slope = (d10.iloc[-1] - d10.iloc[0])
            # rising yields generally negative for equities
            yield_weight = -slope

        # Combine with simple scaling and clamp
        combined = float(cpi_weight * 1.0 + yield_weight * 0.1)
        # bound to reasonable range
        combined = max(min(combined, 2.0), -2.0)
        return combined
    except Exception as e:
        logger.exception("Error computing macro weight: %s", e)
        return 0.0


def detect_news_presence(ticker: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Detect recent news presence using Finnhub.
    Returns: { "news_count": int, "latest_titles": [str, ...], "since_days": int }
    Key: FINNHUB_API_KEY in env or config['secrets'].
    """
    config = config or get_config()
    api_key = os.environ.get("FINNHUB_API_KEY") or _safe_get(config, "secrets", "FINNHUB_API_KEY")
    lookback_days = int(_safe_get(config, "api", "finnhub", "lookback_days", default=3))
    if not api_key:
        logger.debug("FINNHUB_API_KEY missing; skipping news detection for %s", ticker)
        return {"news_count": 0, "latest_titles": [], "since_days": lookback_days}

    # Finnhub company-news endpoint
    to_date = datetime.utcnow().date()
    from_date = to_date - timedelta(days=lookback_days)
    # Finnhub expects symbol like 'AAPL', optionally with exchange prefix for non-US. We'll pass ticker as-is.
    url = "https://finnhub.io/api/v1/company-news"
    params = {"symbol": ticker, "from": from_date.isoformat(), "to": to_date.isoformat(), "token": api_key}
    try:
        resp = requests.get(url, params=params, timeout=8)
        if resp.status_code != 200:
            logger.warning("Finnhub returned %s for %s: %s", resp.status_code, ticker, resp.text[:200])
            return {"news_count": 0, "latest_titles": [], "since_days": lookback_days}
        data = resp.json()
        if not isinstance(data, list):
            # If the ticker doesn't map to company-news, try general news endpoint (top news)
            # fallback: use /news?category=general for headlines (less ideal)
            logger.debug("Finnhub company-news returned non-list; attempting headlines fallback.")
            fallback_url = "https://finnhub.io/api/v1/news"
            params2 = {"category": "general", "token": api_key}
            resp2 = requests.get(fallback_url, params=params2, timeout=6)
            if resp2.status_code != 200:
                return {"news_count": 0, "latest_titles": [], "since_days": lookback_days}
            data2 = resp2.json()
            titles = [item.get("headline") or item.get("title") for item in (data2 or [])][:5]
            return {"news_count": len(titles), "latest_titles": titles, "since_days": lookback_days}

        # Build list of titles
        titles = []
        for item in data:
            # item may have 'headline' or 'summary'
            t = item.get("headline") or item.get("summary") or item.get("title") or ""
            if t:
                titles.append(t)
        return {"news_count": len(titles), "latest_titles": titles[:5], "since_days": lookback_days}
    except Exception as e:
        logger.exception("Error querying Finnhub for %s: %s", ticker, e)
        return {"news_count": 0, "latest_titles": [], "since_days": lookback_days}


def build_stage1_signal(ticker: str, name: str = "", config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Build a signal for a single ticker by combining:
    - price history anomaly (z-score & vol)
    - realtime price (optional)
    - macro weight
    - news presence

    Returns a dict suitable for inclusion in trigger_context.json 'signals' list.
    """
    config = config or get_config()
    try:
        hist = fetch_price_history(ticker, config=config)
        anomaly = compute_price_anomaly(hist, config=config)
        realtime = fetch_realtime_price(ticker, config=config)
        macro_w = compute_macro_weight(config=config)
        news = detect_news_presence(ticker, config=config)

        # Build a composite score
        # Base: absolute z-score
        z = anomaly.get("z_score", 0.0)
        base = float(z)
        # Weight by macro (amplify if macro supports direction); if macro positive, amplify positive anomalies, else amplify negative anomalies
        score = base * (1.0 + abs(macro_w))
        # Adjust for news
        news_count = int(news.get("news_count", 0))
        score = score + (news_count * 0.4 * np.sign(base if base != 0 else 1.0))

        # Normalize some fields
        signal = {
            "ticker": ticker,
            "name": name,
            "generated_at": _now_iso(),
            "anomaly": anomaly,
            "realtime_price": realtime,
            "macro_weight": float(macro_w),
            "news": news,
            "score": float(score),
        }

        # Provide a short reason string (lightweight NB1 style)
        reason_parts = []
        if abs(z) >= _safe_get(config, "trigger_thresholds", "z_threshold", default=2.0):
            reason_parts.append(f"price z={z:.2f}")
        if news_count:
            reason_parts.append(f"{news_count} recent news")
        if abs(macro_w) > 0.1:
            reason_parts.append(f"macro {macro_w:.2f}")
        signal["reason"] = "; ".join(reason_parts) if reason_parts else "no strong single reason"

        return signal
    except Exception as e:
        logger.exception("Error building signal for %s: %s", ticker, e)
        return {
            "ticker": ticker,
            "name": name,
            "generated_at": _now_iso(),
            "error": str(e),
            "score": 0.0
        }


def run_qb1(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Orchestrator for QB1 Stage1 scan.
    - Loads config from fia.config_loader.get_config() if not provided.
    - Loads universe from Google Sheets (or fallback).
    - Iterates universe and builds signals.
    - Applies a trigger threshold to filter signals of interest.
    - Returns a dict shaped for trigger_context.json:
      {
         "generated_at": <iso>,
         "signals": [...],
         "meta": {...}
      }
    """
    config = config or get_config()
    universe = load_universe_from_sheets(config=config)
    if not universe:
        logger.info("Universe empty; run_qb1 will return empty signals list.")
        return {"generated_at": _now_iso(), "signals": [], "meta": {"universe_count": 0}}

    threshold_cfg = _safe_get(config, "trigger_thresholds", default={})
    score_threshold = float(threshold_cfg.get("score", 2.0))

    # Allow config override to limit how many tickers are processed in DRY_RUN or debug
    dry_limit = int(_safe_get(config, "run_settings", "dry_limit", default=0))
    live_mode = bool(_safe_get(config, "run_settings", "live_mode", default=False))

    signals = []
    processed = 0
    start_ts = time.time()
    for entry in universe:
        if dry_limit and processed >= dry_limit:
            break
        ticker = entry.get("ticker")
        name = entry.get("name", "")
        if not ticker:
            continue
        sig = build_stage1_signal(ticker, name=name, config=config)
        from brains.nb1.core import attach_nb1
        signals.append(attach_nb1(sig))
        processed += 1

    # Apply threshold to produce triggers
    triggers = []
    for s in signals:
        score = s.get("score", 0.0)
        if abs(score) >= score_threshold:
            triggers.append(s)

    meta = {
        "universe_count": len(universe),
        "processed": processed,
        "triggers_count": len(triggers),
        "score_threshold": score_threshold,
        "live_mode": live_mode,
        "runtime_seconds": round(time.time() - start_ts, 2),
    }

    out = {
        "generated_at": _now_iso(),
        "signals": signals,
        "triggers": triggers,
        "meta": meta,
    }

    return out


# If module executed as script, run quick local scan (safe: will not call paid APIs without keys)
if __name__ == "__main__":
    cfg = None
    try:
        cfg = get_config()
    except Exception:
        pass
    result = run_qb1(config=cfg)
    # Write to trigger_context.json in current directory for local debugging only
    try:
        with open("trigger_context.json", "w") as fh:
            json.dump(result, fh, indent=2, default=str)
        logger.info("Wrote trigger_context.json with %d signals (%d triggers).", len(result.get("signals", [])), len(result.get("triggers", [])))
    except Exception as e:
        logger.exception("Failed to write trigger_context.json: %s", e)
