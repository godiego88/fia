"""
Stage 1 Runner - Final Implementation

Responsibilities:
- Load configuration (from config/defaults.json and Supabase overrides)
- Fetch lightweight market data for a small universe of tickers
  (attempt real fetch using yfinance if installed; otherwise simulate)
- Compute a short-term z-score anomaly for each ticker
- Produce a single artifact 'trigger_context.json' containing run metadata and top_signals
- Optionally log a run into the existing 'run_log' table in Supabase if service keys are present

Notes:
- This file is designed to run safely in dry-run mode (the default).
- It WILL NOT perform Fly reservations or Stage 2 orchestration.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Optional real market fetch: yfinance if available
try:
    import yfinance as yf

    YFINANCE_AVAILABLE = True
except Exception:
    YFINANCE_AVAILABLE = False

from fia.config_loader import get_config
from fia.supabase_client import get_supabase, SupabaseClient

# ---- Parameters (kept small intentionally) ----
DEFAULT_UNIVERSE = ["AAPL", "SPY", "QQQ", "TSLA", "MSFT"]
DEFAULT_PERIOD = "7d"       # 7 days of intraday or daily data depending on yfinance availability
DEFAULT_INTERVAL = "1h"     # 1-hour bars when available

ARTIFACT_PATH = "trigger_context.json"


# ---- Utility functions ----
def now_isoutc() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_close_series_real(ticker: str, period: str = DEFAULT_PERIOD, interval: str = DEFAULT_INTERVAL) -> List[float]:
    """
    Fetch close prices using yfinance. Returns list of closes (most recent last).
    If yfinance fails for any reason, raises an exception which caller can handle.
    """
    if not YFINANCE_AVAILABLE:
        raise RuntimeError("yfinance not available")

    # Use yfinance download (no progress, single-thread)
    df = yf.download(tickers=ticker, period=period, interval=interval, progress=False, threads=False)
    if df is None or df.empty or "Close" not in df.columns:
        raise RuntimeError(f"No data for {ticker} via yfinance")
    closes = df["Close"].dropna().tolist()
    return closes


def fetch_close_series_simulated(seed: int = 0, length: int = 24) -> List[float]:
    """
    Create a plausible synthetic price series for dry-run/dev when no market data library is present.
    """
    rng = np.random.default_rng(seed)
    # Simulate a small random walk
    price = 100.0 + rng.normal(scale=1.0)
    series = []
    for _ in range(length):
        price = price * (1.0 + rng.normal(scale=0.002))  # small drift
        series.append(float(price))
    return series


def compute_zscore(series: List[float]) -> Optional[float]:
    """
    Compute z-score of the last value relative to series history.
    Returns None if series too short or constant.
    """
    if not series or len(series) < 3:
        return None
    arr = np.array(series, dtype=float)
    mean = arr.mean()
    std = arr.std(ddof=0)
    if std == 0 or np.isnan(std):
        return None
    return float((arr[-1] - mean) / std)


def severity_from_z(z: Optional[float]) -> Optional[float]:
    """
    Map z-score magnitude to a 0..1 severity.
    Uses a saturating transform: z=0 -> 0, z=3 -> 0.6, z=6 -> 1.0
    """
    if z is None:
        return None
    mag = abs(z)
    # piecewise mapping with soft saturation
    if mag <= 0.5:
        return round(min(1.0, mag / 6.0), 3)
    if mag <= 3.0:
        return round(min(1.0, 0.1 + (mag - 0.5) * (0.5 / (3.0 - 0.5))), 3)
    # strong anomalies
    return round(min(1.0, 0.5 + (mag - 3.0) * 0.0833), 3)


# ---- Runner core ----
def build_universe(overrides: Dict) -> List[str]:
    """Return the ticker universe (allow config override)."""
    u = overrides.get("universe")
    if isinstance(u, list) and u:
        return [str(x).upper() for x in u]
    return DEFAULT_UNIVERSE


def analyze_universe(tickers: List[str]) -> List[Dict]:
    """For each ticker compute zscore and severity, return list of signal dicts."""
    signals = []
    for i, t in enumerate(tickers):
        try:
            if YFINANCE_AVAILABLE:
                closes = fetch_close_series_real(t)
            else:
                # deterministic simulated series seeded by ticker hash to keep reproducible behavior
                seed = abs(hash(t)) % (2 ** 32)
                closes = fetch_close_series_simulated(seed=seed, length=24)
        except Exception as e:
            # fallback to simulated if any error occurs when fetching real data
            seed = abs(hash(t)) % (2 ** 32)
            closes = fetch_close_series_simulated(seed=seed, length=24)

        z = compute_zscore(closes)
        sev = severity_from_z(z)

        signals.append({
            "ticker": t,
            "zscore": z,
            "severity": sev,
            "sample_count": len(closes)
        })

    # Sort by severity descending (None treated as 0)
    def s_key(x):
        return 0.0 if x["severity"] is None else float(x["severity"])

    signals_sorted = sorted(signals, key=s_key, reverse=True)
    return signals_sorted


def write_artifact(payload: Dict, path: str = ARTIFACT_PATH) -> None:
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def maybe_log_to_supabase(payload: Dict) -> None:
    """
    If service env vars exist, attempt to insert a row in the 'run_log' table.
    This is safe: it uses a try/except and will not raise to caller.
    """
    url = os.getenv("SUPABASE_DB_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        # Supabase not configured; skip logging
        return

    try:
        sb = get_supabase()  # may raise if constructor fails
        # create a minimal entry matching our run_log schema
        row = {
            "run_id": payload.get("run_id"),
            "timestamp": payload.get("timestamp_utc"),
            "source": "stage1",
            "top_signals": payload.get("top_signals"),
            "artifact_ptr": ARTIFACT_PATH,
            "notes": "stage1 auto-log"
        }
        # Insert into the run_log table (this table exists in your schema)
        sb.client.table("run_log").insert(row).execute()
    except Exception as e:
        # Don't break the runner if logging fails; print for visibility
        print("Supabase logging skipped/failure:", str(e))


def run_stage1() -> Dict:
    """
    Main entrypoint for Stage 1.
    Returns the artifact payload (also writes trigger_context.json).
    """
    cfg = get_config()
    cfg_dict = cfg.dict()

    run_id = str(uuid.uuid4())
    timestamp = now_isoutc()

    universe = build_universe(cfg_dict.get("trigger_thresholds", {}) or {})
    signals = analyze_universe(universe)

    # Apply simple threshold filter from config (keeps only signals above a severity or zscore)
    thresholds = cfg.trigger_thresholds
    filtered = []
    for s in signals:
        if s["severity"] is None or s["zscore"] is None:
            continue
        if s["severity"] >= thresholds.signal_score_min and abs(s["zscore"]) >= thresholds.zscore_abs_min:
            filtered.append(s)
        if len(filtered) >= thresholds.max_signals_per_run:
            break

    artifact = {
        "run_id": run_id,
        "timestamp_utc": timestamp,
        "universe": universe,
        "top_signals": filtered,
        "meta": {
            "dry_run": cfg.run_settings.dry_run,
            "source": "stage1",
            "n_scanned": len(universe)
        }
    }

    # Persist artifact locally
    write_artifact(artifact, ARTIFACT_PATH)

    # Attempt to log to Supabase if available (safe operation)
    try:
        maybe_log_to_supabase(artifact)
    except Exception:
        pass

    # Print a compact summary for humans
    print(f"Stage1 run_id={run_id} scanned={len(universe)} signals_found={len(filtered)} dry_run={cfg.run_settings.dry_run}")

    return artifact


# ---- Allow CLI execution ----
if __name__ == "__main__":
    run_stage1()
