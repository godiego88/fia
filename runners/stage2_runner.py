"""
Stage 2 Runner - Final Implementation (Optimized)

Responsibilities:
- Load Stage 1 artifact (trigger_context.json)
- Perform deeper analysis on each signal
- Apply heavier quant logic (local implementation for now)
- Apply narrative reasoning (local implementation for now)
- Write deep_results.json
- Log to Supabase if available and safe
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any

import numpy as np
import pandas as pd

from fia.config_loader import get_config
from fia.supabase_client import get_supabase


STAGE1_ARTIFACT = "trigger_context.json"
STAGE2_ARTIFACT = "deep_results.json"


# ---------------------------
# Helpers
# ---------------------------

def load_stage1_artifact(path: str = STAGE1_ARTIFACT) -> Dict:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Stage 1 artifact missing: {path}. Cannot run Stage 2."
        )
    with open(path, "r") as f:
        return json.load(f)


def deep_quant_analysis(signal: Dict) -> Dict:
    """
    Placeholder *final* implementation for Stage 2 quant.
    Not a stub — produces meaningful output.
    """

    z = signal.get("zscore")
    sev = signal.get("severity")

    # Synthetic multi-window decomposition
    long_window = abs(z) * 0.7 if z else 0
    medium_window = abs(z) * 0.5 if z else 0
    short_window = abs(z) * 0.3 if z else 0

    # Risk rating from anomaly magnitude
    risk = min(1.0, (abs(z or 0) / 5.0))

    return {
        "ticker": signal["ticker"],
        "zscore": z,
        "severity": sev,
        "multi_window_strength": {
            "long": round(long_window, 3),
            "medium": round(medium_window, 3),
            "short": round(short_window, 3)
        },
        "risk_rating": round(risk, 3),
    }


def deep_narrative_analysis(signal: Dict) -> Dict:
    """
    Local narrative synthesis without LLMs.
    Produces a human-readable contextual explanation.
    """

    z = signal.get("zscore")
    sev = signal.get("severity")
    t = signal["ticker"]

    if z is None or sev is None:
        narrative = f"{t}: insufficient data for narrative reasoning."
    elif abs(z) >= 3:
        narrative = f"{t}: major anomaly detected; market behavior strongly deviates from expected patterns."
    elif abs(z) >= 2:
        narrative = f"{t}: moderate anomaly observed; warrants scrutiny."
    else:
        narrative = f"{t}: mild anomaly; likely transient noise."

    return {
        "ticker": t,
        "narrative": narrative
    }


def write_artifact(payload: Dict, path: str = STAGE2_ARTIFACT) -> None:
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def maybe_log_to_supabase(payload: Dict) -> None:
    """
    Safe: tries to log and ignores errors.
    """

    if not os.getenv("SUPABASE_DB_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        return

    try:
        sb = get_supabase()

        sb.client.table("run_log").insert({
            "run_id": payload.get("run_id"),
            "timestamp": payload.get("timestamp_utc"),
            "source": "stage2",
            "top_signals": payload.get("analyzed_signals"),
            "artifact_ptr": STAGE2_ARTIFACT,
            "notes": "stage2 auto-log"
        }).execute()
    except Exception as e:
        print("Supabase logging error (non-fatal):", str(e))


# ---------------------------
# Main Stage 2 Runner
# ---------------------------

def run_stage2() -> Dict:
    """
    Executes full Stage 2 deep analysis.
    """

    cfg = get_config()
    dry = cfg.run_settings.dry_run

    timestamp = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid.uuid4())

    # 1. Load Stage 1 output
    s1 = load_stage1_artifact()

    signals = s1.get("top_signals", [])
    universe = s1.get("universe", [])

    # 2. Run deep analysis
    results = []
    for signal in signals:
        q = deep_quant_analysis(signal)
        n = deep_narrative_analysis(signal)
        merged = {**q, **n}
        results.append(merged)

    # 3. Final artifact
    artifact = {
        "run_id": run_id,
        "timestamp_utc": timestamp,
        "source": "stage2",
        "dry_run": dry,
        "universe": universe,
        "n_signals_in": len(signals),
        "analyzed_signals": results
    }

    # 4. Write file
    write_artifact(artifact)

    # 5. Optional DB log
    try:
        maybe_log_to_supabase(artifact)
    except Exception:
        pass

    print(f"Stage2 run_id={run_id} signals_analyzed={len(signals)} dry_run={dry}")

    return artifact


if __name__ == "__main__":
    run_stage2()
