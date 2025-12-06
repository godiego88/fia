# runners/stage2_runner.py
"""
Stage 2 Runner — Final Implementation

Consumes Stage 1 output (trigger_context.json),
runs deep QB2 analysis + NB2 narrative, and writes deep_results.json.

Design:
- 100% cost-safe: all external API vendors gated behind API keys.
- Deterministic and robust.
- No placeholders.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

from fia.supabase_client import get_supabase
from brains.qb2.core import run_qb2
from brains.nb2.core import build_nb2


INPUT_PATH = "trigger_context.json"
OUTPUT_PATH = "deep_results.json"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_trigger_context(path: str = INPUT_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Stage2 cannot find {path}")
    with open(path, "r") as f:
        return json.load(f)


def write_output(payload: dict, path: str = OUTPUT_PATH):
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def maybe_log_to_supabase(row: dict):
    url = os.getenv("SUPABASE_DB_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        return
    try:
        sb = get_supabase()
        sb.client.table("run_log").insert(row).execute()
    except Exception as e:
        print("Supabase logging skipped/failure:", str(e))


def run_stage2():
    """
    Main Stage 2 entrypoint.
    Reads Stage 1 triggers, runs QB2 + NB2, writes final deep_results.json.
    """
    ctx = read_trigger_context(INPUT_PATH)

    run_id = str(uuid.uuid4())
    timestamp = now_utc()

    triggers = ctx.get("top_signals") or ctx.get("triggers") or []
    results = []

    for t in triggers:
        ticker = t.get("ticker")
        if not ticker:
            continue

        # Deep quant
        qb2 = run_qb2(ticker)

        # Deep narrative
        nb2 = build_nb2(ticker, qb2)

        results.append({
            "ticker": ticker,
            "qb2": qb2,
            "nb2": nb2,
            "source_signal": t,
        })

    out = {
        "run_id": run_id,
        "timestamp": timestamp,
        "count": len(results),
        "results": results,
    }

    # Write artifact
    write_output(out, OUTPUT_PATH)

    # Optional Supabase logging
    maybe_log_to_supabase({
        "run_id": run_id,
        "timestamp": timestamp,
        "source": "stage2",
        "top_signals": [r["ticker"] for r in results],
        "artifact_ptr": OUTPUT_PATH,
        "notes": "stage2 auto-log"
    })

    print(f"Stage2 completed: {len(results)} deep analyses.")
    return out


if __name__ == "__main__":
    run_stage2()
