# runners/reconcile_runner.py
"""
Reconcile Runner — Final Implementation

Rolls up usage, cleans stale reservations, writes reconcile_report.json.
Safe if Supabase is missing (falls back to empty data).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta

from fia.supabase_client import get_supabase


OUTPUT_PATH = "reconcile_report.json"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_supabase_table(name: str):
    """Return table ref or None if Supabase not configured."""
    url = os.getenv("SUPABASE_DB_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        return None
    try:
        sb = get_supabase()
        return sb.client.table(name)
    except Exception:
        return None


def fetch_table(name: str):
    tbl = safe_supabase_table(name)
    if tbl is None:
        return []
    try:
        resp = tbl.select("*").execute()
        return resp.data or []
    except Exception:
        return []


def delete_reservation(res_id: str):
    tbl = safe_supabase_table("fly_reservations")
    if tbl is None:
        return
    try:
        tbl.delete().eq("id", res_id).execute()
    except Exception:
        pass


def update_monthly_usage(month: str, delta: int):
    tbl = safe_supabase_table("monthly_usage")
    if tbl is None:
        return

    # read existing
    try:
        existing = tbl.select("*").eq("month", month).execute()
        if existing.data:
            current = int(existing.data[0].get("count", 0))
            tbl.update({"count": current + delta}).eq("month", month).execute()
        else:
            tbl.insert({"month": month, "count": delta}).execute()
    except Exception:
        pass


def run_reconcile():
    timestamp = now_utc()
    month_key = datetime.now(timezone.utc).strftime("%Y-%m")

    # --- Fetch data ---
    run_log = fetch_table("run_log")
    reservations = fetch_table("fly_reservations")

    # --- Usage rollup ---
    runs_this_month = sum(1 for r in run_log if r.get("timestamp", "").startswith(month_key))
    update_monthly_usage(month_key, runs_this_month)

    # --- Clean stale reservations (older than 2 days) ---
    stale_cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    stale = []
    for r in reservations:
        ts = r.get("timestamp")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt < stale_cutoff:
                stale.append(r)
        except Exception:
            stale.append(r)

    for r in stale:
        delete_reservation(r.get("id"))

    # --- Produce report ---
    report = {
        "timestamp": timestamp,
        "month": month_key,
        "run_log_count": len(run_log),
        "runs_this_month": runs_this_month,
        "reservations_total": len(reservations),
        "reservations_removed": len(stale),
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    print(f"Reconcile complete. Removed {len(stale)} stale reservations.")
    return report


if __name__ == "__main__":
    run_reconcile()
