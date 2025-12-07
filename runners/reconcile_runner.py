# runners/reconcile_runner.py
from __future__ import annotations
import uuid, json
from datetime import datetime, timezone
from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end, get_supabase
from brains.reconcile.core import reconcile
from brains.qb1.core import run_qb1
from brains.qb2.core import refine_queries
from brains.nb1.core import build_nb1_from_qub
from brains.nb2.core import build_nb2_from_reconcile_inputs

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def write_report(obj, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)

def main():
    cfg = get_config()
    run_id = str(uuid.uuid4())
    safe_log_run_start(run_id, "reconcile", {"started_at": now_iso()})

    # Build a single reconcile summary for full universe (basic)
    universe_qb1 = run_qb1()
    report = []
    for qb1 in universe_qb1:
        qb2 = refine_queries(qb1)
        nb1 = build_nb1_from_qub(qb2)
        nb2 = build_nb2_from_reconcile_inputs(qb1.ticker, qb2, nb1)
        rec = reconcile(qb1, qb2, nb1, nb2)
        report.append(rec.model_dump())
    write_report({"run_id": run_id, "timestamp": now_iso(), "report": report}, cfg.paths.reconcile_report_path)
    safe_log_run_end(run_id, True, {"ended_at": now_iso(), "n_items": len(report)})
    print(f"Reconcile complete: {len(report)} items -> {cfg.paths.reconcile_report_path}")

if __name__ == "__main__":
    main()
