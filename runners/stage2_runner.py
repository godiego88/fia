# runners/stage2_runner.py
from __future__ import annotations
import uuid, json
from datetime import datetime, timezone
from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end
from brains.qb1.core import run_qb1
from brains.qb2.core import refine_queries
from brains.nb1.core import build_nb1_from_qub
from brains.nb2.core import build_nb2_from_reconcile_inputs
from brains.reconcile.core import reconcile

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def read_artifact(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_artifact(obj, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)

def main():
    cfg = get_config()
    run_id = str(uuid.uuid4())
    safe_log_run_start(run_id, "stage2", {"started_at": now_iso()})

    ctx = read_artifact(cfg.paths.trigger_context_path)
    signals = ctx.get("signals", [])
    results = []
    for s in signals:
        qb1 = run_qb1()  # qb1 returns list; we'll map by ticker
        # find qb1 for this ticker
        qb1_item = None
        for q in qb1:
            if q.ticker == s.get("ticker"):
                qb1_item = q
                break
        if qb1_item is None:
            continue
        qb2 = refine_queries(qb1_item)
        nb1 = build_nb1_from_qub(qb2)
        nb2 = build_nb2_from_reconcile_inputs(qb1_item.ticker, qb2, nb1)
        rec = reconcile(qb1_item, qb2, nb1, nb2)
        results.append(rec.model_dump())
    out = {"run_id": run_id, "generated_at": now_iso(), "results": results}
    write_artifact(out, cfg.paths.deep_results_path)
    safe_log_run_end(run_id, True, {"ended_at": now_iso(), "n_results": len(results)})
    print(f"Stage2 complete: {len(results)} results -> {cfg.paths.deep_results_path}")

if __name__ == "__main__":
    main()
