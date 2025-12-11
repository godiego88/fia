# runners/stage2_runner.py
from __future__ import annotations

# Deterministic local .env loading for parity with CI
from dotenv import load_dotenv
load_dotenv()

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

    # CALL QB1 ONCE and create a map for lookups
    qb1_list = run_qb1()
    qb1_map = { q.ticker.upper(): q for q in qb1_list }

    for s in signals:
        ticker = (s.get("ticker") or "").upper()
        qb1_item = qb1_map.get(ticker)
        if qb1_item is None:
            # no qb1 result found for this ticker; skip gracefully
            continue
        qb2 = refine_queries(qb1_item)
        nb1 = build_nb1_from_qub(qb2)
        nb2 = build_nb2_from_reconcile_inputs(qb1_item.ticker, qb2, nb1)
        rec = reconcile(qb1_item, qb2, nb1, nb2)
        
        results.append(rec.model_dump())

        # write individual result row to supabase (safe no-op if not configured)
        try:
            from fia.supabase_client import safe_write_result
            safe_write_result("results", {"run_id": run_id, "ticker": rec.ticker, "payload": rec.model_dump()})
        except Exception:
            pass

    out = {"run_id": run_id, "generated_at": now_iso(), "results": results}
    write_artifact(out, cfg.paths.deep_results_path)

    # attempt to upload artifact to supabase storage and insert an artifact index row
    try:
        from fia.supabase_client import get_supabase, safe_write_result, _logger
        sb = get_supabase()
        artifact_meta = {"run_id": run_id, "path": cfg.paths.deep_results_path, "generated_at": now_iso()}
        if sb and hasattr(sb, "storage"):
            try:
                # safe upload to bucket 'artifacts' (ensure bucket exists)
                with open(cfg.paths.deep_results_path, "rb") as f:
                    sb.storage.from_('artifacts').upload(cfg.paths.deep_results_path, f)
                artifact_meta["uploaded"] = True
            except Exception:
                _logger.exception("artifact upload failed; falling back to metadata only")
                artifact_meta["uploaded"] = False
        else:
            artifact_meta["uploaded"] = False
        # always insert an index row (safe)
        safe_write_result("results", {"run_id": run_id, "artifact": artifact_meta})
    except Exception:
        # nonfatal
        pass

    safe_log_run_end(run_id, True, {"ended_at": now_iso(), "n_results": len(results)})
    print(f"Stage2 complete: {len(results)} results -> {cfg.paths.deep_results_path}")

if __name__ == "__main__":
    main()
