# runners/stage1_runner.py
from __future__ import annotations

# Deterministic local .env loading for parity with CI
from dotenv import load_dotenv
load_dotenv()

import uuid, json
from datetime import datetime, timezone
from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end
from brains.qb1.core import run_qb1

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def write_artifact(obj, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)

def main():
    cfg = get_config()
    run_id = str(uuid.uuid4())
    safe_log_run_start(run_id, "stage1", {"started_at": now_iso()})
    qlist = run_qb1()
    # convert to serializable
    payload = {"run_id": run_id, "generated_at": now_iso(), "signals": [q.model_dump() for q in qlist]}
    write_artifact(payload, cfg.paths.trigger_context_path)
    
    # optional per-signal insert to supabase
    try:
        from fia.supabase_client import safe_write_result
        for q in qlist:
            safe_write_result("signals", {"run_id": run_id, "ticker": q.ticker, "payload": q.model_dump()})
    except Exception:
        # never fail the run for instrumentation attempts
        pass

    safe_log_run_end(run_id, True, {"ended_at": now_iso(), "n_signals": len(qlist)})
    print(f"Stage1 complete: {len(qlist)} signals written to {cfg.paths.trigger_context_path}")

if __name__ == "__main__":
    main()
