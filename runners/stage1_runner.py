from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import json, uuid
from datetime import datetime, timezone

from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end, safe_write_result
from brains.qb1.core import run_qb1


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    cfg = get_config()
    run_id = str(uuid.uuid4())

    safe_log_run_start(run_id, "stage1", {"started_at": now()})

    qlist = run_qb1()

    out = {
        "run_id": run_id,
        "generated_at": now(),
        "signals": [q.model_dump() for q in qlist],
        "trigger_stage2": len(qlist) > 0
    }

    with open(cfg.paths.trigger_context_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    for q in qlist:
        safe_write_result("signals", {
            "run_id": run_id,
            "ticker": q.ticker,
            "payload": q.model_dump()
        })

    safe_log_run_end(run_id, True, {"ended_at": now()})
    print(f"Stage1 done -> {cfg.paths.trigger_context_path}")


if __name__ == "__main__":
    main()
