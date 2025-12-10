from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import json
from datetime import datetime, timezone

from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end, safe_write_result
from brains.reconcile.core import reconcile
from brains.nb1.core import build_nb1_from_qub
from brains.nb2.core import build_nb2_from_reconcile_inputs


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    cfg = get_config()
    run_id = f"reconcile-{int(datetime.now().timestamp())}"

    safe_log_run_start(run_id, "reconcile", {"started_at": now()})

    deep = json.load(open(cfg.paths.deep_results_path))

    out = {"run_id": run_id, "generated_at": now(), "items": []}

    for rec in deep.get("results", []):
        ticker = rec["ticker"]
        qb1 = rec["qb1"]
        qb2 = rec["qb2"]
        nb1 = build_nb1_from_qub(qb2)
        nb2 = build_nb2_from_reconcile_inputs(ticker, qb2, nb1)
        final = reconcile(qb1, qb2, nb1, nb2)

        out["items"].append(final.model_dump())

        safe_write_result("results", {
            "run_id": run_id,
            "ticker": ticker,
            "payload": final.model_dump()
        })

    with open(cfg.paths.reconcile_report_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    safe_log_run_end(run_id, True, {"ended_at": now()})
    print(f"Reconcile complete → {cfg.paths.reconcile_report_path}")


if __name__ == "__main__":
    main()
