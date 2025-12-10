from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import json, uuid, requests, os
from datetime import datetime, timezone

from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    cfg = get_config()
    run_id = str(uuid.uuid4())

    safe_log_run_start(run_id, "stage2-dispatch", {"started_at": now()})

    data = json.load(open(cfg.paths.trigger_context_path))

    fly_url = os.environ.get("FLY_TRIGGER_URL")
    fly_api = os.environ.get("FLY_API_KEY")

    if not fly_url or not fly_api:
        raise RuntimeError("Missing Fly credentials.")

    r = requests.post(
        fly_url,
        headers={"Authorization": f"Bearer {fly_api}",
                 "Content-Type": "application/json"},
        json=data
    )

    if r.status_code not in (200, 202):
        raise RuntimeError(f"Fly dispatch failed: {r.status_code} {r.text}")

    safe_log_run_end(run_id, True, {"ended_at": now()})
    print("Stage2 dispatched to Fly.io")


if __name__ == "__main__":
    main()
