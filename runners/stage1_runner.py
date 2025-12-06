import uuid
from datetime import datetime
from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end
from brains.qb1.core import run_qb1


def main():
    config = get_config()

    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat() + "Z"

    safe_log_run_start(run_id, "stage1", {"started_at": started_at})

    # Read config using dot-access (Pydantic model)
    live_mode = config.run_settings.live_mode
    dry_limit = config.run_settings.dry_limit

    print(f"[Stage1] live_mode={live_mode} dry_limit={dry_limit}")

    result = run_qb1(config=config)

    ended_at = datetime.utcnow().isoformat() + "Z"
    safe_log_run_end(run_id, True, {"ended_at": ended_at, "meta": result.get("meta")})

    print("Stage1 complete.")
    print(f"Signals: {len(result.get('signals'))}")
    print(f"Triggers: {len(result.get('triggers'))}")


if __name__ == "__main__":
    main()
