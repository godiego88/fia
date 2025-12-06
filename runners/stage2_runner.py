import uuid
from datetime import datetime
from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end, get_supabase
from brains.stage2.core import run_stage2


def main():
    config = get_config()
    sb = get_supabase()

    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat() + "Z"

    safe_log_run_start(run_id, "stage2", {"started_at": started_at})

    result = run_stage2(config=config, sb=sb)

    ended_at = datetime.utcnow().isoformat() + "Z"
    safe_log_run_end(run_id, True, {"ended_at": ended_at, "summary": result})

    print("Stage2 complete.")
    print(result)


if __name__ == "__main__":
    main()
