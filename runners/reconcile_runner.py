import uuid
from datetime import datetime
from fia.config_loader import get_config
from fia.supabase_client import safe_log_run_start, safe_log_run_end, get_supabase
from brains.reconcile.core import run_reconcile


def main():
    config = get_config()
    sb = get_supabase()

    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat() + "Z"

    safe_log_run_start(run_id, "reconcile", {"started_at": started_at})

    result = run_reconcile(config=config, sb=sb)

    ended_at = datetime.utcnow().isoformat() + "Z"
    safe_log_run_end(run_id, True, {"ended_at": ended_at, "summary": result})

    print("Reconcile complete.")
    print(result)


if __name__ == "__main__":
    main()
