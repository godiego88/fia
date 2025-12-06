# runners/stage1_runner.py
"""
Stage 1 Runner — Final Production Version
Runs QB1, loads config, logs to Supabase (safe), and writes trigger_context.json.
"""

import os
import json
import logging
from datetime import datetime

from fia.config_loader import get_config
from fia.supabase_client import get_supabase, safe_log_run_start, safe_log_run_end
from brains.qb1.core import run_qb1

logger = logging.getLogger("fia.stage1")
logger.setLevel(os.environ.get("FIA_LOG_LEVEL", "INFO"))
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)


def main():
    # Load config
    config = get_config()
    live_mode = config["run_settings"].get("live_mode", False)

    # Start Supabase run log (safe — no crash if disabled)
    run_id = safe_log_run_start(
        stage="stage1",
        config_snapshot=config,
        live_mode=live_mode
    )

    try:
        # Run QB1 brain
        logger.info("Running QB1…")
        result = run_qb1(config=config)

        # Determine output path
        output_path = config["paths"].get("trigger_context_path", "trigger_context.json")

        # Write artifact
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2, default=str)

        logger.info(f"Stage1: trigger_context.json written to {output_path}")

        # End log
        safe_log_run_end(run_id, success=True, details={"signals": len(result.get("signals", []))})

    except Exception as e:
        logger.exception(f"Stage1 failed: {e}")
        safe_log_run_end(run_id, success=False, details={"error": str(e)})
        raise


if __name__ == "__main__":
    main()
