# fia/supabase_client.py
"""
Supabase client wrapper — compatible with Supabase Python client v2.
Provides a get_supabase() singleton and safe logging helpers.
"""

from __future__ import annotations
import os
import logging
from typing import Optional, Dict, Any

from supabase import create_client, Client

_logger = logging.getLogger("fia.supabase_client")
_logger.setLevel(os.environ.get("FIA_LOG_LEVEL", "INFO"))
if not _logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    _logger.addHandler(ch)

_supabase_instance: Optional[Client] = None


def get_supabase() -> Optional[Client]:
    """
    Return a singleton Supabase client or None if not configured.
    Uses SUPABASE_DB_URL and SUPABASE_SERVICE_ROLE_KEY env vars.
    """
    global _supabase_instance
    if _supabase_instance:
        return _supabase_instance

    url = os.environ.get("SUPABASE_DB_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        _logger.debug("Supabase not configured (env keys missing).")
        return None

    try:
        _supabase_instance = create_client(url, key)
        return _supabase_instance
    except Exception as e:
        _logger.exception("Failed to create supabase client: %s", e)
        return None


def safe_log_run_start(stage: str, config_snapshot: Dict[str, Any], live_mode: bool) -> Optional[str]:
    """
    Insert a run_log row if Supabase configured. Return run_id or None.
    """
    sb = get_supabase()
    if sb is None:
        return None
    try:
        run_id = os.urandom(8).hex()
        row = {
            "run_id": run_id,
            "stage": stage,
            "config_snapshot": config_snapshot,
            "live_mode": live_mode,
            "timestamp": None,
        }
        sb.table("run_log").insert(row).execute()
        return run_id
    except Exception as e:
        _logger.exception("safe_log_run_start failed: %s", e)
        return None


def safe_log_run_end(run_id: Optional[str], success: bool, details: Dict[str, Any]):
    """
    Patch run_log row with end status. Safe no-op if not configured.
    """
    if not run_id:
        return
    sb = get_supabase()
    if sb is None:
        return
    try:
        sb.table("run_log").update({"success": success, "details": details}).eq("run_id", run_id).execute()
    except Exception as e:
        _logger.exception("safe_log_run_end failed: %s", e)
