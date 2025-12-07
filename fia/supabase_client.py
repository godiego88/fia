# fia/supabase_client.py
from __future__ import annotations
import logging
from typing import Optional, Dict, Any
from supabase import create_client
from fia.config_loader import get_config

_logger = logging.getLogger("fia.supabase_client")
_logger.setLevel("INFO")
if not _logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    _logger.addHandler(ch)

_supabase_client = None


def get_supabase():
    """
    Return supabase client or None if not configured.
    Safe to call even if keys missing.
    """
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client

    cfg = get_config()
    url = cfg.secrets.SUPABASE_URL
    key = cfg.secrets.SUPABASE_SERVICE_ROLE_KEY
    if not url or not key:
        _logger.debug("Supabase not configured (missing env/config).")
        return None
    try:
        _supabase_client = create_client(url, key)
        return _supabase_client
    except Exception as e:
        _logger.exception("Failed to create supabase client: %s", e)
        return None


def safe_log_run_start(run_id: str, stage: str, meta: Dict[str, Any]):
    try:
        sb = get_supabase()
        if not sb:
            return
        row = {"run_id": run_id, "stage": stage, "meta": meta}
        sb.table("run_log").insert(row).execute()
    except Exception:
        _logger.exception("safe_log_run_start failed (ignored)")


def safe_log_run_end(run_id: str, success: bool, details: Dict[str, Any]):
    try:
        sb = get_supabase()
        if not sb:
            return
        sb.table("run_log").update({"success": success, "details": details}).eq("run_id", run_id).execute()
    except Exception:
        _logger.exception("safe_log_run_end failed (ignored)")
