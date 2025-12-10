from __future__ import annotations

import os
import logging
from typing import Any, Dict
from supabase import create_client, Client

from fia.config_loader import get_config

_logger = logging.getLogger("fia.supabase")


def get_supabase() -> Client | None:
    cfg = get_config()
    url = cfg.secrets.get("SUPABASE_URL")
    key = cfg.secrets.get("SUPABASE_SERVICE_ROLE_KEY")

    _logger.info(f"Supabase configured={bool(url and key)}")

    if not url or not key:
        return None

    try:
        return create_client(url, key)
    except Exception:
        _logger.exception("Supabase init failed")
        return None


def safe_log_run_start(run_id: str, stage: str, meta: dict):
    try:
        sb = get_supabase()
        if not sb:
            return
        sb.table("run_log").insert({
            "run_id": run_id,
            "stage": stage,
            "meta": meta,
            "started_at": meta.get("started_at"),
            "success": None
        }).execute()
    except Exception:
        _logger.exception("safe_log_run_start failed")


def safe_log_run_end(run_id: str, success: bool, meta: dict):
    try:
        sb = get_supabase()
        if not sb:
            return
        sb.table("run_log").update({
            "success": success,
            "ended_at": meta.get("ended_at"),
            "details": meta
        }).eq("run_id", run_id).execute()
    except Exception:
        _logger.exception("safe_log_run_end failed")


def safe_write_result(table: str, row: Dict[str, Any]):
    try:
        sb = get_supabase()
        if not sb:
            return
        sb.table(table).insert(row).execute()
    except Exception:
        _logger.exception("safe_write_result failed")
