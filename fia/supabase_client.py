import os
import json
from typing import Any, Dict, Optional
from supabase import create_client

"""
Supabase Client Wrapper — Compatible with Supabase-Py v2

This module provides:
- get_supabase(): singleton client wrapper
- safe_log_run_start / safe_log_run_end: guaranteed no-crash logging helpers
- config fetch utilities for config_loader

All Supabase calls are wrapped so local runs DO NOT fail even without Supabase.
"""


class SupabaseClient:
    def __init__(self):
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not self.url or not self.key:
            self.client = None
        else:
            try:
                self.client = create_client(self.url, self.key)
            except Exception:
                self.client = None

    # -----------------------------------------
    # Helpers
    # -----------------------------------------
    def is_available(self) -> bool:
        return self.client is not None

    # -----------------------------------------
    # Config fetch (used by config_loader)
    # -----------------------------------------
    def fetch_config_overrides(self) -> Dict[str, Any]:
        """
        Reads override config from Supabase table: fia_configs

        Returns {} on any failure.
        """
        if not self.client:
            return {}

        try:
            resp = (
                self.client.table("fia_configs")
                .select("*")
                .eq("active", True)
                .execute()
            )
            if resp and resp.data:
                return resp.data[0].get("config", {})
            return {}
        except Exception:
            return {}

    # -----------------------------------------
    # Run logging (Stage1 / Stage2)
    # -----------------------------------------
    def log_run_start(self, stage: str, run_id: str, meta: Dict[str, Any]):
        if not self.client:
            return None
        try:
            self.client.table("fia_runs").insert(
                {
                    "run_id": run_id,
                    "stage": stage,
                    "started_at": meta.get("started_at"),
                    "meta": meta,
                }
            ).execute()
        except Exception:
            return None

    def log_run_end(self, run_id: str, success: bool, details: Dict[str, Any]):
        if not self.client:
            return None
        try:
            self.client.table("fia_runs").update(
                {
                    "ended_at": details.get("ended_at"),
                    "success": success,
                    "details": details,
                }
            ).eq("run_id", run_id).execute()
        except Exception:
            return None


# ---------------------------------------------------------------------
# SINGLETON ACCESS
# ---------------------------------------------------------------------
_supabase_instance: Optional[SupabaseClient] = None


def get_supabase() -> SupabaseClient:
    global _supabase_instance
    if _supabase_instance is None:
        _supabase_instance = SupabaseClient()
    return _supabase_instance


# ---------------------------------------------------------------------
# SAFE LOGGING HELPERS (NO-CRASH)
# ---------------------------------------------------------------------
def safe_log_run_start(run_id: str, stage: str, meta: Dict[str, Any]):
    """
    Wrapper for runners: guaranteed not to crash.
    """
    try:
        sb = get_supabase()
        if sb.is_available():
            sb.log_run_start(stage, run_id, meta)
    except Exception:
        pass


def safe_log_run_end(run_id: str, success: bool, details: Dict[str, Any]):
    """
    Wrapper for runners: guaranteed not to crash.
    """
    try:
        sb = get_supabase()
        if sb.is_available():
            sb.log_run_end(run_id, success, details)
    except Exception:
        pass
