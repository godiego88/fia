from __future__ import annotations
from supabase import create_client, Client
from fia.config_loader import get_config


_supabase: Client | None = None


def get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        cfg = get_config()
        _supabase = create_client(cfg.supabase_url, cfg.supabase_key)
    return _supabase


def log_event(table: str, data: dict):
    """Safe write to Supabase. Does nothing if logging disabled."""
    cfg = get_config()
    if not cfg.run_settings.log_to_supabase:
        return

    client = get_supabase()
    try:
        client.table(table).insert(data).execute()
    except Exception as e:
        print(f"[WARN] Supabase log failed: {e}")
