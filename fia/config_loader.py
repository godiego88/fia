import json
import os
from pathlib import Path
from pydantic import BaseModel, ValidationError
from supabase import create_client, Client


# -----------------------------
# Pydantic Models for Validation
# -----------------------------
class CostGuardrails(BaseModel):
    monthly_hard_stop: float
    reservation_ttl_hours: int


class RunSettings(BaseModel):
    dry_run: bool
    max_concurrent_fly_jobs: int


class TriggerThresholds(BaseModel):
    signal_score_min: float
    zscore_abs_min: float
    max_signals_per_run: int


class Paths(BaseModel):
    stage1_artifact: str
    stage2_artifact: str


class FIAConfig(BaseModel):
    cost_guardrails: CostGuardrails
    run_settings: RunSettings
    trigger_thresholds: TriggerThresholds
    paths: Paths


# ---------
# Loader
# ---------

_config_cache = None  # in-memory cache


def _load_json_file(filename: str) -> dict:
    path = Path("config") / filename
    with open(path, "r") as f:
        return json.load(f)


def _load_supabase_config() -> dict:
    """Fetch config rows from Supabase; return dict where keys match file keys."""

    # If no key is provided, skip DB override (safe during early development)
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    url = os.getenv("SUPABASE_DB_URL")

    if not service_key or not url:
        # Early phase: no DB yet
        return {}

    client: Client = create_client(url, service_key)
    res = client.table("config").select("*").execute()

    rows = res.data or []

    db_config = {}
    for row in rows:
        db_config[row["key"]] = row["value"]

    return db_config


def _merge_configs(base: dict, override: dict) -> dict:
    """Recursive merge: override always wins."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_configs(result[key], value)
        else:
            result[key] = value
    return result


def load_config(force_refresh: bool = False) -> FIAConfig:
    """ Load + merge + validate configuration, returns FIAConfig object """

    global _config_cache
    if _config_cache is not None and not force_refresh:
        return _config_cache

    defaults = _load_json_file("defaults.json")
    db_overrides = _load_supabase_config()
    merged = _merge_configs(defaults, db_overrides)

    try:
        validated = FIAConfig(**merged)
    except ValidationError as e:
        raise RuntimeError(f"Configuration validation failed: {e}")

    _config_cache = validated
    return validated


def get_config() -> FIAConfig:
    """ Public accessor """
    return load_config(force_refresh=False)
