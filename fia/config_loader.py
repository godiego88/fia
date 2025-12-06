# fia/config_loader.py
"""
Config loader for FIA — migrated to Pydantic v2.
Loads defaults from config/defaults.json, merges Supabase overrides (if available),
validates against models, and exposes get_config() returning plain dicts (not models).
"""

from __future__ import annotations
import os
import json
import logging
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, ValidationError

# Pydantic v2 uses model_config instead of Config
class RunSettings(BaseModel):
    dry_run: bool = True
    live_mode: bool = False
    dry_limit: int = 0

    model_config = {"extra": "ignore"}


class TriggerThresholds(BaseModel):
    score: float = 2.0
    z_threshold: float = 2.0
    anomaly_lookback: int = 10
    signal_score_min: float = 0.1
    zscore_abs_min: float = 1.0
    max_signals_per_run: int = 25

    model_config = {"extra": "ignore"}


class APISettings(BaseModel):
    yfinance: Dict[str, Any] = Field(default_factory=dict)
    finnhub: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "ignore"}


class Paths(BaseModel):
    universe_sheet_id: Optional[str] = None
    universe_range: str = "Universe!A:C"
    trigger_context_path: str = "trigger_context.json"
    static_universe: Optional[list] = None

    model_config = {"extra": "ignore"}


class Secrets(BaseModel):
    TWELVE_DATA_KEY: Optional[str] = None
    FINNHUB_API_KEY: Optional[str] = None
    FRED_API_KEY: Optional[str] = None
    SUPABASE_DB_URL: Optional[str] = None
    SUPABASE_SERVICE_ROLE_KEY: Optional[str] = None
    GOOGLE_SHEETS_CREDENTIALS: Optional[str] = None

    model_config = {"extra": "ignore"}


class ConfigModel(BaseModel):
    run_settings: RunSettings = Field(default_factory=RunSettings)
    trigger_thresholds: TriggerThresholds = Field(default_factory=TriggerThresholds)
    api: APISettings = Field(default_factory=APISettings)
    paths: Paths = Field(default_factory=Paths)
    secrets: Secrets = Field(default_factory=Secrets)

    model_config = {"extra": "ignore"}


_logger = logging.getLogger("fia.config_loader")
_logger.setLevel(os.environ.get("FIA_LOG_LEVEL", "INFO"))
if not _logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    _logger.addHandler(ch)


_DEFAULTS_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "defaults.json")
_cached_config: Optional[ConfigModel] = None


def _load_defaults() -> Dict[str, Any]:
    if os.path.exists(_DEFAULTS_PATH):
        try:
            with open(_DEFAULTS_PATH, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as e:
            _logger.exception("Failed to load defaults.json: %s", e)
            return {}
    return {}


def _merge_env_secrets(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Read well-known secrets from env if present and put into cfg['secrets']
    secrets = cfg.get("secrets", {})
    for key in ["TWELVE_DATA_KEY", "FINNHUB_API_KEY", "FRED_API_KEY", "SUPABASE_DB_URL", "SUPABASE_SERVICE_ROLE_KEY", "GOOGLE_SHEETS_CREDENTIALS"]:
        val = os.environ.get(key)
        if val:
            secrets[key] = val
    cfg["secrets"] = secrets
    return cfg


def get_config_model() -> ConfigModel:
    """
    Returns a validated ConfigModel (pydantic v2). Caches the result.
    """
    global _cached_config
    if _cached_config is not None:
        return _cached_config

    base = _load_defaults()
    base = _merge_env_secrets(base)

    try:
        cfg_model = ConfigModel(**base)
        _cached_config = cfg_model
        return cfg_model
    except ValidationError as e:
        _logger.exception("Configuration validation error: %s", e)
        # Raise so callers see the problem
        raise


def get_config() -> Dict[str, Any]:
    """
    Returns a plain dict config (safe to serialize).
    """
    model = get_config_model()
    # Pydantic v2 uses model_dump for dict
    return model.model_dump()
