# fia/config_loader.py
"""
Config loader for FIA — Pydantic v2 compatible.
Loads defaults from config/defaults.json, merges environment secrets,
validates, and returns a plain dict via model_dump().
"""

from __future__ import annotations
import os
import json
import logging
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, ValidationError

# ----- Models (Pydantic v2) -----
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


# ----- Logger & paths -----
_logger = logging.getLogger("fia.config_loader")
_logger.setLevel(os.environ.get("FIA_LOG_LEVEL", "INFO"))
if not _logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    _logger.addHandler(ch)


_DEFAULTS_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "defaults.json")
_cached_model: Optional[ConfigModel] = None


# ----- Helpers -----
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
    secrets = cfg.get("secrets", {}) or {}
    for key in [
        "TWELVE_DATA_KEY",
        "FINNHUB_API_KEY",
        "FRED_API_KEY",
        "SUPABASE_DB_URL",
        "SUPABASE_SERVICE_ROLE_KEY",
        "GOOGLE_SHEETS_CREDENTIALS",
    ]:
        val = os.environ.get(key)
        if val:
            secrets[key] = val
    cfg["secrets"] = secrets
    return cfg


# ----- Public API -----
def get_config_model() -> ConfigModel:
    """
    Return validated ConfigModel (cached).
    """
    global _cached_model
    if _cached_model is not None:
        return _cached_model

    base = _load_defaults()
    base = _merge_env_secrets(base)

    try:
        model = ConfigModel(**base)
        _cached_model = model
        return model
    except ValidationError as e:
        _logger.exception("Configuration validation error: %s", e)
        # re-raise to make errors visible to caller
        raise


def get_config() -> Dict[str, Any]:
    """
    Return a plain dict representation of config (Pydantic v2 model_dump).
    """
    model = get_config_model()
    return model.model_dump()
