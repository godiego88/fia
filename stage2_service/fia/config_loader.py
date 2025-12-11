# fia/config_loader.py
from __future__ import annotations
import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# ---- Models ----
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
    twelvedata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "ignore"}


class Paths(BaseModel):
    universe_sheet_id: Optional[str] = None
    universe_range: str = "Universe!A:C"
    trigger_context_path: str = "trigger_context.json"
    deep_results_path: str = "deep_results.json"
    reconcile_report_path: str = "reconcile_report.json"
    static_universe: Optional[List[str]] = None

    model_config = {"extra": "ignore"}


class Secrets(BaseModel):
    TWELVE_DATA_KEY: Optional[str] = None
    FINNHUB_API_KEY: Optional[str] = None
    FRED_API_KEY: Optional[str] = None
    SUPABASE_URL: Optional[str] = None
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


# ---- Loader functions ----
_DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "config" / "defaults.json"
_cached: Optional[ConfigModel] = None


def _load_defaults() -> Dict[str, Any]:
    if _DEFAULTS_PATH.exists():
        try:
            return json.loads(_DEFAULTS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _merge_env_secrets(cfg: Dict[str, Any]) -> Dict[str, Any]:
    secrets = cfg.get("secrets", {}) or {}
    for key in [
        "TWELVE_DATA_KEY",
        "FINNHUB_API_KEY",
        "FRED_API_KEY",
        "SUPABASE_URL",
        "SUPABASE_SERVICE_ROLE_KEY",
        "GOOGLE_SHEETS_CREDENTIALS",
    ]:
        val = os.environ.get(key)
        if val:
            secrets[key] = val
    cfg["secrets"] = secrets
    return cfg


def get_config() -> ConfigModel:
    """
    Return a validated ConfigModel (Pydantic v2). Strict model access required.
    """
    global _cached
    if _cached is not None:
        return _cached

    base = _load_defaults()
    base = _merge_env_secrets(base)
    cfg = ConfigModel(**base)
    _cached = cfg
    return cfg
