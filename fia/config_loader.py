# fia/config_loader.py
"""
Config Loader (Final — Pydantic v2 Compatible)

Loads config/defaults.json and applies environment overrides.
Also supports Supabase-related settings in v2.
"""

from __future__ import annotations
import json
import os
from pathlib import Path
from pydantic import BaseModel, ValidationError

# ---------------------------------------------------------
# Pydantic Models (These are v2 compatible)
# ---------------------------------------------------------

class TriggerThresholds(BaseModel):
    signal_score_min: float = 0.1
    zscore_abs_min: float = 1.0
    max_signals_per_run: int = 5
    universe: list[str] | None = None


class RunSettings(BaseModel):
    dry_run: bool = True


class FIAConfig(BaseModel):
    trigger_thresholds: TriggerThresholds
    run_settings: RunSettings


# ---------------------------------------------------------
# Loader Function
# ---------------------------------------------------------

def load_json(path: str | Path) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def get_config() -> FIAConfig:
    """
    Load defaults + apply Supabase or environment overrides.
    Returns a FIAConfig object (Pydantic v2).
    """
    base_path = Path(__file__).resolve().parent.parent
    config_path = base_path / "config" / "defaults.json"

    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    # Load JSON defaults
    data = load_json(config_path)

    # Apply environment overrides (Pydantic-safe)
    dry_run_env = os.getenv("FIA_DRY_RUN")
    if dry_run_env is not None:
        data["run_settings"]["dry_run"] = dry_run_env.lower() == "true"

    try:
        cfg = FIAConfig(**data)
    except ValidationError as e:
        print("CONFIG VALIDATION ERROR:", e)
        raise

    return cfg
