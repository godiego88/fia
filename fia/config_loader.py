from __future__ import annotations
import os
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()


class RunSettings(BaseModel):
    live_mode: bool = False
    log_to_supabase: bool = False


class FIAConfig(BaseModel):
    supabase_url: str
    supabase_key: str
    run_settings: RunSettings


def get_config() -> FIAConfig:
    return FIAConfig(
        supabase_url=os.getenv("SUPABASE_URL", ""),
        supabase_key=os.getenv("SUPABASE_KEY", ""),
        run_settings=RunSettings(
            live_mode=os.getenv("LIVE_MODE", "false").lower() == "true",
            log_to_supabase=os.getenv("LOG_TO_SUPABASE", "false").lower() == "true"
        )
    )
