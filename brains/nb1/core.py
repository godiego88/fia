# brains/nb1/core.py
from __future__ import annotations
from pydantic import BaseModel
from typing import Dict, Any, List

class NB1Output(BaseModel):
    ticker: str
    summary: str
    tags: List[str]
    drivers: Dict[str, Any]
    narrative: str

def _tag_for_z(z: float) -> str:
    if z >= 4:
        return "extreme-up"
    if z >= 2:
        return "strong-up"
    if z >= 1:
        return "moderate-up"
    if z <= -4:
        return "extreme-down"
    if z <= -2:
        return "strong-down"
    if z <= -1:
        return "moderate-down"
    return "neutral"

def build_nb1_from_qub(qb2_result) -> NB1Output:
    # qb2_result expected to have ticker and refined_queries
    ticker = qb2_result.ticker
    # Simple deterministic drivers for now
    z = 0.0  # placeholder if you later wire real anomaly
    tags = [_tag_for_z(z), "macro-neutral"]
    summary = f"{ticker} shows an observable price signal."
    drivers = {"z_score": z, "volatility": None, "news_count": 0}
    narrative = f"{summary} No major macro or news drivers detected."
    return NB1Output(ticker=ticker, summary=summary, tags=tags, drivers=drivers, narrative=narrative)
