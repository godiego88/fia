# brains/nb2/core.py
from __future__ import annotations
from pydantic import BaseModel
from typing import List, Dict, Any

class NB2Output(BaseModel):
    ticker: str
    risk_score: float
    catalysts: List[str]
    red_flags: List[str]
    market_context: str
    sentiment: str  # bullish | bearish | neutral

def build_nb2_from_reconcile_inputs(ticker: str, qb2, nb1) -> NB2Output:
    # Simple heuristic-driven implementation
    catalysts = ["earnings", "analyst coverage"] if qb2.relevance_score > 0.5 else ["none"]
    red_flags = ["low liquidity"] if "limited liquidity" in (nb1.drivers.get("notes","") if nb1.drivers else "") else []
    risk = max(0.0, 1.0 - qb2.relevance_score)
    sentiment = "neutral"
    if risk < 0.4:
        sentiment = "bullish"
    elif risk > 0.7:
        sentiment = "bearish"
    market_context = "broader market neutral"
    return NB2Output(ticker=ticker, risk_score=round(risk,3), catalysts=catalysts, red_flags=red_flags, market_context=market_context, sentiment=sentiment)
