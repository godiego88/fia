# brains/reconcile/core.py
from __future__ import annotations
from pydantic import BaseModel
from brains.qb1.core import QB1Output
from brains.qb2.core import QB2Output
from brains.nb1.core import NB1Output
from brains.nb2.core import NB2Output

class ReconcileOutput(BaseModel):
    ticker: str
    qb1: QB1Output
    qb2: QB2Output
    nb1: NB1Output
    nb2: NB2Output
    final_narrative: str
    confidence: float

def reconcile(qb1: QB1Output, qb2: QB2Output, nb1: NB1Output, nb2: NB2Output) -> ReconcileOutput:
    narrative = f"{nb1.narrative} Market context: {nb2.market_context}. Sentiment: {nb2.sentiment}."
    confidence = 0.7 * qb2.relevance_score + 0.3 * (1 - nb2.risk_score)
    return ReconcileOutput(ticker=qb1.ticker, qb1=qb1, qb2=qb2, nb1=nb1, nb2=nb2, final_narrative=narrative, confidence=round(confidence,3))
