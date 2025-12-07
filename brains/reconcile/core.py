# brains/reconcile/core.py
"""
Reconcile core - final, strict Pydantic models and deterministic merge logic.

Accepts the typed outputs from:
 - brains.qb1.core.QB1Output
 - brains.qb2.core.QB2Output
 - brains.nb1.core.NB1Output
 - brains.nb2.core.NB2Output

Produces ReconcileOutput (typed).
"""

from __future__ import annotations
from pydantic import BaseModel
from typing import Any
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
    """
    Simple deterministic reconciliation logic that:
    - synthesizes a final narrative
    - computes a confidence score from qb2 relevance and nb2 risk
    """
    # Compose narrative
    catalysts_text = ", ".join(nb2.catalysts) if nb2.catalysts else "none"
    narrative_parts = [
        nb1.narrative,
        f"Market context: {nb2.market_context}.",
        f"Sentiment: {nb2.sentiment}.",
        f"Catalysts: {catalysts_text}."
    ]
    final_narrative = " ".join(p for p in narrative_parts if p)

    # Confidence heuristic: weighted combination (qb2 relevance positive, nb2 risk negative)
    confidence = 0.0
    try:
        confidence = 0.6 * float(qb2.relevance_score) + 0.4 * (1.0 - float(nb2.risk_score))
        # clamp
        if confidence < 0.0:
            confidence = 0.0
        if confidence > 1.0:
            confidence = 1.0
    except Exception:
        confidence = 0.5

    return ReconcileOutput(
        ticker=qb1.ticker,
        qb1=qb1,
        qb2=qb2,
        nb1=nb1,
        nb2=nb2,
        final_narrative=final_narrative,
        confidence=round(float(confidence), 3),
    )
