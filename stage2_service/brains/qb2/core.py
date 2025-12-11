# brains/qb2/core.py
from __future__ import annotations
from typing import List
from pydantic import BaseModel
from brains.qb1.core import QB1Output

class QB2Output(BaseModel):
    ticker: str
    refined_queries: List[str]
    relevance_score: float

def refine_queries(qb1: QB1Output) -> QB2Output:
    # de-duplicate and normalize, score by simple heuristic
    refined = []
    seen = set()
    for q in qb1.queries:
        norm = q.strip().lower()
        if norm not in seen:
            seen.add(norm)
            refined.append(q.strip())
    # simple relevance: more queries -> higher score (capped)
    score = min(1.0, 0.3 + 0.1 * len(refined))
    return QB2Output(ticker=qb1.ticker, refined_queries=refined, relevance_score=score)
