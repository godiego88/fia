# brains/nb1/core.py
"""
NB1 — Narrative Builder 1 (Final Implementation)
Lightweight, deterministic narrative generator for QB1 signals.

Adds human-readable context to each Stage1 signal:
- short summary
- tags
- driver breakdown
- 1–2 sentence narrative for Stage2 and human review

No external API calls. Fully cost-free. Production-ready.
"""

from __future__ import annotations
from typing import Dict, List


def _tag_for_zscore(z: float) -> str:
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


def _tag_for_macro(w: float) -> str:
    if w > 1.0:
        return "macro-supportive"
    if w > 0.2:
        return "macro-positive"
    if w < -1.0:
        return "macro-headwind"
    if w < -0.2:
        return "macro-negative"
    return "macro-neutral"


def _tag_for_news(count: int) -> str:
    if count >= 5:
        return "heavy-news"
    if count >= 2:
        return "moderate-news"
    if count == 1:
        return "single-news"
    return "no-news"


def attach_nb1(signal: Dict) -> Dict:
    """
    Enrich a QB1 signal with NB1 narrative fields.
    Returns the modified signal.
    """
    z = float(signal.get("anomaly", {}).get("z_score", 0.0))
    macro_w = float(signal.get("macro_weight", 0.0))
    news_count = int(signal.get("news", {}).get("news_count", 0))

    # tags
    tag_z = _tag_for_zscore(z)
    tag_macro = _tag_for_macro(macro_w)
    tag_news = _tag_for_news(news_count)

    tags = [tag_z, tag_macro, tag_news]

    # summary
    direction = "up" if z > 0 else "down"
    summary = f"{signal['ticker']} shows a {direction}ward price anomaly (z={z:.2f})."

    # drivers
    drivers = {
        "price_anomaly": z,
        "macro_weight": macro_w,
        "news_count": news_count,
        "volatility": signal.get("anomaly", {}).get("volatility"),
    }

    # narrative
    sentence_1 = f"The price action indicates a {tag_z.replace('-', ' ')} movement, with a z-score of {z:.2f}."
    sentence_2 = ""
    if macro_w != 0:
        if macro_w > 0:
            sentence_2 = f"Macro conditions offer mild support (macro weight {macro_w:.2f})."
        else:
            sentence_2 = f"Macro conditions are acting as a headwind (macro weight {macro_w:.2f})."
    if news_count > 0:
        sentence_2 = (sentence_2 + " " if sentence_2 else "") + \
                     f"Recent news activity ({news_count} items) may be influencing sentiment."

    narrative = (sentence_1 + " " + sentence_2).strip()

    # attach NB1 object
    signal["nb1"] = {
        "summary": summary,
        "tags": tags,
        "drivers": drivers,
        "narrative": narrative
    }

    return signal


def apply_nb1_to_all(signals: List[Dict]) -> List[Dict]:
    """Apply NB1 enrichment to all signals in a list."""
    return [attach_nb1(s) for s in signals]
