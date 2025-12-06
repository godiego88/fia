# brains/nb2/core.py
"""
NB2 — Deep Narrative Brain (Final Implementation)

Transforms QB2's deep quantitative output into a structured,
human-readable narrative suitable for Stage 2 final reporting.

No external API calls. Fully deterministic and cost-free.
"""

from __future__ import annotations
from typing import Dict, Any, List


# ---------------------------------------------------------
# Helper functions
# ---------------------------------------------------------
def _fmt(v) -> str:
    """Format floats nicely for narrative."""
    if v is None:
        return "N/A"
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)


def _trend_sentence(struct: Dict[str, Any]) -> str:
    trend = struct.get("trend")
    rsi = struct.get("rsi14")

    if not trend:
        return "No identifiable long-term trend."

    base = {
        "uptrend": "The long-term trend is upward",
        "downtrend": "The long-term trend is downward",
        "flat": "The long-term trend is flat",
    }.get(trend, "Trend is unclear")

    if rsi is None:
        return base + "."

    if rsi > 70:
        return base + ", with RSI indicating overbought conditions."
    if rsi < 30:
        return base + ", with RSI indicating oversold conditions."
    return base + ", with RSI in neutral territory."


def _fundamental_sentence(f: Dict[str, Any]) -> str:
    if not f:
        return "No fundamentals available."

    pe = f.get("pe")
    market_cap = f.get("market_cap")

    parts = []
    if pe:
        try:
            pe_val = float(pe)
            if pe_val < 10:
                parts.append("valuation appears low (PE < 10)")
            elif pe_val > 30:
                parts.append("valuation appears rich (PE > 30)")
        except:
            pass

    if market_cap:
        try:
            mc = float(market_cap)
            if mc > 50e9:
                parts.append("large-cap profile")
            elif mc < 2e9:
                parts.append("small-cap profile")
        except:
            pass

    if not parts:
        return "Fundamentals retrieved but no strong valuation signals detected."

    return "Fundamentals indicate: " + "; ".join(parts) + "."


def _compression_sentence(value: float) -> str:
    if value is None:
        return "Range compression unavailable."
    if value < 0.05:
        return "Price is in a tight consolidation range (high compression)."
    if value > 0.15:
        return "Price has shown expanded volatility recently."
    return "Price is in a normal volatility range."


# ---------------------------------------------------------
# NB2 MAIN
# ---------------------------------------------------------
def build_nb2(ticker: str, qb2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert QB2 deep results into a narrative block:
    {
        "summary": "...",
        "trend": "...",
        "fundamentals": "...",
        "volatility": "...",
        "narrative": "..."
    }
    """
    struct = qb2.get("structure", {})
    f = qb2.get("fundamentals", {})

    # Individual sentences
    s_trend = _trend_sentence(struct)
    s_fund = _fundamental_sentence(f)
    s_comp = _compression_sentence(struct.get("range_compression_20d"))
    vol20 = struct.get("vol20")

    # summary sentence
    summary = f"{ticker}: deep-dive analysis integrating trend, valuation, and volatility."

    # Combine into full narrative
    narrative = (
        f"{s_trend} {s_fund} {s_comp} "
        f"20-day volatility: {_fmt(vol20)}."
    ).strip()

    return {
        "ticker": ticker,
        "summary": summary,
        "trend": s_trend,
        "fundamentals": s_fund,
        "volatility": f"20d vol = {_fmt(vol20)}",
        "narrative": narrative,
    }
