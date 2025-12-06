"""
QB1 - Stage 1 Quant Brain
This module performs fast, cost-free anomaly detection for all tickers.
It uses Stage 1 APIs only: yfinance, Twelve Data, Finnhub (headlines), FRED, Google Sheets.
"""

import os
import pandas as pd
from typing import Dict, Any, List

# -----------------------------
# 1. Load universe (Google Sheets)
# -----------------------------
def load_universe_from_sheets() -> List[str]:
    """
    Returns a list of tickers from the Stage 1 sheet.
    """
    # Implement in Step 14
    return []


# -----------------------------
# 2. Fetch historical price data (yfinance)
# -----------------------------
def fetch_price_history(ticker: str, days: int = 14) -> pd.DataFrame:
    """
    Fetch historical OHLC data for the ticker using yfinance.
    """
    # Implement in Step 14
    return pd.DataFrame()


# -----------------------------
# 3. Compute anomaly score (Z-score)
# -----------------------------
def compute_price_anomaly(df: pd.DataFrame) -> float:
    """
    Compute short-term z-score anomaly using historical price data.
    """
    # Implement in Step 14
    return 0.0


# -----------------------------
# 4. Confirm recent move (Twelve Data)
# -----------------------------
def fetch_realtime_price(ticker: str) -> float:
    """
    Pull last traded price from Twelve Data.
    """
    # Implement in Step 14
    return 0.0


# -----------------------------
# 5. Macro backdrop (FRED)
# -----------------------------
def compute_macro_weight() -> float:
    """
    Compute macro weighting factor (inflation trend, yield curve, etc.).
    """
    # Implement in Step 14
    return 1.0


# -----------------------------
# 6. News presence (Finnhub)
# -----------------------------
def detect_news_presence(ticker: str) -> bool:
    """
    Return True if Finnhub reports recent news for this ticker.
    """
    # Implement in Step 14
    return False


# -----------------------------
# 7. Combine results per ticker
# -----------------------------
def build_stage1_signal(ticker: str) -> Dict[str, Any]:
    """
    Builds complete Stage1 signal object for a single ticker.
    """
    df = fetch_price_history(ticker)
    anomaly = compute_price_anomaly(df)
    price_now = fetch_realtime_price(ticker)
    macro = compute_macro_weight()
    news_flag = detect_news_presence(ticker)

    # final scoring logic (completed in Step 14)
    score = anomaly * macro

    should_trigger_stage2 = (abs(score) >= 2.0) or news_flag

    return {
        "ticker": ticker,
        "stage1_scores": {
            "price_anomaly": anomaly,
            "macro_weight": macro,
            "intraday_confirmation": price_now,
            "news_presence": news_flag
        },
        "recommend_stage2": should_trigger_stage2
    }


# -----------------------------
# 8. Main orchestrator
# -----------------------------
def run_qb1() -> List[Dict[str, Any]]:
    """
    Main entry for QB1 — returns Stage1 results for all tickers.
    """
    tickers = load_universe_from_sheets()
    results = []

    for t in tickers:
        try:
            result = build_stage1_signal(t)
            results.append(result)
        except Exception as e:
            results.append({
                "ticker": t,
                "error": str(e),
                "recommend_stage2": False
            })

    return results
