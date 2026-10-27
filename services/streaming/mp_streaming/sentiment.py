"""VADER-based sentiment scoring. Pure Python; called inside Spark foreachBatch (driver-side).
No network I/O; SentimentIntensityAnalyzer is loaded once per process (module-level singleton).

Thresholds are taken from settings.yaml  sentiment.positive_threshold / negative_threshold.
Contract:
  sentiment_label: "positive" | "neutral" | "negative"  (never None once scored)
  sentiment_score: float in [-1.0, 1.0]   (VADER compound score)

data_mode=="test" records are scored but NOT persisted to production analytics collections.
Missing text -> returns (None, None); callers must preserve that as missing, not zero.
"""
from __future__ import annotations

from typing import Optional

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer as _VADER
    _analyzer = _VADER()
except ImportError:  # allow import in envs without vaderSentiment (Spark executor fallback)
    _analyzer = None  # type: ignore


def score_text(text: str | None, pos_threshold: float = 0.05, neg_threshold: float = -0.05) -> tuple[Optional[str], Optional[float]]:
    """Return (label, compound_score) for text; (None, None) when text is empty/missing or VADER unavailable."""
    if not text or not text.strip() or _analyzer is None:
        return None, None
    compound = _analyzer.polarity_scores(text)["compound"]
    if compound >= pos_threshold:
        label = "positive"
    elif compound <= neg_threshold:
        label = "negative"
    else:
        label = "neutral"
    return label, round(compound, 6)
