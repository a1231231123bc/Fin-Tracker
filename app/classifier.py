from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass

from .taxonomy import SUBCATEGORIES, SUBCATEGORY_TO_CATEGORY


@dataclass(frozen=True)
class Prediction:
    category: str
    subcategory: str
    confidence: float
    reason: str


def normalize_text(text: str) -> str:
    lowered = text.lower().strip()
    cleaned = re.sub(r"[^a-zа-я0-9\s]", " ", lowered, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def predict_category(note: str, alias_subcategory: str | None = None) -> Prediction | None:
    normalized = normalize_text(note)
    if not normalized:
        return None

    if alias_subcategory and alias_subcategory in SUBCATEGORY_TO_CATEGORY:
        return Prediction(
            category=SUBCATEGORY_TO_CATEGORY[alias_subcategory],
            subcategory=alias_subcategory,
            confidence=0.97,
            reason="alias",
        )

    score: dict[str, float] = defaultdict(float)
    for sub in SUBCATEGORIES:
        for word in sub.keywords:
            if word in normalized:
                score[sub.key] += 1.0
                if normalized == word or normalized.startswith(word + " "):
                    score[sub.key] += 0.35

    if not score:
        return None

    ranked = sorted(score.items(), key=lambda x: x[1], reverse=True)
    best_subcategory, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0

    confidence = min(0.98, 0.55 + (best_score * 0.12) - (second_score * 0.07))
    confidence = max(0.2, confidence)

    return Prediction(
        category=SUBCATEGORY_TO_CATEGORY[best_subcategory],
        subcategory=best_subcategory,
        confidence=round(confidence, 2),
        reason="rules",
    )
