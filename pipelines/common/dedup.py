"""Simple dedup logic based on title similarity."""

from __future__ import annotations

from difflib import SequenceMatcher

from pipelines.common.schema import IntelligenceItem


def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def dedup_items(
    items: list[IntelligenceItem],
    threshold: float = 0.85,
) -> list[IntelligenceItem]:
    """Remove items with near-duplicate titles. Keep the first occurrence."""
    unique: list[IntelligenceItem] = []
    for item in items:
        is_dup = any(
            title_similarity(item.raw_title, u.raw_title) >= threshold
            for u in unique
        )
        if not is_dup:
            unique.append(item)
    return unique
