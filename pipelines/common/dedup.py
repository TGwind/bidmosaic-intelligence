"""Simple dedup logic based on title similarity."""

from __future__ import annotations

from difflib import SequenceMatcher

from pipelines.common.schema import IntelligenceItem

NGRAM_SIZE = 3
JACCARD_PRE_THRESHOLD = 0.5


def _ngrams(text: str) -> set[str]:
    """Generate character n-gram set for a string."""
    t = text.lower()
    if len(t) < NGRAM_SIZE:
        return {t}
    return {t[i : i + NGRAM_SIZE] for i in range(len(t) - NGRAM_SIZE + 1)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def dedup_items(
    items: list[IntelligenceItem],
    threshold: float = 0.85,
) -> list[IntelligenceItem]:
    """Remove items with near-duplicate titles. Keep the first occurrence.

    Uses 3-gram Jaccard similarity as a fast pre-filter to avoid
    expensive SequenceMatcher calls on clearly dissimilar pairs.
    """
    unique: list[IntelligenceItem] = []
    unique_ngrams: list[set[str]] = []

    for item in items:
        item_ngrams = _ngrams(item.raw_title)
        is_dup = False
        for u, u_ng in zip(unique, unique_ngrams):
            # Fast pre-filter: skip expensive comparison if Jaccard is low
            if _jaccard(item_ngrams, u_ng) < JACCARD_PRE_THRESHOLD:
                continue
            if title_similarity(item.raw_title, u.raw_title) >= threshold:
                is_dup = True
                break
        if not is_dup:
            unique.append(item)
            unique_ngrams.append(item_ngrams)

    return unique
