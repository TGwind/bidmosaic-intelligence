"""Utilities for loading raw JSON back into IntelligenceItems and cleaning."""

from __future__ import annotations

import json
import re
from pathlib import Path

from pipelines.common.schema import IntelligenceItem


def strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def load_raw_items(raw_dir: Path) -> list[IntelligenceItem]:
    """Load all IntelligenceItems from a raw data directory."""
    items = []
    for filepath in sorted(raw_dir.glob("*.json")):
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        items.append(IntelligenceItem.from_dict(data))
    return items


def clean_items(items: list[IntelligenceItem]) -> list[IntelligenceItem]:
    """Strip HTML and normalize whitespace in raw content."""
    for item in items:
        item.raw_content = strip_html(item.raw_content)
        item.raw_title = strip_html(item.raw_title)
    return items
