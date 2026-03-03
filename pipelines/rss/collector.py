"""RSS feed collector - fetches and parses RSS feeds into IntelligenceItems."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import yaml
from dateutil import parser as dateparser

from pipelines.common.schema import IntelligenceItem

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FEEDS_CONFIG = Path(__file__).resolve().parent / "feeds.yaml"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "rss"


def load_feeds() -> list[dict]:
    """Load all feed configs from feeds.yaml."""
    with open(FEEDS_CONFIG) as f:
        cfg = yaml.safe_load(f)

    feeds = []
    for _category, feed_list in cfg.get("feeds", {}).items():
        feeds.extend(feed_list)
    return feeds


def parse_entry(entry: dict, feed_meta: dict) -> IntelligenceItem:
    """Convert a single feedparser entry to an IntelligenceItem."""
    published = ""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
    elif hasattr(entry, "published") and entry.published:
        try:
            published = dateparser.parse(entry.published).isoformat()
        except (ValueError, TypeError):
            published = datetime.now(timezone.utc).isoformat()
    else:
        published = datetime.now(timezone.utc).isoformat()

    # Extract content: prefer summary, fall back to content
    content = ""
    if hasattr(entry, "summary") and entry.summary:
        content = entry.summary
    elif hasattr(entry, "content") and entry.content:
        content = entry.content[0].get("value", "")

    return IntelligenceItem(
        source_pipeline="rss_" + feed_meta.get("domain", "general"),
        raw_title=entry.get("title", "Untitled"),
        raw_content=content,
        source_url=entry.get("link", ""),
        source_name=feed_meta["name"],
        domain=feed_meta.get("domain", ""),
        data_type="news",
        collected_at=published,
    )


def collect_all() -> list[IntelligenceItem]:
    """Collect from all configured RSS feeds."""
    feeds = load_feeds()
    all_items: list[IntelligenceItem] = []

    for feed_meta in feeds:
        url = feed_meta["url"]
        print(f"Fetching: {feed_meta['name']} ({url})")
        try:
            parsed = feedparser.parse(url)
            for entry in parsed.entries[:20]:  # max 20 per feed
                item = parse_entry(entry, feed_meta)
                if item.raw_title and item.source_url:
                    all_items.append(item)
            print(f"  -> {len(parsed.entries)} entries found")
        except Exception as e:
            print(f"  -> ERROR: {e}")

    return all_items


def save_raw(items: list[IntelligenceItem]) -> Path:
    """Save raw items to data/raw/rss/{date}/ as JSON files."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = RAW_DIR / today
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in items:
        filepath = output_dir / f"{item.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(item.to_dict(), f, ensure_ascii=False, indent=2)

    print(f"Saved {len(items)} items to {output_dir}")
    return output_dir


def main():
    items = collect_all()
    print(f"\nTotal collected: {len(items)} items")
    if items:
        save_raw(items)


if __name__ == "__main__":
    main()
