"""RSS feed collector - fetches and parses RSS feeds into IntelligenceItems."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    defaults = cfg.get("defaults", {})
    feeds = []
    for category, feed_list in cfg.get("feeds", {}).items():
        for feed in feed_list:
            merged = {**defaults, **feed}
            if not merged.get("enabled", True):
                continue
            merged["category"] = category
            feeds.append(merged)

    feeds.sort(key=lambda item: item.get("authority_score", 0), reverse=True)
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
        metadata={
            "feed_url": feed_meta.get("url", ""),
            "category": feed_meta.get("category", ""),
            "source_type": feed_meta.get("source_type", "media"),
            "quality_tier": feed_meta.get("quality_tier", "standard"),
            "authority_score": feed_meta.get("authority_score", 0),
        },
    )


MAX_WORKERS = 10


def _fetch_feed(feed_meta: dict) -> list[IntelligenceItem]:
    """Fetch a single RSS feed and return parsed items."""
    url = feed_meta["url"]
    max_entries = int(feed_meta.get("max_entries", 20))
    print(
        "Fetching: "
        f"{feed_meta['name']} "
        f"[score={feed_meta.get('authority_score', 0)}, "
        f"type={feed_meta.get('source_type', 'media')}] "
        f"({url})",
        flush=True,
    )
    items = []
    try:
        parsed = feedparser.parse(url)
        for entry in parsed.entries[:max_entries]:
            item = parse_entry(entry, feed_meta)
            if item.raw_title and item.source_url:
                items.append(item)
        print(f"  -> {len(parsed.entries)} entries found", flush=True)
    except Exception as e:
        print(f"  -> ERROR: {e}", flush=True)
    return items


def collect_all() -> list[IntelligenceItem]:
    """Collect from all configured RSS feeds concurrently."""
    feeds = load_feeds()
    all_items: list[IntelligenceItem] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_feed, feed_meta): feed_meta for feed_meta in feeds}
        for future in as_completed(futures):
            all_items.extend(future.result())

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
