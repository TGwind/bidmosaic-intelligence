"""Rebuild newsletter queue from published CMS content."""

from __future__ import annotations

import argparse
import os

from publisher.newsletter_builder import (
    build_daily_brief,
    build_weekly_digest,
    load_published_cms_items,
    save_to_queue,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild newsletter queue from published CMS content.")
    parser.add_argument(
        "--type",
        choices=["daily", "weekly", "both"],
        default="both",
        help="Queue type to rebuild.",
    )
    parser.add_argument(
        "--content-dir",
        default="",
        help="Override ASTRO_CONTENT_DIR for this run.",
    )
    parser.add_argument(
        "--daily-days",
        type=int,
        default=1,
        help="How many recent days of published content to consider for daily queue.",
    )
    parser.add_argument(
        "--weekly-days",
        type=int,
        default=7,
        help="How many recent days of published content to consider for weekly queue.",
    )
    args = parser.parse_args()

    if args.content_dir:
        os.environ["ASTRO_CONTENT_DIR"] = args.content_dir

    if args.type in ("daily", "both"):
        daily_items = load_published_cms_items(days=args.daily_days)
        daily = build_daily_brief(daily_items)
        if daily["item_count"] > 0:
            save_to_queue(daily, "daily")
            print(f"Rebuilt daily queue with {daily['item_count']} items.")
        else:
            print("No published content matched daily queue rebuild.")

    if args.type in ("weekly", "both"):
        weekly_items = load_published_cms_items(days=args.weekly_days)
        weekly = build_weekly_digest(weekly_items)
        if weekly["item_count"] > 0:
            save_to_queue(weekly, "weekly")
            print(f"Rebuilt weekly queue with {weekly['item_count']} items.")
        else:
            print("No published content matched weekly queue rebuild.")


if __name__ == "__main__":
    main()
