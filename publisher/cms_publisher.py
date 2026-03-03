"""Generate Keystatic-compatible MD files from processed IntelligenceItems.

Outputs to the sibling bidmosaic-astro project's content directory,
or to a configurable output path via ASTRO_CONTENT_DIR env var.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from pipelines.common.schema import IntelligenceItem

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Default: sibling astro project's content dir
DEFAULT_ASTRO_CONTENT = PROJECT_ROOT.parent / "bidmosaic-astro" / "src" / "content" / "insights"
INSIGHTS_DIR = Path(os.environ.get("ASTRO_CONTENT_DIR", str(DEFAULT_ASTRO_CONTENT)))


def slugify(text: str) -> str:
    """Generate URL-friendly slug from text."""
    text = text.lower().strip()
    text = re.sub(r"[：:，,。.！!？?、；;""''【】\[\]（）()]", " ", text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = text.strip("-")
    return text[:80] if text else "untitled"


def generate_md(item: IntelligenceItem) -> str:
    """Generate Keystatic-compatible markdown with YAML frontmatter."""
    title = item.generated_title or item.raw_title
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    tags_yaml = "\n".join(f"  - {t}" for t in item.tags) if item.tags else "  - general"
    tier = "pro" if item.importance_score >= 7 and item.generated_analysis else "free"

    frontmatter = f"""---
title: "{title}"
summary: "{item.generated_summary}"
domain: "{item.domain}"
tags:
{tags_yaml}
source: "{item.source_name}"
sourceUrl: "{item.source_url}"
importance: {item.importance_score}
publishedAt: "{date}"
tier: "{tier}"
status: "draft"
---"""

    body = item.generated_summary or item.raw_content[:500]

    if item.generated_analysis:
        body += f"\n\n## 深度分析\n\n{item.generated_analysis}"

    if item.source_url:
        body += f"\n\n---\n\n来源: [{item.source_name}]({item.source_url})"

    return f"{frontmatter}\n\n{body}\n"


def publish_items(items: list[IntelligenceItem], min_score: int = 5) -> list[Path]:
    """Publish qualifying items as Keystatic MD files."""
    INSIGHTS_DIR.mkdir(parents=True, exist_ok=True)

    published = []
    for item in items:
        if item.importance_score < min_score:
            continue

        slug = slugify(item.generated_title or item.raw_title)
        date_prefix = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"{date_prefix}-{slug}.md"
        filepath = INSIGHTS_DIR / filename

        md = generate_md(item)
        filepath.write_text(md, encoding="utf-8")
        published.append(filepath)

    print(f"Published {len(published)} insights to {INSIGHTS_DIR}")
    return published


def main():
    """Load today's processed data and publish to CMS."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    processed_dir = PROCESSED_DIR / today

    if not processed_dir.exists():
        dirs = sorted(PROCESSED_DIR.iterdir()) if PROCESSED_DIR.exists() else []
        if not dirs:
            print("No processed data found.")
            return
        processed_dir = dirs[-1]

    items = []
    for fp in sorted(processed_dir.glob("*.json")):
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        items.append(IntelligenceItem.from_dict(data))

    print(f"Loaded {len(items)} processed items from {processed_dir}")
    publish_items(items)


if __name__ == "__main__":
    main()
