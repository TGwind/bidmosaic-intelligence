"""Unified AI processing pipeline.

Reads raw IntelligenceItems, runs MiniMax 2.5 for:
  1. Dedup & filter
  2. Summarize (title + one-liner)
  3. Classify (domain + tags)
  4. Score importance (1-10)
  5. Output processed JSON
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pipelines.common.schema import IntelligenceItem
from pipelines.common.minimax_client import MiniMaxClient
from pipelines.common.dedup import dedup_items
from pipelines.rss.transformer import load_raw_items, clean_items

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def load_prompt(name: str) -> str:
    return (PROMPTS_DIR / f"{name}.txt").read_text(encoding="utf-8")


def parse_json_response(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code fences."""
    text = text.strip()
    match = re.search(r"\{[^}]+\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {}


def summarize(client: MiniMaxClient, item: IntelligenceItem) -> None:
    prompt = load_prompt("summarize").format(
        title=item.raw_title,
        content=item.raw_content[:2000],
    )
    resp = client.chat(prompt)
    parsed = parse_json_response(resp)
    item.generated_title = parsed.get("title", item.raw_title)
    item.generated_summary = parsed.get("summary", "")


def classify(client: MiniMaxClient, item: IntelligenceItem) -> None:
    prompt = load_prompt("classify").format(
        title=item.generated_title or item.raw_title,
        summary=item.generated_summary,
    )
    resp = client.chat(prompt)
    parsed = parse_json_response(resp)
    item.domain = parsed.get("domain", item.domain or "general")
    item.tags = parsed.get("tags", [])


def score(client: MiniMaxClient, item: IntelligenceItem) -> None:
    prompt = load_prompt("score").format(
        title=item.generated_title or item.raw_title,
        summary=item.generated_summary,
        source=item.source_name,
        domain=item.domain,
    )
    resp = client.chat(prompt)
    digits = re.findall(r"\d+", resp)
    item.importance_score = int(digits[0]) if digits else 5


def analyze_deep(client: MiniMaxClient, item: IntelligenceItem) -> None:
    """Generate deep analysis for high-scoring items (Pro content)."""
    prompt = load_prompt("analyze_deep").format(
        title=item.generated_title or item.raw_title,
        summary=item.generated_summary,
        content=item.raw_content[:3000],
    )
    item.generated_analysis = client.chat(prompt)


def save_processed(items: list[IntelligenceItem]) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = PROCESSED_DIR / today
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in items:
        filepath = output_dir / f"{item.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(item.to_dict(), f, ensure_ascii=False, indent=2)

    return output_dir


def process_pipeline(raw_dir: Path) -> list[IntelligenceItem]:
    """Run the full AI processing pipeline on raw items."""
    items = load_raw_items(raw_dir)
    items = clean_items(items)
    print(f"Loaded {len(items)} raw items")

    # Task 1: Dedup
    items = dedup_items(items)
    print(f"After dedup: {len(items)} items")

    # Process with MiniMax
    with MiniMaxClient() as client:
        for i, item in enumerate(items):
            print(f"Processing [{i+1}/{len(items)}]: {item.raw_title[:50]}...")

            # Task 2: Summarize
            summarize(client, item)

            # Task 3: Classify
            classify(client, item)

            # Task 4: Score
            score(client, item)

            # Deep analysis for important items (>=7)
            if item.importance_score >= 7:
                analyze_deep(client, item)

            item.processed_at = datetime.now(timezone.utc).isoformat()

    # Task 5: Save
    output_dir = save_processed(items)
    print(f"Saved processed items to {output_dir}")

    return items


def main():
    """Entry point: find today's raw data and process it."""
    raw_base = PROJECT_ROOT / "data" / "raw" / "rss"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = raw_base / today

    if not raw_dir.exists():
        dirs = sorted(raw_base.iterdir()) if raw_base.exists() else []
        if not dirs:
            print("No raw data found.")
            return
        raw_dir = dirs[-1]

    print(f"Processing raw data from: {raw_dir}")
    items = process_pipeline(raw_dir)

    high = [i for i in items if i.importance_score >= 7]
    mid = [i for i in items if 5 <= i.importance_score < 7]
    low = [i for i in items if i.importance_score < 5]
    print(f"\nResults: {len(high)} high / {len(mid)} mid / {len(low)} low importance")


if __name__ == "__main__":
    main()
