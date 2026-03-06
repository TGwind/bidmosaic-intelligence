"""Unified AI processing pipeline.

Reads raw IntelligenceItems, runs MiniMax 2.5 for:
  1. Dedup & filter
  2. Summarize + Classify + Score (single API call)
  3. Deep analysis for high-scoring items
  4. Output processed JSON
"""

from __future__ import annotations

import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from pipelines.common.schema import IntelligenceItem
from pipelines.common.minimax_client import MiniMaxClient
from pipelines.common.dedup import dedup_items
from pipelines.rss.transformer import load_raw_items, clean_items

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Concurrency settings
MAX_WORKERS = 8


def load_prompt(name: str) -> str:
    return (PROMPTS_DIR / f"{name}.txt").read_text(encoding="utf-8")


def parse_json_response(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code fences."""
    text = text.strip()
    # Try to find a JSON object with nested structures (arrays, objects)
    # Use a more robust approach: find { and match to the last }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    return {}


def process_single(client: MiniMaxClient, item: IntelligenceItem, prompt_template: str) -> None:
    """Process a single item: summarize + classify + score in one API call."""
    prompt = prompt_template.format(
        title=item.raw_title,
        source=item.source_name,
        content=item.raw_content[:2000],
    )
    resp = client.chat(prompt)
    parsed = parse_json_response(resp)

    item.generated_title = parsed.get("title", item.raw_title)
    item.generated_summary = parsed.get("summary", "")
    item.domain = parsed.get("domain", item.domain or "general")
    item.tags = parsed.get("tags", [])
    item.importance_score = int(parsed.get("score", 5))


def analyze_deep(client: MiniMaxClient, item: IntelligenceItem, prompt_template: str) -> None:
    """Generate deep analysis for high-scoring items (Pro content)."""
    prompt = prompt_template.format(
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


def _process_item(
    client: MiniMaxClient,
    item: IntelligenceItem,
    idx: int,
    total: int,
    process_prompt: str,
    deep_prompt: str,
) -> IntelligenceItem:
    """Worker function for concurrent processing."""
    try:
        process_single(client, item, process_prompt)
        if item.importance_score >= 7:
            analyze_deep(client, item, deep_prompt)
        item.processed_at = datetime.now(timezone.utc).isoformat()
        print(f"  [{idx}/{total}] score={item.importance_score} {item.generated_title[:40]}", flush=True)
    except Exception as e:
        print(f"  [{idx}/{total}] ERROR: {item.raw_title[:40]}... -> {e}", flush=True)
        item.importance_score = 0
        item.processed_at = datetime.now(timezone.utc).isoformat()
    return item


def process_pipeline(raw_dir: Path) -> list[IntelligenceItem]:
    """Run the full AI processing pipeline on raw items."""
    items = load_raw_items(raw_dir)
    items = clean_items(items)
    print(f"Loaded {len(items)} raw items", flush=True)

    # Dedup
    items = dedup_items(items)
    print(f"After dedup: {len(items)} items", flush=True)

    # Pre-load prompts (avoid repeated disk IO per item)
    process_prompt = load_prompt("process_all")
    deep_prompt = load_prompt("analyze_deep")

    # Concurrent processing with MiniMax
    total = len(items)
    print(f"Processing {total} items with {MAX_WORKERS} workers...", flush=True)

    with MiniMaxClient() as client:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_process_item, client, item, i + 1, total, process_prompt, deep_prompt): item
                for i, item in enumerate(items)
            }
            for future in as_completed(futures):
                future.result()  # raise any uncaught exceptions

    # Save
    output_dir = save_processed(items)
    print(f"Saved processed items to {output_dir}", flush=True)

    return items


def main():
    """Entry point: find today's raw data and process it.

    Usage:
        python -m ai_processor.processor          # process RSS data (default)
        python -m ai_processor.processor trending  # process trending data
    """
    source_type = sys.argv[1] if len(sys.argv) > 1 else "rss"
    raw_base = PROJECT_ROOT / "data" / "raw" / source_type
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = raw_base / today

    if not raw_dir.exists():
        dirs = sorted(raw_base.iterdir()) if raw_base.exists() else []
        if not dirs:
            print(f"No {source_type} raw data found.")
            return
        raw_dir = dirs[-1]

    print(f"Processing {source_type} data from: {raw_dir}", flush=True)
    items = process_pipeline(raw_dir)

    high = [i for i in items if i.importance_score >= 7]
    mid = [i for i in items if 5 <= i.importance_score < 7]
    low = [i for i in items if i.importance_score < 5]
    print(f"\nResults: {len(high)} high / {len(mid)} mid / {len(low)} low importance", flush=True)


if __name__ == "__main__":
    main()
