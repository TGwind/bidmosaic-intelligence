"""Stock-specific AI processor.

Processes stock market data items with specialized analysis prompts.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pipelines.common.schema import IntelligenceItem
from pipelines.common.minimax_client import MiniMaxClient
from pipelines.rss.transformer import load_raw_items

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def load_prompt(name: str) -> str:
    return (PROMPTS_DIR / f"{name}.txt").read_text(encoding="utf-8")


def parse_json_response(text: str) -> dict:
    text = text.strip()
    # Try to find JSON in the response
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return {}


def analyze_stock(client: MiniMaxClient, item: IntelligenceItem) -> None:
    """Run stock-specific AI analysis."""
    prompt = load_prompt("stock_analyze").format(content=item.raw_content)
    resp = client.chat(prompt)
    parsed = parse_json_response(resp)

    trend = parsed.get("trend", "震荡")
    signal = parsed.get("signal", "观望")
    analysis = parsed.get("analysis", "")
    risk = parsed.get("risk", "")

    # Set generated fields
    item.generated_title = f"{item.raw_title} | {signal}"
    item.generated_summary = f"趋势: {trend} · 信号: {signal}"
    item.generated_analysis = analysis
    if risk:
        item.generated_analysis += f"\n\n风险提示: {risk}"

    item.tags = ["A股", trend, signal]

    # Score based on signal strength
    signal_scores = {"买入": 9, "增持": 8, "卖出": 8, "减持": 7, "持有": 6, "观望": 5}
    item.importance_score = signal_scores.get(signal, 6)

    # Also factor in price change magnitude
    pct = abs(item.market_data.price_change.get("change_pct", 0))
    if pct >= 5:
        item.importance_score = max(item.importance_score, 8)
    elif pct >= 3:
        item.importance_score = max(item.importance_score, 7)

    item.metadata["trend"] = trend
    item.metadata["signal"] = signal
    item.metadata["risk"] = risk


def analyze_market_overview(client: MiniMaxClient, item: IntelligenceItem) -> None:
    """Analyze overall market conditions."""
    prompt = f"""你是一位资深A股分析师。请对今日大盘行情进行简要点评（100-150字）。

行情数据：
{item.raw_content}

要求：
- 点评今日市场整体表现
- 分析主要指数走势差异
- 给出短期市场情绪判断
- 语言简洁专业

请直接返回分析文本。"""

    item.generated_title = "A股大盘行情速览"
    item.generated_analysis = client.chat(prompt)
    item.generated_summary = item.raw_content
    item.importance_score = 8  # Market overview always important
    item.tags = ["A股", "大盘", "行情"]


def process_stock_items(raw_dir: Path) -> list[IntelligenceItem]:
    """Process stock items from raw data directory."""
    items = load_raw_items(raw_dir)
    print(f"Loaded {len(items)} stock items")

    with MiniMaxClient() as client:
        for i, item in enumerate(items):
            print(f"Processing [{i+1}/{len(items)}]: {item.raw_title[:50]}...")

            if "大盘行情" in item.raw_title:
                analyze_market_overview(client, item)
            else:
                analyze_stock(client, item)

            item.processed_at = datetime.now(timezone.utc).isoformat()

    # Save processed
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = PROCESSED_DIR / today
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in items:
        filepath = output_dir / f"{item.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(item.to_dict(), f, ensure_ascii=False, indent=2)

    print(f"Saved {len(items)} processed stock items to {output_dir}")
    return items


def main():
    raw_base = PROJECT_ROOT / "data" / "raw" / "stock"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = raw_base / today

    if not raw_dir.exists():
        dirs = sorted(raw_base.iterdir()) if raw_base.exists() else []
        if not dirs:
            print("No stock raw data found.")
            return
        raw_dir = dirs[-1]

    print(f"Processing stock data from: {raw_dir}")
    items = process_stock_items(raw_dir)

    buy_sell = [i for i in items if i.metadata.get("signal") in ("买入", "增持", "卖出", "减持")]
    print(f"\nActive signals: {len(buy_sell)}")
    for item in buy_sell:
        print(f"  {item.metadata.get('signal')}: {item.raw_title}")


if __name__ == "__main__":
    main()
