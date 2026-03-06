"""Stock market data collector using Sina Finance API."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml

from pipelines.common.schema import IntelligenceItem, MarketData

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_FILE = Path(__file__).resolve().parent / "config.yaml"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "stock"

SINA_HQ_URL = "https://hq.sinajs.cn/list="
SINA_HEADERS = {"Referer": "https://finance.sina.com.cn"}


def load_config() -> dict:
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _sina_prefix(symbol: str) -> str:
    """Convert symbol to Sina format: sh600519 or sz000858."""
    if symbol.startswith(("6", "5")):
        return f"sh{symbol}"
    return f"sz{symbol}"


def fetch_sina_quotes(symbols: list[dict], client: httpx.Client | None = None) -> list[dict]:
    """Batch fetch quotes from Sina Finance API.

    Sina response format (comma-separated):
    name, open, prev_close, price, high, low, bid, ask, volume, turnover,
    ... (bid/ask levels), date, time, status
    """
    sina_codes = ",".join(_sina_prefix(s["symbol"]) for s in symbols)
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=15, headers=SINA_HEADERS)

    try:
        resp = client.get(f"{SINA_HQ_URL}{sina_codes}")
        resp.encoding = "gbk"
        text = resp.text
    finally:
        if own_client:
            client.close()

    quotes = []
    for line in text.strip().split("\n"):
        match = re.match(r'var hq_str_(\w+)="(.+)";', line)
        if not match:
            continue

        sina_code = match.group(1)
        raw_symbol = sina_code[2:]  # Remove sh/sz prefix
        fields = match.group(2).split(",")

        if len(fields) < 32:
            continue

        # Find matching config entry
        cfg_entry = next((s for s in symbols if s["symbol"] == raw_symbol), None)
        if not cfg_entry:
            continue

        name = fields[0]
        price = float(fields[3]) if fields[3] else 0
        prev_close = float(fields[2]) if fields[2] else 0
        open_price = float(fields[1]) if fields[1] else 0
        high = float(fields[4]) if fields[4] else 0
        low = float(fields[5]) if fields[5] else 0
        volume = float(fields[8]) if fields[8] else 0  # shares
        turnover = float(fields[9]) if fields[9] else 0  # CNY

        change_amt = price - prev_close if prev_close else 0
        change_pct = (change_amt / prev_close * 100) if prev_close else 0
        amplitude = ((high - low) / prev_close * 100) if prev_close else 0

        quotes.append({
            "symbol": raw_symbol,
            "name": cfg_entry.get("name", name),
            "price": price,
            "change_pct": round(change_pct, 2),
            "change_amt": round(change_amt, 2),
            "volume": volume,
            "turnover": turnover,
            "high": high,
            "low": low,
            "open": open_price,
            "prev_close": prev_close,
            "amplitude": round(amplitude, 2),
            "turnover_rate": 0,
            "pe_ratio": 0,
            "market_cap": 0,
        })

    return quotes


def fetch_index_quotes(client: httpx.Client | None = None) -> dict:
    """Fetch major index quotes."""
    index_codes = {
        "sh000001": ("000001", "上证指数"),
        "sh000300": ("000300", "沪深300"),
        "sz399001": ("399001", "深证成指"),
        "sz399006": ("399006", "创业板指"),
    }

    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=15, headers=SINA_HEADERS)
    try:
        codes = ",".join(index_codes.keys())
        resp = client.get(f"{SINA_HQ_URL}{codes}")
        resp.encoding = "gbk"
    finally:
        if own_client:
            client.close()

    indices = {}
    for line in resp.text.strip().split("\n"):
        match = re.match(r'var hq_str_(\w+)="(.+)";', line)
        if not match:
            continue

        sina_code = match.group(1)
        fields = match.group(2).split(",")

        if sina_code not in index_codes or len(fields) < 4:
            continue

        code, name = index_codes[sina_code]
        price = float(fields[3]) if fields[3] else 0
        prev_close = float(fields[2]) if fields[2] else 0
        change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0

        indices[code] = {
            "name": name,
            "price": round(price, 2),
            "change_pct": round(change_pct, 2),
        }

    return indices


def build_stock_item(quote: dict) -> IntelligenceItem:
    symbol = quote["symbol"]
    name = quote["name"]
    pct = quote["change_pct"]
    direction = "涨" if pct > 0 else "跌" if pct < 0 else "平"
    price = quote["price"]

    turnover_yi = quote["turnover"] / 1e8 if quote["turnover"] else 0

    raw_content = (
        f"{name}({symbol}) 最新价 {price:.2f}元，"
        f"{direction}{abs(pct):.2f}%，涨跌额 {quote['change_amt']:+.2f}元，"
        f"今开 {quote['open']:.2f}元，最高 {quote['high']:.2f}元，最低 {quote['low']:.2f}元，"
        f"昨收 {quote['prev_close']:.2f}元，成交额 {turnover_yi:.2f}亿元，"
        f"振幅 {quote['amplitude']:.2f}%。"
    )

    return IntelligenceItem(
        source_pipeline="stock_analysis",
        raw_title=f"{name}({symbol}) 今日{direction}{abs(pct):.2f}% 报{price:.2f}元",
        raw_content=raw_content,
        source_url=f"https://finance.sina.com.cn/realstock/company/{_sina_prefix(symbol)}/nc.shtml",
        source_name="新浪财经",
        domain="finance",
        data_type="market_data",
        metadata={
            "source_type": "aggregator",
            "quality_tier": "secondary",
            "authority_score": 55,
            "data_kind": "quote",
        },
        market_data=MarketData(
            symbols=[symbol],
            price_change={
                "price": price,
                "change_pct": pct,
                "change_amt": quote["change_amt"],
            },
            indicators={
                "volume": quote["volume"],
                "turnover": quote["turnover"],
                "turnover_rate": quote["turnover_rate"],
                "pe_ratio": quote["pe_ratio"],
                "amplitude": quote["amplitude"],
                "market_cap": quote["market_cap"],
            },
        ),
    )


def build_market_overview_item(indices: dict) -> IntelligenceItem | None:
    if not indices:
        return None

    lines = []
    for _code, info in indices.items():
        pct = info["change_pct"]
        arrow = "↑" if pct > 0 else "↓" if pct < 0 else "→"
        lines.append(f"{info['name']} {info['price']:.2f} {arrow}{abs(pct):.2f}%")

    raw_content = "A股大盘行情：" + "；".join(lines)

    return IntelligenceItem(
        source_pipeline="stock_analysis",
        raw_title="A股大盘行情速览",
        raw_content=raw_content,
        source_url="https://finance.sina.com.cn/realstock/company/sh000001/nc.shtml",
        source_name="新浪财经",
        domain="finance",
        data_type="market_data",
        metadata={
            "source_type": "aggregator",
            "quality_tier": "secondary",
            "authority_score": 55,
            "data_kind": "quote",
        },
        market_data=MarketData(
            symbols=list(indices.keys()),
            indicators=indices,
        ),
    )


def collect_all() -> list[IntelligenceItem]:
    config = load_config()
    items: list[IntelligenceItem] = []

    with httpx.Client(timeout=15, headers=SINA_HEADERS) as client:
        # Market overview
        print("Fetching market indices...")
        indices = fetch_index_quotes(client)
        for code, info in indices.items():
            pct = info["change_pct"]
            arrow = "↑" if pct > 0 else "↓"
            print(f"  {info['name']}: {info['price']:.2f} {arrow}{abs(pct):.2f}%")

        overview_item = build_market_overview_item(indices)
        if overview_item:
            items.append(overview_item)

        # Individual stocks
        print("Fetching stock quotes...")
        a_shares = config.get("watchlist", {}).get("a_share", [])
        quotes = fetch_sina_quotes(a_shares, client)

        for quote in quotes:
            pct = quote["change_pct"]
            print(f"  {quote['name']}({quote['symbol']}): {quote['price']:.2f} ({pct:+.2f}%)")
            items.append(build_stock_item(quote))

    return items


def save_raw(items: list[IntelligenceItem]) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = RAW_DIR / today
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in items:
        filepath = output_dir / f"{item.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(item.to_dict(), f, ensure_ascii=False, indent=2)

    print(f"Saved {len(items)} stock items to {output_dir}")
    return output_dir


def main():
    items = collect_all()
    print(f"\nTotal collected: {len(items)} stock items")
    if items:
        save_raw(items)


if __name__ == "__main__":
    main()
