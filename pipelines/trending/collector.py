"""Trending topics collector - direct platform API calls.

Fetches hot/trending data from Chinese platforms without external dependencies.
Supports: Toutiao, Baidu, Bilibili. Extensible to more platforms.
Falls back to DailyHotApi instance if DAILYHOT_API_URL is configured.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx

from pipelines.common.schema import IntelligenceItem

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "trending"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


# ---------- Platform fetchers ----------

def _fetch_toutiao(client: httpx.Client, max_entries: int = 15) -> list[dict]:
    """Fetch Toutiao (今日头条) trending."""
    resp = client.get(
        "https://www.toutiao.com/hot-event/hot-board/",
        params={"origin": "toutiao_pc"},
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])[:max_entries]
    return [
        {
            "title": item.get("Title", ""),
            "url": item.get("Url", ""),
            "hot": item.get("HotValue", 0),
            "desc": item.get("Title", ""),
        }
        for item in data if item.get("Title")
    ]


def _fetch_bilibili(client: httpx.Client, max_entries: int = 12) -> list[dict]:
    """Fetch Bilibili (B站) trending."""
    resp = client.get("https://api.bilibili.com/x/web-interface/ranking/v2", params={"rid": 0, "type": "all"})
    resp.raise_for_status()
    items = resp.json().get("data", {}).get("list", [])[:max_entries]
    return [
        {
            "title": item.get("title", ""),
            "url": f"https://www.bilibili.com/video/{item.get('bvid', '')}",
            "hot": item.get("stat", {}).get("view", 0),
            "desc": item.get("desc", ""),
        }
        for item in items if item.get("title")
    ]


def _fetch_juejin(client: httpx.Client, max_entries: int = 10) -> list[dict]:
    """Fetch Juejin (掘金) hot articles."""
    resp = client.post(
        "https://api.juejin.cn/content_api/v1/content/article_rank",
        json={"category_id": "1", "type": "hot"},
        params={"aid": "2608", "uuid": "0"},
    )
    resp.raise_for_status()
    items = resp.json().get("data", [])[:max_entries]
    return [
        {
            "title": item.get("content", {}).get("title", ""),
            "url": f"https://juejin.cn/post/{item.get('content', {}).get('content_id', '')}",
            "hot": item.get("content_counter", {}).get("hot_rank", 0),
            "desc": "",
        }
        for item in items if item.get("content", {}).get("title")
    ]


def _fetch_dailyhot(client: httpx.Client, platform: str, max_entries: int = 15) -> list[dict]:
    """Fetch from DailyHotApi instance (fallback/extra platforms)."""
    api_base = os.environ.get("DAILYHOT_API_URL", "").rstrip("/")
    if not api_base:
        return []
    resp = client.get(f"{api_base}/{platform}")
    resp.raise_for_status()
    entries = resp.json().get("data", [])[:max_entries]
    return [
        {
            "title": e.get("title", ""),
            "url": e.get("url") or e.get("mobileUrl", ""),
            "hot": e.get("hot", 0),
            "desc": e.get("desc", ""),
        }
        for e in entries if e.get("title")
    ]


# ---------- Platform registry ----------

PLATFORMS = [
    {"id": "toutiao", "name": "今日头条", "domain": "general", "fetcher": _fetch_toutiao, "authority": 72, "max": 15},
    {"id": "bilibili", "name": "B站热门",  "domain": "general", "fetcher": _fetch_bilibili, "authority": 65, "max": 12},
    {"id": "juejin",   "name": "掘金热榜",  "domain": "tech",    "fetcher": _fetch_juejin,   "authority": 73, "max": 10},
]


def _to_items(entries: list[dict], platform: dict) -> list[IntelligenceItem]:
    items = []
    for e in entries:
        if not e.get("title"):
            continue
        items.append(IntelligenceItem(
            source_pipeline=f"trending_{platform['id']}",
            raw_title=e["title"],
            raw_content=e.get("desc") or e["title"],
            source_url=e.get("url", ""),
            source_name=platform["name"],
            domain=platform["domain"],
            data_type="news",
            metadata={
                "source_type": "trending",
                "quality_tier": "secondary",
                "authority_score": platform["authority"],
                "hot_score": e.get("hot", 0),
                "platform": platform["id"],
            },
        ))
    return items


def collect_all() -> list[IntelligenceItem]:
    all_items: list[IntelligenceItem] = []

    with httpx.Client(timeout=15, headers={"User-Agent": UA}) as client:
        for platform in PLATFORMS:
            pid = platform["id"]
            name = platform["name"]
            print(f"Fetching: {name}", flush=True)
            try:
                entries = platform["fetcher"](client, platform["max"])
                items = _to_items(entries, platform)
                all_items.extend(items)
                print(f"  -> {len(items)} entries", flush=True)
            except Exception as e:
                print(f"  -> ERROR: {e}", flush=True)

        # DailyHotApi extra platforms (if configured)
        api_base = os.environ.get("DAILYHOT_API_URL", "")
        if api_base:
            for pid, name, domain in [("weibo", "微博热搜", "general"), ("zhihu", "知乎热榜", "tech")]:
                pinfo = {"id": pid, "name": name, "domain": domain, "authority": 74}
                print(f"Fetching: {name} (via DailyHotApi)", flush=True)
                try:
                    entries = _fetch_dailyhot(client, pid, 15)
                    items = _to_items(entries, pinfo)
                    all_items.extend(items)
                    print(f"  -> {len(items)} entries", flush=True)
                except Exception as e:
                    print(f"  -> ERROR: {e}", flush=True)

    return all_items


def save_raw(items: list[IntelligenceItem]) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = RAW_DIR / today
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in items:
        filepath = output_dir / f"{item.id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(item.to_dict(), f, ensure_ascii=False, indent=2)

    print(f"Saved {len(items)} trending items to {output_dir}")
    return output_dir


def main():
    items = collect_all()
    print(f"\nTotal collected: {len(items)} trending items")
    if items:
        save_raw(items)


if __name__ == "__main__":
    main()
