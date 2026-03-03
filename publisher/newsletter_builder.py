"""Build newsletter email content from processed IntelligenceItems."""

from __future__ import annotations

import json
import os
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import yaml

from pipelines.common.schema import IntelligenceItem

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
QUEUE_DIR = PROJECT_ROOT / "data" / "newsletter_queue"

SITE_URL = os.environ.get("SITE_URL", "https://bidmosaic.com")
UNSUBSCRIBE_URL_PLACEHOLDER = "{{unsubscribe_url}}"
DEFAULT_ASTRO_CONTENT = PROJECT_ROOT.parent / "bidmosaic-astro" / "src" / "content" / "insights"
ASTRO_CONTENT_DIR = Path(os.environ.get("ASTRO_CONTENT_DIR", str(DEFAULT_ASTRO_CONTENT)))


def _sorted_processed_dirs() -> list[Path]:
    if not PROCESSED_DIR.exists():
        return []
    return sorted([p for p in PROCESSED_DIR.iterdir() if p.is_dir()])


def load_processed_items(date: str | None = None, days: int = 1) -> list[IntelligenceItem]:
    if date:
        target_dirs = [PROCESSED_DIR / date]
    else:
        dirs = _sorted_processed_dirs()
        if not dirs:
            return []
        target_dirs = dirs[-days:]

    items = []
    for target_dir in target_dirs:
        if not target_dir.exists():
            continue
        for fp in sorted(target_dir.glob("*.json")):
            with open(fp, encoding="utf-8") as f:
                items.append(IntelligenceItem.from_dict(json.load(f)))
    return items


def _dedup_items(items: list[IntelligenceItem]) -> list[IntelligenceItem]:
    unique: OrderedDict[str, IntelligenceItem] = OrderedDict()
    for item in sorted(
        items,
        key=lambda x: x.processed_at or x.collected_at,
        reverse=True,
    ):
        key = item.source_url or item.generated_title or item.raw_title or item.id
        if key not in unique:
            unique[key] = item
    return list(unique.values())


def _parse_markdown_frontmatter(filepath: Path) -> tuple[dict, str]:
    text = filepath.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}, text
    try:
        _sep, frontmatter, body = text.split("---\n", 2)
    except ValueError:
        return {}, text
    return yaml.safe_load(frontmatter) or {}, body.strip()


def load_published_cms_items(days: int = 7) -> list[IntelligenceItem]:
    if not ASTRO_CONTENT_DIR.exists():
        return []

    cutoff = datetime.now(timezone.utc).date().toordinal() - max(days - 1, 0)
    items: list[IntelligenceItem] = []
    for fp in sorted(ASTRO_CONTENT_DIR.glob("*.md")):
        frontmatter, body = _parse_markdown_frontmatter(fp)
        if frontmatter.get("status") != "published":
            continue
        published_at = str(frontmatter.get("publishedAt") or "")
        if not published_at:
            continue
        try:
            published_date = datetime.fromisoformat(published_at).date().toordinal()
        except ValueError:
            continue
        if published_date < cutoff:
            continue

        items.append(
            IntelligenceItem(
                source_pipeline="cms_published",
                raw_title=frontmatter.get("title", fp.stem),
                raw_content=body,
                source_url=frontmatter.get("sourceUrl", f"{SITE_URL}/insights"),
                source_name=frontmatter.get("source", "BidMosaic"),
                domain=frontmatter.get("domain", "general"),
                data_type="analysis",
                generated_title=frontmatter.get("title", fp.stem),
                generated_summary=frontmatter.get("summary", ""),
                generated_analysis=body,
                tags=frontmatter.get("tags", []),
                importance_score=int(frontmatter.get("importance", 5)),
                collected_at=published_at,
                processed_at=published_at,
                metadata={"tier": frontmatter.get("tier", "free"), "cms_file": str(fp)},
            )
        )
    return items


def build_weekly_digest(items: list[IntelligenceItem]) -> dict:
    """Build weekly digest: top 5-10 items with score >= 7."""
    items = _dedup_items(items)
    top_items = sorted(
        [i for i in items if i.importance_score >= 7],
        key=lambda x: x.importance_score,
        reverse=True,
    )[:10]

    today = datetime.now(timezone.utc).strftime("%Y.%m.%d")

    text_items = []
    for idx, item in enumerate(top_items, 1):
        title = item.generated_title or item.raw_title
        text_items.append(f"{idx}. {title} — {item.generated_summary}")

    text_body = f"情报周报 — {today}\n\n要闻速读\n" + "\n".join(text_items)
    text_body += f"\n\n查看完整报告: {SITE_URL}/insights"

    html_items = ""
    for idx, item in enumerate(top_items, 1):
        title = item.generated_title or item.raw_title
        html_items += f"""
        <tr><td style="padding:12px 0;border-bottom:1px solid #eee;">
          <strong>{idx}. {title}</strong>
          <br><span style="color:#666;font-size:14px;">{item.generated_summary}</span>
          <br><span style="color:#999;font-size:12px;">{item.source_name} · {item.domain}</span>
        </td></tr>"""

    html_body = _wrap_email_html(f"""
    <h2 style="color:#1a1a1a;margin-bottom:4px;">情报周报</h2>
    <p style="color:#666;margin-top:0;">{today}</p>
    <table width="100%" cellpadding="0" cellspacing="0">{html_items}</table>
    <p style="margin-top:24px;">
      <a href="{SITE_URL}/insights" style="background:#1a1a1a;color:#fff;padding:10px 24px;text-decoration:none;border-radius:4px;">查看完整报告</a>
    </p>""")

    return {
        "subject": f"[BidMosaic] 情报周报 — {today}",
        "text": text_body,
        "html": html_body,
        "item_count": len(top_items),
    }


def build_daily_brief(items: list[IntelligenceItem]) -> dict:
    """Build daily brief: items with score >= 5."""
    items = _dedup_items(items)
    today_items = sorted(
        [i for i in items if i.importance_score >= 5],
        key=lambda x: x.importance_score,
        reverse=True,
    )[:15]

    today = datetime.now(timezone.utc).strftime("%Y.%m.%d")
    headlines = [i for i in today_items if i.importance_score >= 7]
    others = [i for i in today_items if i.importance_score < 7]

    text_lines = [f"今日情报速递 — {today}\n", "要闻速读"]
    for idx, item in enumerate(headlines, 1):
        title = item.generated_title or item.raw_title
        text_lines.append(f"{idx}. {title} — {item.generated_summary}")
    if others:
        text_lines.append("\n其他值得关注")
        for idx, item in enumerate(others, 1):
            title = item.generated_title or item.raw_title
            text_lines.append(f"{idx}. {title} — {item.generated_summary}")

    text_body = "\n".join(text_lines) + f"\n\n查看详情: {SITE_URL}/insights"

    html_headlines = ""
    for item in headlines:
        title = item.generated_title or item.raw_title
        html_headlines += f"<li style='margin-bottom:8px;'><strong>{title}</strong><br><span style='color:#666;font-size:14px;'>{item.generated_summary}</span></li>"

    html_others = ""
    for item in others:
        title = item.generated_title or item.raw_title
        html_others += f"<li style='margin-bottom:6px;color:#444;'>{title}</li>"

    html_body = _wrap_email_html(f"""
    <h2 style="color:#1a1a1a;margin-bottom:4px;">今日情报速递</h2>
    <p style="color:#666;margin-top:0;">{today}</p>
    <h3 style="color:#333;">要闻速读</h3>
    <ol style="padding-left:20px;">{html_headlines}</ol>
    {"<h3 style='color:#333;'>其他关注</h3><ul style='padding-left:20px;'>" + html_others + "</ul>" if html_others else ""}
    <p style="margin-top:24px;">
      <a href="{SITE_URL}/insights" style="background:#1a1a1a;color:#fff;padding:10px 24px;text-decoration:none;border-radius:4px;">查看详情</a>
    </p>""")

    return {
        "subject": f"[BidMosaic] 今日情报速递 — {today}",
        "text": text_body,
        "html": html_body,
        "item_count": len(today_items),
    }


def _wrap_email_html(content: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:24px 16px;">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;">
        <tr><td style="padding:32px 24px;background:#1a1a1a;">
          <h1 style="margin:0;color:#fff;font-size:20px;">BidMosaic Intelligence</h1>
        </td></tr>
        <tr><td style="padding:24px;">{content}</td></tr>
        <tr><td style="padding:16px 24px;background:#fafafa;border-top:1px solid #eee;font-size:12px;color:#999;">
          <p>BidMosaic 情报订阅服务 · <a href="{UNSUBSCRIBE_URL_PLACEHOLDER}" style="color:#999;">退订</a></p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def save_to_queue(newsletter: dict, queue_type: str = "weekly") -> Path:
    queue_dir = QUEUE_DIR / queue_type
    queue_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = queue_dir / f"{today}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(newsletter, f, ensure_ascii=False, indent=2)

    print(f"Saved {queue_type} newsletter to queue: {filepath}")
    return filepath


def main():
    daily_items = load_published_cms_items(days=1)
    weekly_items = load_published_cms_items(days=7)
    if not daily_items and not weekly_items:
        print("No published CMS items found.")
        return

    weekly = build_weekly_digest(weekly_items)
    print(f"Weekly digest: {weekly['item_count']} items")
    if weekly["item_count"] > 0:
        save_to_queue(weekly, "weekly")
    else:
        print("Skip weekly queue: no published items matched.")

    daily = build_daily_brief(daily_items)
    print(f"Daily brief: {daily['item_count']} items")
    if daily["item_count"] > 0:
        save_to_queue(daily, "daily")
    else:
        print("Skip daily queue: no published items matched.")


if __name__ == "__main__":
    main()
