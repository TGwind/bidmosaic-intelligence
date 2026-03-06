"""Build newsletter email content from processed IntelligenceItems."""

from __future__ import annotations

import argparse
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
        try:
            frontmatter, body = _parse_markdown_frontmatter(fp)
        except Exception:
            continue
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


# ─── Stock Report Builder ───────────────────────────────────────────


def _esc(text: str) -> str:
    """Escape HTML entities."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _stock_name(item: IntelligenceItem) -> str:
    return item.raw_title.split("(")[0].strip() if "(" in item.raw_title else item.raw_title.split("|")[0].strip()


def _signal_color(signal: str) -> tuple[str, str, str]:
    """Return (bg_color, text_color, border_color) for a signal."""
    colors = {
        "买入": ("#fef2f2", "#dc2626", "#fca5a5"),
        "增持": ("#fff7ed", "#ea580c", "#fdba74"),
        "持有": ("#ecfeff", "#0891b2", "#67e8f9"),
        "减持": ("#f0fdf4", "#16a34a", "#86efac"),
        "卖出": ("#f0fdf4", "#15803d", "#86efac"),
        "观望": ("#f9fafb", "#6b7280", "#d1d5db"),
    }
    return colors.get(signal, colors["观望"])


def _change_color(pct: float) -> str:
    """A-share convention: red for up, green for down."""
    if pct > 0:
        return "#dc2626"
    elif pct < 0:
        return "#16a34a"
    return "#6b7280"


def _build_stock_card_html(item: IntelligenceItem) -> str:
    """Build HTML card for a single stock with decision dashboard."""
    meta = item.metadata
    dashboard = meta.get("dashboard", {})
    signal = meta.get("signal", "观望")
    trend = meta.get("trend", "震荡")
    confidence = meta.get("confidence", "中")
    sentiment_score = meta.get("sentiment_score", 50)

    price_change = item.market_data.price_change
    price = price_change.get("price", 0)
    change_pct = price_change.get("change_pct", 0)
    change_amt = price_change.get("change_amt", 0)
    indicators = item.market_data.indicators

    # Extract stock name from raw_title
    stock_name = item.raw_title.split("(")[0].strip() if "(" in item.raw_title else item.raw_title.split("|")[0].strip()

    bg, txt, border = _signal_color(signal)
    pct_color = _change_color(change_pct)

    # Core conclusion
    core = dashboard.get("core_conclusion", {})
    one_sentence = _esc(core.get("one_sentence", item.generated_analysis[:60] if item.generated_analysis else ""))
    time_sense = _esc(core.get("time_sensitivity", "本周内"))
    pos_advice = core.get("position_advice", {})

    # Data perspective
    data_p = dashboard.get("data_perspective", {})
    ma_alignment = _esc(data_p.get("ma_alignment", ""))
    is_bullish = data_p.get("is_bullish", False)
    trend_score = data_p.get("trend_score", "")
    bias_ma5 = data_p.get("bias_ma5", "")
    bias_status = _esc(data_p.get("bias_status", ""))
    support = _esc(str(data_p.get("support_level", "")))
    resistance = _esc(str(data_p.get("resistance_level", "")))
    vol_status = _esc(data_p.get("volume_status", ""))
    vol_meaning = _esc(data_p.get("volume_meaning", ""))

    # Battle plan
    battle = dashboard.get("battle_plan", {})
    ideal_buy = _esc(str(battle.get("ideal_buy", "")))
    secondary_buy = _esc(str(battle.get("secondary_buy", "")))
    stop_loss = _esc(str(battle.get("stop_loss", "")))
    take_profit = _esc(str(battle.get("take_profit", "")))
    position_str = _esc(str(battle.get("suggested_position", "")))
    entry_plan = _esc(str(battle.get("entry_plan", "")))

    # Checklist
    checklist = dashboard.get("checklist", [])
    checklist_html = ""
    for c in checklist:
        c_esc = _esc(c)
        if c.startswith("✅"):
            checklist_html += f'<div style="color:#16a34a;font-size:13px;padding:2px 0;">{c_esc}</div>'
        elif c.startswith("❌"):
            checklist_html += f'<div style="color:#dc2626;font-size:13px;padding:2px 0;">{c_esc}</div>'
        else:
            checklist_html += f'<div style="color:#d97706;font-size:13px;padding:2px 0;">{c_esc}</div>'

    # Risk & catalysts
    risk_alerts = dashboard.get("risk_alerts", [])
    catalysts = dashboard.get("positive_catalysts", [])

    risks_html = ""
    if risk_alerts:
        for r in risk_alerts:
            risks_html += f'<div style="color:#dc2626;font-size:13px;padding:2px 0;">⚠ {_esc(r)}</div>'

    catalysts_html = ""
    if catalysts:
        for c in catalysts:
            catalysts_html += f'<div style="color:#16a34a;font-size:13px;padding:2px 0;">✦ {_esc(c)}</div>'

    # Outlook
    short_term = _esc(dashboard.get("short_term_outlook", ""))
    medium_term = _esc(dashboard.get("medium_term_outlook", ""))

    # Analysis text
    analysis = _esc(item.generated_analysis or "")

    # Turnover in 亿
    turnover = indicators.get("turnover", 0)
    turnover_yi = f"{turnover / 1e8:.2f}" if turnover else "-"
    amplitude = indicators.get("amplitude", 0)

    # Score bar width
    score_width = max(min(sentiment_score, 100), 0)
    score_color = "#dc2626" if score_width >= 70 else "#d97706" if score_width >= 50 else "#16a34a"

    # Build the card
    html = f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;border-collapse:separate;">
      <!-- Header: Stock name + signal + price -->
      <tr><td style="padding:16px 20px;background:linear-gradient(135deg,{bg},{bg}ee);">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          <td>
            <span style="font-size:18px;font-weight:700;color:#1a1a1a;">{_esc(stock_name)}</span>
            <span style="display:inline-block;margin-left:10px;padding:3px 10px;border-radius:20px;font-size:12px;font-weight:600;color:{txt};background:{bg};border:1px solid {border};">{_esc(signal)}</span>
            <span style="display:inline-block;margin-left:6px;padding:3px 8px;border-radius:20px;font-size:11px;color:#6b7280;background:#f3f4f6;">{_esc(trend)}</span>
          </td>
          <td style="text-align:right;">
            <div style="font-size:22px;font-weight:700;color:{pct_color};">{price:.2f}</div>
            <div style="font-size:13px;color:{pct_color};font-weight:500;">{"+" if change_pct > 0 else ""}{change_pct:.2f}% ({change_amt:+.2f})</div>
          </td>
        </tr></table>
      </td></tr>

      <!-- Score bar -->
      <tr><td style="padding:8px 20px;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          <td style="font-size:12px;color:#6b7280;width:80px;">综合评分</td>
          <td>
            <div style="background:#f3f4f6;border-radius:10px;height:8px;overflow:hidden;">
              <div style="background:{score_color};width:{score_width}%;height:8px;border-radius:10px;"></div>
            </div>
          </td>
          <td style="font-size:14px;font-weight:700;color:{score_color};width:50px;text-align:right;">{sentiment_score}</td>
        </tr></table>
      </td></tr>

      <!-- Core conclusion -->
      <tr><td style="padding:12px 20px;">
        <div style="background:#eff6ff;border-left:4px solid #3b82f6;padding:12px 16px;border-radius:0 8px 8px 0;">
          <div style="font-size:11px;color:#3b82f6;font-weight:600;text-transform:uppercase;margin-bottom:4px;">核心结论</div>
          <div style="font-size:14px;color:#1e3a5f;font-weight:600;">{one_sentence}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:4px;">时效: {time_sense} · 置信度: {_esc(confidence)}</div>
        </div>
      </td></tr>'''

    # Position advice table (if available)
    if pos_advice:
        no_pos = _esc(pos_advice.get("no_position", ""))
        has_pos = _esc(pos_advice.get("has_position", ""))
        if no_pos or has_pos:
            html += f'''
      <tr><td style="padding:0 20px 12px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;border-collapse:separate;">
          <tr style="background:#f9fafb;">
            <td style="padding:8px 12px;font-size:12px;font-weight:600;color:#6b7280;width:35%;border-right:1px solid #e5e7eb;">持仓情况</td>
            <td style="padding:8px 12px;font-size:12px;font-weight:600;color:#6b7280;">操作建议</td>
          </tr>
          <tr>
            <td style="padding:8px 12px;font-size:13px;color:#374151;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">空仓者</td>
            <td style="padding:8px 12px;font-size:13px;color:#374151;border-top:1px solid #e5e7eb;">{no_pos}</td>
          </tr>
          <tr>
            <td style="padding:8px 12px;font-size:13px;color:#374151;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">持仓者</td>
            <td style="padding:8px 12px;font-size:13px;color:#374151;border-top:1px solid #e5e7eb;">{has_pos}</td>
          </tr>
        </table>
      </td></tr>'''

    # Market data row
    html += f'''
      <tr><td style="padding:0 20px 12px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;border-collapse:separate;">
          <tr style="background:#f9fafb;">
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">今开</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">最高</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">最低</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">成交额</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;">振幅</td>
          </tr>
          <tr>
            <td style="padding:6px 10px;font-size:13px;text-align:center;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{price_change.get("open", "-")}</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;color:#dc2626;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{price_change.get("high", "-")}</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;color:#16a34a;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{price_change.get("low", "-")}</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{turnover_yi}亿</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;border-top:1px solid #e5e7eb;">{amplitude:.2f}%</td>
          </tr>
        </table>
      </td></tr>'''

    # Data perspective (if available)
    if ma_alignment or support or vol_status:
        bullish_badge = '<span style="color:#16a34a;font-weight:600;">✅ 是</span>' if is_bullish else '<span style="color:#dc2626;font-weight:600;">❌ 否</span>'
        bias_color = "#16a34a" if bias_status == "安全" else "#d97706" if bias_status == "警戒" else "#dc2626"
        html += f'''
      <tr><td style="padding:0 20px 12px;">
        <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:8px;">📊 技术面透视</div>
        <table width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;color:#374151;">
          {"<tr><td style='padding:3px 0;'>均线排列: " + ma_alignment + " · 多头: " + bullish_badge + ("" if not trend_score else f" · 强度: {trend_score}/100") + "</td></tr>" if ma_alignment else ""}
          {"<tr><td style='padding:3px 0;'>乖离率(MA5): <span style=&quot;color:" + bias_color + ";font-weight:600;&quot;>" + str(bias_ma5) + "% " + bias_status + "</span></td></tr>" if bias_ma5 else ""}
          {"<tr><td style='padding:3px 0;'>支撑位: " + support + " · 压力位: " + resistance + "</td></tr>" if support else ""}
          {"<tr><td style='padding:3px 0;'>量能: " + vol_status + (" · " + vol_meaning if vol_meaning else "") + "</td></tr>" if vol_status else ""}
        </table>
      </td></tr>'''

    # Battle plan (sniper points)
    if ideal_buy or stop_loss or take_profit:
        html += f'''
      <tr><td style="padding:0 20px 12px;">
        <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:8px;">🎯 作战计划</div>
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;border-collapse:separate;">
          <tr style="background:#f9fafb;">
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">理想买入</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">次优买入</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;border-right:1px solid #e5e7eb;">止损位</td>
            <td style="padding:6px 10px;font-size:11px;color:#6b7280;text-align:center;">目标位</td>
          </tr>
          <tr>
            <td style="padding:6px 10px;font-size:13px;text-align:center;color:#3b82f6;font-weight:600;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{ideal_buy or "-"}</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;color:#6366f1;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{secondary_buy or "-"}</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;color:#dc2626;font-weight:600;border-right:1px solid #e5e7eb;border-top:1px solid #e5e7eb;">{stop_loss or "-"}</td>
            <td style="padding:6px 10px;font-size:13px;text-align:center;color:#16a34a;font-weight:600;border-top:1px solid #e5e7eb;">{take_profit or "-"}</td>
          </tr>
        </table>
        {"<div style='font-size:12px;color:#6b7280;margin-top:6px;'>仓位: " + position_str + (" · " + entry_plan if entry_plan else "") + "</div>" if position_str else ""}
      </td></tr>'''

    # Checklist
    if checklist_html:
        html += f'''
      <tr><td style="padding:0 20px 12px;">
        <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:6px;">✓ 检查清单</div>
        <div style="background:#f9fafb;border-radius:8px;padding:10px 14px;">
          {checklist_html}
        </div>
      </td></tr>'''

    # Risk & catalysts side by side
    if risks_html or catalysts_html:
        html += f'''
      <tr><td style="padding:0 20px 12px;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          {"<td style='vertical-align:top;width:50%;padding-right:8px;'><div style=&quot;font-size:12px;font-weight:600;color:#dc2626;margin-bottom:4px;&quot;>风险警报</div>" + risks_html + "</td>" if risks_html else ""}
          {"<td style='vertical-align:top;width:50%;padding-left:8px;'><div style=&quot;font-size:12px;font-weight:600;color:#16a34a;margin-bottom:4px;&quot;>利好催化</div>" + catalysts_html + "</td>" if catalysts_html else ""}
        </tr></table>
      </td></tr>'''

    # Analysis text
    if analysis:
        html += f'''
      <tr><td style="padding:0 20px 12px;">
        <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:6px;">📝 综合分析</div>
        <div style="font-size:13px;color:#4b5563;line-height:1.6;">{analysis}</div>
      </td></tr>'''

    # Outlook
    if short_term or medium_term:
        html += f'''
      <tr><td style="padding:0 20px 16px;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
          {"<td style='vertical-align:top;width:50%;padding-right:8px;'><div style=&quot;font-size:12px;font-weight:600;color:#6b7280;margin-bottom:3px;&quot;>短期展望 (1-3日)</div><div style=&quot;font-size:12px;color:#4b5563;&quot;>" + short_term + "</div></td>" if short_term else ""}
          {"<td style='vertical-align:top;width:50%;padding-left:8px;'><div style=&quot;font-size:12px;font-weight:600;color:#6b7280;margin-bottom:3px;&quot;>中期展望 (1-2周)</div><div style=&quot;font-size:12px;color:#4b5563;&quot;>" + medium_term + "</div></td>" if medium_term else ""}
        </tr></table>
      </td></tr>'''

    html += '</table>'
    return html


def _build_market_overview_html(item: IntelligenceItem) -> str:
    """Build market overview card."""
    indices = item.market_data.indicators
    analysis = _esc(item.generated_analysis or "")

    rows = ""
    for code, info in indices.items():
        if not isinstance(info, dict):
            continue
        pct = info.get("change_pct", 0)
        color = _change_color(pct)
        rows += f'''
          <tr>
            <td style="padding:8px 12px;font-size:14px;font-weight:600;border-top:1px solid #e5e7eb;">{_esc(info.get("name", code))}</td>
            <td style="padding:8px 12px;font-size:14px;text-align:right;border-top:1px solid #e5e7eb;">{info.get("price", 0):.2f}</td>
            <td style="padding:8px 12px;font-size:14px;text-align:right;color:{color};font-weight:600;border-top:1px solid #e5e7eb;">{"+" if pct > 0 else ""}{pct:.2f}%</td>
          </tr>'''

    return f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;border-collapse:separate;">
      <tr><td style="padding:16px 20px;background:linear-gradient(135deg,#f0f9ff,#e0f2fe);">
        <div style="font-size:16px;font-weight:700;color:#0c4a6e;">📈 A股大盘行情速览</div>
      </td></tr>
      <tr><td style="padding:0 20px 12px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;border-collapse:separate;margin-top:12px;">
          <tr style="background:#f9fafb;">
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;">指数</td>
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:right;">点位</td>
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:right;">涨跌幅</td>
          </tr>
          {rows}
        </table>
      </td></tr>
      {"<tr><td style='padding:0 20px 16px;font-size:13px;color:#4b5563;line-height:1.6;'>" + analysis + "</td></tr>" if analysis else ""}
    </table>'''


def build_stock_report(items: list[IntelligenceItem]) -> dict:
    """Build stock closing recap email."""
    items = _dedup_items(items)
    stock_items = [i for i in items if i.source_pipeline == "stock_analysis"]

    if not stock_items:
        return {"subject": "", "text": "", "html": "", "item_count": 0}

    today = datetime.now(timezone.utc).strftime("%Y.%m.%d")

    # Separate market overview and individual stocks
    overview_items = [i for i in stock_items if "大盘行情" in (i.raw_title or "")]
    individual_items = [i for i in stock_items if "大盘行情" not in (i.raw_title or "")]

    # Sort by sentiment score (high first), then importance
    individual_items.sort(
        key=lambda x: (x.metadata.get("sentiment_score", 50), x.importance_score),
        reverse=True,
    )

    # Stats
    buy_count = sum(1 for i in individual_items if i.metadata.get("signal") in ("买入", "增持"))
    sell_count = sum(1 for i in individual_items if i.metadata.get("signal") in ("卖出", "减持"))
    hold_count = len(individual_items) - buy_count - sell_count
    positive_count = sum(1 for i in individual_items if i.market_data.price_change.get("change_pct", 0) > 0)
    negative_count = sum(1 for i in individual_items if i.market_data.price_change.get("change_pct", 0) < 0)
    flat_count = len(individual_items) - positive_count - negative_count
    avg_change = sum(i.market_data.price_change.get("change_pct", 0) for i in individual_items) / len(individual_items)
    top_gainer = max(individual_items, key=lambda x: x.market_data.price_change.get("change_pct", 0))
    top_loser = min(individual_items, key=lambda x: x.market_data.price_change.get("change_pct", 0))
    strongest_signal = max(
        individual_items,
        key=lambda x: (
            x.metadata.get("sentiment_score", 50),
            x.importance_score,
            x.market_data.price_change.get("change_pct", 0),
        ),
    )
    movers = sorted(individual_items, key=lambda x: x.market_data.price_change.get("change_pct", 0), reverse=True)
    top_risers = movers[:3]
    top_fallers = list(reversed(movers[-3:]))

    # Build summary table
    summary_rows = ""
    for item in individual_items:
        signal = item.metadata.get("signal", "观望")
        score = item.metadata.get("sentiment_score", 50)
        pct = item.market_data.price_change.get("change_pct", 0)
        price = item.market_data.price_change.get("price", 0)
        name = _stock_name(item)
        _, txt, _ = _signal_color(signal)
        pct_color = _change_color(pct)
        score_color = "#dc2626" if score >= 70 else "#d97706" if score >= 50 else "#16a34a"

        summary_rows += f'''
          <tr>
            <td style="padding:8px 12px;font-size:13px;font-weight:600;border-top:1px solid #e5e7eb;">{_esc(name)}</td>
            <td style="padding:8px 12px;font-size:13px;text-align:center;border-top:1px solid #e5e7eb;color:{txt};font-weight:600;">{_esc(signal)}</td>
            <td style="padding:8px 12px;font-size:13px;text-align:center;border-top:1px solid #e5e7eb;color:{score_color};font-weight:700;">{score}</td>
            <td style="padding:8px 12px;font-size:13px;text-align:right;border-top:1px solid #e5e7eb;">{price:.2f}</td>
            <td style="padding:8px 12px;font-size:13px;text-align:right;border-top:1px solid #e5e7eb;color:{pct_color};font-weight:600;">{"+" if pct > 0 else ""}{pct:.2f}%</td>
          </tr>'''

    # Build cards
    overview_html = ""
    for item in overview_items:
        overview_html += _build_market_overview_html(item)

    cards_html = ""
    for item in individual_items:
        cards_html += _build_stock_card_html(item)

    html_content = f'''
    <div style="text-align:center;margin-bottom:20px;">
      <h2 style="color:#1a1a1a;margin:0 0 4px;">📈 股市收盘复盘</h2>
      <p style="color:#6b7280;margin:0;font-size:14px;">{today} · 收盘后自动生成 · 共复盘 {len(individual_items)} 只股票</p>
      <div style="margin-top:12px;">
        <span style="display:inline-block;padding:4px 14px;border-radius:20px;font-size:13px;background:#fef2f2;color:#dc2626;margin:0 4px;">🟢 买入 {buy_count}</span>
        <span style="display:inline-block;padding:4px 14px;border-radius:20px;font-size:13px;background:#ecfeff;color:#0891b2;margin:0 4px;">🟡 观望 {hold_count}</span>
        <span style="display:inline-block;padding:4px 14px;border-radius:20px;font-size:13px;background:#f0fdf4;color:#16a34a;margin:0 4px;">🔴 卖出 {sell_count}</span>
      </div>
    </div>

    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;border-collapse:separate;">
      <tr><td style="padding:12px 20px;background:#f9fafb;">
        <div style="font-size:14px;font-weight:700;color:#374151;">🧭 今日统计</div>
      </td></tr>
      <tr><td style="padding:12px 20px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-spacing:0 8px;">
          <tr>
            <td style="width:50%;padding-right:8px;vertical-align:top;">
              <div style="padding:12px;border:1px solid #e5e7eb;border-radius:10px;background:#ffffff;">
                <div style="font-size:12px;color:#6b7280;">上涨 / 下跌 / 平盘</div>
                <div style="font-size:18px;font-weight:700;color:#111827;margin-top:4px;">{positive_count} / {negative_count} / {flat_count}</div>
              </div>
            </td>
            <td style="width:50%;padding-left:8px;vertical-align:top;">
              <div style="padding:12px;border:1px solid #e5e7eb;border-radius:10px;background:#ffffff;">
                <div style="font-size:12px;color:#6b7280;">平均涨跌幅</div>
                <div style="font-size:18px;font-weight:700;color:{_change_color(avg_change)};margin-top:4px;">{avg_change:+.2f}%</div>
              </div>
            </td>
          </tr>
          <tr>
            <td style="width:50%;padding-right:8px;vertical-align:top;">
              <div style="padding:12px;border:1px solid #e5e7eb;border-radius:10px;background:#ffffff;">
                <div style="font-size:12px;color:#6b7280;">最强标的</div>
                <div style="font-size:16px;font-weight:700;color:#111827;margin-top:4px;">{_esc(_stock_name(strongest_signal))}</div>
                <div style="font-size:12px;color:#6b7280;margin-top:4px;">{_esc(strongest_signal.metadata.get("signal", "观望"))} · 评分 {strongest_signal.metadata.get("sentiment_score", 50)}</div>
              </div>
            </td>
            <td style="width:50%;padding-left:8px;vertical-align:top;">
              <div style="padding:12px;border:1px solid #e5e7eb;border-radius:10px;background:#ffffff;">
                <div style="font-size:12px;color:#6b7280;">最大波动</div>
                <div style="font-size:16px;font-weight:700;color:#111827;margin-top:4px;">{_esc(_stock_name(top_gainer))} / {_esc(_stock_name(top_loser))}</div>
                <div style="font-size:12px;color:#6b7280;margin-top:4px;">{top_gainer.market_data.price_change.get("change_pct", 0):+.2f}% / {top_loser.market_data.price_change.get("change_pct", 0):+.2f}%</div>
              </div>
            </td>
          </tr>
        </table>
      </td></tr>
    </table>

    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;border-collapse:separate;">
      <tr><td style="padding:12px 20px;background:#f9fafb;">
        <div style="font-size:14px;font-weight:700;color:#374151;">🔥 今日强弱榜</div>
      </td></tr>
      <tr><td style="padding:12px 20px;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="width:50%;padding-right:8px;vertical-align:top;">
              <div style="font-size:12px;font-weight:600;color:#dc2626;margin-bottom:8px;">涨幅靠前</div>
              {"".join(
                  f"<div style='padding:8px 10px;border:1px solid #fee2e2;border-radius:8px;margin-bottom:8px;background:#fef2f2;'>"
                  f"<div style='font-size:13px;font-weight:600;color:#111827;'>{_esc(_stock_name(item))}</div>"
                  f"<div style='font-size:12px;color:#dc2626;margin-top:2px;'>{item.market_data.price_change.get('change_pct', 0):+.2f}% · {item.metadata.get('signal', '观望')}</div>"
                  f"</div>"
                  for item in top_risers
              )}
            </td>
            <td style="width:50%;padding-left:8px;vertical-align:top;">
              <div style="font-size:12px;font-weight:600;color:#16a34a;margin-bottom:8px;">跌幅靠前</div>
              {"".join(
                  f"<div style='padding:8px 10px;border:1px solid #dcfce7;border-radius:8px;margin-bottom:8px;background:#f0fdf4;'>"
                  f"<div style='font-size:13px;font-weight:600;color:#111827;'>{_esc(_stock_name(item))}</div>"
                  f"<div style='font-size:12px;color:#16a34a;margin-top:2px;'>{item.market_data.price_change.get('change_pct', 0):+.2f}% · {item.metadata.get('signal', '观望')}</div>"
                  f"</div>"
                  for item in top_fallers
              )}
            </td>
          </tr>
        </table>
      </td></tr>
    </table>

    <!-- Summary Table -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;border-collapse:separate;">
      <tr><td style="padding:12px 20px;background:#f9fafb;">
        <div style="font-size:14px;font-weight:700;color:#374151;">📋 个股信号总览</div>
      </td></tr>
      <tr><td style="padding:0 20px 12px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;border-collapse:separate;margin-top:8px;">
          <tr style="background:#f9fafb;">
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;">股票</td>
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:center;">信号</td>
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:center;">评分</td>
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:right;">现价</td>
            <td style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:right;">涨跌</td>
          </tr>
          {summary_rows}
        </table>
      </td></tr>
    </table>

    {overview_html}

    <!-- Individual Stock Analysis -->
    {cards_html}

    <div style="text-align:center;margin-top:20px;padding:16px;background:#f9fafb;border-radius:8px;">
      <p style="font-size:12px;color:#9ca3af;margin:0;">⚠️ 以上分析仅供参考，不构成投资建议。投资有风险，入市需谨慎。</p>
    </div>'''

    html_body = _wrap_email_html(html_content)

    # Plain text version
    text_lines = [f"股市收盘复盘 — {today}", f"收盘后自动生成，共复盘 {len(individual_items)} 只股票\n"]
    text_lines.append(f"买入: {buy_count} | 观望: {hold_count} | 卖出: {sell_count}")
    text_lines.append(f"上涨: {positive_count} | 下跌: {negative_count} | 平盘: {flat_count}")
    text_lines.append(f"平均涨跌幅: {avg_change:+.2f}%")
    text_lines.append(
        f"最强标的: {_stock_name(strongest_signal)} ({strongest_signal.metadata.get('signal', '观望')} / 评分{strongest_signal.metadata.get('sentiment_score', 50)})"
    )
    text_lines.append(
        f"最大波动: {_stock_name(top_gainer)} {top_gainer.market_data.price_change.get('change_pct', 0):+.2f}% / "
        f"{_stock_name(top_loser)} {top_loser.market_data.price_change.get('change_pct', 0):+.2f}%\n"
    )
    text_lines.append("涨幅靠前:")
    for item in top_risers:
        text_lines.append(f"  {_stock_name(item)}: {item.market_data.price_change.get('change_pct', 0):+.2f}% · {item.metadata.get('signal', '观望')}")
    text_lines.append("跌幅靠前:")
    for item in top_fallers:
        text_lines.append(f"  {_stock_name(item)}: {item.market_data.price_change.get('change_pct', 0):+.2f}% · {item.metadata.get('signal', '观望')}")
    text_lines.append("\n信号总览:")
    for item in individual_items:
        name = _stock_name(item)
        signal = item.metadata.get("signal", "观望")
        score = item.metadata.get("sentiment_score", 50)
        pct = item.market_data.price_change.get("change_pct", 0)
        text_lines.append(f"  {name}: {signal} (评分{score}) {pct:+.2f}%")

    text_lines.append("\n详细分析:")
    for item in individual_items:
        name = _stock_name(item)
        text_lines.append(f"\n{'='*40}")
        text_lines.append(f"{name} | {item.metadata.get('signal', '观望')}")
        if item.generated_analysis:
            text_lines.append(item.generated_analysis)

    text_lines.append("\n⚠️ 以上分析仅供参考，不构成投资建议。")
    text_body = "\n".join(text_lines)

    return {
        "subject": f"[BidMosaic] 股市收盘复盘 — {today}",
        "text": text_body,
        "html": html_body,
        "item_count": len(stock_items),
    }


# ─── General Newsletter Builders ────────────────────────────────────


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
      <table width="640" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <tr><td style="padding:24px 24px 16px;background:linear-gradient(135deg,#0f172a,#1e293b);">
          <h1 style="margin:0;color:#fff;font-size:18px;font-weight:600;">BidMosaic Intelligence</h1>
          <p style="margin:4px 0 0;color:#94a3b8;font-size:12px;">智能情报分析平台</p>
        </td></tr>
        <tr><td style="padding:24px;">{content}</td></tr>
        <tr><td style="padding:16px 24px;background:#f8fafc;border-top:1px solid #e2e8f0;font-size:12px;color:#94a3b8;">
          <p style="margin:0;">BidMosaic 情报订阅 · <a href="{UNSUBSCRIBE_URL_PLACEHOLDER}" style="color:#94a3b8;">退订</a></p>
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
    parser = argparse.ArgumentParser(description="Build BidMosaic newsletter queues.")
    parser.add_argument(
        "type",
        nargs="?",
        default="all",
        choices=["all", "stock", "daily", "weekly"],
        help="Queue type to build.",
    )
    parser.add_argument("--days", type=int, default=1, help="Processed days to read for stock queue.")
    args = parser.parse_args()

    # Build stock report from processed data
    all_items = load_processed_items(days=args.days)
    stock_items = [i for i in all_items if i.source_pipeline == "stock_analysis"]

    if args.type in ("all", "stock") and stock_items:
        stock_report = build_stock_report(stock_items)
        print(f"Stock report: {stock_report['item_count']} items")
        if stock_report["item_count"] > 0:
            save_to_queue(stock_report, "stock")

    # Build general newsletters from CMS items
    daily_items = load_published_cms_items(days=1)
    weekly_items = load_published_cms_items(days=7)

    if args.type in ("all", "daily", "weekly") and (daily_items or weekly_items):
        weekly = build_weekly_digest(weekly_items)
        if args.type in ("all", "weekly"):
            print(f"Weekly digest: {weekly['item_count']} items")
        if args.type in ("all", "weekly") and weekly["item_count"] > 0:
            save_to_queue(weekly, "weekly")

        daily = build_daily_brief(daily_items)
        if args.type in ("all", "daily"):
            print(f"Daily brief: {daily['item_count']} items")
        if args.type in ("all", "daily") and daily["item_count"] > 0:
            save_to_queue(daily, "daily")

    if args.type == "stock" and not stock_items:
        print("No stock items found for stock queue.")
    elif args.type == "daily" and not daily_items:
        print("No CMS items found for daily queue.")
    elif args.type == "weekly" and not weekly_items:
        print("No CMS items found for weekly queue.")
    elif args.type == "all" and not stock_items and not daily_items and not weekly_items:
        print("No items found for any newsletter type.")


if __name__ == "__main__":
    main()
