"""Push newsletter digest via webhooks (Enterprise WeChat / Telegram).

Reads from the same newsletter queue as email, formats and posts
to configured webhook endpoints.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUEUE_DIR = PROJECT_ROOT / "data" / "newsletter_queue"
SITE_URL = os.environ.get("SITE_URL", "https://bidmosaic.com")


def push_wecom(webhook_url: str, newsletter: dict) -> dict:
    """Send digest to Enterprise WeChat (企业微信) group bot.

    Webhook docs: https://developer.work.weixin.qq.com/document/path/91770
    Markdown content limit: 4096 bytes.
    """
    subject = newsletter["subject"]
    text = newsletter.get("text", "")

    # Trim to fit 4096 byte limit (leave room for header/footer)
    lines = text.split("\n")
    md_lines = [f"## {subject}\n"]
    byte_count = len(md_lines[0].encode("utf-8"))
    for line in lines:
        if line.startswith(subject.split("—")[0].strip()):
            continue  # skip redundant header
        encoded = line.encode("utf-8")
        if byte_count + len(encoded) + 200 > 4096:
            md_lines.append("\n...")
            break
        md_lines.append(line)
        byte_count += len(encoded) + 1

    md_lines.append(f"\n[查看详情]({SITE_URL}/insights)")
    content = "\n".join(md_lines)

    resp = httpx.post(
        webhook_url,
        json={"msgtype": "markdown", "markdown": {"content": content}},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def push_telegram(bot_token: str, chat_id: str, newsletter: dict) -> dict:
    """Send digest to Telegram chat/channel."""
    subject = newsletter["subject"]
    text = newsletter.get("text", "")

    # Telegram message limit: 4096 chars
    message = f"*{subject}*\n\n{text[:3800]}\n\n[查看详情]({SITE_URL}/insights)"

    resp = httpx.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    newsletter_type = os.environ.get("NEWSLETTER_TYPE", "daily")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    queue_file = QUEUE_DIR / newsletter_type / f"{today}.json"
    if not queue_file.exists():
        print(f"No {newsletter_type} newsletter in queue for {today}.")
        return

    with open(queue_file, encoding="utf-8") as f:
        newsletter = json.load(f)

    sent = 0

    # Enterprise WeChat webhook
    wecom_url = os.environ.get("WECOM_WEBHOOK_URL")
    if wecom_url:
        try:
            result = push_wecom(wecom_url, newsletter)
            errcode = result.get("errcode", -1)
            if errcode == 0:
                print(f"WeChat Work webhook: sent OK")
                sent += 1
            else:
                print(f"WeChat Work webhook: error {result}")
        except Exception as e:
            print(f"WeChat Work webhook: FAILED - {e}")
    else:
        print("WECOM_WEBHOOK_URL not set, skipping WeChat Work push.")

    # Telegram bot
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        try:
            result = push_telegram(tg_token, tg_chat, newsletter)
            if result.get("ok"):
                print(f"Telegram: sent OK")
                sent += 1
            else:
                print(f"Telegram: error {result}")
        except Exception as e:
            print(f"Telegram: FAILED - {e}")
    else:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set, skipping Telegram push.")

    print(f"Done. Pushed to {sent} channel(s).")


if __name__ == "__main__":
    main()
