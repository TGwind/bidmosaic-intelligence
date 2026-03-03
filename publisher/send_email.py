"""Send newsletter emails via Resend API."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from urllib.parse import quote
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUEUE_DIR = PROJECT_ROOT / "data" / "newsletter_queue"

RESEND_API_URL = "https://api.resend.com/emails"
SITE_URL = os.environ.get("SITE_URL", "https://bidmosaic.com")


def send_via_resend(to: list[str], subject: str, html: str, text: str) -> dict:
    api_key = os.environ["RESEND_API_KEY"]
    from_email = os.environ.get("FROM_EMAIL", "intelligence@bidmosaic.com")

    resp = httpx.post(
        RESEND_API_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "from": f"BidMosaic Intelligence <{from_email}>",
            "to": to,
            "subject": subject,
            "html": html,
            "text": text,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def load_subscribers() -> list[dict]:
    """Load active subscribers. TODO: replace with DB query."""
    subs_file = PROJECT_ROOT / "data" / "subscribers.json"
    if subs_file.exists():
        with open(subs_file, encoding="utf-8") as f:
            data = json.load(f)
        subscribers = data.get("subscribers")
        if isinstance(subscribers, list):
            return [item for item in subscribers if item.get("status") == "active" and item.get("email")]
        emails = data.get("emails", [])
        if isinstance(emails, list):
            return [{"email": email, "status": "active"} for email in emails]
    return []


def personalize_newsletter(newsletter: dict, email: str) -> tuple[str, str]:
    unsubscribe_url = f"{SITE_URL}/api/unsubscribe?email={quote(email)}"
    html = newsletter["html"].replace("{{unsubscribe_url}}", unsubscribe_url)
    text = newsletter["text"] + f"\n\n退订: {unsubscribe_url}"
    return html, text


def main():
    newsletter_type = os.environ.get("NEWSLETTER_TYPE", "daily")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    queue_file = QUEUE_DIR / newsletter_type / f"{today}.json"
    if not queue_file.exists():
        print(f"No {newsletter_type} newsletter in queue for {today}.")
        return

    with open(queue_file, encoding="utf-8") as f:
        newsletter = json.load(f)

    subscribers = load_subscribers()
    if not subscribers:
        print("No subscribers found. Skipping send.")
        return

    print(f"Sending {newsletter_type} newsletter to {len(subscribers)} subscribers...")
    for idx, subscriber in enumerate(subscribers, 1):
        email = subscriber["email"]
        html, text = personalize_newsletter(newsletter, email)
        result = send_via_resend(
            to=[email],
            subject=newsletter["subject"],
            html=html,
            text=text,
        )
        print(f"  Sent {idx}/{len(subscribers)}: {email} -> {result}")

    print("Done.")


if __name__ == "__main__":
    main()
