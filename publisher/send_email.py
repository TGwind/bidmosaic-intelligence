"""Send newsletter emails via Gmail SMTP or Resend API."""

from __future__ import annotations

import json
import os
import smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone
from urllib.parse import quote
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUEUE_DIR = PROJECT_ROOT / "data" / "newsletter_queue"

SITE_URL = os.environ.get("SITE_URL", "https://bidmosaic.com")


def send_via_smtp(to: list[str], subject: str, html: str, text: str) -> dict:
    """Send email via Gmail SMTP."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "465"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    from_email = os.environ.get("FROM_EMAIL", smtp_user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"BidMosaic Intelligence <{from_email}>"
    msg["To"] = ", ".join(to)
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, to, msg.as_string())

    return {"status": "sent", "method": "smtp"}


def send_via_resend(to: list[str], subject: str, html: str, text: str) -> dict:
    import httpx

    api_key = os.environ["RESEND_API_KEY"]
    from_email = os.environ.get("FROM_EMAIL", "intelligence@bidmosaic.com")

    resp = httpx.post(
        "https://api.resend.com/emails",
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


def send_email(to: list[str], subject: str, html: str, text: str) -> dict:
    """Auto-detect: use SMTP if configured, else Resend."""
    if os.environ.get("SMTP_USER"):
        return send_via_smtp(to, subject, html, text)
    return send_via_resend(to, subject, html, text)


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

    total = len(subscribers)
    print(f"Sending {newsletter_type} newsletter to {total} subscribers...")

    def _send_one(idx: int, subscriber: dict) -> tuple[str, dict | str]:
        email = subscriber["email"]
        try:
            html, text = personalize_newsletter(newsletter, email)
            result = send_email(
                to=[email],
                subject=newsletter["subject"],
                html=html,
                text=text,
            )
            return email, result
        except Exception as e:
            return email, f"ERROR: {e}"

    sent = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_send_one, i, sub): sub
            for i, sub in enumerate(subscribers, 1)
        }
        for future in as_completed(futures):
            email, result = future.result()
            if isinstance(result, str) and result.startswith("ERROR"):
                failed += 1
                print(f"  FAILED: {email} -> {result}", flush=True)
            else:
                sent += 1
                print(f"  Sent: {email} -> {result}", flush=True)

    print(f"Done. Sent: {sent}, Failed: {failed}")


if __name__ == "__main__":
    main()
