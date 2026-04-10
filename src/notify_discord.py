import requests
from datetime import datetime

def send_discord(webhook_url: str, title: str, message: str):
    ts = datetime.now().strftime("%m/%d/%y %H:%M")
    payload = {
        "content": f"**{title}**\n{message}\n_{ts}_"
    }

    response = requests.post(webhook_url, json=payload, timeout=10)

    if response.status_code not in (200, 204):
        raise RuntimeError(
            f"Discord webhook failed: {response.status_code} {response.text}"
        )