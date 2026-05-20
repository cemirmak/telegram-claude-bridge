#!/usr/bin/env python3
"""
Webhook kurulum scripti.
Railway deploy bittikten sonra BİR KEZ çalıştırın:

    TELEGRAM_TOKEN=xxx RAILWAY_URL=https://your-app.up.railway.app python setup_webhook.py
"""

import os
import sys
import httpx

TOKEN = os.environ.get("TELEGRAM_TOKEN") or input("Telegram Bot Token: ").strip()
URL   = os.environ.get("RAILWAY_URL")    or input("Railway URL (https://...): ").strip()

WEBHOOK_URL = f"{URL.rstrip('/')}/webhook"
API         = f"https://api.telegram.org/bot{TOKEN}"

def run():
    with httpx.Client() as client:
        # Mevcut webhook'u sil
        client.get(f"{API}/deleteWebhook?drop_pending_updates=true")

        # Yeni webhook'u kaydet
        r = client.post(f"{API}/setWebhook", json={
            "url": WEBHOOK_URL,
            "allowed_updates": ["message", "edited_message"],
            "drop_pending_updates": True,
        })
        data = r.json()

        if data.get("ok"):
            print(f"✅ Webhook ayarlandı → {WEBHOOK_URL}")
        else:
            print(f"❌ Hata: {data}")
            sys.exit(1)

        # Bilgileri doğrula
        info = client.get(f"{API}/getWebhookInfo").json()
        print(f"\nWebhook bilgisi:\n  URL        : {info['result']['url']}")
        print(f"  Pending msg: {info['result'].get('pending_update_count', 0)}")
        last_err = info['result'].get('last_error_message')
        if last_err:
            print(f"  Son hata   : {last_err}")

if __name__ == "__main__":
    run()
