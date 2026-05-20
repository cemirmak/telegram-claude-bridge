"""
Telegram → Claude Managed Agents köprüsü
Render.com'da çalışır (FastAPI + httpx)

Ortam değişkenleri (Render.com Environment):
  CLAUDE_API_KEY     — Anthropic API anahtarı
  TELEGRAM_TOKEN     — Telegram bot token (@BotFather)
  AGENT_ID           — agent_01TStkKvFmCiGM7cVXtnmypB
  SESSION_ID         — sesn_01PWp9mY32e5pijw4XAt82f7  (opsiyonel; yoksa yeni session açılır)
  ENV_ID             — env_01EhVfhGqqc9yytZZE3ieaSb
"""

import os, asyncio, logging, time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

# ─── Yapılandırma ───────────────────────────────────────────────────────────
CLAUDE_API_KEY  = os.environ["CLAUDE_API_KEY"]
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
AGENT_ID        = os.environ["AGENT_ID"]
SESSION_ID      = os.environ.get("SESSION_ID", "")
ENV_ID          = os.environ["ENV_ID"]

CLAUDE_BASE     = "https://api.anthropic.com"
TELEGRAM_BASE   = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

CLAUDE_HEADERS = {
    "x-api-key":         CLAUDE_API_KEY,
    "anthropic-version": "2023-06-01",
    "anthropic-beta":    "managed-agents-2026-04-01",
    "content-type":      "application/json",
}

POLL_INTERVAL = 2
POLL_TIMEOUT  = 120

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Global ─────────────────────────────────────────────────────────────────
_active_session_id: str = SESSION_ID

# ─── Session yönetimi ───────────────────────────────────────────────────────

async def get_or_create_session() -> str:
    global _active_session_id

    if _active_session_id:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{CLAUDE_BASE}/v1/sessions/{_active_session_id}",
                headers=CLAUDE_HEADERS,
                timeout=10,
            )
        if r.status_code == 200:
            status = r.json().get("status", "")
            if status not in ("archived", "terminated"):
                log.info(f"Mevcut session: {_active_session_id} (status={status})")
                return _active_session_id
            log.warning(f"Session kullanılamaz ({status}), yenisi açılıyor.")

    payload = {
        "agent":       {"type": "agent", "id": AGENT_ID},
        "environment": {"type": "environment", "id": ENV_ID},
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{CLAUDE_BASE}/v1/sessions",
            headers=CLAUDE_HEADERS,
            json=payload,
            timeout=30,
        )
    r.raise_for_status()
    _active_session_id = r.json()["id"]
    log.info(f"Yeni session açıldı: {_active_session_id}")
    return _active_session_id

# ─── Claude ─────────────────────────────────────────────────────────────────

async def ask_claude(user_text: str) -> str:
    session_id = await get_or_create_session()

    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{CLAUDE_BASE}/v1/sessions/{session_id}/events",
            headers=CLAUDE_HEADERS,
            json={"events": [{"type": "user.message", "content": [{"type": "text", "text": user_text}]}]},
            timeout=30,
        )
    if r.status_code not in (200, 201):
        log.error(f"Event gönderme hatası: {r.status_code} {r.text}")
        return f"❌ Agent'a mesaj gönderilemedi: {r.status_code}"

    log.info(f"Mesaj gönderildi (session={session_id})")

    deadline = time.time() + POLL_TIMEOUT
    last_count = 0

    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{CLAUDE_BASE}/v1/sessions/{session_id}/events",
                headers=CLAUDE_HEADERS,
                timeout=15,
            )

        if r.status_code != 200:
            log.warning(f"Events hatası: {r.status_code}")
            continue

        data = r.json()
        events = data.get("events", data.get("data", []))

        if len(events) != last_count:
            log.info(f"Events: {len(events)} adet — {[e.get('type') for e in events]}")
            last_count = len(events)

        for ev in events:
            if ev.get("type") == "session.status_idle":
                stop = ev.get("stop_reason") or {}
                if (stop.get("type") if isinstance(stop, dict) else stop) == "end_turn":
                    answer = _extract_answer(events)
                    log.info(f"Cevap alındı ({len(answer)} karakter)")
                    return answer

        errors = [e for e in events if e.get("type") == "session.error"]
        if errors:
            msg = errors[-1].get("error", {}).get("message", str(errors[-1]))
            log.error(f"Session error: {msg}")
            return f"❌ Agent hatası: {msg}"

    log.warning(f"Zaman aşımı ({POLL_TIMEOUT}s)")
    return "⏱ Agent zamanında cevap veremedi. Lütfen tekrar deneyin."


def _extract_answer(events: list) -> str:
    msgs = [e for e in events if e.get("type") == "agent.message"]
    if not msgs:
        return "🤔 Cevap oluşturuldu ama metin bulunamadı."
    parts = [b["text"] for b in msgs[-1].get("content", []) if isinstance(b, dict) and b.get("type") == "text"]
    return "\n".join(parts) if parts else "🤔 Boş cevap."

# ─── Telegram ───────────────────────────────────────────────────────────────

async def send_telegram(chat_id: int, text: str) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{TELEGRAM_BASE}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )

async def handle_message(chat_id: int, text: str) -> None:
    try:
        reply = await ask_claude(text)
    except Exception as exc:
        log.exception("ask_claude hatası")
        reply = f"❌ Beklenmedik hata: {exc}"
    await send_telegram(chat_id, reply)

# ─── FastAPI ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        sid = await get_or_create_session()
        log.info(f"✅ Hazır. Aktif session: {sid}")
    except Exception as exc:
        log.error(f"Session başlatılamadı: {exc}")
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request, background: BackgroundTasks):
    body = await request.json()
    msg  = body.get("message", {})

    chat_id = msg.get("chat", {}).get("id")
    if not chat_id:
        return JSONResponse({"ok": True})

    text = msg.get("text", "").strip()

    if not text:
        await send_telegram(chat_id, "⚠️ Yalnızca metin mesajı gönderebilirsiniz.")
        return JSONResponse({"ok": True})

    if text.startswith("/start"):
        await send_telegram(chat_id, "👋 Merhaba! REDZARRAM mağaza asistanına hoş geldiniz.")
        return JSONResponse({"ok": True})

    background.add_task(handle_message, chat_id, text)
    await send_telegram(chat_id, "⏳ Cevap yazılıyor…")
    return JSONResponse({"ok": True})

@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id}
