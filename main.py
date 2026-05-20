"""
Telegram → Claude Managed Agents köprüsü
Render.com'da çalışır (FastAPI + httpx + Whisper)

Ortam değişkenleri (Render.com Environment):
  CLAUDE_API_KEY     — Anthropic API anahtarı
  TELEGRAM_TOKEN     — Telegram bot token (@BotFather)
  AGENT_ID           — agent_01TStkKvFmCiGM7cVXtnmypB
  SESSION_ID         — sesn_01PWp9mY32e5pijw4XAt82f7  (opsiyonel; yoksa yeni session açılır)
  ENV_ID             — env_01EhVfhGqqc9yytZZE3ieaSb
"""

import os, asyncio, json, logging, tempfile, time
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

POLL_INTERVAL   = 2
POLL_TIMEOUT    = 120
WHISPER_MODEL   = "base"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Global değişkenler ─────────────────────────────────────────────────────

_active_session_id: str = SESSION_ID
_whisper_model = None   # startup'ta yüklenir, her ses mesajında tekrar indirilmez

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
            data = r.json()
            status = data.get("status", "")
            if status not in ("archived", "terminated"):
                log.info(f"Mevcut session kullanılıyor: {_active_session_id} (status={status})")
                return _active_session_id
            log.warning(f"Session {_active_session_id} kullanılamaz (status={status}), yenisi açılıyor.")

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


# ─── Claude'a mesaj gönder ve cevabı bekle ──────────────────────────────────

async def ask_claude(user_text: str) -> str:
    session_id = await get_or_create_session()

    send_payload = {
        "events": [
            {
                "type": "user.message",
                "content": [{"type": "text", "text": user_text}],
            }
        ]
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{CLAUDE_BASE}/v1/sessions/{session_id}/events",
            headers=CLAUDE_HEADERS,
            json=send_payload,
            timeout=30,
        )
    if r.status_code not in (200, 201):
        log.error(f"Event gönderme hatası: {r.status_code} {r.text}")
        return f"❌ Agent'a mesaj gönderilemedi: {r.status_code}"

    log.info(f"Mesaj gönderildi, cevap bekleniyor… (session={session_id})")

    deadline = time.time() + POLL_TIMEOUT
    last_event_count = 0

    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{CLAUDE_BASE}/v1/sessions/{session_id}/events",
                headers=CLAUDE_HEADERS,
                timeout=15,
            )

        if r.status_code != 200:
            log.warning(f"Events isteği başarısız: {r.status_code} {r.text}")
            continue

        data       = r.json()
        all_events = data.get("events", data.get("data", []))

        if len(all_events) != last_event_count:
            log.info(f"Events: {len(all_events)} adet, tipler: {[e.get('type') for e in all_events]}")
            last_event_count = len(all_events)

        idle_events = [e for e in all_events if e.get("type") == "session.status_idle"]
        for ev in idle_events:
            stop = ev.get("stop_reason") or {}
            stop_type = stop.get("type") if isinstance(stop, dict) else stop
            if stop_type == "end_turn":
                answer = _extract_agent_message(all_events)
                log.info(f"Agent cevabı alındı ({len(answer)} karakter)")
                return answer

        errors = [e for e in all_events if e.get("type") == "session.error"]
        if errors:
            err = errors[-1]
            msg = err.get("error", {}).get("message", str(err))
            log.error(f"Session error: {msg}")
            return f"❌ Agent hatası: {msg}"

    log.warning(f"Zaman aşımı ({POLL_TIMEOUT}s), {last_event_count} event vardı.")
    return "⏱ Agent zamanında cevap veremedi. Lütfen tekrar deneyin."


def _extract_agent_message(events: list) -> str:
    agent_msgs = [e for e in events if e.get("type") == "agent.message"]
    if not agent_msgs:
        return "🤔 Agent cevap oluşturdu ama metin bulunamadı."
    last = agent_msgs[-1]
    content = last.get("content", [])
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block["text"])
    return "\n".join(parts) if parts else str(content)


# ─── Telegram ───────────────────────────────────────────────────────────────

async def send_telegram(chat_id: int, text: str) -> None:
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    async with httpx.AsyncClient() as client:
        await client.post(f"{TELEGRAM_BASE}/sendMessage", json=payload, timeout=10)


async def handle_message(chat_id: int, text: str) -> None:
    try:
        reply = await ask_claude(text)
    except Exception as exc:
        log.exception("ask_claude hatası")
        reply = f"❌ Beklenmedik hata: {exc}"
    await send_telegram(chat_id, reply)


async def handle_voice(chat_id: int, file_id: str) -> None:
    global _whisper_model

    if _whisper_model is None:
        await send_telegram(chat_id, "⚠️ Whisper henüz yüklenmedi, lütfen birkaç saniye bekleyip tekrar deneyin.")
        return

    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{TELEGRAM_BASE}/getFile", params={"file_id": file_id}, timeout=10
            )
            file_path = r.json()["result"]["file_path"]
            audio_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
            audio_data = (await client.get(audio_url, timeout=30)).content

        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
            f.write(audio_data)
            tmp_path = f.name

        result = _whisper_model.transcribe(tmp_path)
        text = result["text"].strip()
        os.unlink(tmp_path)

        if not text:
            await send_telegram(chat_id, "⚠️ Ses anlaşılamadı, lütfen tekrar deneyin.")
            return

        log.info(f"Whisper transkript: {text!r}")
        await send_telegram(chat_id, f"🎙 _Duydum:_ {text}")
        await handle_message(chat_id, text)

    except Exception as exc:
        log.exception("Ses işleme hatası")
        await send_telegram(chat_id, f"❌ Ses işlenemedi: {exc}")


# ─── FastAPI ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _whisper_model

    # Whisper modelini startup'ta yükle (bir kez indirilir, bellekte kalır)
    try:
        import whisper
        log.info("Whisper modeli yükleniyor, lütfen bekleyin…")
        _whisper_model = whisper.load_model(WHISPER_MODEL)
        log.info("✅ Whisper modeli yüklendi")
    except ImportError:
        log.warning("⚠️ Whisper kurulu değil, ses desteği kapalı")
    except Exception as exc:
        log.error(f"Whisper yüklenemedi: {exc}")

    # Session varlığını doğrula
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

    text  = msg.get("text", "").strip()
    voice = msg.get("voice") or msg.get("audio")

    if voice:
        background.add_task(handle_voice, chat_id, voice["file_id"])
        await send_telegram(chat_id, "🎙 Ses mesajı alındı, işleniyor…")
    elif text:
        if text.startswith("/start"):
            await send_telegram(chat_id, "👋 Merhaba! REDZARRAM mağaza asistanına hoş geldiniz.")
            return JSONResponse({"ok": True})
        background.add_task(handle_message, chat_id, text)
        await send_telegram(chat_id, "⏳ Cevap yazılıyor…")
    else:
        await send_telegram(chat_id, "⚠️ Yalnızca metin veya ses gönderebilirsiniz.")

    return JSONResponse({"ok": True})


@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id, "whisper": _whisper_model is not None}
