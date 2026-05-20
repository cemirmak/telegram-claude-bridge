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
SESSION_ID      = os.environ.get("SESSION_ID", "")   # opsiyonel; yoksa otomatik açılır
ENV_ID          = os.environ["ENV_ID"]

CLAUDE_BASE     = "https://api.anthropic.com"
TELEGRAM_BASE   = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Tüm Managed Agents isteklerinde bu iki header ZORUNLU
CLAUDE_HEADERS = {
    "x-api-key":         CLAUDE_API_KEY,
    "anthropic-version": "2023-06-01",
    "anthropic-beta":    "managed-agents-2026-04-01",   # ← EKSİK OLAN BU'YDU
    "content-type":      "application/json",
}

POLL_INTERVAL   = 2     # saniye — events ne sıklıkla kontrol edilsin
POLL_TIMEOUT    = 120   # saniye — en fazla bu kadar bekle
WHISPER_MODEL   = "base"  # küçük model; large için daha iyi doğruluk ama yavaş

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Session yönetimi ───────────────────────────────────────────────────────

_active_session_id: str = SESSION_ID  # global; restart'ta ortam değişkeninden gelir

async def get_or_create_session() -> str:
    """Mevcut session varsa kullan, yoksa yenisini aç."""
    global _active_session_id

    if _active_session_id:
        # Var olan session'ın durumunu kontrol et
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

    # Yeni session aç
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
    """Kullanıcı metnini agent'a gönder, cevap döndür."""
    session_id = await get_or_create_session()

    # 1) Kullanıcı mesajını gönder
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

    # 2) Events endpoint'ini poll et — GET /v1/sessions/{id}/events
    #    Bu endpoint DÜZELTİLDİ: daha önce GET /v1/sessions/{id} kullanılıyordu (yanlış)
    deadline = time.time() + POLL_TIMEOUT
    last_event_count = 0

    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{CLAUDE_BASE}/v1/sessions/{session_id}/events",   # ← DOĞRU ENDPOINT
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

        # session.status_idle ile end_turn gelince agent tamamlamış demektir
        idle_events = [e for e in all_events if e.get("type") == "session.status_idle"]
        for ev in idle_events:
            stop = ev.get("stop_reason") or {}
            stop_type = stop.get("type") if isinstance(stop, dict) else stop
            if stop_type == "end_turn":
                # Agent cevabını bul
                answer = _extract_agent_message(all_events)
                log.info(f"Agent cevabı alındı ({len(answer)} karakter)")
                return answer

        # Hata eventi varsa yakala
        errors = [e for e in all_events if e.get("type") == "session.error"]
        if errors:
            err = errors[-1]
            msg = err.get("error", {}).get("message", str(err))
            log.error(f"Session error: {msg}")
            return f"❌ Agent hatası: {msg}"

    # Zaman aşımı
    log.warning(f"Zaman aşımı ({POLL_TIMEOUT}s), {last_event_count} event vardı.")
    return "⏱ Agent zamanında cevap veremedi. Lütfen tekrar deneyin."


def _extract_agent_message(events: list) -> str:
    """Events listesinden son agent.message metnini çıkar."""
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
    """Arka planda çalışır; Telegram'a cevabı gönderir."""
    try:
        reply = await ask_claude(text)
    except Exception as exc:
        log.exception("ask_claude hatası")
        reply = f"❌ Beklenmedik hata: {exc}"
    await send_telegram(chat_id, reply)


async def handle_voice(chat_id: int, file_id: str) -> None:
    """Sesli mesajı Whisper ile metne çevirip agent'a gönderir."""
    try:
        import whisper  # pip install openai-whisper

        # Telegram'dan dosyayı indir
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

        model = whisper.load_model(WHISPER_MODEL)
        result = model.transcribe(tmp_path)
        text = result["text"].strip()
        os.unlink(tmp_path)

        if not text:
            await send_telegram(chat_id, "⚠️ Ses anlaşılamadı, lütfen tekrar deneyin.")
            return

        log.info(f"Whisper transkript: {text!r}")
        await send_telegram(chat_id, f"🎙 _Duydum:_ {text}")
        await handle_message(chat_id, text)

    except ImportError:
        await send_telegram(chat_id, "⚠️ Ses desteği şu an devre dışı (whisper kurulu değil).")
    except Exception as exc:
        log.exception("Ses işleme hatası")
        await send_telegram(chat_id, f"❌ Ses işlenemedi: {exc}")


# ─── FastAPI ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Başlangıçta session varlığını doğrula (isteğe bağlı)
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
        await send_telegram(chat_id, "⏳ Cevap Yazılıyor")
    else:
        await send_telegram(chat_id, "⚠️ Yalnızca metin veya ses gönderebilirsiniz.")

    return JSONResponse({"ok": True})


@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id}
