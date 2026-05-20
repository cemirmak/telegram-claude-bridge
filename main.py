"""
Telegram → Claude API Bridge
Railway üzerinde çalışan webhook servisi.
"""

import os
import json
import logging
import tempfile
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config (Railway environment variables) ───────────────────────────────────
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
CLAUDE_API_KEY  = os.environ["CLAUDE_API_KEY"]
CLAUDE_MODEL    = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
SYSTEM_PROMPT   = os.environ.get(
    "SYSTEM_PROMPT",
    "Sen yardımsever bir asistansın. Kullanıcıyla Türkçe konuş, sade ve net cevaplar ver."
)
WHISPER_LANG    = os.environ.get("WHISPER_LANG", "tr")   # ses dili
MAX_HISTORY     = int(os.environ.get("MAX_HISTORY", "20"))

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
CLAUDE_API   = "https://api.anthropic.com/v1/messages"

# ── In-memory sohbet geçmişi (chat_id → mesaj listesi) ───────────────────────
conversations: dict[str, list[dict]] = {}

# ── Whisper model (lazy load) ─────────────────────────────────────────────────
_whisper_model = None

def get_whisper():
    global _whisper_model
    if _whisper_model is None:
        logger.info("Whisper modeli yükleniyor (ilk seferinde uzun sürebilir)…")
        from faster_whisper import WhisperModel
        _whisper_model = WhisperModel("tiny", device="cpu", compute_type="int8")
        logger.info("Whisper hazır.")
    return _whisper_model


# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="Telegram-Claude Bridge")


@app.get("/health")
async def health():
    return {"status": "ok", "model": CLAUDE_MODEL}


@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)

    background_tasks.add_task(process_update, data)
    return JSONResponse({"ok": True})


# ── Update işleyici ───────────────────────────────────────────────────────────
async def process_update(data: dict):
    message = data.get("message") or data.get("edited_message")
    if not message:
        return

    chat_id  = str(message["chat"]["id"])
    username = message.get("from", {}).get("username", "?")
    logger.info(f"Mesaj geldi | chat={chat_id} user=@{username}")

    # /reset komutu → geçmişi sil
    if message.get("text", "").strip().lower() in ("/reset", "/start"):
        conversations.pop(chat_id, None)
        await send_message(chat_id, "🔄 Sohbet sıfırlandı. Merhaba!")
        return

    # Ses → metin
    if "voice" in message:
        await send_typing(chat_id)
        text = await transcribe_voice(message["voice"]["file_id"])
        if not text:
            await send_message(chat_id, "⚠️ Ses mesajı çözümlenemedi, lütfen tekrar deneyin.")
            return
        logger.info(f"Transkript: {text[:80]}")
        await send_message(chat_id, f"🎙️ _{text}_\n\nİşleniyor…")

    elif "text" in message:
        text = message["text"].strip()
    else:
        await send_message(chat_id, "Bu mesaj türü desteklenmiyor. Lütfen metin veya ses gönderin.")
        return

    await send_typing(chat_id)
    reply = await ask_claude(chat_id, text)
    await send_message(chat_id, reply)


# ── Claude ────────────────────────────────────────────────────────────────────
async def ask_claude(chat_id: str, user_text: str) -> str:
    AGENT_ID = os.environ.get("AGENT_ID", "agent_01TStkKvFmCiGM7cVXtnmypB")
    ENV_ID = os.environ.get("ENV_ID", "env__E31eaSb")
    
    import asyncio
    
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            headers = {
                "x-api-key": CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "managed-agents-2026-04-01",
                "content-type": "application/json",
            }
            
            # Yeni session oluştur
            resp = await client.post(
                "https://api.anthropic.com/v1/sessions",
                headers=headers,
                json={
                    "agent": AGENT_ID,
                    "environment_id": ENV_ID,
                },
            )
            resp.raise_for_status()
            session_id = resp.json()["id"]
            
            # Mesaj gönder
            await client.post(
                f"https://api.anthropic.com/v1/sessions/{session_id}/events",
                headers=headers,
                json={
                    "events": [{
                        "type": "user.message",
                        "content": [{"type": "text", "text": user_text}]
                    }]
                },
            )
            
            # Cevabı bekle
            agent_reply = ""
            for _ in range(36):  # max 3 dakika
                await asyncio.sleep(5)
                
                transcript = await client.get(
                    f"https://api.anthropic.com/v1/sessions/{session_id}",
                    headers=headers,
                )
                transcript.raise_for_status()
                data = transcript.json()
                events = data.get("events", [])
                status = data.get("status", "")

                logger.info(f"Session status: {status}, Events count: {len(events)}, Event types: {[e.get('type') for e in events]}")
                for event in reversed(events):
                    if event.get("type") == "agent.message":
                        for block in event.get("content", []):
                            if block.get("type") == "text":
                                agent_reply = block["text"]
                        break
                
                if agent_reply:
                    break
            
            return agent_reply or "⚠️ Agent cevap vermedi."

    except httpx.HTTPStatusError as e:
        logger.error(f"API hatası: {e.response.status_code} – {e.response.text}")
        return f"⚠️ API hatası: {e.response.status_code}"
    except Exception as e:
        logger.error(f"Hata: {e}")
        return f"⚠️ Hata: {str(e)}"
# ── Ses transkripsiyonu ───────────────────────────────────────────────────────
async def transcribe_voice(file_id: str) -> str | None:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Telegram'dan dosya yolunu al
            file_info = await client.get(f"{TELEGRAM_API}/getFile?file_id={file_id}")
            file_path = file_info.json()["result"]["file_path"]

            # Ses dosyasını indir
            audio = await client.get(
                f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
            )

        # Geçici dosyaya yaz
        suffix = "." + file_path.split(".")[-1]  # .ogg veya .oga
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            f.write(audio.content)
            tmp = f.name

        # Whisper ile transkript
        model = get_whisper()
        segments, info = model.transcribe(tmp, language=WHISPER_LANG)
        text = " ".join(seg.text.strip() for seg in segments)
        Path(tmp).unlink(missing_ok=True)
        return text.strip() or None

    except Exception as e:
        logger.error(f"Transkripsiyon hatası: {e}")
        return None


# ── Telegram yardımcıları ─────────────────────────────────────────────────────
async def send_message(chat_id: str, text: str):
    """Markdown destekli mesaj gönder; hata olursa düz metin dene."""
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(f"{TELEGRAM_API}/sendMessage", json=payload)
        if r.status_code != 200:
            # Markdown parse hatası olabilir; düz metin ile tekrar dene
            payload.pop("parse_mode")
            await client.post(f"{TELEGRAM_API}/sendMessage", json=payload)


async def send_typing(chat_id: str):
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(
            f"{TELEGRAM_API}/sendChatAction",
            json={"chat_id": chat_id, "action": "typing"},
        )
