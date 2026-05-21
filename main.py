"""
Telegram → Claude Managed Agents köprüsü
Render.com'da çalışır (FastAPI + httpx)

Ortam değişkenleri (Render.com Environment):
  CLAUDE_API_KEY        — Anthropic API anahtarı
  TELEGRAM_TOKEN        — Telegram bot token (@BotFather)
  AGENT_ID              — agent_01TStkKvFmCiGM7cVXtnmypB
  SESSION_ID            — sesn_01PWp9mY32e5pijw4XAt82f7
  ENV_ID                — env_01EhVfhGqqc9yytZZE3ieaSb
  META_ACCESS_TOKEN     — Meta Graph API token
  INSTAGRAM_ACCOUNT_ID  — 17841426737963461
  FACEBOOK_PAGE_ID      — 1048704551666095
  IMGBB_API_KEY         — ImgBB API anahtarı
"""

import os, asyncio, logging, time, base64
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

# ─── Yapılandırma ───────────────────────────────────────────────────────────
CLAUDE_API_KEY       = os.environ["CLAUDE_API_KEY"]
TELEGRAM_TOKEN       = os.environ["TELEGRAM_TOKEN"]
AGENT_ID             = os.environ["AGENT_ID"]
SESSION_ID           = os.environ.get("SESSION_ID", "")
ENV_ID               = os.environ["ENV_ID"]
META_ACCESS_TOKEN    = os.environ.get("META_ACCESS_TOKEN", "")
INSTAGRAM_ACCOUNT_ID = os.environ.get("INSTAGRAM_ACCOUNT_ID", "")
FACEBOOK_PAGE_ID     = os.environ.get("FACEBOOK_PAGE_ID", "")
IMGBB_API_KEY        = os.environ.get("IMGBB_API_KEY", "")

CLAUDE_BASE   = "https://api.anthropic.com"
TELEGRAM_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
GRAPH_BASE    = "https://graph.facebook.com/v19.0"
IMGBB_BASE    = "https://api.imgbb.com/1/upload"

CLAUDE_HEADERS = {
    "x-api-key":         CLAUDE_API_KEY,
    "anthropic-version": "2023-06-01",
    "anthropic-beta":    "managed-agents-2026-04-01",
    "content-type":      "application/json",
}

POLL_INTERVAL = 2
POLL_TIMEOUT  = 300

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

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
            log.info(f"Events: {len(events)} adet")
            last_count = len(events)

        status_events = [
            e for e in events
            if e.get("type") in ("session.status_idle", "session.status_running")
        ]
        if status_events:
            last = status_events[-1]
            if last.get("type") == "session.status_idle":
                stop = last.get("stop_reason") or {}
                stop_type = stop.get("type") if isinstance(stop, dict) else stop
                if stop_type == "end_turn":
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

# ─── ImgBB ──────────────────────────────────────────────────────────────────

async def upload_to_imgbb(image_url: str) -> str:
    """Trendyol CDN görselini ImgBB'ye yükler, public URL döndürür."""
    async with httpx.AsyncClient(timeout=30) as client:
        # Görseli indir
        r = await client.get(image_url)
        if r.status_code != 200:
            log.error(f"Görsel indirilemedi: {image_url}")
            return ""
        image_data = base64.b64encode(r.content).decode("utf-8")

        # ImgBB'ye yükle
        r = await client.post(
            IMGBB_BASE,
            data={
                "key": IMGBB_API_KEY,
                "image": image_data,
            },
        )
        result = r.json()
        if result.get("success"):
            url = result["data"]["url"]
            log.info(f"ImgBB yüklendi: {url}")
            return url
        else:
            log.error(f"ImgBB hatası: {result}")
            return ""

# ─── Meta Graph API ─────────────────────────────────────────────────────────

async def instagram_carousel_post(image_urls: list, caption: str) -> dict:
    """Görselleri ImgBB'ye yükleyip Instagram carousel post atar."""
    async with httpx.AsyncClient(timeout=60) as client:

        # 1) Her görsel için container oluştur
        container_ids = []
        for url in image_urls:
            r = await client.post(
                f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media",
                params={
                    "image_url": url,
                    "is_carousel_item": "true",
                    "access_token": META_ACCESS_TOKEN,
                },
            )
            data = r.json()
            if "id" not in data:
                log.error(f"Görsel container hatası: {data}")
                continue
            container_ids.append(data["id"])

        if not container_ids:
            return {"error": "Hiç görsel container oluşturulamadı"}

        # 2) Carousel container oluştur
        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media",
            params={
                "media_type": "CAROUSEL",
                "children": ",".join(container_ids),
                "caption": caption,
                "access_token": META_ACCESS_TOKEN,
            },
        )
        carousel = r.json()
        if "id" not in carousel:
            return {"error": str(carousel)}

        # 3) Yayınla
        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media_publish",
            params={
                "creation_id": carousel["id"],
                "access_token": META_ACCESS_TOKEN,
            },
        )
        result = r.json()
        log.info(f"Instagram carousel yayınlandı: {result}")
        return result


async def facebook_carousel_post(image_urls: list, caption: str) -> dict:
    """Facebook sayfasına çoklu resim post atar."""
    async with httpx.AsyncClient(timeout=60) as client:

        photo_ids = []
        for url in image_urls:
            r = await client.post(
                f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/photos",
                params={
                    "url": url,
                    "published": "false",
                    "access_token": META_ACCESS_TOKEN,
                },
            )
            data = r.json()
            if "id" not in data:
                log.error(f"Facebook foto hatası: {data}")
                continue
            photo_ids.append({"media_fbid": data["id"]})

        if not photo_ids:
            return {"error": "Hiç fotoğraf yüklenemedi"}

        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/feed",
            params={"access_token": META_ACCESS_TOKEN},
            json={
                "message": caption,
                "attached_media": photo_ids,
            },
        )
        result = r.json()
        log.info(f"Facebook post yayınlandı: {result}")
        return result


async def post_to_social_media(trendyol_image_urls: list, caption: str, platforms: list) -> dict:
    """
    Trendyol CDN URL'lerini ImgBB'ye yükler,
    sonra Instagram ve/veya Facebook'a carousel post atar.
    """
    # ImgBB'ye yükle
    log.info(f"{len(trendyol_image_urls)} görsel ImgBB'ye yükleniyor...")
    imgbb_urls = []
    for url in trendyol_image_urls[:10]:  # Instagram max 10
        imgbb_url = await upload_to_imgbb(url)
        if imgbb_url:
            imgbb_urls.append(imgbb_url)

    if not imgbb_urls:
        return {"error": "Hiç görsel ImgBB'ye yüklenemedi"}

    log.info(f"{len(imgbb_urls)} görsel yüklendi, paylaşılıyor...")

    results = {}
    if "instagram" in platforms:
        results["instagram"] = await instagram_carousel_post(imgbb_urls, caption)
    if "facebook" in platforms:
        results["facebook"] = await facebook_carousel_post(imgbb_urls, caption)

    return results

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


@app.post("/post-product")
async def post_product(request: Request, background: BackgroundTasks):
    """
    Ürünü sosyal medyaya paylaşır.
    Body: {
        "image_urls": [...],   # Trendyol CDN URL'leri
        "caption": "...",
        "platforms": ["instagram", "facebook"]
    }
    """
    body = await request.json()
    image_urls = body.get("image_urls", [])
    caption    = body.get("caption", "")
    platforms  = body.get("platforms", ["instagram", "facebook"])

    if not image_urls or not caption:
        return JSONResponse({"error": "image_urls ve caption zorunlu"}, status_code=400)

    # Arka planda çalıştır (uzun sürebilir)
    results = await post_to_social_media(image_urls, caption, platforms)
    return JSONResponse(results)


@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id}
