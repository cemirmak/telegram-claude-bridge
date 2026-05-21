"""
Telegram → Claude Managed Agents köprüsü
Render.com'da çalışır (FastAPI + httpx + APScheduler)

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
  TELEGRAM_CHAT_ID      — Bildirim gönderilecek Telegram chat ID
"""

import os, asyncio, logging, time, base64, random
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
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
TELEGRAM_CHAT_ID     = os.environ.get("TELEGRAM_CHAT_ID", "")

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

# Ürün Excel dosyası — Render'da /tmp'ye kopyalanır
EXCEL_PATH = "urunler.xlsx"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_active_session_id: str = SESSION_ID
_posted_today: set = set()  # Bugün paylaşılan model kodları

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
                return _active_session_id

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
        return f"❌ Agent'a mesaj gönderilemedi: {r.status_code}"

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
            continue

        data = r.json()
        events = data.get("events", data.get("data", []))

        if len(events) != last_count:
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
                    return _extract_answer(events)

        errors = [e for e in events if e.get("type") == "session.error"]
        if errors:
            msg = errors[-1].get("error", {}).get("message", str(errors[-1]))
            return f"❌ Agent hatası: {msg}"

    return "⏱ Agent zamanında cevap veremedi."


def _extract_answer(events: list) -> str:
    msgs = [e for e in events if e.get("type") == "agent.message"]
    if not msgs:
        return "🤔 Cevap bulunamadı."
    parts = [b["text"] for b in msgs[-1].get("content", []) if isinstance(b, dict) and b.get("type") == "text"]
    return "\n".join(parts) if parts else "🤔 Boş cevap."

# ─── ImgBB ──────────────────────────────────────────────────────────────────

async def upload_to_imgbb(image_url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(image_url, follow_redirects=True)
            if r.status_code != 200:
                log.error(f"Görsel indirilemedi ({r.status_code}): {image_url}")
                return ""
            image_data = base64.b64encode(r.content).decode("utf-8")

            r = await client.post(
                IMGBB_BASE,
                data={"key": IMGBB_API_KEY, "image": image_data},
            )
            result = r.json()
            if result.get("success"):
                url = result["data"]["url"]
                log.info(f"ImgBB: {url}")
                return url
            log.error(f"ImgBB hatası: {result}")
            return ""
    except Exception as e:
        log.error(f"ImgBB exception: {e}")
        return ""

# ─── Meta Graph API ─────────────────────────────────────────────────────────

async def instagram_carousel_post(image_urls: list, caption: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
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
            if "id" in data:
                container_ids.append(data["id"])
            else:
                log.error(f"Instagram container hatası: {data}")

        if not container_ids:
            return {"error": "Hiç container oluşturulamadı"}

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

        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media_publish",
            params={
                "creation_id": carousel["id"],
                "access_token": META_ACCESS_TOKEN,
            },
        )
        return r.json()


async def facebook_carousel_post(image_urls: list, caption: str) -> dict:
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
            if "id" in data:
                photo_ids.append({"media_fbid": data["id"]})
            else:
                log.error(f"Facebook foto hatası: {data}")

        if not photo_ids:
            return {"error": "Hiç fotoğraf yüklenemedi"}

        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/feed",
            params={"access_token": META_ACCESS_TOKEN},
            json={"message": caption, "attached_media": photo_ids},
        )
        return r.json()


async def post_to_social_media(trendyol_image_urls: list, caption: str) -> dict:
    log.info(f"{len(trendyol_image_urls)} görsel ImgBB'ye yükleniyor...")
    imgbb_urls = []
    for url in trendyol_image_urls[:10]:
        imgbb_url = await upload_to_imgbb(url)
        if imgbb_url:
            imgbb_urls.append(imgbb_url)

    if not imgbb_urls:
        return {"error": "Hiç görsel ImgBB'ye yüklenemedi"}

    log.info(f"{len(imgbb_urls)} görsel yüklendi, paylaşılıyor...")

    ig_result = await instagram_carousel_post(imgbb_urls, caption)
    fb_result = await facebook_carousel_post(imgbb_urls, caption)

    return {"instagram": ig_result, "facebook": fb_result}

# ─── Ürün seçimi ve caption oluşturma ───────────────────────────────────────

def load_products() -> pd.DataFrame:
    """Excel'den benzersiz ürünleri yükle."""
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="Ürünler")
        unique = df.drop_duplicates(subset=["Model Kodu"])
        return unique
    except Exception as e:
        log.error(f"Excel okunamadı: {e}")
        return pd.DataFrame()


def pick_product(df: pd.DataFrame) -> dict:
    """Bugün paylaşılmamış rastgele bir ürün seç."""
    global _posted_today

    # Gece yarısı sıfırla
    today = datetime.now().strftime("%Y-%m-%d")
    if not hasattr(pick_product, "_date") or pick_product._date != today:
        pick_product._date = today
        _posted_today = set()

    available = df[~df["Model Kodu"].isin(_posted_today)]
    if available.empty:
        _posted_today = set()
        available = df

    row = available.sample(1).iloc[0]
    _posted_today.add(row["Model Kodu"])

    gorsel_cols = [f"Görsel {i}" for i in range(1, 9)]
    image_urls = [row[c] for c in gorsel_cols if pd.notna(row.get(c)) and str(row.get(c)).startswith("http")]

    fiyat_col = "Trendyol'da Satılacak Fiyat (KDV Dahil)"
    return {
        "name":       row["Ürün Adı"],
        "price":      row.get(fiyat_col, ""),
        "link":       row.get("Trendyol.com Linki", ""),
        "desc":       str(row.get("Ürün Açıklaması", "")).replace(";", " ").strip(),
        "image_urls": image_urls,
        "model":      row["Model Kodu"],
    }


async def generate_caption(product: dict) -> str:
    """Claude'a caption yazdır."""
    prompt = f"""Şu ürün için Instagram/Facebook carousel post caption'ı yaz:

Ürün: {product['name']}
Fiyat: {product['price']}₺
Link: {product['link']}
Açıklama: {product['desc'][:300]}

ZORUNLU FORMAT (sırayla):
1. Satır 1: {product['link']}
2. Türkçe etkileyici açıklama (2-3 cümle)
3. 💰 Fiyat: {product['price']}₺
4. En az 15 hashtag (#KotEtek #MiniEtek #REDZARRAM vb.)

Sadece caption metnini döndür, başka hiçbir şey yazma."""

    caption = await ask_claude(prompt)
    # Agent gereksiz metin eklerse temizle
    if "❌" in caption or "⏱" in caption:
        # Fallback caption
        caption = f"""{product['link']}

{product['name']} — Tarzını yansıt, stilini konuştur! ✨

💰 Fiyat: {product['price']}₺

#REDZARRAM #Trendyol #MiniEtek #KotEtek #Moda #Fashion #TürkModa #Style #Outfit #OOTD #GünlükKombin #KadınModa #TrendyolModa #Şık #Kombin"""

    return caption


async def scheduled_post():
    """Zamanlayıcı tarafından çağrılır — ürün seçer ve paylaşır."""
    log.info("⏰ Zamanlanmış paylaşım başlıyor...")

    df = load_products()
    if df.empty:
        log.error("Ürün listesi boş, paylaşım yapılamadı")
        await notify_telegram("❌ Ürün listesi bulunamadı, paylaşım yapılamadı.")
        return

    product = pick_product(df)
    log.info(f"Seçilen ürün: {product['name']}")

    if not product["image_urls"]:
        log.error(f"Görsel bulunamadı: {product['name']}")
        return

    caption = await generate_caption(product)
    results = await post_to_social_media(product["image_urls"], caption)

    ig_ok = "id" in str(results.get("instagram", {}))
    fb_ok = "id" in str(results.get("facebook", {}))

    msg = f"""📱 *Otomatik Paylaşım*

Ürün: {product['name']}
Instagram: {"✅" if ig_ok else "❌"}
Facebook: {"✅" if fb_ok else "❌"}"""

    await notify_telegram(msg)
    log.info(f"Paylaşım tamamlandı: {results}")


async def notify_telegram(text: str):
    """Belirli bir chat'e bildirim gönder."""
    if not TELEGRAM_CHAT_ID:
        return
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{TELEGRAM_BASE}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )

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

scheduler = AsyncIOScheduler(timezone="Europe/Istanbul")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Session başlat
    try:
        sid = await get_or_create_session()
        log.info(f"✅ Hazır. Aktif session: {sid}")
    except Exception as exc:
        log.error(f"Session başlatılamadı: {exc}")

    # Zamanlayıcıyı başlat — Türkiye saati 10:00, 14:00, 20:00
    scheduler.add_job(scheduled_post, CronTrigger(hour=10, minute=0))
    scheduler.add_job(scheduled_post, CronTrigger(hour=14, minute=0))
    scheduler.add_job(scheduled_post, CronTrigger(hour=20, minute=0))
    scheduler.start()
    log.info("⏰ Zamanlayıcı başlatıldı: 10:00, 14:00, 20:00 (TR saati)")

    yield

    scheduler.shutdown()


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

    # Manuel paylaşım tetikleyici
    if text.startswith("/post_now"):
        background.add_task(scheduled_post)
        await send_telegram(chat_id, "📤 Manuel paylaşım başlatıldı...")
        return JSONResponse({"ok": True})

    background.add_task(handle_message, chat_id, text)
    await send_telegram(chat_id, "⏳ Cevap yazılıyor…")
    return JSONResponse({"ok": True})


@app.post("/post-product")
async def post_product(request: Request):
    body = await request.json()
    image_urls = body.get("image_urls", [])
    caption    = body.get("caption", "")

    if not image_urls or not caption:
        return JSONResponse({"error": "image_urls ve caption zorunlu"}, status_code=400)

    results = await post_to_social_media(image_urls, caption)
    return JSONResponse(results)


@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id}
