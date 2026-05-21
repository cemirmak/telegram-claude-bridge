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

import os, asyncio, logging, time, base64
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
EXCEL_PATH    = "urunler.xlsx"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_active_session_id: str = SESSION_ID
_posted_today: set = set()
_posted_date: str = ""

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
                if (stop.get("type") if isinstance(stop, dict) else stop) == "end_turn":
                    return _extract_answer(events)

        errors = [e for e in events if e.get("type") == "session.error"]
        if errors:
            return f"❌ Agent hatası: {errors[-1].get('error', {}).get('message', '')}"

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
                return ""
            image_data = base64.b64encode(r.content).decode("utf-8")
            r = await client.post(
                IMGBB_BASE,
                data={"key": IMGBB_API_KEY, "image": image_data},
            )
            result = r.json()
            if result.get("success"):
                return result["data"]["url"]
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
                params={"image_url": url, "is_carousel_item": "true", "access_token": META_ACCESS_TOKEN},
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
            params={"media_type": "CAROUSEL", "children": ",".join(container_ids), "caption": caption, "access_token": META_ACCESS_TOKEN},
        )
        carousel = r.json()
        if "id" not in carousel:
            return {"error": str(carousel)}

        await asyncio.sleep(15)

        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media_publish",
            params={"creation_id": carousel["id"], "access_token": META_ACCESS_TOKEN},
        )
        return r.json()


async def instagram_reels_post(video_url: str, caption: str) -> dict:
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media",
            params={
                "media_type": "REELS",
                "video_url": video_url,
                "caption": caption,
                "access_token": META_ACCESS_TOKEN,
            },
        )
        data = r.json()
        if "id" not in data:
            return {"error": str(data)}

        await asyncio.sleep(30)

        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media_publish",
            params={"creation_id": data["id"], "access_token": META_ACCESS_TOKEN},
        )
        return r.json()


async def instagram_story_post(image_url: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media",
            params={"media_type": "STORIES", "image_url": image_url, "access_token": META_ACCESS_TOKEN},
        )
        data = r.json()
        if "id" not in data:
            return {"error": str(data)}

        await asyncio.sleep(10)

        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media_publish",
            params={"creation_id": data["id"], "access_token": META_ACCESS_TOKEN},
        )
        return r.json()


async def facebook_reels_post(video_url: str, caption: str) -> dict:
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/video_reels",
            params={"upload_phase": "start", "access_token": META_ACCESS_TOKEN},
        )
        data = r.json()
        video_id = data.get("video_id")
        if not video_id:
            return {"error": str(data)}

        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/video_reels",
            params={
                "upload_phase": "finish",
                "video_id": video_id,
                "video_url": video_url,
                "description": caption,
                "access_token": META_ACCESS_TOKEN,
            },
        )
        return r.json()


async def facebook_story_post(image_url: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/photo_stories",
            params={"url": image_url, "access_token": META_ACCESS_TOKEN},
        )
        return r.json()


async def facebook_carousel_post(image_urls: list, caption: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        photo_ids = []
        for url in image_urls:
            r = await client.post(
                f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/photos",
                params={"url": url, "published": "false", "access_token": META_ACCESS_TOKEN},
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

# ─── Ürün seçimi ────────────────────────────────────────────────────────────

def load_products() -> pd.DataFrame:
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="Ürünler")
        return df.drop_duplicates(subset=["Model Kodu"])
    except Exception as e:
        log.error(f"Excel okunamadı: {e}")
        return pd.DataFrame()


def pick_product(df: pd.DataFrame, require_video: bool = False) -> dict:
    global _posted_today, _posted_date

    today = datetime.now().strftime("%Y-%m-%d")
    if _posted_date != today:
        _posted_date = today
        _posted_today = set()

    available = df[~df["Model Kodu"].isin(_posted_today)]
    if available.empty:
        _posted_today = set()
        available = df

    # Video gerekliyse sadece video URL'si olan ürünleri seç
    if require_video and "Video URL" in df.columns:
        with_video = available[available["Video URL"].notna() & (available["Video URL"] != "")]
        if not with_video.empty:
            available = with_video

    row = available.sample(1).iloc[0]
    _posted_today.add(row["Model Kodu"])

    gorsel_cols = [f"Görsel {i}" for i in range(1, 9)]
    image_urls = [
        row[c] for c in gorsel_cols
        if pd.notna(row.get(c)) and str(row.get(c)).startswith("http")
    ]

    fiyat_col = "Trendyol'da Satılacak Fiyat (KDV Dahil)"

    video_url = ""
    if "Video URL" in df.columns and pd.notna(row.get("Video URL")):
        v = str(row.get("Video URL", "")).strip()
        if v.startswith("http"):
            video_url = v

    return {
        "name":       row["Ürün Adı"],
        "price":      row.get(fiyat_col, ""),
        "link":       row.get("Trendyol.com Linki", ""),
        "desc":       str(row.get("Ürün Açıklaması", "")).replace(";", " ").strip(),
        "image_urls": image_urls,
        "model":      row["Model Kodu"],
        "video_url":  video_url,
    }


async def generate_caption(product: dict) -> str:
    prompt = f"""Şu ürün için Instagram/Facebook post caption'ı yaz:

Ürün: {product['name']}
Fiyat: {product['price']}₺
Link: {product['link']}
Açıklama: {product['desc'][:300]}

ZORUNLU FORMAT:
1. Satır 1: {product['link']}
2. Türkçe etkileyici açıklama (2-3 cümle)
3. 💰 Fiyat: {product['price']}₺
4. En az 15 hashtag

Sadece caption metnini döndür."""

    caption = await ask_claude(prompt)

    if any(x in caption for x in ["❌", "⏱", "🤔"]):
        caption = f"""{product['link']}

{product['name']} — Tarzını yansıt, stilini konuştur! ✨

💰 Fiyat: {product['price']}₺

#REDZARRAM #Trendyol #MiniEtek #KotEtek #Moda #Fashion #TürkModa #Style #Outfit #OOTD #GünlükKombin #KadınModa #TrendyolModa #Şık #Kombin"""

    return caption

# ─── Zamanlanmış görevler ───────────────────────────────────────────────────

async def scheduled_carousel():
    """Günde 3x — 10:00, 14:00, 20:00 — Carousel post."""
    log.info("⏰ Carousel paylaşımı başlıyor...")

    df = load_products()
    if df.empty:
        await notify_telegram("❌ Ürün listesi bulunamadı.")
        return

    product = pick_product(df)
    log.info(f"Carousel ürün: {product['name']}")

    if not product["image_urls"]:
        return

    imgbb_urls = []
    for url in product["image_urls"][:10]:
        imgbb_url = await upload_to_imgbb(url)
        if imgbb_url:
            imgbb_urls.append(imgbb_url)

    if not imgbb_urls:
        await notify_telegram("❌ Görsel yüklenemedi.")
        return

    caption = await generate_caption(product)

    ig = await instagram_carousel_post(imgbb_urls, caption)
    fb = await facebook_carousel_post(imgbb_urls, caption)

    ig_ok = "id" in str(ig)
    fb_ok = "id" in str(fb)

    await notify_telegram(f"📸 *Carousel Post*\n{product['name']}\nIG: {'✅' if ig_ok else '❌'} | FB: {'✅' if fb_ok else '❌'}")


async def scheduled_reels():
    """Günde 1x — 20:30 — Reels video."""
    log.info("⏰ Reels paylaşımı başlıyor...")

    df = load_products()
    if df.empty:
        await notify_telegram("❌ Ürün listesi bulunamadı.")
        return

    product = pick_product(df, require_video=True)

    if not product["video_url"]:
        log.info("Video URL bulunamadı, Reels atlandı.")
        await notify_telegram("⚠️ Reels: Video URL'si olan ürün bulunamadı.")
        return

    log.info(f"Reels ürün: {product['name']} | Video: {product['video_url']}")

    caption = await generate_caption(product)

    ig = await instagram_reels_post(product["video_url"], caption)
    fb = await facebook_reels_post(product["video_url"], caption)

    ig_ok = "id" in str(ig)
    fb_ok = "id" in str(fb)

    await notify_telegram(f"🎬 *Reels*\n{product['name']}\nIG: {'✅' if ig_ok else '❌'} | FB: {'✅' if fb_ok else '❌'}")


async def scheduled_story():
    """Günde 1x — 00:00 — Story."""
    log.info("⏰ Story paylaşımı başlıyor...")

    df = load_products()
    if df.empty:
        await notify_telegram("❌ Ürün listesi bulunamadı.")
        return

    product = pick_product(df)

    if not product["image_urls"]:
        return

    imgbb_url = await upload_to_imgbb(product["image_urls"][0])
    if not imgbb_url:
        await notify_telegram("❌ Story: Görsel yüklenemedi.")
        return

    ig = await instagram_story_post(imgbb_url)
    fb = await facebook_story_post(imgbb_url)

    ig_ok = "id" in str(ig)
    fb_ok = "id" in str(fb)

    await notify_telegram(f"📖 *Story*\n{product['name']}\nIG: {'✅' if ig_ok else '❌'} | FB: {'✅' if fb_ok else '❌'}")


async def notify_telegram(text: str):
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
    try:
        sid = await get_or_create_session()
        log.info(f"✅ Hazır. Aktif session: {sid}")
    except Exception as exc:
        log.error(f"Session başlatılamadı: {exc}")

    # Carousel — 10:00, 14:00, 20:00
    scheduler.add_job(scheduled_carousel, CronTrigger(hour=10, minute=0))
    scheduler.add_job(scheduled_carousel, CronTrigger(hour=14, minute=0))
    scheduler.add_job(scheduled_carousel, CronTrigger(hour=20, minute=0))
    # Reels — 20:30
    scheduler.add_job(scheduled_reels, CronTrigger(hour=20, minute=30))
    # Story — 00:00
    scheduler.add_job(scheduled_story, CronTrigger(hour=0, minute=0))

    scheduler.start()
    log.info("⏰ Zamanlayıcı: Carousel 10:00/14:00/20:00 | Reels 20:30 | Story 00:00 (TR)")

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

    if text.startswith("/post_now"):
        background.add_task(scheduled_carousel)
        await send_telegram(chat_id, "📸 Manuel carousel paylaşımı başlatıldı...")
        return JSONResponse({"ok": True})

    if text.startswith("/reels_now"):
        background.add_task(scheduled_reels)
        await send_telegram(chat_id, "🎬 Manuel Reels paylaşımı başlatıldı...")
        return JSONResponse({"ok": True})

    if text.startswith("/story_now"):
        background.add_task(scheduled_story)
        await send_telegram(chat_id, "📖 Manuel Story paylaşımı başlatıldı...")
        return JSONResponse({"ok": True})

    background.add_task(handle_message, chat_id, text)
    await send_telegram(chat_id, "⏳ Cevap yazılıyor…")
    return JSONResponse({"ok": True})


@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id}
