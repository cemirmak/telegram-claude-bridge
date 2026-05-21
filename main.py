"""
Telegram → Claude Managed Agents köprüsü
Render.com'da çalışır (FastAPI + httpx + APScheduler)

Zamanlama (TR saati):
  Her 5 dakika         → Trendyol yeni sipariş kontrolü + tedarikçi bildirimi
  10:00, 14:00, 20:00  → Carousel post (Instagram + Facebook)
  20:30                → Reels (sadece Instagram)
  00:00                → Story (Instagram + Facebook)
"""

import os, asyncio, logging, time, base64, json, re, io
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import httpx
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse

# ─── Yapılandırma ───────────────────────────────────────────────────────────
CLAUDE_API_KEY        = os.environ["CLAUDE_API_KEY"]
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
AGENT_ID              = os.environ["AGENT_ID"]
SESSION_ID            = os.environ.get("SESSION_ID", "")
ENV_ID                = os.environ["ENV_ID"]
META_ACCESS_TOKEN     = os.environ.get("META_ACCESS_TOKEN", "")
FACEBOOK_ACCESS_TOKEN = os.environ.get("FACEBOOK_ACCESS_TOKEN", "")
INSTAGRAM_ACCOUNT_ID  = os.environ.get("INSTAGRAM_ACCOUNT_ID", "")
FACEBOOK_PAGE_ID      = os.environ.get("FACEBOOK_PAGE_ID", "")
IMGBB_API_KEY         = os.environ.get("IMGBB_API_KEY", "")
TELEGRAM_CHAT_ID      = os.environ.get("TELEGRAM_CHAT_ID", "")

# Trendyol
TRENDYOL_API_KEY     = os.environ.get("TRENDYOL_API_KEY", "")
TRENDYOL_API_SECRET  = os.environ.get("TRENDYOL_API_SECRET", "")
TRENDYOL_SUPPLIER_ID = os.environ.get("TRENDYOL_SUPPLIER_ID", "1075171")

# Tedarikçi Telegram chat ID'leri
SUPPLIER_CHAT_IDS = {
    "Yusuf Cem":  int(os.environ.get("CEM_IRMAK_CHAT_ID", "6275247970")),
    "Cem Irmak":  int(os.environ.get("CEM_IRMAK_CHAT_ID", "6275247970")),
}

CLAUDE_BASE   = "https://api.anthropic.com"
TELEGRAM_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
GRAPH_BASE    = "https://graph.facebook.com/v19.0"
IMGBB_BASE    = "https://api.imgbb.com/1/upload"
TRENDYOL_BASE = "https://apigw.trendyol.com/integration"

CLAUDE_HEADERS = {
    "x-api-key":         CLAUDE_API_KEY,
    "anthropic-version": "2023-06-01",
    "anthropic-beta":    "managed-agents-2026-04-01",
    "content-type":      "application/json",
}

POLL_INTERVAL = 2
POLL_TIMEOUT  = 300
EXCEL_PATH    = "urunler.xlsx"
SUPPLIER_JSON = "tedarikci-eslesme.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_active_session_id: str   = SESSION_ID
_posted_today: set         = set()
_posted_date: str          = ""
_notified_orders: set      = set()
SUPPLIER_DATA: list        = []
BARCODE_SUPPLIER_MAP: dict = {}

# ─── Tedarikçi verisi ────────────────────────────────────────────────────────

def load_supplier_data():
    global SUPPLIER_DATA, BARCODE_SUPPLIER_MAP
    try:
        with open(SUPPLIER_JSON, "r", encoding="utf-8") as f:
            SUPPLIER_DATA = json.load(f)

        model_map = {}
        for item in SUPPLIER_DATA:
            barkod    = str(item.get("barkod", "")).strip()
            tedarikci = str(item.get("tedarikci_adi", "")).strip()
            if barkod and tedarikci:
                model_map[barkod.lower()] = tedarikci

        df = pd.read_excel(EXCEL_PATH, sheet_name="Ürünler")
        count = 0
        for _, row in df.iterrows():
            sz_barkod = str(row.get("Barkod", "")).strip()
            model     = str(row.get("Model Kodu", "")).strip()
            beden     = str(row.get("Beden", "")).strip()
            key       = f"{model}-{beden}".lower()
            tedarikci = model_map.get(key, "")
            if sz_barkod and tedarikci:
                BARCODE_SUPPLIER_MAP[sz_barkod] = tedarikci
                count += 1

        log.info(f"Tedarikçi haritası: {count} barkod eşleştirildi (JSON: {len(SUPPLIER_DATA)} kayıt)")
    except Exception as e:
        log.error(f"Tedarikçi verisi yüklenemedi: {e}")


def match_supplier(barcode: str) -> str:
    return BARCODE_SUPPLIER_MAP.get(str(barcode).strip(), "")

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

    deadline   = time.time() + POLL_TIMEOUT
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

        data   = r.json()
        events = data.get("events", data.get("data", []))
        if len(events) != last_count:
            last_count = len(events)

        status_events = [e for e in events if e.get("type") in ("session.status_idle", "session.status_running")]
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

# ─── Trendyol API ───────────────────────────────────────────────────────────

def get_trendyol_headers() -> dict:
    token = base64.b64encode(f"{TRENDYOL_API_KEY}:{TRENDYOL_API_SECRET}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "User-Agent":    f"{TRENDYOL_SUPPLIER_ID} - SelfIntegration",
    }


async def fetch_orders(days: int = 1) -> list:
    end_ms   = int(datetime.now().timestamp() * 1000)
    start_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    url        = f"{TRENDYOL_BASE}/order/sellers/{TRENDYOL_SUPPLIER_ID}/orders"
    all_orders = []
    page       = 0

    while True:
        params = {
            "startDate": start_ms, "endDate": end_ms,
            "size": 200, "page": page,
            "orderByField": "OrderDate", "orderByDirection": "DESC",
        }
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(url, headers=get_trendyol_headers(), params=params)
            if r.status_code != 200:
                break
            data        = r.json()
            content     = data.get("content", [])
            total_pages = data.get("totalPages", 1)
            all_orders.extend(content)
            if page >= total_pages - 1 or not content:
                break
            page += 1
        except Exception as e:
            log.error(f"Trendyol fetch hatası: {e}")
            break

    return all_orders


async def fetch_new_orders() -> list:
    end_ms   = int(datetime.now().timestamp() * 1000)
    start_ms = int((datetime.now() - timedelta(minutes=30)).timestamp() * 1000)

    url    = f"{TRENDYOL_BASE}/order/sellers/{TRENDYOL_SUPPLIER_ID}/orders"
    params = {
        "startDate": start_ms, "endDate": end_ms,
        "size": 50, "page": 0,
        "orderByField": "OrderDate", "orderByDirection": "DESC",
        "status": "Created",
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=get_trendyol_headers(), params=params)
        if r.status_code != 200:
            return []
        return r.json().get("content", [])
    except Exception as e:
        log.error(f"Trendyol fetch hatası: {e}")
        return []


def build_excel(orders: list, days: int) -> bytes:
    """Sipariş listesinden Excel dosyası oluşturur, bytes döndürür."""
    rows = []
    for order in orders:
        order_no = order.get("orderNumber", "")
        order_date = datetime.fromtimestamp(
            order.get("orderDate", 0) / 1000
        ).strftime("%d.%m.%Y %H:%M") if order.get("orderDate") else ""
        status = order.get("status", "")

        for line in order.get("lines", []):
            barcode  = line.get("barcode", "")
            supplier = match_supplier(barcode) or "Diğer"
            rows.append({
                "Sipariş No":    order_no,
                "Tarih":         order_date,
                "Durum":         status,
                "Ürün Adı":      line.get("productName", ""),
                "Beden":         line.get("productSize", ""),
                "Adet":          line.get("quantity", 1),
                "Tutar (₺)":     line.get("lineGrossAmount", line.get("amount", 0)),
                "Barkod":        barcode,
                "Tedarikçi":     supplier,
            })

    df_orders = pd.DataFrame(rows)

    # Tedarikçi özet
    if not df_orders.empty:
        supplier_summary = df_orders.groupby("Tedarikçi").agg(
            Sipariş_Adedi=("Sipariş No", "nunique"),
            Toplam_Adet=("Adet", "sum"),
            Toplam_Tutar=("Tutar (₺)", "sum"),
        ).reset_index()
    else:
        supplier_summary = pd.DataFrame()

    # Ürün özet
    if not df_orders.empty:
        product_summary = df_orders.groupby("Ürün Adı").agg(
            Toplam_Adet=("Adet", "sum"),
            Toplam_Tutar=("Tutar (₺)", "sum"),
        ).sort_values("Toplam_Adet", ascending=False).reset_index()
    else:
        product_summary = pd.DataFrame()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_orders.to_excel(writer, sheet_name="Tüm Siparişler", index=False)
        if not supplier_summary.empty:
            supplier_summary.to_excel(writer, sheet_name="Tedarikçi Bazlı", index=False)
        if not product_summary.empty:
            product_summary.to_excel(writer, sheet_name="Ürün Bazlı", index=False)

    return buf.getvalue()


def summarize_orders(orders: list, days: int) -> str:
    if not orders:
        return f"Son {days} günde hiç sipariş bulunamadı."

    total_amount   = 0.0
    total_qty      = 0
    supplier_stats: dict = {}
    product_stats: dict  = {}
    cancelled = 0
    returned  = 0

    for order in orders:
        status = order.get("status", "")
        if status in ("Cancelled",):
            cancelled += 1
        if status in ("Returned", "UnDelivered"):
            returned += 1

        for line in order.get("lines", []):
            qty    = line.get("quantity", 1)
            amount = line.get("lineGrossAmount", line.get("amount", 0))
            name   = line.get("productName", "")
            barkod = line.get("barcode", "")

            total_amount += amount
            total_qty    += qty

            supplier = match_supplier(barkod) or "Diğer"
            if supplier not in supplier_stats:
                supplier_stats[supplier] = {"qty": 0, "amount": 0.0}
            supplier_stats[supplier]["qty"]    += qty
            supplier_stats[supplier]["amount"] += amount

            if name not in product_stats:
                product_stats[name] = {"qty": 0, "amount": 0.0}
            product_stats[name]["qty"]    += qty
            product_stats[name]["amount"] += amount

    supplier_lines = []
    for sup, stats in sorted(supplier_stats.items(), key=lambda x: x[1]["amount"], reverse=True):
        pay = (stats["amount"] / total_amount * 100) if total_amount else 0
        supplier_lines.append(f"  • {sup}: {stats['qty']} adet | {stats['amount']:.2f}₺ (%{pay:.0f})")

    top5 = sorted(product_stats.items(), key=lambda x: x[1]["qty"], reverse=True)[:5]
    top5_lines = [f"  {i+1}. {name[:45]} → {s['qty']} adet | {s['amount']:.2f}₺"
                  for i, (name, s) in enumerate(top5)]

    komisyon = total_amount * 0.215
    net_kar  = total_amount - komisyon - (total_qty * 27.5)

    return f"""📊 *Son {days} Günlük Sipariş Özeti*
_{datetime.now().strftime('%d.%m.%Y %H:%M')}_

💰 *Finansal*
• Toplam Sipariş: {len(orders)} paket
• Toplam Satış: {total_amount:.2f}₺
• Trendyol Komisyonu (%21.5): -{komisyon:.2f}₺
• Tahmini Net Kâr: {net_kar:.2f}₺
• İade/İptal: {returned + cancelled} adet

👥 *Tedarikçi Bazlı*
{chr(10).join(supplier_lines)}

🏆 *En Çok Satan 5 Ürün*
{chr(10).join(top5_lines)}

_✅ Gerçek Trendyol verisi_"""


def extract_days(text: str) -> int:
    text = text.lower()
    if "bugün" in text or "bugünkü" in text:
        return 1
    if "bu hafta" in text or "haftalık" in text:
        return 7
    if "bu ay" in text or "aylık" in text:
        return 30
    match = re.search(r"(\d+)\s*gün", text)
    if match:
        return min(int(match.group(1)), 30)
    return 3


def is_order_report_request(text: str) -> bool:
    keywords = ["sipariş", "satış", "rapor", "özet", "kazanç", "kâr", "gelir", "ciro", "kaç sipariş", "kaç satış"]
    return any(kw in text.lower() for kw in keywords)


def is_excel_request(text: str) -> bool:
    keywords = ["excel", "xlsx", "dosya", "indir", "tablo"]
    return any(kw in text.lower() for kw in keywords) and is_order_report_request(text)

# ─── Telegram dosya gönderme ─────────────────────────────────────────────────

async def send_excel_to_telegram(chat_id: int, excel_bytes: bytes, filename: str) -> None:
    async with httpx.AsyncClient(timeout=60) as client:
        await client.post(
            f"{TELEGRAM_BASE}/sendDocument",
            data={"chat_id": str(chat_id)},
            files={"document": (filename, excel_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        )

# ─── Sipariş kontrolü ────────────────────────────────────────────────────────

async def check_new_orders():
    global _notified_orders

    if not TRENDYOL_API_KEY or not TRENDYOL_API_SECRET:
        return

    orders = await fetch_new_orders()
    if not orders:
        return

    for order in orders:
        order_id = str(order.get("id") or order.get("shipmentPackageId", ""))
        if not order_id or order_id in _notified_orders:
            continue

        for line in order.get("lines", []):
            product_name = line.get("productName", "")
            barcode      = line.get("barcode", "")
            quantity     = line.get("quantity", 1)
            size         = line.get("productSize", "")
            amount       = line.get("amount", 0)

            supplier = match_supplier(barcode)
            chat_id  = SUPPLIER_CHAT_IDS.get(supplier, 0)

            log.info(f"Sipariş: {product_name} | Barkod: {barcode} | Tedarikçi: {supplier or 'bulunamadı'}")

            if chat_id:
                msg = f"""🛍 *Yeni Sipariş!*

📦 *{product_name}*
📏 Beden: {size}
🔢 Adet: {quantity}
💰 Tutar: {amount:.2f}₺
🏪 Sipariş No: {order.get('orderNumber', order_id)}

Lütfen hazırlayınız ✅"""
                await send_telegram_to(chat_id, msg)

        _notified_orders.add(order_id)
        if len(_notified_orders) > 1000:
            _notified_orders = set(list(_notified_orders)[-500:])

# ─── ImgBB ──────────────────────────────────────────────────────────────────

async def upload_to_imgbb(image_url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(image_url, follow_redirects=True)
            if r.status_code != 200:
                return ""
            image_data = base64.b64encode(r.content).decode("utf-8")
            r = await client.post(IMGBB_BASE, data={"key": IMGBB_API_KEY, "image": image_data})
            result = r.json()
            if result.get("success"):
                return result["data"]["url"]
            return ""
    except Exception as e:
        log.error(f"ImgBB exception: {e}")
        return ""

# ─── Instagram API ───────────────────────────────────────────────────────────

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
            params={"media_type": "REELS", "video_url": video_url, "caption": caption, "access_token": META_ACCESS_TOKEN},
        )
        if r.status_code != 200:
            return {"error": r.text}
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
        if r.status_code != 200:
            return {"error": r.text}
        data = r.json()
        if "id" not in data:
            return {"error": str(data)}
        await asyncio.sleep(10)
        r = await client.post(
            f"{GRAPH_BASE}/{INSTAGRAM_ACCOUNT_ID}/media_publish",
            params={"creation_id": data["id"], "access_token": META_ACCESS_TOKEN},
        )
        return r.json()

# ─── Facebook API ────────────────────────────────────────────────────────────

async def facebook_carousel_post(image_urls: list, caption: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        photo_ids = []
        for url in image_urls:
            r = await client.post(
                f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/photos",
                params={"url": url, "published": "false", "access_token": FACEBOOK_ACCESS_TOKEN},
            )
            data = r.json()
            if "id" in data:
                photo_ids.append({"media_fbid": data["id"]})
            await asyncio.sleep(1)

        if not photo_ids:
            return {"error": "Hiç fotoğraf yüklenemedi"}

        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/feed",
            params={"access_token": FACEBOOK_ACCESS_TOKEN},
            json={"message": caption, "attached_media": photo_ids},
        )
        return r.json()


async def facebook_story_post(image_url: str) -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/photos",
            params={"url": image_url, "published": "false", "access_token": FACEBOOK_ACCESS_TOKEN},
        )
        if r.status_code != 200:
            return {"error": r.text}
        photo_id = r.json().get("id")
        if not photo_id:
            return {"error": "photo_id alınamadı"}
        r = await client.post(
            f"{GRAPH_BASE}/{FACEBOOK_PAGE_ID}/photo_stories",
            params={"photo_id": photo_id, "access_token": FACEBOOK_ACCESS_TOKEN},
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

    if require_video and "Video URL" in df.columns:
        with_video = available[
            available["Video URL"].notna() &
            available["Video URL"].astype(str).str.startswith("http")
        ]
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
    log.info("⏰ Carousel paylaşımı başlıyor...")
    df = load_products()
    if df.empty:
        await notify_telegram("❌ Ürün listesi bulunamadı.")
        return

    product = pick_product(df)
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

    ig_ok = "id" in str(ig) and "error" not in ig
    fb_ok = "id" in str(fb) and "error" not in fb

    await notify_telegram(f"📸 *Carousel Post*\n{product['name']}\nIG: {'✅' if ig_ok else '❌'} | FB: {'✅' if fb_ok else '❌'}")


async def scheduled_reels():
    log.info("⏰ Reels paylaşımı başlıyor...")
    df = load_products()
    if df.empty:
        await notify_telegram("❌ Ürün listesi bulunamadı.")
        return

    product = pick_product(df, require_video=True)
    if not product["video_url"]:
        await notify_telegram("⚠️ Reels: Video URL'si olan ürün bulunamadı.")
        return

    caption = await generate_caption(product)
    ig = await instagram_reels_post(product["video_url"], caption)
    ig_ok = "id" in str(ig) and "error" not in ig
    await notify_telegram(f"🎬 *Reels*\n{product['name']}\nIG: {'✅' if ig_ok else '❌'}")


async def scheduled_story():
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

    ig_ok = "id" in str(ig) and "error" not in ig
    fb_ok = "id" in str(fb) and "error" not in fb
    await notify_telegram(f"📖 *Story*\n{product['name']}\nIG: {'✅' if ig_ok else '❌'} | FB: {'✅' if fb_ok else '❌'}")


async def notify_telegram(text: str):
    if not TELEGRAM_CHAT_ID:
        return
    await send_telegram_to(int(TELEGRAM_CHAT_ID), text)


async def send_telegram_to(chat_id: int, text: str) -> None:
    if not chat_id:
        return
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{TELEGRAM_BASE}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )

# ─── Telegram ───────────────────────────────────────────────────────────────

async def send_telegram(chat_id: int, text: str) -> None:
    await send_telegram_to(chat_id, text)


async def handle_order_report(chat_id: int, text: str) -> None:
    days = extract_days(text)
    want_excel = is_excel_request(text)

    await send_telegram(chat_id, f"⏳ Son {days} günlük gerçek veriler çekiliyor...")
    orders = await fetch_orders(days)

    # Her zaman metin özeti gönder
    summary = summarize_orders(orders, days)
    await send_telegram(chat_id, summary)

    # Excel isteniyorsa veya sipariş varsa Excel de gönder
    if want_excel and orders:
        filename = f"siparis_raporu_{days}gun_{datetime.now().strftime('%Y%m%d')}.xlsx"
        excel_bytes = build_excel(orders, days)
        await send_excel_to_telegram(chat_id, excel_bytes, filename)
        log.info(f"Excel gönderildi: {filename}")


async def handle_message(chat_id: int, text: str) -> None:
    try:
        if is_order_report_request(text):
            await handle_order_report(chat_id, text)
        else:
            reply = await ask_claude(text)
            await send_telegram(chat_id, reply)
    except Exception as exc:
        log.exception("handle_message hatası")
        await send_telegram(chat_id, f"❌ Beklenmedik hata: {exc}")

# ─── FastAPI ────────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler(timezone="Europe/Istanbul")

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_supplier_data()

    try:
        sid = await get_or_create_session()
        log.info(f"✅ Hazır. Aktif session: {sid}")
    except Exception as exc:
        log.error(f"Session başlatılamadı: {exc}")

    scheduler.add_job(scheduled_carousel, CronTrigger(hour=10, minute=0))
    scheduler.add_job(scheduled_carousel, CronTrigger(hour=14, minute=0))
    scheduler.add_job(scheduled_carousel, CronTrigger(hour=20, minute=0))
    scheduler.add_job(scheduled_reels,    CronTrigger(hour=20, minute=30))
    scheduler.add_job(scheduled_story,    CronTrigger(hour=0,  minute=0))
    scheduler.add_job(check_new_orders,   IntervalTrigger(minutes=5))

    scheduler.start()
    log.info("⏰ Carousel 10:00/14:00/20:00 | Reels 20:30 | Story 00:00 | Sipariş her 5dk (TR)")

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
        await send_telegram(chat_id, "📸 Carousel paylaşımı başlatıldı...")
        return JSONResponse({"ok": True})

    if text.startswith("/reels_now"):
        background.add_task(scheduled_reels)
        await send_telegram(chat_id, "🎬 Reels paylaşımı başlatıldı...")
        return JSONResponse({"ok": True})

    if text.startswith("/story_now"):
        background.add_task(scheduled_story)
        await send_telegram(chat_id, "📖 Story paylaşımı başlatıldı...")
        return JSONResponse({"ok": True})

    if text.startswith("/check_orders"):
        background.add_task(check_new_orders)
        await send_telegram(chat_id, "🔍 Sipariş kontrolü başlatıldı...")
        return JSONResponse({"ok": True})

    background.add_task(handle_message, chat_id, text)
    await send_telegram(chat_id, "⏳ Cevap yazılıyor…")
    return JSONResponse({"ok": True})


@app.get("/")
async def health():
    return {"status": "ok", "session": _active_session_id}
