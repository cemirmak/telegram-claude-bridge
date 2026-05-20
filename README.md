# Telegram → Claude Bridge 🤖

Telegram mesajlarını Claude API'sine köprüleyen Railway servisi.  
Sesli mesajları otomatik metne çevirir (Whisper tiny modeli).

---

## 🚀 Railway'e Deploy Adımları

### 1. GitHub'a Yükle
```bash
git init
git add .
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/KULLANICI/telegram-claude-bridge.git
git push -u origin main
```

### 2. Railway'de Yeni Proje
- railway.com → **New Project** → **GitHub Repository**
- Bu repo'yu seçin, deploy edin.

### 3. Environment Variables Ekle
Railway dashboard → projeniz → **Variables** sekmesi:

| Değişken | Değer |
|---|---|
| `TELEGRAM_TOKEN` | BotFather'dan aldığınız token |
| `CLAUDE_API_KEY` | Anthropic API anahtarınız |
| `CLAUDE_MODEL` | `claude-sonnet-4-20250514` *(opsiyonel)* |
| `SYSTEM_PROMPT` | Bot kişiliği *(opsiyonel)* |
| `WHISPER_LANG` | `tr` *(ses dili, opsiyonel)* |
| `MAX_HISTORY` | `20` *(bağlam penceresi, opsiyonel)* |

### 4. Public URL Al
Railway dashboard → **Settings** → **Networking** → **Generate Domain**  
URL şu formatta olacak: `https://xxx.up.railway.app`

### 5. Webhook'u Kur
```bash
pip install httpx
TELEGRAM_TOKEN=xxx RAILWAY_URL=https://xxx.up.railway.app python setup_webhook.py
```

### 6. Test Et
Telegram'da botunuza mesaj gönderin! 🎉

---

## 💬 Bot Komutları

| Komut | Açıklama |
|---|---|
| `/start` | Sohbeti başlat / sıfırla |
| `/reset` | Konuşma geçmişini temizle |
| *(herhangi metin)* | Claude'a sor |
| *(sesli mesaj)* | Otomatik metne çevir, Claude'a sor |

---

## 🏗️ Mimari

```
Telegram Kullanıcı
      │
      ▼  (HTTPS webhook)
Railway FastAPI Servisi
      │
      ├─► Claude API  (metin cevap)
      │
      └─► Whisper     (ses → metin, CPU)
```

---

## ⚠️ Notlar

- Whisper `tiny` modeli ilk başlatmada ~75 MB indirir (Railway'de 30-60 sn sürebilir).
- Free tier için Railway'de **sleep** modu olabilir; ilk mesaj gecikebilir.
- Konuşma geçmişi in-memory tutulur; servis yeniden başlarsa sıfırlanır.
- Kalıcı geçmiş için Railway'e PostgreSQL ekleyip kodu güncelleyebilirsiniz.
