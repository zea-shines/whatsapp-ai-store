# 🤖 WhatsApp AI Store Assistant & Management Dashboard

Sistem backend terintegrasi berbasis **Node.js** untuk mengelola toko online otomatis melalui WhatsApp dengan dukungan **AI (DeepSeek)**, database **PostgreSQL**, serta **Dashboard Real-Time** berbasis WebSocket.

---

## 🔑 Fitur Utama

- 🧠 **AI Integration (DeepSeek API):** Layanan balasan otomatis WhatsApp yang memahami konteks produk toko secara cerdas tanpa basa-basi.
- 💬 **Live Chat Dashboard:** Fitur intervensi manual oleh Admin untuk mengambil alih percakapan pelanggan kapan saja (Auto-toggle AI/Manual).
- ⚡ **Real-time Synchronization:** Komunikasi dua arah menggunakan **WebSocket (WS)** untuk pembaruan status toko dan kontak secara instan.
- 🛡️ **Enterprise Security:**
  - Enkripsi password menggunakan `bcrypt`.
  - Otentikasi berbasis **JWT Cookie (HttpOnly & SameSite)**.
  - Perlindungan dari serangan brute force (**Login Rate Limiter & IP Lockout**).
  - Sanitasi input anti-XSS.
- 📦 **Database PostgreSQL:** Penyimpanan log percakapan, produk, riwayat pesanan, dan konfigurasi sistem yang aman dan terstruktur.
- 🔄 **Message Queue System:** Menangani lonjakan pesan masuk secara berurutan tanpa membuat server *overload*.

---

## 🛠️ Tech Stack

- **Backend:** Node.js, Express.js, WebSocket (`ws`)
- **Database:** PostgreSQL (`pg`)
- **AI Engine:** DeepSeek Chat API
- **External API:** Meta / WhatsApp Cloud API
- **Security:** JWT, Bcrypt.js, Cookie-Parser
- **Frontend:** HTML5, Tailwind CSS, JavaScript (Vanilla ES6+)

---

## 🚀 Cara Menjalankan (Environment Termux / Linux)

1. **Clone Repositori:**
   ```bash
   git clone [https://github.com/zea-shines/toko-whatsapp-ai.git](https://github.com/zea-shines/toko-whatsapp-ai.git)
   cd toko-whatsapp-ai
   npm install 
   PORT=3000
JWT_SECRET=rahasia_jwt_kamu
DASHBOARD_PW=password_admin_kamu

DB_USER=postgres
DB_HOST=localhost
DB_NAME=store_db
DB_PASSWORD=password_db
DB_PORT=5432

META_ACCESS_TOKEN=token_meta_whatsapp
META_PHONE_NUMBER_ID=id_nomor_meta
META_VERIFY_TOKEN=token_verifikasi_webhook

DEEPSEEK_API_KEY=api_key_deepseek

node zea.js
