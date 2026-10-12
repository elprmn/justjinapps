# Bybit USDT Perpetual Discord Signal Bot — Short-term build

Discord slash-command bot for Bybit linear USDT perpetual public market data. It does **not** place orders and needs no Bybit API key. This is an experimental signal scanner, not a validated profitable strategy.

## Implemented
- Strict entry timeframe options: **5m, 15m, 30m only**. No 1h signals.
- Multi-timeframe logic: 30m trend filter, 15m momentum confirmation, selected timeframe entry trigger.
- Indicators: EMA20/50/200, RSI, MACD, ATR, relative volume and 20-candle range breakout/breakdown.
- Quality filters: 24h turnover, top-of-book bid/ask spread estimate, ATR volatility guard, closed candles only.
- LONG/SHORT only when trend and momentum align; otherwise NO TRADE.
- ATR/structure-based SL, TP reference and RR; rejects excessive stop distance.
- Bounded concurrency, REST error/rate-limit handling, configurable scan interval and universe size.
- Persistent Redis signal history, duplicate prevention across restarts, TP/SL outcome evaluator.
- Same candle touches TP and SL => `AMBIGUOUS` rather than assuming a favorable result; unresolved signals expire.
- `/status`, `/signal`, `/scanner`, `/market`, `/history`, `/performance`, `/evaluate`.

## Important limitations
- Live ticker feed uses Bybit public WebSocket for up to 80 liquid USDT pairs, reconnects automatically, and falls back to REST for individual ticker requests when stream cache is stale. `/realtime` reports connection/cache health. Candle analysis still uses REST closed candles; the scheduled signal scanner is interval-based, not a tick-by-tick signal generator.
- Funding rate is displayed when supplied by the ticker endpoint, but is not currently a hard signal filter. Open interest, liquidation feed and order-book depth are not implemented.
- Evaluator uses candle high/low, not tick-by-tick execution. It does not model fees, funding paid, slippage, partial fills or exchange-specific order rules. `R` totals are gross estimates.
- The score is a heuristic, **not a probability of winning**. No historical performance is established. Run paper trading and a proper out-of-sample backtest before relying on it.
- No automatic trade execution; no private Bybit credentials should be added.
- Redis data survives bot redeploys when the Redis service itself retains its data. Check the Redis provider persistence/backup and eviction settings; do not use an ephemeral Redis instance for important history.

## Discord setup
1. Create an application at https://discord.com/developers/applications.
2. Create a bot and copy its token privately. Never post the token publicly.
3. Install with `bot` and `applications.commands` scopes. Grant View Channels, Send Messages, Embed Links.
4. Copy server and alert-channel IDs (Developer Mode in Discord).

## Deploy to PaaS
1. Deploy this folder as a **background worker/service**, not a static site.
2. Start command: `python bot.py` (Procfile included).
3. Add environment variables from `.env.example`.
4. Riwayat sinyal disimpan di Redis (`REDIS_URL`), bukan file lokal. Lihat bagian Railway di bawah.
5. Start conservatively: `MAX_SCAN_SYMBOLS=15`, `SCAN_INTERVAL_SECONDS=120`, `SCAN_TIMEFRAMES=5,15,30`.
6. Check logs for `Login sebagai ...`; run `/status`, `/market BTCUSDT`, `/signal BTCUSDT` selecting a timeframe, then `/scanner`.

## Bybit public endpoints
- `GET /v5/market/tickers?category=linear`
- `GET /v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&limit=250`

## Suggested next engineering phase
1. Add WebSocket market streams with reconnect/backoff and stale-data guards.
2. Add order-book depth, funding-rate thresholds and open-interest change filters.
3. Build a reproducible historical backtest with fees/funding/slippage and walk-forward testing.
4. Export signal history to CSV and add per-timeframe/per-symbol breakdowns.
5. Only consider live order execution after a separate explicit design, security review and extensive paper trading.

## Deploy di Railway dengan Redis

1. Upload/push seluruh isi folder ini ke repository GitHub, lalu buat service Railway dari repository tersebut.
2. Tambahkan service **Redis** (atau Valkey yang kompatibel dengan Redis) pada project Railway.
3. Buka service Redis dan pastikan tersedia variable koneksi seperti `REDIS_URL`.
4. Pada service bot, buka **Variables** lalu tambahkan `REDIS_URL` menggunakan reference variable Railway, misalnya `${{Redis.REDIS_URL}}`. Jika nama service Redis berbeda, ganti `Redis` dengan nama service yang persis sama.
5. Tambahkan `DISCORD_TOKEN`, `GUILD_ID`, dan `ALERT_CHANNEL_ID` pada service bot. Jangan commit file `.env` atau token ke GitHub.
6. Pastikan Start Command `python bot.py` atau gunakan Procfile `worker: python bot.py`. Jalankan satu replica worker agar scanner tidak mengirim sinyal duplikat.
7. Deploy dan lihat **Deploy Logs**. Bot akan melakukan ping ke Redis saat mulai; jika koneksi benar, log akan menampilkan `Redis signal store connected`.
8. Di Discord coba `/status`, `/realtime`, `/market pair:BTCUSDT`, `/signal`, `/scanner`, `/history`, `/performance`, dan `/evaluate`.

### Catatan Redis

- Bot menyimpan riwayat sinyal sebagai record JSON di Redis, dengan indeks ID untuk daftar riwayat dan pencegahan duplikasi.
- `REDIS_URL` wajib diisi untuk deployment ini. Jika kosong atau salah, bot akan berhenti dengan pesan error yang jelas.
- Redis/Valkey berbeda dari PostgreSQL: bukan database SQL. Pastikan layanan Redis memiliki persistensi/backup yang sesuai dan kebijakan eviction tidak menghapus key bot. Jangan menghapus service Redis jika ingin mempertahankan riwayat.
- Paket ini tidak mengimpor otomatis riwayat dari database PostgreSQL lama. Jika bot sebelumnya sudah menyimpan riwayat di PostgreSQL, data lama tetap di sana sampai dilakukan migrasi terpisah.

### Catatan operasional

- Bot hanya menggunakan **Bybit public market data**; tidak perlu API key Bybit dan tidak mengeksekusi order.
- Feed ticker WebSocket adalah real-time; candle analisis diambil melalui REST dan candle yang masih terbentuk dikecualikan.
- Jika koneksi WebSocket terputus, bot mencoba menyambung kembali dan REST tetap tersedia sebagai fallback.
- Gunakan satu instance bot untuk menghindari scanner berjalan ganda. Slash commands mungkin memerlukan waktu untuk muncul jika tidak mengisi `GUILD_ID`.
- Sinyal, skor, TP/SL, dan statistik bersifat informasional/edukasi, bukan jaminan profit atau nasihat keuangan.

### Troubleshooting

**Slash command (/scanner dll.) tidak muncul**
- Log harus menampilkan `Slash command tersinkron ...: status, signal, scanner, ...`. Jika tidak ada, lihat error di baris sebelumnya.
- `Missing Access` / `Forbidden`: bot belum di-invite dengan scope **`applications.commands`** (selain `bot`) atau tidak ada di server `GUILD_ID`. Invite ulang lewat Developer Portal → OAuth2 → URL Generator.
- `GUILD_ID` harus angka ID server (tanpa tanda kutip). Jika kosong/salah, command didaftarkan global dan bisa lambat muncul.
- Setelah deploy, restart Discord (Ctrl+R / tutup-buka app) agar daftar command ter-refresh.

**`403 Forbidden` dari api.bybit.com**
- Bybit memblokir IP/region tertentu (termasuk banyak IP datacenter AS). Pindahkan service ke region **Singapore (Southeast Asia)** di Railway → Service → Settings → Deploy → Region, lalu redeploy.
- Bot otomatis mencoba host cadangan `api.bytick.com`. Host bisa diganti dengan `BYBIT_REST_URLS` (pisah koma) dan `BYBIT_WS_URL` (opsional).
- Saat diblok, bot memakai backoff (5s → maks 300s) sehingga tidak membanjiri API.
