# Bybit USDT Perpetual Discord Signal Bot — v4 (Trend Pullback + Follow Trade)

Bot sinyal Discord untuk Bybit linear USDT perpetual (data publik, **tanpa API key, tanpa eksekusi order**).
Ini scanner eksperimental, **bukan strategi yang terbukti profit**. Jalankan paper/follow dulu dan baca `/performance`.

## Metode: Trend Pullback (TF entry 5m/15m, filter 15m/30m)
Syarat **wajib** untuk SIGNAL (LONG; SHORT kebalikannya):
1. **Tren 30m:** close > EMA200, EMA20 > EMA50, close > EMA50, dan **ADX ≥ 20** (bukan sideways).
2. **15m searah:** EMA20 > EMA50. TF entry: EMA20 > EMA50.
3. **Pullback:** dalam 8 candle terakhir harga menyentuh EMA20 tanpa close di bawah EMA50; sentuhan terjadi di 2 candle terakhir.
4. **Trigger:** candle close kembali di atas EMA20, searah, dengan **rejection wick (≥40% range) atau engulfing**, volume ≥ rata-rata, RSI naik dari area pullback.
5. **Anti-chase:** close tidak boleh > 1,5×ATR dari EMA20.
6. **Filter BTC:** untuk altcoin, arah yang melawan gerakan BTC 15m (> 1,5 ATR dalam 3 candle) diblok.
7. **Fee:** setup ditolak jika fee pulang-pergi memakan > 0,25R (SL terlalu sempit). Akibatnya BTC/ETH di 5m sering tidak lolos; itu disengaja.
8. **News:** isi `NEWS_BLACKOUTS` (UTC) agar tidak ada sinyal ±30 menit dari rilis besar.

SL = di luar swing pullback + 0,2×ATR (min 0,8×ATR, maks 2,5×ATR). TP = `TP_R` × risiko (default **1R**).
**WATCH** = tren valid dan pullback sedang berlangsung, belum ada candle konfirmasi (tidak masuk statistik, tidak bisa di-follow).
Skor 0-100 hanya untuk **peringkat**, bukan probabilitas menang.

Breakeven winrate (setelah fee): dengan TP 1R dan fee ≈0,13R, perlu winrate ≈ 56% hanya untuk impas. Verifikasi lewat `/performance` dan `/riwayat` sebelum percaya.

## Follow Trade
- Tiap pesan **SIGNAL** punya tombol **✅ Ikut Trade** dan **📋 Riwayat Saya**.
- Saat ikut, bot mencatat harga pasar saat itu sebagai harga masuk. Ditolak jika harga sudah menyentuh TP/SL atau sudah ≥ `FOLLOW_MAX_DRIFT_R` (0,5R) menuju TP.
- Saat TP/SL tersentuh, bot menghitung hasil tiap follower dari harga masuk masing-masing (gross R dan net R setelah fee), mengubah footer pesan sinyal, menonaktifkan tombol, dan me-reply dengan mention para follower.
- `/riwayat [Semua | Berjalan | Selesai]` menampilkan WR, total net R, dan 12 trade terbaru (hanya terlihat oleh kamu).
- Tombol bersifat persisten: tetap berfungsi setelah bot restart/redeploy.
- Ini **simulasi**, bukan order nyata.

## Perintah
`/status` `/signal` `/scanner` `/market` `/history` `/riwayat` `/performance` `/evaluate` `/realtime`
`/signal` yang ditolak menampilkan **alasan NO TRADE**.

## Evaluator
- Memakai high/low candle tertutup. TP dan SL di candle yang sama = `AMBIGUOUS`, **dihitung -1R** (konservatif).
- R dilaporkan **gross dan net** (dikurangi `FEE_PCT`). Slippage/funding belum dimodelkan.
- `/performance`: winrate konservatif = TP/(TP+SL+Ambigu), plus ringkasan per timeframe.

## Embed
Embed SIGNAL kini 3 baris deskripsi (Entry/SL/TP · RR/skor/fee/spread · alasan), tanpa field, chart lebih pendek (900×410). WATCH satu baris per pair dan tanpa chart (aktifkan dengan `WATCH_CHART=1`).

## Deploy (Railway + Upstash Redis)
1. Buat database di Upstash Console (Redis), lalu buka menu **REST API** dan salin REST URL serta REST token.
2. Push folder ini ke GitHub dan deploy service bot di Railway. Pada Variables bot, isi `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`, `DISCORD_TOKEN`, `GUILD_ID`, `ALERT_CHANNEL_ID` (+ opsional lainnya dari `.env.example`). Jangan gunakan `REDIS_URL` dari service Redis Railway untuk versi ini.
3. Start command `python bot.py` (Procfile: `worker: python bot.py`). **Satu replica** saja.
4. Izin bot di channel alert: View Channel, Send Messages, Embed Links, **Attach Files**, **Read Message History** (untuk update pesan sinyal dan reply hasil). Scope invite: `bot` + `applications.commands`.
5. Di Upstash, pilih kebijakan persistensi/retensi yang sesuai dan pantau kuota request. Jangan menghapus database saat bot menyimpan riwayat follow.
6. Start konservatif: `MAX_SCAN_SYMBOLS=15`, `SCAN_INTERVAL_SECONDS=120`, `SCAN_TIMEFRAMES=5,15`.

## Troubleshooting
- **Sinyal tidak terkirim otomatis tapi `/scanner` jalan:** jalankan `/status`, bagian *Alert otomatis* menunjukkan penyebabnya (Upstash Redis belum terhubung, `ALERT_CHANNEL_ID` salah, atau izin channel kurang). `/scanner` tidak butuh Redis/channel, alert otomatis butuh keduanya. Hanya setup **SIGNAL** (konfirmasi sudah close) yang dikirim sebagai sinyal; sisanya muncul sebagai WATCH.
- **Slash command tidak muncul:** cek log `Slash command tersinkron ...`; invite ulang dengan scope `applications.commands`; `GUILD_ID` harus angka.
- **403 dari api.bybit.com:** pindahkan region Railway ke Singapore; bot mencoba host cadangan `api.bytick.com` (`BYBIT_REST_URLS`).
- **Tombol "interaction failed" setelah update:** pastikan memakai discord.py ≥ 2.4 (dynamic items).
- **Sinyal terlalu jarang:** longgarkan bertahap `ADX_MIN`, `MAX_FEE_R`, `MAX_SPREAD_PCT`, atau tambah `MAX_SCAN_SYMBOLS`. Jangan menurunkan syarat pullback/trigger tanpa data `/performance`.
- Data lama dari versi sebelumnya tetap terbaca; field baru (`net_r`, `fee_r`) kosong untuk sinyal lama.


## Strategi Scalping (paper trading)

Hanya strategi yang diminta pengguna yang tersedia: `breakout_retest` dan `bb_rsi`. Default `.env.example` adalah `SIGNAL_STRATEGY=both` agar kedua strategi yang diminta aktif. Gunakan `breakout_retest` atau `bb_rsi` bila ingin menjalankan satu strategi saja. Timeframe entry tetap `5,15` dengan filter kondisi tren/ADX 30m.

- **Breakout + Retest:** menunggu breakout range, retest level, candle searah, volume minimum, konfirmasi 15m dan tren 30m, serta filter likuiditas/BTC/biaya.
- **Bollinger + RSI:** menunggu candle sebelumnya keluar dari band lalu candle tertutup kembali masuk, RSI berbalik dari area ekstrem, candle konfirmasi, dan ADX 30m tidak terlalu tinggi. Target dibatasi oleh BB midline dan `TP_R`.
- Variabel: `BREAKOUT_LOOKBACK`, `BREAKOUT_RETEST_BARS`, `BREAKOUT_MIN_VOLUME`, `BB_RSI_OVERSOLD`, `BB_RSI_OVERBOUGHT`, `BB_MAX_ADX_30M`, `TP_R`.
- Semua sinyal tetap simulasi/paper trading. Parameter ini belum dinyatakan optimal atau profitabel; validasi lewat backtest/out-of-sample dan `/performance`.

Tombol Riwayat Saya menyediakan filter Semua/Berjalan/Selesai. Statistik personal mencakup win rate berdasarkan hasil net positif, net R setelah estimasi fee, rata-rata net R, dan profit factor. Hasil follow dihitung dari harga entry follower ke level SL/TP masing-masing sinyal. Bot tetap hanya melakukan simulasi; tidak mengirim order nyata. Uji dengan paper trading dan evaluasi sampel yang cukup sebelum mempertimbangkan trading sungguhan.
