# Bybit USDT Perpetual Discord Signal Bot — v4 (Adaptive Scalping + Follow Trade)

Bot sinyal Discord untuk Bybit linear USDT perpetual (data publik, **tanpa API key, tanpa eksekusi order**).
Ini scanner eksperimental, **bukan strategi yang terbukti profit**. Jalankan paper/follow dulu dan baca `/performance`.

## Metode: Adaptive Scalping (entry 5m/15m, regime filter 30m)

Default `SIGNAL_STRATEGY=adaptive` memilih metode sesuai kondisi pasar 30m:
- **Tren kuat** (ADX ≥ `REGIME_TREND_ADX`, arah EMA/close sejalan): Breakout + Retest dan EMA Pullback + VWAP.
- **Range** (ADX ≤ `REGIME_RANGE_ADX`): Bollinger Bands + RSI, Liquidity Sweep + Reversal, dan MACD Divergence.
- **Transisi**: jika `ACTIVE_FILTER=1`, bot mengevaluasi kelima metode; jika `0`, hanya Breakout + Retest dan BB + RSI.

Metode yang tersedia: `breakout_retest`, `bb_rsi`, `both`, `all`, `adaptive`. `MIN_SIGNAL_SCORE=65` pada `.env.example` merupakan setelan lebih aktif; naikkan jika terlalu banyak sinyal. Skor 0–100 adalah ranking heuristik, **bukan probabilitas menang**.

- **Breakout + Retest:** breakout rentang lalu retest dan close kembali mempertahankan level, dengan filter volume, tren 15m/30m, BTC, likuiditas, dan biaya.
- **Bollinger Bands + RSI:** candle tertutup sebelumnya keluar band lalu candle terbaru kembali masuk, RSI berbalik, dan ADX 30m tidak terlalu tinggi.
- **EMA Pullback + VWAP:** tren 15m/30m, pullback EMA20, close searah VWAP dan candle konfirmasi.
- **MACD + Divergence:** swing harga membuat extreme baru sementara MACD histogram tidak mengonfirmasi, lalu candle reversal menutup searah.
- **Liquidity Sweep + Reversal:** harga menyapu swing high/low sebelumnya lalu menutup kembali ke dalam rentang.

## Biaya dan hasil simulasi

Net R mengurangi estimasi fee round-trip (`FEE_PCT`), spread saat sinyal, slippage round-trip (`SLIPPAGE_PCT`) dan funding jika sisi posisi diperkirakan membayar (`FUNDING_ESTIMATE=1`). Funding dihitung sebagai estimasi konservatif satu event, bukan data funding aktual berdasarkan waktu posisi; biaya sebenarnya dapat berbeda. Setup ditolak jika estimasi biaya terlalu besar relatif terhadap risiko (`MAX_FEE_R`). Tidak ada klaim profitabilitas: tes sintaks/smoke test bukan backtest historis. Lakukan backtest dengan data Bybit historis, biaya realistis, lalu out-of-sample sebelum mengambil keputusan.

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
- R dilaporkan gross dan net setelah estimasi fee, spread, slippage (`SLIPPAGE_PCT`), serta funding sisi pembayar bila `FUNDING_ESTIMATE=1`. Funding hanya estimasi konservatif satu event; holding time dan funding timestamp aktual tidak disimulasikan. Hasil tetap paper trading, bukan backtest atau jaminan profit.
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

Strategi default adalah `SIGNAL_STRATEGY=adaptive`: pada tren kuat memakai Breakout + Retest, EMA Pullback + VWAP, dan MACD Divergence; pada range memakai Bollinger Bands + RSI, Liquidity Sweep + Reversal, dan MACD Divergence; di kondisi transisi mengevaluasi kelima metode. Opsi manual: `breakout_retest`, `bb_rsi`, `both`, atau `all`. Timeframe entry tetap `5,15` dengan filter rezim 30m.

- **Breakout + Retest:** menunggu breakout range, retest level, candle searah, volume minimum, konfirmasi 15m dan tren 30m, serta filter likuiditas/BTC/biaya.
- **Bollinger + RSI:** menunggu candle sebelumnya keluar dari band lalu candle tertutup kembali masuk, RSI berbalik dari area ekstrem, candle konfirmasi, dan ADX 30m tidak terlalu tinggi. Target dibatasi oleh BB midline dan `TP_R`.
- Variabel: `BREAKOUT_LOOKBACK`, `BREAKOUT_RETEST_BARS`, `BREAKOUT_MIN_VOLUME`, `BB_RSI_OVERSOLD`, `BB_RSI_OVERBOUGHT`, `BB_MAX_ADX_30M`, `TP_R`.
- Semua sinyal tetap simulasi/paper trading. Parameter ini belum dinyatakan optimal atau profitabel; validasi lewat backtest/out-of-sample dan `/performance`.

Tombol Riwayat Saya menyediakan filter Semua/Berjalan/Selesai. Statistik personal mencakup win rate berdasarkan hasil net positif, net R setelah estimasi fee, rata-rata net R, dan profit factor. Hasil follow dihitung dari harga entry follower ke level SL/TP masing-masing sinyal. Bot tetap hanya melakukan simulasi; tidak mengirim order nyata. Uji dengan paper trading dan evaluasi sampel yang cukup sebelum mempertimbangkan trading sungguhan.
