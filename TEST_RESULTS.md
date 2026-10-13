# Test Results

- `python -m py_compile *.py`: PASS
- `python -m unittest discover -s tests -v`: PASS (7 tests)
- Live dashboard HTML: updated to responsive dark glassmorphism layout with sidebar, portfolio metrics, active positions, followed-pair snapshot, position PNL visualization, and live snapshot statistics.
- Dashboard refresh interval: 15 seconds.
- Data note: displayed market rows and chart are derived from followed open positions only; not a historical equity curve or broad exchange ticker feed.
- Live Discord, Railway public domain, and Upstash/Bybit integration: NOT tested in this environment.

## Perbaikan 404 Railway (web dashboard)
- `python -m py_compile bot.py`: PASS.
- Web server sekarang dinyalakan di `main()` SEBELUM login Discord (sebelumnya baru jalan di akhir `setup_hook`, setelah login + sync slash command + Bybit/Redis).
- Route baru: `/dashboard` (landing, tidak 404), `/health` dan `/healthz` (status JSON), trailing slash dinormalkan, 404 berupa pesan jelas.
- `scheduled_scan` tidak lagi mati permanen kalau `start_services()` error.
- Belum diuji runtime di sandbox (aiohttp/discord.py tidak terpasang, tanpa internet): test unit dan server asli perlu dicek di Railway setelah deploy.

## Ganti MACD Divergence -> Squeeze Breakout + riwayat dashboard
- `python -m py_compile *.py`: PASS. `python -m unittest discover -s tests`: PASS (8 tests, aiohttp di-stub karena sandbox tanpa internet).
- Test baru: Squeeze Breakout memicu LONG pada data sintetis (squeeze + breakout + volume), menolak SHORT dan menolak tanpa lonjakan volume; routing adaptive memastikan MACD sudah tidak ada.
- Dashboard dirender di Chromium (390px & 1280px) dengan data tiruan: tanpa error JS, tanpa scroll samping, filter dan "lebih banyak" bekerja.
- BELUM diuji: Discord, Railway, Upstash, Bybit sungguhan. Squeeze Breakout BELUM di-backtest; ini bukan klaim profit.

## Tambah StochRSI Pullback + Supertrend Flip
- `python -m unittest discover -s tests`: PASS (11 tests; aiohttp di-stub). Test mencakup LONG/SHORT StochRSI (+ penolakan saat tren 15m/30m berlawanan), Supertrend flip (+ penolakan arah berlawanan dan tanpa lonjakan volume), dan validitas garis Supertrend.
- Data sintetis, bukan backtest historis. Kedua metode BELUM terbukti profitable; validasi lewat paper trading dan tabel Performa per Strategi di dashboard.

## Tambah SR Bounce + konfluensi S/R
- `python -m unittest discover -s tests`: PASS (14 tests; aiohttp di-stub). Mencakup deteksi level, SR bounce LONG/SHORT, penolakan (level tembus, di tengah range, melawan tren 30m), dan konfluensi skor (+ dekat level, - TP terhalang, tidak ganda untuk SR_BOUNCE, bisa dimatikan).
- Simulasi end-to-end `analyze_symbol` dengan client palsu: level 30m terpasang dan sinyal SR_BOUNCE keluar. Data sintetis, bukan backtest; belum diuji di Bybit/Discord/Railway sungguhan.
