# Bybit USDT Perpetual Discord Signal Bot — v4 (Adaptive Scalping + Follow Trade)

Bot sinyal Discord untuk Bybit linear USDT perpetual (data publik, **tanpa API key, tanpa eksekusi order**).
Ini scanner eksperimental, **bukan strategi yang terbukti profit**. Jalankan paper/follow dulu dan baca `/performance`.

## Metode: Adaptive Scalping (entry 5m/15m, regime filter 30m)

Default `SIGNAL_STRATEGY=adaptive` memilih metode sesuai kondisi pasar 30m:
- **Tren kuat** (ADX ≥ `REGIME_TREND_ADX`, arah EMA/close sejalan): Breakout + Retest, EMA Pullback + VWAP, StochRSI Pullback, Supertrend Flip, dan SR Bounce.
- **Range** (ADX ≤ `REGIME_RANGE_ADX`): Bollinger Bands + RSI, Liquidity Sweep + Reversal, Squeeze Breakout, dan SR Bounce.
- **Transisi**: jika `ACTIVE_FILTER=1`, bot mengevaluasi kedelapan metode; jika `0`, hanya Breakout + Retest dan BB + RSI.

Metode yang tersedia: `breakout_retest`, `bb_rsi`, `squeeze`, `stochrsi`, `supertrend`, `sr`, `both`, `all`, `adaptive`. `MIN_SIGNAL_SCORE=65` pada `.env.example` merupakan setelan lebih aktif; naikkan jika terlalu banyak sinyal. Skor 0–100 adalah ranking heuristik, **bukan probabilitas menang**.

- **Breakout + Retest:** breakout rentang lalu retest dan close kembali mempertahankan level, dengan filter volume, tren 15m/30m, BTC, likuiditas, dan biaya.
- **Bollinger Bands + RSI:** candle tertutup sebelumnya keluar band lalu candle terbaru kembali masuk, RSI berbalik, dan ADX 30m tidak terlalu tinggi.
- **EMA Pullback + VWAP:** tren 15m/30m, pullback EMA20, close searah VWAP dan candle konfirmasi.
- **Squeeze Breakout (baru, menggantikan MACD Divergence):** lebar Bollinger menyempit (≥3 dari 6 candle terakhir di persentil bawah `SQUEEZE_PCTL` dari 100 candle), lalu candle momentum (body ≥55% range) menutup di luar band searah tren 15m/30m dengan volume ≥ `SQUEEZE_MIN_VOLUME`× SMA20. Tidak dikejar jika sudah >3 ATR dari BB mid. SL di bawah/atas 3 candle terakhir, TP = `TP_R`.
- **StochRSI Pullback (baru):** tren rapi (EMA 9>21>50 di entry TF, EMA20>EMA50 di 15m, close>EMA50 di 30m), harga pullback menyentuh EMA21 dan StochRSI sempat di area jenuh (`STOCH_OVERSOLD`/`STOCH_OVERBOUGHT`), lalu K memotong D searah tren dengan candle konfirmasi. SL di swing 6 candle, TP `TP_R`.
- **Supertrend Flip (baru):** Supertrend (`ST_PERIOD`, `ST_MULT`) berbalik arah pada candle terakhir, searah EMA200 entry TF + tren 15m/30m + EMA200 30m, body candle ≥50% range, ADX ≥ `ST_MIN_ADX`, volume ≥ `ST_MIN_VOLUME`×. SL di garis Supertrend; ditolak jika garis lebih jauh dari batas SL. `ST_MULT` ≥ 2 membuat hampir semua flip ditolak karena SL terlalu lebar, jadi default 1,5.
- **SR Bounce (baru):** level Support/Resistance dibentuk dari pivot high/low (`SR_LOOKBACK` candle, dikelompokkan dalam 0,3 ATR, minimal `SR_MIN_TOUCHES` sentuhan) di timeframe entry, ditambah level 30m (level yang cocok di dua TF diberi tanda "30m" dan skor lebih tinggi). Entry saat candle menyentuh zona level, menolaknya (wick ≥40% atau engulfing) dan menutup kembali di sisi level; ditolak jika melawan tren 30m yang kuat atau level sudah ditembus. SL di balik level/wick, TP = `TP_R` atau level lawan terdekat, minimal 1R.
- **S/R sebagai konfirmasi semua strategi** (`SR_CONFLUENCE=1`): skor +4 (+6 jika level 30m) bila entry dekat level searah (support untuk LONG, resistance untuk SHORT), dan −6 (−10 jika level 30m) bila ada level lawan di antara entry dan TP. Ini hanya mengubah skor/alasan, bukan menolak setup; filter `MIN_SIGNAL_SCORE` yang menentukan. Set `SR_CONFLUENCE=0` untuk mematikan.
- **Liquidity Sweep + Reversal:** harga menyapu swing high/low sebelumnya lalu menutup kembali ke dalam rentang.

## Biaya dan hasil simulasi

Net R mengurangi estimasi fee round-trip (`FEE_PCT`), spread saat sinyal, slippage round-trip (`SLIPPAGE_PCT`) dan funding jika sisi posisi diperkirakan membayar (`FUNDING_ESTIMATE=1`). Funding dihitung sebagai estimasi konservatif satu event, bukan data funding aktual berdasarkan waktu posisi; biaya sebenarnya dapat berbeda. Setup ditolak jika estimasi biaya terlalu besar relatif terhadap risiko (`MAX_FEE_R`). Tidak ada klaim profitabilitas: tes sintaks/smoke test bukan backtest historis. Lakukan backtest dengan data Bybit historis, biaya realistis, lalu out-of-sample sebelum mengambil keputusan.

## Follow Trade
- Tiap pesan **SIGNAL** punya tombol **✅ Ikut Trade** dan **📋 Riwayat Saya**.
- Saat ikut, bot mencatat harga pasar saat itu sebagai harga masuk. Ditolak jika harga sudah menyentuh TP/SL atau sudah ≥ `FOLLOW_MAX_DRIFT_R` (0,5R) menuju TP.
- Saat TP/SL tersentuh, bot menghitung hasil tiap follower dari harga masuk masing-masing (gross R dan net R setelah fee), mengubah footer pesan sinyal, menonaktifkan tombol, dan me-reply dengan mention para follower.
- `/trade` membuat kartu dashboard PNG bergaya Modern Card dari snapshot harga terbaru, berisi ringkasan Net/Gross/Avg PNL serta kartu tiap posisi (arah, PNL, Entry/Last, SL/TP). Gambar dibuat saat command dipanggil dan dikirim sebagai attachment Discord. Jika rendering gambar gagal, bot mengirim embed teks lengkap sebagai fallback. ROI ditampilkan pada asumsi 1x/tanpa leverage karena leverage/modal tidak dikonfigurasi.
- `/riwayat [Semua | Berjalan | Selesai]` menampilkan WR, total net R, dan 12 trade terbaru (hanya terlihat oleh kamu); trade yang masih berjalan juga memuat PnL/net estimasi jika harga tersedia.
- Tombol bersifat persisten: tetap berfungsi setelah bot restart/redeploy.
- Ini **simulasi**, bukan order nyata.

## Perintah
`/status` `/signal` `/scanner` `/market` `/history` `/trade` `/riwayat` `/performance` `/evaluate` `/realtime`
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
3. Start command `python bot.py` (Procfile: `web: python bot.py`). **Satu replica** saja.
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

Strategi default adalah `SIGNAL_STRATEGY=adaptive`: pada tren kuat memakai Breakout + Retest dan EMA Pullback + VWAP; pada range memakai Bollinger Bands + RSI, Liquidity Sweep + Reversal, dan Squeeze Breakout; di kondisi transisi mengevaluasi kelima metode. Opsi manual: `breakout_retest`, `bb_rsi`, `both`, atau `all`. Timeframe entry tetap `5,15` dengan filter rezim 30m.

- **Breakout + Retest:** menunggu breakout range, retest level, candle searah, volume minimum, konfirmasi 15m dan tren 30m, serta filter likuiditas/BTC/biaya.
- **Bollinger + RSI:** menunggu candle sebelumnya keluar dari band lalu candle tertutup kembali masuk, RSI berbalik dari area ekstrem, candle konfirmasi, dan ADX 30m tidak terlalu tinggi. Target dibatasi oleh BB midline dan `TP_R`.
- Variabel: `BREAKOUT_LOOKBACK`, `BREAKOUT_RETEST_BARS`, `BREAKOUT_MIN_VOLUME`, `BB_RSI_OVERSOLD`, `BB_RSI_OVERBOUGHT`, `BB_MAX_ADX_30M`, `TP_R`.
- Semua sinyal tetap simulasi/paper trading. Parameter ini belum dinyatakan optimal atau profitabel; validasi lewat backtest/out-of-sample dan `/performance`.

Tombol Riwayat Saya menyediakan filter Semua/Berjalan/Selesai. Statistik personal mencakup win rate berdasarkan hasil net positif, net R setelah estimasi fee, rata-rata net R, dan profit factor. Hasil follow dihitung dari harga entry follower ke level SL/TP masing-masing sinyal. Command `/trade` mengambil snapshot harga saat dipanggil (bukan live-updating message setiap detik); PnL harga dan ROI 1x sama secara persentase sebelum biaya, sedangkan net merupakan estimasi setelah biaya. Bot tetap hanya melakukan simulasi; tidak mengirim order nyata. Uji dengan paper trading dan evaluasi sampel yang cukup sebelum mempertimbangkan trading sungguhan.

## Command `/reset` (administrator only)

`/reset` menampilkan konfirmasi privat. Jika admin menekan **RESET SEMUA DATA**, bot menghapus semua key Upstash yang cocok dengan `REDIS_PREFIX:*` (default `bybit_signal_bot:*`) dan menghapus pesan yang dikirim bot di channel `ALERT_CHANNEL_ID`. Command tidak menggunakan `FLUSHDB`/`FLUSHALL`, tidak menghapus key aplikasi lain di database yang sama, tidak menghapus pesan pengguna atau channel/server Discord, serta tidak menghapus token maupun Environment Variables. Reset menghapus history, sinyal, follow trade, dan dedupe bot sehingga tidak dapat dibatalkan. Pastikan bot punya izin `View Channel`, `Read Message History`, dan `Manage Messages`/izin menghapus pesan di channel alert.

## `/trade` text snapshot + live visual dashboard

`/trade` now replies with a clean text-only snapshot (no Discord embed or PNG attachment) and a personal live dashboard link. The web dashboard is served by the same Railway process, uses a responsive dark glassmorphism theme, and refreshes its live position data every 15 seconds.

Configure these Railway variables:

- `DASHBOARD_BASE_URL`: the app's public HTTPS domain, for example `https://your-app.up.railway.app` (without a trailing slash).
- `DASHBOARD_SECRET`: a long random secret used to sign per-user dashboard links. If omitted, the bot token is used as the signing secret.

Railway must expose the app's `PORT` (the bot binds the web dashboard to `0.0.0.0:$PORT`). The generated link contains a signed user ID and works as a bearer link: anyone who receives it can view that user's followed positions. Do not share it publicly. The dashboard is read-only and does not place real orders.

### Live web dashboard UI

The private `/trade` dashboard is a responsive dark glassmorphism UI inspired by the JustJinApps visual reference. It includes a sidebar, portfolio PNL summary, active position cards, followed-pair market overview, a current-position PNL visualization, live snapshot stats, and a paper-trading/privacy note. The dashboard refreshes its own data every 15 seconds. Market rows and PNL visualization are based on followed open positions returned by the bot; they are not external market-wide tickers or historical account-performance data.



## Dashboard: riwayat trade & total PNL
Halaman `/trade` (tautan personal dari Discord) kini menampilkan:
- **Total PNL · All Time:** total net (realized + posisi terbuka), realized net %, Net R, win rate, jumlah trade selesai, profit factor, max drawdown, rata-rata R, best/worst trade, dan **equity curve** (kumulatif net % realized).
- **Trade History:** seluruh trade selesai (TP/SL/Ambigu/Expired) terbaru dulu, dengan filter Semua/Menang/Kalah dan tombol "Tampilkan lebih banyak". Kolom: waktu tutup, pair, side, TF, hasil, Net R, Net %, strategi, entry, exit.
- **Performa per Strategi:** jumlah trade, win rate, dan Net R tiap strategi, supaya kelihatan strategi mana yang gagal.

Net % = Net R × jarak SL (% dari harga entry follower), sudah dikurangi estimasi fee, spread, slippage, dan funding. Jumlah follow yang dimuat diatur `HISTORY_LIMIT` (default 1000 terbaru). Trade lama bertanda `MACD_DIVERGENCE` tetap tampil di riwayat; hanya sinyal baru yang tidak lagi memakai metode itu.


## Scalping signal quality safeguards

- `SIGNAL_STRATEGY` is validated against the actual supported strategy router. Supported manual values include `breakout_retest`, `bb_rsi`, `both`, `ema_vwap`, `liquidity_sweep`, `squeeze`, `stochrsi`, `supertrend`, `sr`, `all`, and `adaptive`.
- The default `adaptive` route uses a smaller core. Trend regime prioritizes Breakout Retest and EMA/VWAP Pullback; range regime uses BB+RSI. StochRSI/Supertrend experiments are opt-in with `ADAPTIVE_INCLUDE_TREND_EXPERIMENTS=1`. Liquidity Sweep is opt-in with `ADAPTIVE_INCLUDE_SWEEP=1`; test each independently before enabling.
- `BTC_FILTER_FAIL_CLOSED=1` rejects altcoin setups when BTC filter data cannot be fetched instead of silently bypassing that risk check.
- `SR_HARD_BLOCK=1` rejects a candidate when its target is obstructed by a higher-timeframe support/resistance level.
- Candle fetching excludes the currently forming candle. This project remains a Discord signal/simulation bot; these safeguards do not make it an exchange order executor and do not establish profitability.

Run offline checks with `python -m py_compile market.py bot.py database.py` and `python -m unittest discover -s tests -v`. These are code/unit checks, not a historical backtest.


## Additional setup-quality controls

The scanner includes conservative filters intended to reject weaker scalping setups; these are heuristics and are not a claim of profitability.

- `MIN_SETUP_SCORE=75`: minimum internal quality score (0-100) after confluence adjustments. Tune only with out-of-sample backtests.
- `ANTI_CHASE_ENABLED=1`: avoids entering extended trend/breakout setups. `MAX_EXTENDED_ATR` controls the extension threshold.
- `CHOP_FILTER_ENABLED=1`: rejects trend-following setups in low-ADX/choppy conditions using EMA-cross frequency and price efficiency.
- `FALSE_BREAKOUT_FILTER_ENABLED=1`: checks breakout candle body, close location and relative volume.
- `vwap_reclaim`: optional strategy route for VWAP reclaim + retest (`SIGNAL_STRATEGY=vwap_reclaim`). In adaptive mode it is considered in trend/transition regimes.
- Liquidity sweep requires reclaim beyond the swing level, meaningful candle body/close location and at least 0.8x average volume.
- Rejection reasons are written to application logs; accepted signal records retain raw score and quality factors for later analysis.

These filters are rule-based, not validated win probabilities. Run historical backtests with realistic fees/slippage and paper-trade before relying on alerts. The bot uses public market data and does not execute exchange orders.
