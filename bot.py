import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from database import Database
import discord
from discord import app_commands
from discord.ext import commands, tasks

from market import BybitMarket, analyze_symbol, fmt_price, INTERVALS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("signalbot")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
GUILD_ID = os.getenv("GUILD_ID", "").strip().strip("\"\'").strip()
ALERT_CHANNEL_ID = os.getenv("ALERT_CHANNEL_ID", "").strip().strip("\"\'").strip()
SCAN_INTERVAL_SECONDS = max(60, int(os.getenv("SCAN_INTERVAL_SECONDS", "120")))
MAX_SCAN_SYMBOLS = max(5, min(60, int(os.getenv("MAX_SCAN_SYMBOLS", "25"))))
MIN_SIGNAL_SCORE = max(50, min(100, int(os.getenv("MIN_SIGNAL_SCORE", "75"))))
MIN_TURNOVER_24H = max(1_000_000, float(os.getenv("MIN_TURNOVER_24H", "5000000")))
SCAN_TIMEFRAMES = tuple(x.strip() for x in os.getenv("SCAN_TIMEFRAMES", "5,15,30").split(",") if x.strip() in INTERVALS)
SIGNAL_MAX_AGE_MINUTES = max(30, int(os.getenv("SIGNAL_MAX_AGE_MINUTES", "180")))
COOLDOWN_MINUTES = max(5, int(os.getenv("COOLDOWN_MINUTES", "30")))
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN belum diisi pada environment variables.")
if not SCAN_TIMEFRAMES:
    SCAN_TIMEFRAMES = ("5", "15", "30")

intents = discord.Intents.default()
market: Optional[BybitMarket] = None
db: Optional[Database] = None
scan_lock = asyncio.Lock()


class SignalBot(commands.Bot):
    async def setup_hook(self):
        # Slash command DISYNC PERTAMA, sebelum Redis/Bybit, supaya kegagalan
        # layanan lain tidak membuat command hilang dari Discord.
        await sync_commands()
        await start_services()
        if not scheduled_scan.is_running():
            scheduled_scan.change_interval(seconds=SCAN_INTERVAL_SECONDS)
            scheduled_scan.start()

    async def close(self):
        if scheduled_scan.is_running():
            scheduled_scan.cancel()
        if market:
            await market.close()
        if db:
            await db.close()
        await super().close()


bot = SignalBot(command_prefix="!", intents=intents)


async def sync_commands():
    if GUILD_ID and not GUILD_ID.isdigit():
        log.warning("GUILD_ID tidak valid (%r); harus angka ID server. Memakai sync global.", GUILD_ID)
    if GUILD_ID.isdigit():
        try:
            guild = discord.Object(id=int(GUILD_ID))
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            log.info("Slash command tersinkron ke guild %s: %s", GUILD_ID, ", ".join(c.name for c in synced))
            return
        except discord.Forbidden:
            log.error("Sync ke guild %s ditolak (Missing Access). Pastikan bot ada di server tsb dan di-invite dengan "
                      "scope 'applications.commands'. Mencoba sync global...", GUILD_ID)
        except Exception:
            log.exception("Gagal sync ke guild %s, mencoba sync global...", GUILD_ID)
    try:
        synced = await bot.tree.sync()
        log.info("Slash command tersinkron global: %s (bisa butuh beberapa menit untuk muncul)", ", ".join(c.name for c in synced))
    except Exception:
        log.exception("Gagal sync slash commands")


async def init_db():
    global db
    candidate = Database()
    await candidate.connect()  # jika gagal, db tetap None (tidak setengah terhubung)
    db = candidate
    log.info("Redis signal store connected")


async def start_services():
    """Idempotent: aman dipanggil berulang; dicoba lagi oleh scheduled_scan bila sebelumnya gagal."""
    global market
    if market is None:
        m = BybitMarket()
        await m.start()
        market = m
    if db is None:
        try:
            await init_db()
        except Exception:
            log.exception("Redis belum terhubung (cek REDIS_URL / reference variable Railway). Akan dicoba lagi.")


async def store_signal(s):
    return await db.add_signal(s, COOLDOWN_MINUTES)


def signal_embed(s):
    is_long = s["direction"] == "LONG"
    embed = discord.Embed(title=f'{s["symbol"]} · {s["direction"]}',
        description=f'**TF entry:** {s["timeframe"]}m · **Skor teknikal:** {s["score"]}/100',
        color=discord.Color.green() if is_long else discord.Color.red(), timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Entry referensi", value=fmt_price(s["entry"]), inline=True)
    embed.add_field(name="Stop Loss", value=fmt_price(s["sl"]), inline=True)
    embed.add_field(name="Take Profit", value=fmt_price(s["tp"]), inline=True)
    embed.add_field(name="Risk/Reward", value=f'1 : {s["rr"]:.2f}', inline=True)
    embed.add_field(name="Spread", value=f'{s.get("spread_pct", 0):.3f}%', inline=True)
    if s.get("funding_pct") is not None:
        embed.add_field(name="Funding rate", value=f'{s["funding_pct"]:.4f}%', inline=True)
    embed.add_field(name="Konfirmasi", value="\n".join(f"• {x}" for x in s["reasons"][:6]), inline=False)
    embed.set_footer(text="Sinyal simulasi/edukasi; bukan probabilitas profit. Tidak ada order otomatis.")
    return embed


async def scan_once(limit=MAX_SCAN_SYMBOLS):
    async with scan_lock:
        tickers = await market.get_tickers()
        candidates = []
        for t in tickers:
            symbol = t.get("symbol", "")
            if not symbol.endswith("USDT") or t.get("contractType", "LinearPerpetual") not in ("LinearPerpetual", ""):
                continue
            try:
                turnover = float(t.get("turnover24h", 0) or 0)
                bid, ask = float(t.get("bid1Price", 0) or 0), float(t.get("ask1Price", 0) or 0)
                spread = ((ask-bid)/((ask+bid)/2)*100) if bid > 0 and ask >= bid else 999
            except (ValueError, TypeError, ZeroDivisionError):
                continue
            if turnover >= MIN_TURNOVER_24H and spread <= 0.15:
                candidates.append(t)
        candidates.sort(key=lambda t: float(t.get("turnover24h", 0) or 0), reverse=True)
        # Bounded concurrency prevents flooding the public REST API.
        sem = asyncio.Semaphore(5)
        async def one(ticker, tf):
            async with sem:
                try:
                    return await analyze_symbol(market, ticker["symbol"], tf, ticker=ticker)
                except Exception as exc:
                    log.warning("Analisis %s TF%s gagal: %s", ticker.get("symbol"), tf, exc)
                    return None
        jobs = [one(t, tf) for t in candidates[:limit] for tf in SCAN_TIMEFRAMES]
        raw = await asyncio.gather(*jobs)
        results = [s for s in raw if s and s["score"] >= MIN_SIGNAL_SCORE]
        return sorted(results, key=lambda s: (s["score"], s.get("turnover24h", 0)), reverse=True)


async def evaluate_open_signals():
    """Resolve signal outcomes using closed candles; same-candle TP+SL is ambiguous."""
    rows = await db.get_open_signals(40)
    now = datetime.now(timezone.utc)
    for row in rows:
        sid, symbol, tf, direction = row["id"], row["symbol"], row["timeframe"], row["direction"]
        entry, sl, tp = row["entry"], row["stop_loss"], row["take_profit"]
        candle_start, created_at = row.get("candle_start"), row["created_at"]
        try:
            df = await market.get_klines(symbol, tf, 100)
            if df.empty:
                continue
            after = df[df["start"] > (candle_start or 0)]
            if after.empty:
                continue
            outcome = None; close_price = None; result_r = None
            for _, c in after.iterrows():
                high, low = float(c["high"]), float(c["low"])
                if direction == "LONG":
                    hit_sl, hit_tp = low <= sl, high >= tp
                    risk = entry - sl
                    if hit_sl and hit_tp:
                        outcome, close_price = "AMBIGUOUS", float(c["close"]); break
                    if hit_sl:
                        outcome, close_price, result_r = "SL", sl, -1.0; break
                    if hit_tp:
                        outcome, close_price, result_r = "TP", tp, (tp-entry)/risk; break
                else:
                    hit_sl, hit_tp = high >= sl, low <= tp
                    risk = sl - entry
                    if hit_sl and hit_tp:
                        outcome, close_price = "AMBIGUOUS", float(c["close"]); break
                    if hit_sl:
                        outcome, close_price, result_r = "SL", sl, -1.0; break
                    if hit_tp:
                        outcome, close_price, result_r = "TP", tp, (entry-tp)/risk; break
            age_minutes = (now - datetime.fromisoformat(created_at)).total_seconds()/60
            if outcome is None and age_minutes >= SIGNAL_MAX_AGE_MINUTES:
                outcome, close_price = "EXPIRED", float(after.iloc[-1]["close"])
                risk = (entry-sl) if direction == "LONG" else (sl-entry)
                result_r = ((close_price-entry)/risk if direction == "LONG" else (entry-close_price)/risk)
            if outcome:
                await db.update_signal(sid, status=outcome, outcome=outcome, closed_at=now.isoformat(),
                                       close_price=close_price, result_r=result_r)
        except Exception as exc:
            log.warning("Evaluator gagal untuk signal %s: %s", sid, exc)


@tasks.loop(seconds=60)
async def scheduled_scan():
    if not market or not db:
        await start_services()
        if not market or not db:
            return
    try:
        results = await scan_once()
        await evaluate_open_signals()
        channel_id = int(ALERT_CHANNEL_ID) if ALERT_CHANNEL_ID.isdigit() else None
        channel = bot.get_channel(channel_id) if channel_id else None
        if channel:
            sent = 0
            for s in results:
                if sent >= 5:
                    break
                if await store_signal(s):
                    await channel.send(embed=signal_embed(s))
                    sent += 1
    except Exception:
        log.exception("Scheduled scan error")


@scheduled_scan.before_loop
async def before_scheduled_scan():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    log.info("Login sebagai %s", bot.user)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    original = getattr(error, "original", error)
    log.error("Slash command /%s error: %r", getattr(interaction.command, "name", "?"), original, exc_info=original)
    if isinstance(original, AttributeError) and (market is None or db is None):
        msg = "Layanan (Redis/market data) belum siap. Cek Deploy Logs Railway, lalu coba lagi."
    else:
        msg = f"Terjadi error: {original}"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.tree.command(name="status", description="Cek status bot, konfigurasi dan market data")
async def status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        tickers = await market.get_tickers()
        await interaction.followup.send(
            f"✅ Bot aktif · Pair linear terdeteksi: **{len(tickers)}**\n"
            f"TF aktif: **{', '.join(SCAN_TIMEFRAMES)} menit** · Interval scan: **{SCAN_INTERVAL_SECONDS}s**\n"
            f"Batas pair per scan: **{MAX_SCAN_SYMBOLS}** · Sinyal minimum: **{MIN_SIGNAL_SCORE}/100**\n"
            "Data: Bybit public API · Mode: sinyal saja, tanpa eksekusi order.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"Market data belum tersedia: {e}", ephemeral=True)


@bot.tree.command(name="signal", description="Analisis pair pada TF 5m, 15m atau 30m")
@app_commands.describe(pair="Contoh BTCUSDT", timeframe="Timeframe entry")
@app_commands.choices(timeframe=[app_commands.Choice(name="5 menit", value="5"),
    app_commands.Choice(name="15 menit", value="15"), app_commands.Choice(name="30 menit", value="30")])
async def signal(interaction: discord.Interaction, pair: str, timeframe: app_commands.Choice[str]):
    await interaction.response.defer(thinking=True)
    symbol = pair.upper().replace("/", "").replace("-", "").strip()
    if not symbol.endswith("USDT"): symbol += "USDT"
    try:
        ticker = await market.get_ticker(symbol)
        s = await analyze_symbol(market, symbol, timeframe.value, ticker=ticker)
        if not s:
            await interaction.followup.send(f"{symbol}: setup tidak memenuhi konfirmasi/likuiditas pada TF {timeframe.value}m. **NO TRADE**.")
        else:
            await interaction.followup.send(embed=signal_embed(s))
    except Exception as e:
        log.exception("Signal command error")
        await interaction.followup.send(f"Gagal menganalisis {symbol}: {e}")


@bot.tree.command(name="scanner", description="Cari kandidat sinyal terbaik")
@app_commands.describe(jumlah="Jumlah hasil (1-10)")
async def scanner(interaction: discord.Interaction, jumlah: app_commands.Range[int, 1, 10] = 5):
    await interaction.response.defer(thinking=True)
    try:
        results = await scan_once()
        if not results:
            await interaction.followup.send("Tidak ada setup yang lolos semua filter. **NO TRADE**."); return
        embed = discord.Embed(title="Bybit USDT Perpetual · Short-term Scanner", color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        for s in results[:jumlah]:
            embed.add_field(name=f'{s["symbol"]} · {s["direction"]} · {s["score"]}/100',
                value=f'TF {s["timeframe"]}m | Entry {fmt_price(s["entry"])} | SL {fmt_price(s["sl"])} | TP {fmt_price(s["tp"])} | RR 1:{s["rr"]:.2f} | spread {s["spread_pct"]:.3f}%', inline=False)
        embed.set_footer(text="Skor teknikal bukan probabilitas profit; level referensi, bukan jaminan fill.")
        await interaction.followup.send(embed=embed)
    except Exception as e:
        log.exception("Scanner error"); await interaction.followup.send(f"Scanner gagal: {e}")


@bot.tree.command(name="market", description="Lihat harga dan likuiditas pair")
async def market_cmd(interaction: discord.Interaction, pair: str):
    await interaction.response.defer(thinking=True)
    symbol = pair.upper().replace("/", "").replace("-", "").strip()
    if not symbol.endswith("USDT"): symbol += "USDT"
    try:
        t = await market.get_ticker(symbol)
        bid, ask = float(t.get("bid1Price", 0) or 0), float(t.get("ask1Price", 0) or 0)
        spread = ((ask-bid)/((ask+bid)/2)*100) if bid > 0 and ask >= bid else 0
        await interaction.followup.send(f"**{symbol}**\nLast: `{t.get('lastPrice','n/a')}`\n24h change: `{float(t.get('price24hPcnt',0) or 0)*100:.2f}%`\nTurnover 24h: `{float(t.get('turnover24h',0) or 0):,.0f}` USDT\nSpread estimasi: `{spread:.3f}%`\nFunding rate: `{t.get('fundingRate','n/a')}`")
    except Exception as e: await interaction.followup.send(f"Gagal mengambil data {symbol}: {e}")


@bot.tree.command(name="history", description="Lihat 10 sinyal terakhir")
async def history(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = await db.recent_signals(10)
    if not rows:
        await interaction.followup.send("Belum ada sinyal tersimpan.", ephemeral=True); return
    lines = [f'`{r["symbol"]}` {r["direction"]} TF {r["timeframe"]}m · {r["score"]}/100 · Entry {fmt_price(r["entry"])} · SL {fmt_price(r["stop_loss"])} · TP {fmt_price(r["take_profit"])} · **{r.get("outcome") or "OPEN"}**' for r in rows]
    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(name="performance", description="Statistik hasil evaluasi sinyal")
async def performance(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    total, tp, sl, ambiguous, expired, sum_r = await db.performance()
    resolved = (tp or 0) + (sl or 0)
    winrate = (tp or 0) / resolved * 100 if resolved else 0
    await interaction.followup.send(f"**Statistik evaluator**\nTotal sinyal: **{total or 0}**\nTP: **{tp or 0}** · SL: **{sl or 0}** · Win rate TP/(TP+SL): **{winrate:.1f}%**\nAmbiguous (TP & SL candle sama): **{ambiguous or 0}** · Expired: **{expired or 0}**\nAkumulasi hasil: **{sum_r or 0:.2f}R** sebelum fee, funding, spread/slippage.\n\nIni evaluasi historis sederhana, bukan backtest tervalidasi atau prediksi profit.", ephemeral=True)


@bot.tree.command(name="evaluate", description="Jalankan evaluasi TP/SL sekarang")
async def evaluate_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await evaluate_open_signals()
    await interaction.followup.send("Evaluasi sinyal OPEN selesai. Cek `/performance` dan `/history`.", ephemeral=True)


@bot.tree.command(name="realtime", description="Cek koneksi feed harga real-time Bybit")
async def realtime_cmd(interaction: discord.Interaction):
    state = market.realtime_status() if market else {"connected": False, "cached_symbols": 0, "last_message_age": None}
    age = f"{state['last_message_age']} detik lalu" if state["last_message_age"] is not None else "belum menerima data"
    await interaction.response.send_message(
        f"**Bybit WebSocket ticker**\nStatus: **{'TERHUBUNG' if state['connected'] else 'MENYAMBUNG / REST fallback'}**\n"
        f"Pair dalam cache: **{state['cached_symbols']}**\nUpdate terakhir: **{age}**\n"
        "Analisis sinyal tetap memakai candle tertutup dari REST agar level entry/SL/TP tidak dihitung dari candle berjalan.",
        ephemeral=True
    )


async def main():
    async with bot:
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
