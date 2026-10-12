import io
import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from database import Database
import discord
from discord import app_commands
from discord.ext import commands, tasks

from market import BybitMarket, analyze_symbol, fmt_price, INTERVALS, FEE_PCT, SLIPPAGE_PCT, MAX_SPREAD_PCT, SIGNAL_STRATEGY
from chart import render_chart, CHART_ENABLED

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("signalbot")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
GUILD_ID = os.getenv("GUILD_ID", "").strip().strip("\"\'").strip()
ALERT_CHANNEL_ID = os.getenv("ALERT_CHANNEL_ID", "").strip().strip("\"\'").strip()
SCAN_INTERVAL_SECONDS = max(60, int(os.getenv("SCAN_INTERVAL_SECONDS", "120")))
MAX_SCAN_SYMBOLS = max(5, min(60, int(os.getenv("MAX_SCAN_SYMBOLS", "25"))))
MIN_SIGNAL_SCORE = max(50, min(100, int(os.getenv("MIN_SIGNAL_SCORE", "70"))))
MIN_TURNOVER_24H = max(1_000_000, float(os.getenv("MIN_TURNOVER_24H", "5000000")))
SCAN_TIMEFRAMES = tuple(x.strip() for x in os.getenv("SCAN_TIMEFRAMES", "5,15").split(",") if x.strip() in INTERVALS)
SIGNAL_MAX_AGE_MINUTES = max(30, int(os.getenv("SIGNAL_MAX_AGE_MINUTES", "180")))
COOLDOWN_MINUTES = max(5, int(os.getenv("COOLDOWN_MINUTES", "30")))
# WATCH = indikasi awal long/short (tren 30m searah, skor >= MIN_WATCH_SCORE) walau belum jadi SIGNAL penuh.
MIN_WATCH_SCORE = max(40, min(MIN_SIGNAL_SCORE, int(os.getenv("MIN_WATCH_SCORE", "55"))))
WATCH_COOLDOWN_MINUTES = max(5, int(os.getenv("WATCH_COOLDOWN_MINUTES", "30")))
WATCH_MAX_PER_SCAN = max(1, min(10, int(os.getenv("WATCH_MAX_PER_SCAN", "5"))))
WATCH_CHART = os.getenv("WATCH_CHART", "0").strip().lower() in ("1", "true", "yes", "on")  # chart di embed WATCH (default mati: hemat halaman)
FOLLOW_MAX_DRIFT_R = float(os.getenv("FOLLOW_MAX_DRIFT_R", "0.5"))  # tolak ikut jika harga sudah +X R menuju TP (terlambat)
ALERT_ROLE_ID = os.getenv("ALERT_ROLE_ID", "").strip().strip("\"\'").strip()  # opsional: role yang di-ping
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN belum diisi pada environment variables.")
if not SCAN_TIMEFRAMES:
    SCAN_TIMEFRAMES = ("5", "15")

intents = discord.Intents.default()
market: Optional[BybitMarket] = None
db: Optional[Database] = None
scan_lock = asyncio.Lock()


class SignalBot(commands.Bot):
    async def setup_hook(self):
        # Slash command DISYNC PERTAMA, sebelum Upstash Redis/Bybit, supaya kegagalan
        # layanan lain tidak membuat command hilang dari Discord.
        self.add_dynamic_items(FollowButton, HistoryButton, HistoryFilterButton)  # tombol persisten: tetap hidup setelah restart/redeploy
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
    log.info("Upstash Redis signal store connected")


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
            log.exception("Upstash Redis belum terhubung (cek UPSTASH_REDIS_REST_URL dan UPSTASH_REDIS_REST_TOKEN). Akan dicoba lagi.")


# ============================================================ helper tampilan
OUTCOME_ICON = {"TP": "✅", "SL": "❌", "AMBIGUOUS": "⚠️", "EXPIRED": "⌛", "OPEN": "⏳"}
OUTCOME_LABEL = {"TP": "TP", "SL": "SL", "AMBIGUOUS": "Ambigu (dihitung loss)", "EXPIRED": "Kedaluwarsa"}


def _pct(a, b):
    return abs(a - b) / b * 100 if b else 0.0


def _ts(iso):
    try:
        return int(datetime.fromisoformat(iso).timestamp())
    except (TypeError, ValueError):
        return 0


def classify(s):
    """SIGNAL = pullback + candle konfirmasi sudah close (stage TRIGGER); WATCH = pullback aktif, belum konfirmasi."""
    if s.get("stage") == "TRIGGER" and s["score"] >= MIN_SIGNAL_SCORE:
        return "SIGNAL"
    if s["score"] >= MIN_WATCH_SCORE:
        return "WATCH"
    return None


def footer_text(sid, status, followers):
    left = f"#{sid} · " if sid else "Cek manual · "
    return f"{left}{status} · 👥 {followers} ikut · simulasi, bukan nasihat finansial" if sid else \
        f"{left}tanpa tombol follow · simulasi, bukan nasihat finansial"


def signal_embed(s, sid=None, followers=0):
    """Embed ringkas: 3 baris deskripsi, tanpa field, supaya tidak memanjang ke bawah."""
    is_long = s["direction"] == "LONG"
    tier = s.get("tier") or classify(s)
    dot = "🟢" if is_long else "🔴"
    tag = {"SIGNAL": "", "WATCH": " · 👀 WATCH"}.get(tier, " · ⚪ lemah")
    risk, reward = _pct(s["entry"], s["sl"]), _pct(s["tp"], s["entry"])
    line2 = f'RR 1:{s["rr"]:g} · Skor {s["score"]} · {s.get("strategy", SIGNAL_STRATEGY).upper()} · Biaya ≈{s.get("fee_r", 0):.2f}R · Spread {s.get("spread_pct", 0):.3f}%'
    if s.get("funding_pct") is not None:
        line2 += f' · Fund {s["funding_pct"]:+.3f}%'
    desc = (f'**Entry** `{fmt_price(s["entry"])}` · **SL** `{fmt_price(s["sl"])}` (-{risk:.2f}%) · '
            f'**TP** `{fmt_price(s["tp"])}` (+{reward:.2f}%)\n{line2}\n' + " · ".join(s["reasons"][:4]))
    embed = discord.Embed(title=f'{dot} {s["direction"]} · {s["symbol"]} · {s["timeframe"]}m{tag}', description=desc,
                          color=discord.Color.green() if is_long else discord.Color.red(), timestamp=datetime.now(timezone.utc))
    embed.set_footer(text=footer_text(sid, "⏳ Berjalan", followers))
    return embed


def _line(s, with_tier=False):
    icon = "🟢" if s["direction"] == "LONG" else "🔴"
    pre = ("✅ " if s.get("tier") == "SIGNAL" else "👀 ") if with_tier else ""
    return (f'{pre}{icon} **{s["symbol"]}** {s["direction"][0]}·{s["timeframe"]}m · {s["score"]} · '
            f'`{fmt_price(s["entry"])}` SL `{fmt_price(s["sl"])}` TP `{fmt_price(s["tp"])}`')


def watch_embed(items):
    desc = "Pullback ke EMA20 terdeteksi, **belum entry** — tunggu candle konfirmasi.\n" + "\n".join(_line(s) for s in items)
    embed = discord.Embed(title="👀 WATCH · pullback aktif", description=desc,
                          color=discord.Color.gold(), timestamp=datetime.now(timezone.utc))
    embed.set_footer(text="Level = referensi. Skor bukan probabilitas profit. Tanpa order otomatis.")
    return embed


async def chart_kwargs(s, embed, force=True):
    """Kembalikan kwargs send(): embed + file chart. Jika render gagal, kirim embed saja."""
    if not CHART_ENABLED or not force or s.get("chart_df") is None:
        return {"embed": embed}
    try:
        png = await asyncio.to_thread(render_chart, s)
    except Exception:
        log.exception("Render chart gagal untuk %s", s.get("symbol"))
        return {"embed": embed}
    fname = f'{s["symbol"]}_{s["timeframe"]}m.png'
    embed.set_image(url=f"attachment://{fname}")
    return {"embed": embed, "file": discord.File(io.BytesIO(png), filename=fname)}


# ============================================================ Follow Trade (tombol)
class FollowButton(discord.ui.DynamicItem[discord.ui.Button], template=r"follow:(?P<sid>\d+)"):
    def __init__(self, sid: int, disabled: bool = False, label: str = "Ikut Trade"):
        super().__init__(discord.ui.Button(label=label, emoji="✅", style=discord.ButtonStyle.success,
                                           custom_id=f"follow:{sid}", disabled=disabled))
        self.sid = int(sid)

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(int(match["sid"]))

    async def callback(self, interaction: discord.Interaction):
        await handle_follow(interaction, self.sid)


class HistoryButton(discord.ui.DynamicItem[discord.ui.Button], template=r"fhist"):
    def __init__(self):
        super().__init__(discord.ui.Button(label="Riwayat Saya", emoji="📋", style=discord.ButtonStyle.secondary,
                                           custom_id="fhist"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls()

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await interaction.followup.send(embed=await build_history_embed(interaction.user, "all"), view=make_history_view(), ephemeral=True)


class HistoryFilterButton(discord.ui.DynamicItem[discord.ui.Button], template=r"fhist:(?P<mode>all|open|closed)"):
    def __init__(self, mode: str):
        labels = {"all": ("Semua", "📋"), "open": ("Berjalan", "⏳"), "closed": ("Selesai", "✅")}
        label, emoji = labels[mode]
        super().__init__(discord.ui.Button(label=label, emoji=emoji, style=discord.ButtonStyle.secondary,
                                           custom_id=f"fhist:{mode}"))
        self.mode = mode

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):
        return cls(match["mode"])

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await interaction.followup.send(embed=await build_history_embed(interaction.user, self.mode),
                                         view=make_history_view(), ephemeral=True)


def make_history_view():
    view = discord.ui.View(timeout=None)
    for mode in ("all", "open", "closed"):
        view.add_item(HistoryFilterButton(mode))
    return view


class ResetConfirmView(discord.ui.View):
    """Two-step destructive reset; only the administrator who invoked it can confirm."""
    def __init__(self, requester_id: int):
        super().__init__(timeout=60)
        self.requester_id = requester_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Hanya admin yang memulai reset yang boleh memakai tombol ini.", ephemeral=True)
            return False
        if not interaction.guild or not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Reset hanya bisa dilakukan administrator server.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="RESET SEMUA DATA", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        for child in self.children:
            child.disabled = True
        try:
            if db is None:
                await interaction.followup.send("Reset dibatalkan: Upstash belum terhubung.", ephemeral=True)
                return
            deleted_keys = await db.reset_all_data()
            deleted_messages = 0
            channel = await get_alert_channel()
            if channel is not None and hasattr(channel, "history"):
                try:
                    async for message in channel.history(limit=None, oldest_first=True):
                        if bot.user and message.author.id == bot.user.id:
                            try:
                                await message.delete()
                                deleted_messages += 1
                            except discord.NotFound:
                                pass
                            except discord.Forbidden:
                                log.warning("Tidak punya izin menghapus pesan bot di channel alert")
                                break
                except (discord.Forbidden, discord.HTTPException) as exc:
                    log.warning("Gagal membersihkan pesan Discord bot: %s", exc)
            await interaction.edit_original_response(content=(
                "✅ **Reset selesai.**\n"
                f"• Key Upstash dalam namespace `{db.prefix}:*` dihapus: **{deleted_keys}**\n"
                f"• Pesan milik bot yang dihapus dari channel alert: **{deleted_messages}**\n"
                "Token, Environment Variables, konfigurasi Railway, dan pesan pengguna tidak dihapus."
            ), view=self)
        except Exception as exc:
            log.exception("Reset command gagal")
            await interaction.followup.send(f"❌ Reset gagal atau mungkin hanya selesai sebagian: `{str(exc)[:250]}`. Periksa log Railway dan Upstash sebelum mencoba lagi.", ephemeral=True)

    @discord.ui.button(label="Batal", style=discord.ButtonStyle.secondary, emoji="✖️")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Reset dibatalkan. Tidak ada data yang dihapus.", view=self)


def make_view(sid, closed=False):
    view = discord.ui.View(timeout=None)
    view.add_item(FollowButton(sid, disabled=closed, label="Selesai" if closed else "Ikut Trade"))
    view.add_item(HistoryButton())
    return view


async def set_message_footer(message, text, view=None, color=None):
    """Ubah footer (status/jumlah follower) pada pesan sinyal; gagal diam-diam jika pesan tak bisa diedit."""
    try:
        if message is None or not message.embeds:
            return
        emb = message.embeds[0]
        emb.set_footer(text=text)
        if color is not None:
            emb.color = color
        kwargs = {"embed": emb}
        if view is not None:
            kwargs["view"] = view
        await message.edit(**kwargs)
    except discord.HTTPException as exc:
        log.warning("Gagal update pesan sinyal: %s", exc)


async def handle_follow(interaction: discord.Interaction, sid: int):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if db is None or market is None:
        await interaction.followup.send("Layanan belum siap, coba lagi sebentar.", ephemeral=True); return
    row = await db.get_signal(sid)
    if not row:
        await interaction.followup.send("Sinyal tidak ditemukan.", ephemeral=True); return
    if row.get("status") != "OPEN":
        await interaction.followup.send(f'{OUTCOME_ICON.get(row.get("outcome"), "")} Sinyal #{sid} sudah selesai '
                                        f'({OUTCOME_LABEL.get(row.get("outcome"), row.get("status"))}); tidak bisa diikuti.', ephemeral=True); return
    uid = interaction.user.id
    prev = await db.get_follow(sid, uid)
    if prev:
        await interaction.followup.send(f'Kamu sudah ikut #{sid} @ `{fmt_price(prev["follow_price"])}`. Hasil masuk `/riwayat` saat TP/SL.', ephemeral=True); return
    entry, sl, tp, long = row["entry"], row["stop_loss"], row["take_profit"], row["direction"] == "LONG"
    try:
        price = float((await market.get_ticker(row["symbol"])).get("lastPrice") or 0)
    except Exception:
        price = 0.0
    if price <= 0:
        price = entry
    crossed = (price <= sl or price >= tp) if long else (price >= sl or price <= tp)
    if crossed:
        await interaction.followup.send("Harga sudah menyentuh level TP/SL; sinyal akan segera ditutup. Tidak bisa ikut.", ephemeral=True); return
    risk = abs(entry - sl) or 1e-12
    drift_r = ((price - entry) if long else (entry - price)) / risk
    if drift_r >= FOLLOW_MAX_DRIFT_R:
        await interaction.followup.send(f"Terlambat: harga sudah {drift_r:+.2f}R menuju TP dari entry sinyal. Tunggu sinyal berikutnya.", ephemeral=True); return
    rec = await db.add_follow(sid, uid, interaction.user.display_name, row, price)
    if rec is None:
        await interaction.followup.send("Kamu sudah ikut sinyal ini.", ephemeral=True); return
    n = await db.count_followers(sid)
    note = f"\nHarga sudah bergeser {drift_r:+.2f}R dari entry sinyal; R-mu dihitung dari harga ini." if abs(drift_r) >= 0.1 else ""
    await interaction.followup.send(
        f'✅ Ikut **#{sid} {row["symbol"]} {row["direction"]} {row["timeframe"]}m**\n'
        f'Masuk `{fmt_price(price)}` · SL `{fmt_price(sl)}` · TP `{fmt_price(tp)}`{note}\n'
        f'PnL/ROI berjalan bisa dicek lewat `/trade`; hasil akhir dicatat otomatis saat TP/SL → `/riwayat`', ephemeral=True)
    await set_message_footer(interaction.message, footer_text(sid, "⏳ Berjalan", n))


async def live_follow_metrics(rec):
    """Snapshot PnL/ROI harga untuk follow OPEN; ROI 1x karena leverage/modal tidak dikonfigurasi."""
    if market is None:
        return None
    try:
        ticker = await market.get_ticker(rec["symbol"])
        price = float(ticker.get("lastPrice") or 0)
        entry = float(rec.get("follow_price") or 0)
        if price <= 0 or entry <= 0:
            return None
        move_pct = ((price - entry) / entry * 100) if rec.get("direction") == "LONG" else ((entry - price) / entry * 100)
        cost_pct = rec.get("total_cost_pct")
        if cost_pct is None:
            cost_pct = (float(rec.get("fee_cost_pct") or FEE_PCT) +
                        float(rec.get("spread_pct") or 0.0) +
                        float(rec.get("slippage_pct") or SLIPPAGE_PCT) +
                        float(rec.get("funding_cost_pct") or 0.0))
        cost_pct = max(0.0, float(cost_pct or 0.0))
        return {"price": price, "move_pct": move_pct, "net_pct": move_pct - cost_pct,
                "cost_pct": cost_pct, "age_ts": _ts(rec.get("followed_at"))}
    except Exception as exc:
        log.debug("Gagal mengambil PnL berjalan %s: %s", rec.get("symbol"), exc)
        return None


async def build_live_trades_embed(user):
    """Build text fallback embed plus a rendered Modern Card PNG dashboard."""
    recs = await db.user_follows(user.id, 200)
    open_recs = [r for r in recs if r.get("status") == "OPEN"][:20]
    if not open_recs:
        embed = discord.Embed(
            title="TRADE DASHBOARD",
            description=(
                "━━━━━━━━━━━━━━━━━━━━\n"
                "**Tidak ada posisi aktif**\n"
                "Mulai dengan menekan **Ikut Trade** pada sinyal yang ingin kamu pantau."
            ),
            color=discord.Color.from_rgb(35, 42, 55),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_author(name="PREMIUM TRADING · PORTFOLIO")
        embed.add_field(name="STATUS", value="`● STANDBY`", inline=True)
        embed.add_field(name="OPEN POSITIONS", value="`00`", inline=True)
        embed.add_field(name="MODE", value="`PAPER`", inline=True)
        embed.set_footer(text="BYBIT MARKET SNAPSHOT  •  Paper trading only — no real orders")
        try:
            png = render_modern_card_dashboard([], 0.0, 0.0, 0.0, user.display_name)
        except Exception:
            log.exception("Gagal merender gambar dashboard kosong; memakai fallback teks")
            png = None
        return embed, png

    net_values = []
    image_positions = []
    gross_values = []
    fields = []
    for rec in open_recs:
        live = await live_follow_metrics(rec)
        side = "LONG" if rec.get("direction") == "LONG" else "SHORT"
        age_ts = _ts(rec.get("followed_at"))
        age = f'<t:{age_ts}:R>' if age_ts else "n/a"
        entry = float(rec.get("follow_price") or 0)
        sl = float(rec.get("sl") or 0)
        tp = float(rec.get("tp") or 0)
        side_icon = "🟢" if side == "LONG" else "🔴"
        if live:
            pnl = live["move_pct"]
            net = live["net_pct"]
            gross_values.append(pnl)
            net_values.append(net)
            image_positions.append({
                "symbol": rec.get("symbol", "UNKNOWN"), "side": side,
                "timeframe": rec.get("timeframe", "?"), "net": net, "gross": pnl,
                "entry": entry, "last": live["price"], "sl": sl, "tp": tp,
                "age_ts": age_ts, "age": age,
            })
            net_icon = "🟩" if net >= 0 else "🟥"
            # Keep each position as its own full-width card-like embed field for mobile.
            # Avoid custom emoji dependencies; compact metadata line works on every server.
            value = (
                f"{net_icon} **NET PNL**  `{net:+.2f}%`  ·  **GROSS**  `{pnl:+.2f}%`\n"
                f"`ENTRY` **{fmt_price(entry)}**  →  `LAST` **{fmt_price(live['price'])}**\n"
                f"🛡️ `SL` {fmt_price(sl)}  ·  🎯 `TP` {fmt_price(tp)}\n"
                f"<t:{age_ts}:R>" if age_ts else
                f"{net_icon} **NET PNL**  `{net:+.2f}%`  ·  **GROSS**  `{pnl:+.2f}%`\n"
                f"`ENTRY` **{fmt_price(entry)}**  →  `LAST` **{fmt_price(live['price'])}**\n"
                f"🛡️ `SL` {fmt_price(sl)}  ·  🎯 `TP` {fmt_price(tp)}\nDiikuti: waktu tidak tersedia"
            )
        else:
            value = (
                "⚪ **DATA HARGA BELUM TERSEDIA**\n"
                f"`ENTRY` **{fmt_price(entry)}**\n"
                f"🛡️ `SL` {fmt_price(sl)}  ·  🎯 `TP` {fmt_price(tp)}\n"
                f"Diikuti: {age}"
            )
        fields.append((
            f"{side_icon}  {rec['symbol']}  /  {side}  ·  {rec['timeframe']}M  ·  OPEN",
            value[:1024],
        ))

    total_net = sum(net_values) if net_values else 0.0
    total_gross = sum(gross_values) if gross_values else 0.0
    avg_net = total_net / len(net_values) if net_values else 0.0
    color = (discord.Color.from_rgb(28, 185, 105) if total_net >= 0
             else discord.Color.from_rgb(230, 72, 82)) if net_values else discord.Color.from_rgb(67, 80, 102)
    net_label = f"`{total_net:+.2f}%`" if net_values else "`—`"
    gross_label = f"`{total_gross:+.2f}%`" if gross_values else "`—`"
    avg_label = f"`{avg_net:+.2f}%`" if net_values else "`—`"
    embed = discord.Embed(
        title="PORTFOLIO OVERVIEW",
        description=(
            "`LIVE SNAPSHOT`  ·  BYBIT PERPETUALS\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Net PNL merupakan estimasi setelah biaya; nilai dihitung dari posisi yang sedang kamu ikuti."
        ),
        color=color,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(name="PREMIUM TRADING DASHBOARD", icon_url="https://www.bybit.com/favicon.ico")
    embed.add_field(name="NET PNL  ·  EST.", value=f"# {net_label}", inline=True)
    embed.add_field(name="GROSS PNL", value=f"↗ {gross_label}", inline=True)
    embed.add_field(name="AVG NET / TRADE", value=avg_label, inline=True)
    embed.add_field(name="ACTIVE POSITIONS", value=f"`{len(open_recs):02d}`", inline=True)
    embed.add_field(name="PRICING", value="`MARKET SNAPSHOT`", inline=True)
    embed.add_field(name="ACCOUNT MODE", value="`PAPER · 1x ROI`", inline=True)
    embed.add_field(name="━━━━━━━━  OPEN POSITIONS  ━━━━━━━━", value="Rincian posisi · harga diperbarui saat `/trade` dipanggil", inline=False)
    for title, value in fields:
        embed.add_field(name=title, value=value, inline=False)
    embed.set_footer(text="ROI estimasi 1x tanpa leverage · Fee, spread, slippage & funding diestimasi · Bukan order nyata")
    try:
        png = render_modern_card_dashboard(image_positions, total_net, total_gross, avg_net, user.display_name)
    except Exception:
        log.exception("Gagal merender gambar dashboard; memakai fallback embed teks")
        png = None
    return embed, png


def render_modern_card_dashboard(positions, total_net, total_gross, avg_net, display_name):
    """Render a mobile-first Modern Card PNG. All values come from the live /trade snapshot."""
    from PIL import Image, ImageDraw, ImageFont
    import math

    width = 1100
    pad = 54
    card_h = 250
    gap = 24
    height = 390 + (card_h + gap) * max(1, min(len(positions), 12)) + 70
    height = min(height, 390 + (card_h + gap) * 12 + 70)
    im = Image.new("RGB", (width, height), (7, 14, 27))
    d = ImageDraw.Draw(im)
    try:
        regular = ImageFont.truetype("DejaVuSans.ttf", 25)
        small = ImageFont.truetype("DejaVuSans.ttf", 20)
        medium = ImageFont.truetype("DejaVuSans-Bold.ttf", 26)
        large = ImageFont.truetype("DejaVuSans-Bold.ttf", 54)
        title_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 34)
        metric_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 32)
    except OSError:
        regular = small = medium = large = title_font = metric_font = ImageFont.load_default()

    bg_card = (14, 27, 46); border = (31, 57, 80); white = (239, 246, 255)
    muted = (148, 169, 194); green = (37, 230, 167); red = (255, 76, 108)
    d.rounded_rectangle((pad, 34, width-pad, 140), radius=28, fill=(12, 31, 48), outline=(20, 135, 119), width=2)
    d.text((pad+28, 55), "JUSTJINAPPS  /  TRADING DASHBOARD", font=title_font, fill=white)
    d.text((pad+30, 105), f"{display_name}  ·  BYBIT PERPETUALS  ·  PAPER TRADING", font=small, fill=muted)

    top, bottom = 164, 345
    box_gap = 18
    box_w = (width - 2*pad - 2*box_gap)//3
    metrics = [("NET PNL · EST.", total_net, green if total_net >= 0 else red),
               ("GROSS PNL", total_gross, green if total_gross >= 0 else red),
               ("AVG NET / TRADE", avg_net, green if avg_net >= 0 else red)]
    for i, (label, val, col) in enumerate(metrics):
        x = pad + i*(box_w+box_gap)
        d.rounded_rectangle((x, top, x+box_w, bottom), radius=24, fill=bg_card, outline=border, width=2)
        d.text((x+22, top+24), label, font=small, fill=muted)
        d.text((x+22, top+72), f"{val:+.2f}%", font=metric_font, fill=col)
        d.text((x+22, top+126), "LIVE SNAPSHOT", font=small, fill=(93, 119, 149))

    y = 375
    d.text((pad, y), f"OPEN POSITIONS  ·  {len(positions):02d}", font=title_font, fill=white)
    y += 60
    if not positions:
        d.rounded_rectangle((pad, y, width-pad, y+150), radius=24, fill=bg_card, outline=border, width=2)
        d.text((pad+28, y+35), "NO ACTIVE POSITIONS", font=medium, fill=muted)
        d.text((pad+28, y+83), "Join a signal to see live PNL here.", font=regular, fill=white)
        y += 150
    for pos in positions[:12]:
        if y + card_h > height - 50:
            break
        col = green if pos["net"] >= 0 else red
        d.rounded_rectangle((pad, y, width-pad, y+card_h), radius=24, fill=bg_card, outline=border, width=2)
        d.rounded_rectangle((pad, y, pad+10, y+card_h), radius=5, fill=col)
        d.text((pad+30, y+22), f'{pos["symbol"]}  ·  {pos["side"]}  ·  {pos["timeframe"]}M', font=medium, fill=white)
        d.rounded_rectangle((width-pad-145, y+18, width-pad-30, y+58), radius=18, fill=(14, 74, 63) if pos["net"] >= 0 else (86, 28, 48))
        d.text((width-pad-128, y+25), "OPEN", font=small, fill=green if pos["net"] >= 0 else red)
        d.text((pad+30, y+76), f'{pos["net"]:+.2f}% NET', font=large, fill=col)
        d.text((pad+430, y+85), f'GROSS  {pos["gross"]:+.2f}%', font=medium, fill=muted)
        d.line((pad+28, y+145, width-pad-28, y+145), fill=border, width=2)
        d.text((pad+30, y+163), f'ENTRY  {pos["entry"]:.10g}   →   LAST  {pos["last"]:.10g}', font=small, fill=white)
        d.text((pad+30, y+202), f'SL  {pos["sl"]:.10g}      TP  {pos["tp"]:.10g}      {pos.get("age", "time n/a")}', font=small, fill=muted)
        y += card_h + gap
    d.text((pad, height-43), "ROI estimate at 1x · Fees/slippage estimated · No real orders", font=small, fill=muted)
    output = io.BytesIO()
    im.save(output, format="PNG", optimize=True)
    return output.getvalue()


async def build_history_embed(user, mode="all"):
    recs = await db.user_follows(user.id, 200)
    open_ = [r for r in recs if r["status"] == "OPEN"]
    closed = [r for r in recs if r["status"] != "OPEN"]
    wins = sum((r.get("net_r") or 0) > 0 for r in closed)
    net = sum(r.get("net_r") or 0 for r in closed)
    wr = wins / len(closed) * 100 if closed else 0
    gains = sum(max(0.0, float(r.get("net_r") or 0)) for r in closed)
    losses = abs(sum(min(0.0, float(r.get("net_r") or 0)) for r in closed))
    pf = gains / losses if losses else (float("inf") if gains else 0.0)
    avg_net = net / len(closed) if closed else 0.0
    shown = {"open": open_, "closed": closed}.get(mode, recs)[:12]
    lines = []
    for r in shown:
        side = r["direction"][0]
        if r["status"] == "OPEN":
            live = await live_follow_metrics(r)
            if live:
                lines.append(f'⏳ **{r["symbol"]}** {side}·{r["timeframe"]}m · PnL **{live["move_pct"]:+.2f}%** · net est **{live["net_pct"]:+.2f}%** · #{r["signal_id"]} · <t:{_ts(r["followed_at"])}:R>')
            else:
                lines.append(f'⏳ **{r["symbol"]}** {side}·{r["timeframe"]}m · PnL n/a · #{r["signal_id"]} @ `{fmt_price(r["follow_price"])}` · <t:{_ts(r["followed_at"])}:R>')
        else:
            lines.append(f'{OUTCOME_ICON.get(r["status"], "")} **{r["symbol"]}** {side}·{r["timeframe"]}m · {r["status"]} · '
                         f'**{(r.get("net_r") or 0):+.2f}R net** · keluar `{fmt_price(r.get("close_price") or 0)}` · {r.get("strategy", "-")} · <t:{_ts(r["closed_at"])}:R>')
    pf_text = "∞" if pf == float("inf") else f"{pf:.2f}"
    head = (f'Selesai **{len(closed)}** · Win **{wins}** · Loss **{len(closed) - wins}** · WR **{wr:.1f}%** · Net **{net:+.2f}R**'
            f' · Avg **{avg_net:+.2f}R** · PF **{pf_text}** · Berjalan **{len(open_)}**')
    desc = head + "\n\n" + ("\n".join(lines) if lines else "_Belum ada trade. Tekan **Ikut Trade** pada sinyal baru._")
    embed = discord.Embed(title=f"📋 Riwayat Follow · {user.display_name}", description=desc[:4000], color=discord.Color.blurple())
    embed.set_footer(text=f"Net = estimasi fee + spread + slippage + funding (jika diperkirakan membayar). Simulasi, bukan order nyata. Menampilkan {len(shown)} terbaru.")
    return embed


async def settle_follows(sig, outcome, close_price, closed_at):
    """Hitung hasil tiap follower dari harga masuk mereka sendiri. Mengembalikan daftar follower yang dituntaskan."""
    sid, long = sig["id"], sig["direction"] == "LONG"
    sl, tp = sig["stop_loss"], sig["take_profit"]
    done = []
    for f in await db.followers_of(sid):
        if f.get("status") != "OPEN":
            continue
        fp = f["follow_price"]
        # R dihitung dari entry follower ke SL, bukan dari entry sinyal. Ini menjaga
        # hasil konsisten saat pengguna follow pada harga berbeda.
        risk = abs(fp - sl) or 1e-12
        exit_price = sl if outcome in ("SL", "AMBIGUOUS") else tp if outcome == "TP" else close_price
        gross = ((exit_price - fp) if long else (fp - exit_price)) / risk
        risk_pct = risk / fp * 100 if fp else 0.0
        # Estimasi round-trip: fee taker + spread saat sinyal. Bukan simulasi slippage sempurna.
        cost_pct = float(sig.get("total_cost_pct") if sig.get("total_cost_pct") is not None else (FEE_PCT + float(sig.get("spread_pct") or 0.0) + SLIPPAGE_PCT))
        fee_r = cost_pct / risk_pct if risk_pct else 0.0
        rec = await db.update_follow(sid, f["user_id"], status=outcome, result_r=gross, fee_r=fee_r, net_r=gross - fee_r,
                                     close_price=close_price, closed_at=closed_at)
        if rec:
            done.append(rec)
    return done


async def announce_close(sig, outcome, gross, net, followers):
    """Update pesan sinyal (status + tombol nonaktif) dan mention follower dengan hasilnya."""
    channel = await get_alert_channel()
    if not channel:
        return
    msg = None
    if sig.get("message_id"):
        try:
            msg = await channel.fetch_message(int(sig["message_id"]))
        except discord.HTTPException:
            msg = None
    n = await db.count_followers(sig["id"])
    label = OUTCOME_LABEL.get(outcome, outcome)
    await set_message_footer(msg, f'#{sig["id"]} · {OUTCOME_ICON.get(outcome, "")} {label} · {gross:+.2f}R (net {net:+.2f}R) · 👥 {n} ikut',
                             view=make_view(sig["id"], closed=True), color=discord.Color.dark_grey())
    if followers:
        mentions = " ".join(f'<@{f["user_id"]}>' for f in followers[:25])
        text = (f'{OUTCOME_ICON.get(outcome, "")} **#{sig["id"]} {sig["symbol"]} {sig["direction"]}** selesai: **{label}** · '
                f'{gross:+.2f}R · cek `/riwayat`\n{mentions}')
        allowed = discord.AllowedMentions(users=True, roles=False, everyone=False)
        try:
            if msg is not None:
                await msg.reply(text, allowed_mentions=allowed)
            else:
                await channel.send(text, allowed_mentions=allowed)
        except discord.HTTPException as exc:
            log.warning("Gagal kirim notifikasi hasil #%s: %s", sig["id"], exc)


# ============================================================ scan & evaluasi
async def store_signal(s):
    return await db.add_signal(s, COOLDOWN_MINUTES)


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
            if turnover >= MIN_TURNOVER_24H and spread <= MAX_SPREAD_PCT:
                candidates.append(t)
        candidates.sort(key=lambda t: float(t.get("turnover24h", 0) or 0), reverse=True)
        sem = asyncio.Semaphore(5)  # bounded concurrency agar REST publik tidak dibanjiri

        async def one(ticker, tf):
            async with sem:
                try:
                    return await analyze_symbol(market, ticker["symbol"], tf, ticker=ticker)
                except Exception as exc:
                    log.warning("Analisis %s TF%s gagal: %s", ticker.get("symbol"), tf, exc)
                    return None
        raw = await asyncio.gather(*[one(t, tf) for t in candidates[:limit] for tf in SCAN_TIMEFRAMES])
        found = [s for s in raw if s]
        results = []
        for s in found:
            tier = classify(s)
            if tier:
                s["tier"] = tier
                results.append(s)
        results.sort(key=lambda s: (s["tier"] == "SIGNAL", s["score"], s.get("turnover24h", 0)), reverse=True)
        log.info("Scan: %d kandidat likuid, %d setup strategi adaptif, skor tertinggi=%s, SIGNAL=%d, WATCH=%d",
                 len(candidates), len(found), max((s["score"] for s in found), default="-"),
                 sum(s["tier"] == "SIGNAL" for s in results), sum(s["tier"] == "WATCH" for s in results))
        return results


def resolve_outcome(direction, sl, tp, candles):
    """TP/SL pertama yang tersentuh; keduanya di candle yang sama => AMBIGUOUS."""
    for _, c in candles.iterrows():
        high, low = float(c["high"]), float(c["low"])
        hit_sl = low <= sl if direction == "LONG" else high >= sl
        hit_tp = high >= tp if direction == "LONG" else low <= tp
        if hit_sl and hit_tp:
            return "AMBIGUOUS", float(c["close"])
        if hit_sl:
            return "SL", sl
        if hit_tp:
            return "TP", tp
    return None, None


def signal_result_r(sig, outcome, close_price):
    """(gross_r, fee_r, net_r). AMBIGUOUS dihitung -1R (asumsi konservatif)."""
    entry, sl = sig["entry"], sig["stop_loss"]
    long = sig["direction"] == "LONG"
    risk = abs(entry - sl) or 1e-12
    if outcome == "TP":
        gross = abs(sig["take_profit"] - entry) / risk
    elif outcome in ("SL", "AMBIGUOUS"):
        gross = -1.0
    else:
        gross = ((close_price - entry) if long else (entry - close_price)) / risk
    risk_pct = risk / entry * 100
    cost_pct = float(sig.get("total_cost_pct") if sig.get("total_cost_pct") is not None else (FEE_PCT + float(sig.get("spread_pct") or 0.0) + SLIPPAGE_PCT))
    fee_r = cost_pct / risk_pct if risk_pct else 0.0
    return gross, fee_r, gross - fee_r


async def evaluate_open_signals():
    rows = await db.get_open_signals(40)
    now = datetime.now(timezone.utc)
    for row in rows:
        sid, symbol, tf = row["id"], row["symbol"], row["timeframe"]
        try:
            df = await market.get_klines_cached(symbol, tf, ttl=20)
            if df.empty:
                continue
            after = df[df["start"] > (row.get("candle_start") or 0)]
            if after.empty:
                continue
            outcome, close_price = resolve_outcome(row["direction"], row["stop_loss"], row["take_profit"], after)
            age_minutes = (now - datetime.fromisoformat(row["created_at"])).total_seconds() / 60
            if outcome is None and age_minutes >= SIGNAL_MAX_AGE_MINUTES:
                outcome, close_price = "EXPIRED", float(after.iloc[-1]["close"])
            if not outcome:
                continue
            gross, fee_r, net = signal_result_r(row, outcome, close_price)
            closed_at = now.isoformat()
            sig = await db.update_signal(sid, status=outcome, outcome=outcome, closed_at=closed_at,
                                         close_price=close_price, result_r=gross, fee_r=fee_r, net_r=net) or row
            followers = await settle_follows(sig, outcome, close_price, closed_at)
            await announce_close(sig, outcome, gross, net, followers)
        except Exception as exc:
            log.warning("Evaluator gagal untuk signal %s: %s", sid, exc)


_alert_warned = False


async def get_alert_channel():
    global _alert_warned
    if not ALERT_CHANNEL_ID.isdigit():
        if not _alert_warned:
            log.warning("ALERT_CHANNEL_ID belum diisi/valid; notifikasi otomatis tidak dikirim.")
            _alert_warned = True
        return None
    cid = int(ALERT_CHANNEL_ID)
    channel = bot.get_channel(cid)
    if channel is None:
        try:
            channel = await bot.fetch_channel(cid)
        except discord.HTTPException:
            if not _alert_warned:
                log.warning("Channel alert %s tidak ditemukan/tidak bisa diakses bot (cek ID & izin View Channel).", cid)
                _alert_warned = True
            return None
    return channel


scan_state = {"at": None, "found": 0, "signals": 0, "watch": 0, "sent": 0, "watch_sent": 0, "error": None}


def _alert_problem():
    """Alasan alert otomatis tidak bisa jalan sekarang (None = siap)."""
    if market is None:
        return "market data belum aktif"
    if db is None:
        return "Upstash Redis belum terhubung (cek UPSTASH_REDIS_REST_URL dan UPSTASH_REDIS_REST_TOKEN)"
    if not ALERT_CHANNEL_ID.isdigit():
        return "ALERT_CHANNEL_ID belum diisi / bukan angka"
    return None


def _missing_perms(channel):
    """Izin yang kurang pada channel alert (kosong = lengkap)."""
    try:
        me = channel.guild.me
        p = channel.permissions_for(me)
        need = {"view_channel": "View Channel", "send_messages": "Send Messages", "embed_links": "Embed Links",
                "attach_files": "Attach Files", "read_message_history": "Read Message History"}
        return [label for attr, label in need.items() if not getattr(p, attr, True)]
    except Exception:
        return []


@tasks.loop(seconds=60)
async def scheduled_scan():
    if not market or not db:
        await start_services()
    problem = _alert_problem()
    if problem:
        scan_state.update(at=datetime.now(timezone.utc), error=problem)
        log.warning("Alert otomatis TIDAK jalan: %s", problem)
        return
    try:
        results = await scan_once()
        await evaluate_open_signals()
        channel = await get_alert_channel()
        n_sig = sum(s["tier"] == "SIGNAL" for s in results)
        n_watch = sum(s["tier"] == "WATCH" for s in results)
        scan_state.update(at=datetime.now(timezone.utc), found=len(results), signals=n_sig, watch=n_watch,
                          sent=0, watch_sent=0, error=None)
        if not channel:
            scan_state["error"] = "channel alert tidak ditemukan / bot tidak bisa mengaksesnya"
            return
        missing = _missing_perms(channel)
        if missing:
            scan_state["error"] = "bot kekurangan izin di channel alert: " + ", ".join(missing)
            log.warning("Alert otomatis TIDAK jalan: %s", scan_state["error"])
            return
        mention = f"<@&{ALERT_ROLE_ID}>" if ALERT_ROLE_ID.isdigit() else None  # ping hanya di pesan pertama
        sent_keys, sent = set(), 0
        for s in results:
            if s["tier"] != "SIGNAL":
                continue
            if sent >= 5:
                break
            sid = await store_signal(s)
            if not sid:
                continue
            try:
                s["id"] = sid
                kw = await chart_kwargs(s, signal_embed(s, sid))
                msg = await channel.send(content=mention, view=make_view(sid), **kw)
            except Exception as exc:
                # Gagal kirim: batalkan penyimpanan agar sinyal dicoba lagi di scan berikutnya, bukan hilang diam-diam.
                await db.discard_signal(sid)
                scan_state["error"] = f"gagal kirim sinyal {s['symbol']}: {exc}"
                log.exception("Gagal kirim sinyal %s; penyimpanan dibatalkan", s["symbol"])
                break
            await db.update_signal(sid, channel_id=channel.id, message_id=msg.id)
            mention = None
            sent_keys.add((s["symbol"], s["direction"]))
            sent += 1
        scan_state["sent"] = sent
        fresh = []
        for s in results:
            if len(fresh) >= WATCH_MAX_PER_SCAN:
                break
            if s["tier"] != "WATCH" or (s["symbol"], s["direction"]) in sent_keys:
                continue
            if await db.claim_watch(s, WATCH_COOLDOWN_MINUTES):
                fresh.append(s)
        if fresh:
            kw = await chart_kwargs(fresh[0], watch_embed(fresh), force=WATCH_CHART)
            await channel.send(content=mention, **kw)
            scan_state["watch_sent"] = len(fresh)
        log.info("Alert: SIGNAL terkirim=%d, WATCH terkirim=%d", sent, len(fresh))
    except Exception as exc:
        scan_state["error"] = f"error scan: {exc}"
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
        msg = "Layanan (Upstash Redis/market data) belum siap. Cek Deploy Logs Railway, lalu coba lagi."
    else:
        msg = f"Terjadi error: {original}"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.HTTPException:
        pass


# ============================================================ slash commands
async def alert_diagnostics():
    """Ringkasan kesehatan alert otomatis untuk /status."""
    lines = ["**Alert otomatis**"]
    lines.append(f"Upstash Redis: {'✅ terhubung' if db else '❌ belum terhubung'}")
    ch = await get_alert_channel() if ALERT_CHANNEL_ID.isdigit() else None
    if ch is None:
        lines.append("Channel alert: ❌ tidak valid / tidak bisa diakses (cek ALERT_CHANNEL_ID)")
    else:
        miss = _missing_perms(ch)
        lines.append("Channel alert: ✅ <#%s>%s" % (ch.id, "" if not miss else " · ❌ izin kurang: " + ", ".join(miss)))
    if scan_state["at"]:
        lines.append(f"Scan terakhir: <t:{int(scan_state['at'].timestamp())}:R> · lolos {scan_state['found']} "
                     f"(SIGNAL {scan_state['signals']}, WATCH {scan_state['watch']}) · terkirim SIGNAL {scan_state['sent']}, WATCH {scan_state['watch_sent']}")
    else:
        lines.append("Scan terakhir: belum ada")
    lines.append(f"Masalah terakhir: {scan_state['error'] or '— tidak ada'}")
    return "\n".join(lines)


@bot.tree.command(name="reset", description="Reset data bot di Upstash dan hapus pesan alert milik bot")
@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)
async def reset(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Command ini hanya untuk administrator server.", ephemeral=True)
        return
    embed = discord.Embed(
        title="⚠️ Konfirmasi Reset Total Bot",
        description=(
            "Tindakan ini **tidak bisa dibatalkan**. Jika dikonfirmasi:\n\n"
            f"• Semua key Redis dengan prefix `{db.prefix if db else 'bybit_signal_bot'}:*` akan dihapus.\n"
            "• Pesan yang dikirim bot di channel alert akan dihapus.\n"
            "• History, Follow Trade, sinyal, dan data dedupe bot akan hilang.\n\n"
            "**Tidak** menghapus pesan pengguna, channel/server Discord, token, atau Environment Variables."
        ),
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="Hanya administrator yang memulai command dapat mengonfirmasi · tombol kedaluwarsa 60 detik")
    await interaction.response.send_message(embed=embed, view=ResetConfirmView(interaction.user.id), ephemeral=True)


@bot.tree.command(name="status", description="Cek status bot, konfigurasi dan market data")
async def status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        tickers = await market.get_tickers()
        await interaction.followup.send(
            f"✅ Bot aktif · Pair linear: **{len(tickers)}** · Metode: **Adaptive Scalping**\n"
            f"TF: **{', '.join(SCAN_TIMEFRAMES)}m** · Scan tiap **{SCAN_INTERVAL_SECONDS}s** · Max pair **{MAX_SCAN_SYMBOLS}**\n"
            f"Skor SIGNAL ≥ **{MIN_SIGNAL_SCORE}** · WATCH ≥ **{MIN_WATCH_SCORE}** · Metode **{SIGNAL_STRATEGY.upper()}** · Fee **{FEE_PCT:g}%** + slippage **{SLIPPAGE_PCT:g}%**\n"
            "Data: Bybit public API · Mode: sinyal + follow simulasi, tanpa eksekusi order.\n"
            + await alert_diagnostics(), ephemeral=True)
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
        diag = {}
        s = await analyze_symbol(market, symbol, timeframe.value, ticker=ticker, diag=diag)
        if not s:
            why = "; ".join(diag.get("reasons", [])[:2]) or "tidak memenuhi syarat"
            await interaction.followup.send(f"**{symbol}** TF {timeframe.value}m — **NO TRADE**\nAlasan: {why}")
        else:
            s["tier"] = classify(s)
            await interaction.followup.send(**await chart_kwargs(s, signal_embed(s)))
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
            await interaction.followup.send(f"Tidak ada setup strategi aktif dengan skor ≥ {MIN_WATCH_SCORE}. **NO TRADE**."); return
        top = results[:jumlah]
        embed = discord.Embed(title="🔎 Scanner · Adaptive Scalping", description="\n".join(_line(s, True) for s in top),
                              color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        embed.set_footer(text="Level = referensi, bukan jaminan fill. Skor bukan probabilitas." + (" Chart = setup teratas." if CHART_ENABLED else ""))
        await interaction.followup.send(**await chart_kwargs(top[0], embed))
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
    lines = []
    for r in rows:
        o = r.get("outcome") or "OPEN"
        res = f' · {r["net_r"]:+.2f}R' if r.get("net_r") is not None else ""
        lines.append(f'{OUTCOME_ICON.get(o, "")} #{r["id"]} `{r["symbol"]}` {r["direction"][0]}·{r["timeframe"]}m · {r["score"]} · **{o}**{res}')
    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(name="trade", description="Lihat PnL/ROI berjalan dari trade yang kamu follow")
async def trade_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if db is None or market is None:
        await interaction.followup.send("Layanan belum siap, coba lagi sebentar.", ephemeral=True)
        return
    embed, png = await build_live_trades_embed(interaction.user)
    if png:
        file = discord.File(io.BytesIO(png), filename="trade-dashboard.png")
        # In image mode, keep the Discord embed as a clean caption; the PNG is the dashboard.
        embed.clear_fields()
        embed.title = "📊 TRADE DASHBOARD · MODERN CARD"
        embed.description = "Live snapshot dari posisi yang kamu ikuti. Detail PNL dan level posisi ada pada kartu gambar di bawah."
        embed.set_image(url="attachment://trade-dashboard.png")
        await interaction.followup.send(embed=embed, file=file, ephemeral=True)
    else:
        # Reliable fallback: text embed retains the same live values and all existing fields.
        await interaction.followup.send(
            content="⚠️ Gambar dashboard gagal dibuat; menampilkan ringkasan teks.",
            embed=embed, ephemeral=True
        )


@bot.tree.command(name="riwayat", description="Riwayat trade yang kamu ikuti (Follow Trade)")
@app_commands.describe(tampilkan="Filter tampilan")
@app_commands.choices(tampilkan=[app_commands.Choice(name="Semua", value="all"),
    app_commands.Choice(name="Berjalan", value="open"), app_commands.Choice(name="Selesai (TP/SL)", value="closed")])
async def riwayat(interaction: discord.Interaction, tampilkan: Optional[app_commands.Choice[str]] = None):
    await interaction.response.defer(ephemeral=True)
    mode = tampilkan.value if tampilkan else "all"
    await interaction.followup.send(embed=await build_history_embed(interaction.user, mode), view=make_history_view(), ephemeral=True)


@bot.tree.command(name="performance", description="Statistik hasil evaluasi sinyal")
async def performance(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    p = await db.performance()
    decided = p["tp"] + p["sl"] + p["ambiguous"]
    wr = p["tp"] / decided * 100 if decided else 0
    tf_lines = "\n".join(f'• {tf}m: {b["n"]} trade · win {b["win"]} · net {b["net"]:+.2f}R' for tf, b in sorted(p["by_tf"].items()))
    await interaction.followup.send(
        f'**Statistik evaluator** (ambigu dihitung loss)\n'
        f'Total **{p["total"]}** · Berjalan **{p["open"]}**\n'
        f'✅ TP **{p["tp"]}** · ❌ SL **{p["sl"]}** · ⚠️ Ambigu **{p["ambiguous"]}** · ⌛ Expired **{p["expired"]}**\n'
        f'Winrate konservatif TP/(TP+SL+Ambigu): **{wr:.1f}%**\n'
        f'Hasil: gross **{p["gross_r"]:+.2f}R** · net **{p["net_r"]:+.2f}R** (fee ≈{FEE_PCT:g}%)\n'
        + (tf_lines + "\n" if tf_lines else "") +
        "\nEvaluasi sederhana (high/low candle), bukan backtest tervalidasi.", ephemeral=True)


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
