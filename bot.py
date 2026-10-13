import io
import os
import asyncio
import logging
import hmac
import hashlib
from urllib.parse import quote
from datetime import datetime, timezone
from typing import Optional

from database import Database
import discord
from discord import app_commands
from discord.ext import commands, tasks
from aiohttp import web

from market import BybitMarket, analyze_symbol, fmt_price, INTERVALS, FEE_PCT, SLIPPAGE_PCT, MAX_SPREAD_PCT, SIGNAL_STRATEGY
from chart import render_chart, CHART_ENABLED

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("signalbot")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
GUILD_ID = os.getenv("GUILD_ID", "").strip().strip("\"\'").strip()
ALERT_CHANNEL_ID = os.getenv("ALERT_CHANNEL_ID", "").strip().strip("\"\'").strip()
DASHBOARD_BASE_URL = os.getenv("DASHBOARD_BASE_URL", "").strip().rstrip("/")
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "").strip() or os.getenv("DISCORD_TOKEN", "")
HISTORY_LIMIT = max(50, int(os.getenv("HISTORY_LIMIT", "1000") or 1000))  # jumlah follow terbaru yang dimuat untuk riwayat dashboard
_dashboard_runner = None
bot_state = {"discord": "starting"}
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
        await start_dashboard_server()
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
        global _dashboard_runner
        if _dashboard_runner:
            await _dashboard_runner.cleanup()
            _dashboard_runner = None
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


# ============================================================ live web dashboard
DASHBOARD_HTML = r"""<!doctype html>
<html lang="id"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#030b18"><title>JustInApps · Live Trading Dashboard</title>
<style>
:root{color-scheme:dark;--bg:#030b18;--panel:rgba(5,24,42,.82);--panel2:rgba(8,34,55,.72);--line:#103b55;--muted:#8aaac8;--text:#eaf6ff;--green:#00e6b2;--red:#ff496e;--cyan:#21d7f4;--blue:#168cff}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(ellipse at 52% -12%,#0d3555 0,transparent 40%),radial-gradient(ellipse at 5% 90%,#05264a 0,transparent 35%),var(--bg);font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--text);min-height:100vh}button{font:inherit}.app{display:grid;grid-template-columns:238px minmax(0,1fr);min-height:100vh}.sidebar{position:sticky;top:0;height:100vh;padding:28px 17px 18px;border-right:1px solid #0b2b43;background:linear-gradient(180deg,rgba(4,20,37,.96),rgba(2,12,25,.96));display:flex;flex-direction:column;gap:26px;overflow:hidden}.sidebar:after{content:"";position:absolute;left:-70px;bottom:90px;width:340px;height:220px;background:linear-gradient(145deg,transparent 36%,#082b4b 37%,#061a32 55%,transparent 56%);opacity:.48;transform:skewY(-18deg);pointer-events:none}.brand{display:flex;align-items:center;gap:12px;padding:2px 7px 22px}.logo{width:46px;height:46px;flex-shrink:0;display:grid;place-items:center;border-radius:15px;background:linear-gradient(135deg,#168bff,#00e6b2);color:white;font-size:24px;font-weight:950;box-shadow:0 0 28px #0b9cff45}.brandname{font-size:17px;font-weight:850;letter-spacing:-.04em}.brandname small{display:block;font-size:12px;color:#8db6dc;font-weight:500;letter-spacing:0;margin-top:3px}.nav{display:grid;gap:8px;position:relative;z-index:1}.nav a{display:flex;align-items:center;gap:13px;text-decoration:none;color:#a8c7e4;padding:13px 14px;border-radius:12px;font-size:14px;border:1px solid transparent}.nav a.active{color:var(--cyan);background:linear-gradient(100deg,#06336a,#082447);border-color:#0b3764;box-shadow:inset 3px 0 var(--cyan),0 5px 20px #00162a}.navicon{width:20px;text-align:center;font-size:18px}.paperbox{position:relative;z-index:1;margin:0 7px;padding:14px;border:1px solid #0a344a;border-radius:15px;background:rgba(3,24,42,.78)}.paper{display:inline-flex;align-items:center;gap:7px;color:var(--green);font-size:10px;font-weight:850;letter-spacing:.08em;padding:6px 8px;border:1px solid #07574e;background:#032d2c;border-radius:999px}.dot{width:7px;height:7px;background:var(--green);border-radius:50%;box-shadow:0 0 10px var(--green)}.paperbox p{font-size:11px;color:var(--muted);line-height:1.5;margin:9px 0 0}.livebox{position:relative;z-index:1;margin-top:auto;border:1px solid #0a344a;background:#06182a;border-radius:14px;padding:13px;display:flex;gap:10px;align-items:center}.liveicon{font-size:22px;color:#66cfff}.livebox b{font-size:12px;display:block}.livebox small{font-size:10px;color:var(--muted)}.live-dot{margin-left:auto;width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 12px var(--green)}.content{min-width:0;padding:27px clamp(16px,2.5vw,38px) 24px;max-width:1600px;width:100%;margin:auto}.topbar{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:24px}.greeting h1{font-size:clamp(19px,2vw,27px);margin:0 0 5px;letter-spacing:-.04em}.greeting p{margin:0;font-size:13px;color:#8db9dc}.topright{display:flex;align-items:center;gap:10px}.clock{border:1px solid #0c3550;background:#051b2e;border-radius:999px;padding:11px 14px;font-size:11px;color:#bdd8f0;white-space:nowrap}.bell{width:39px;height:39px;display:grid;place-items:center;border:1px solid #0d3a56;border-radius:50%;background:#061b2e;color:#c9e8ff}.layout{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(270px,.95fr);gap:18px;align-items:start}.leftcol,.rightcol{display:grid;gap:18px;min-width:0}.panel{border:1px solid #0d3c57;border-radius:19px;background:linear-gradient(145deg,rgba(6,28,48,.88),rgba(2,15,30,.92));box-shadow:0 14px 36px #0002;overflow:hidden;backdrop-filter:blur(18px)}.panelhead{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:18px 19px 0}.paneltitle{font-size:14px;font-weight:750;color:#cde8ff}.panelhint{font-size:10px;color:var(--muted)}.hero{display:grid;grid-template-columns:minmax(150px,1.1fr) minmax(100px,.7fr) minmax(100px,.7fr) minmax(110px,.85fr);gap:18px;align-items:center;padding:24px 25px;position:relative;border-color:#087a77;background:radial-gradient(ellipse at 28% 0%,#06374b 0,transparent 58%),linear-gradient(120deg,rgba(3,39,54,.96),rgba(3,20,36,.94));box-shadow:inset 4px 0 #00e6b2,0 0 35px #00cdb314}.hero:after{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(90deg,transparent,#00e6b207,transparent)}.heroMain,.metric{position:relative;z-index:1;min-width:0}.label{font-size:11px;color:#91b9d9;margin-bottom:9px}.heroValue{font-size:clamp(29px,3.2vw,45px);font-weight:900;letter-spacing:-.055em;line-height:1.1;white-space:nowrap}.heroUsd{font-size:14px;font-weight:700;margin-top:8px}.spark{width:100%;height:58px;margin-top:12px;overflow:visible}.metric{border-left:1px solid #15415a;padding-left:17px}.metric b{display:block;font-size:21px;letter-spacing:-.03em;margin:8px 0 5px}.metric small{font-size:11px;color:#9bbbd3}.sectionhead{display:flex;align-items:center;justify-content:space-between;padding:19px 19px 13px}.sectionhead h2{font-size:15px;margin:0;color:#cde8ff}.viewall{font-size:11px;color:var(--cyan);border:1px solid #0b3c56;background:#06233a;border-radius:10px;padding:8px 11px}.positions{padding:0 10px 10px;display:grid;gap:12px}.position{border:1px solid #0d4a65;border-radius:17px;background:linear-gradient(135deg,rgba(5,31,51,.94),rgba(2,17,33,.98));overflow:hidden;box-shadow:inset 0 1px #a6efff08}.poshead{display:flex;align-items:center;gap:11px;padding:15px 16px 8px}.coin{width:43px;height:43px;display:grid;place-items:center;border-radius:50%;font-size:20px;font-weight:900;background:linear-gradient(145deg,#ffb23e,#e87908);color:white;box-shadow:0 4px 18px #f99b2525;flex-shrink:0}.coin.alt{background:linear-gradient(145deg,#4acbff,#0875e8)}.symbol{font-size:17px;font-weight:850;letter-spacing:-.03em}.tag{font-size:11px;font-weight:850;color:var(--green);background:#053c39;border:1px solid #087d6e;border-radius:9px;padding:7px 9px;white-space:nowrap}.tag.short{color:#ff7997;background:#401b30;border-color:#71334c}.timeframe{font-size:11px;color:#b6d7ef;background:#09243a;padding:7px 9px;border-radius:9px;margin-left:-7px;white-space:nowrap}.spacer{flex:1}.status{font-size:11px;font-weight:900;color:var(--green);background:#064a43;border:1px solid #087a69;padding:8px 10px;border-radius:10px}.posmain{display:grid;grid-template-columns:minmax(0,1fr) minmax(145px,.7fr);gap:14px;padding:9px 18px 17px;align-items:center}.pnl{font-size:clamp(34px,4vw,48px);font-weight:950;letter-spacing:-.06em;line-height:1.05;white-space:nowrap}.usd{font-size:15px;font-weight:700;margin-top:7px}.prices{border-left:1px solid #12415a;padding-left:17px;display:grid;gap:10px}.price{display:flex;justify-content:space-between;align-items:center;gap:10px;font-size:13px}.price span{color:#9cc5e4}.price b{font-size:14px;white-space:nowrap}.levels{display:flex;align-items:center;gap:9px;flex-wrap:wrap;padding:0 17px 16px}.level{font-size:11px;font-weight:750;color:#ff6483;background:#281629;border:1px solid #64263e;border-radius:10px;padding:9px 11px;white-space:nowrap}.level.tp{color:var(--green);background:#032d2c;border-color:#086456}.age{margin-left:auto;color:#a1c2df;font-size:12px;white-space:nowrap}.rightpanel{padding-bottom:14px}.marketlist{padding:8px 16px 0;display:grid}.marketrow{display:grid;grid-template-columns:29px minmax(70px,1fr) auto auto;gap:9px;align-items:center;padding:12px 0;border-bottom:1px solid #0b2d45;font-size:12px}.marketrow:last-child{border-bottom:0}.marketcoin{width:28px;height:28px;border-radius:50%;display:grid;place-items:center;background:#123c57;font-weight:900;color:#e6f8ff}.marketname{font-weight:650;overflow:hidden;text-overflow:ellipsis}.marketprice{color:#d8ecfb;font-weight:750}.change{font-weight:800;color:var(--green)}.chartpanel{padding-bottom:15px}.charttools{display:flex;gap:6px}.charttools span{font-size:10px;color:#9bc6e7;padding:6px 8px;border-radius:8px}.charttools span.active{color:var(--cyan);background:#073352;border:1px solid #0b6a91}.chartwrap{padding:17px 15px 0}.chartlegend{display:flex;justify-content:space-between;color:#86aac8;font-size:10px;margin-top:8px}.chartsvg{width:100%;height:auto;display:block}.statsgrid{padding:14px 16px 0;display:grid;grid-template-columns:1fr 1fr;gap:9px}.statbox{border:1px solid #0d344c;background:#061b2d;border-radius:12px;padding:12px}.statbox span{display:block;font-size:11px;color:#91b8d7}.statbox b{display:block;font-size:19px;margin-top:7px}.footer{display:flex;align-items:center;justify-content:space-between;gap:12px;color:#668ba9;font-size:11px;padding:22px 3px 0}.footer strong{color:#9ec8e7;font-weight:650}.footright{color:var(--cyan)}.empty{margin:0 8px 10px;padding:30px 16px;text-align:center;color:var(--muted);border:1px dashed #17445d;border-radius:15px;font-size:13px}.hidden{display:none!important}
@media(max-width:1100px){.app{grid-template-columns:190px minmax(0,1fr)}.sidebar{padding:20px 11px}.layout{grid-template-columns:minmax(0,1.3fr) minmax(245px,.8fr);gap:12px}.hero{grid-template-columns:1.2fr .8fr .8fr;gap:12px;padding:20px}.metric:last-child{display:none}.content{padding:20px 16px}.posmain{grid-template-columns:minmax(0,1fr) minmax(125px,.75fr);padding-left:14px;padding-right:14px}}
@media(max-width:760px){.app{display:block}.sidebar{position:relative;height:auto;padding:13px 14px;border-right:0;border-bottom:1px solid #0c3049;display:block}.brand{padding:0;gap:10px}.logo{width:38px;height:38px;font-size:20px}.brandname{font-size:15px}.brandname small{font-size:10px}.nav{display:flex;overflow:auto;gap:6px;margin-top:13px}.nav a{white-space:nowrap;padding:9px 11px;font-size:11px;gap:7px}.navicon{font-size:14px;width:auto}.paperbox,.livebox{display:none}.content{padding:17px 12px 22px}.topbar{align-items:flex-start;margin-bottom:16px}.greeting h1{font-size:19px}.greeting p{font-size:11px;line-height:1.4}.topright{gap:0}.clock{font-size:9px;padding:9px 10px}.bell{display:none}.topright .paper{display:none}.layout{grid-template-columns:1fr;gap:13px}.leftcol,.rightcol{gap:13px}.hero{grid-template-columns:1fr 1fr;gap:15px;padding:18px}.heroMain{grid-column:1/-1;display:grid;grid-template-columns:1fr 110px;align-items:center}.heroMain .label{grid-column:1/-1;margin-bottom:6px}.heroValue{font-size:38px}.heroUsd{font-size:12px}.spark{height:55px;margin:0}.metric{padding-left:12px}.metric:nth-of-type(3){border-left:1px solid #15415a}.metric b{font-size:18px}.metric small{font-size:10px}.metric:last-child{display:block}.sectionhead{padding:15px 14px 11px}.positions{padding:0 7px 8px;gap:9px}.poshead{gap:7px;padding:12px 11px 7px}.coin{width:34px;height:34px;font-size:16px}.symbol{font-size:14px}.tag,.timeframe{font-size:9px;padding:6px 6px}.status{font-size:9px;padding:6px}.posmain{grid-template-columns:minmax(0,1fr) minmax(118px,.8fr);gap:9px;padding:8px 12px 13px}.pnl{font-size:clamp(30px,8vw,39px)}.usd{font-size:12px}.prices{padding-left:11px;gap:10px}.price{font-size:11px}.price b{font-size:11px}.levels{gap:6px;padding:0 11px 12px}.level{font-size:10px;padding:7px 8px}.age{font-size:10px}.panel{border-radius:16px}.panelhead{padding:15px 14px 0}.marketrow{font-size:11px}.footer{padding-top:17px;font-size:9px}}
@media(max-width:390px){.poshead{gap:5px}.coin{width:30px;height:30px}.symbol{font-size:12px}.tag,.timeframe{padding:5px;font-size:8px}.status{padding:5px;font-size:8px}.posmain{grid-template-columns:minmax(0,1fr) minmax(108px,.8fr)}.pnl{font-size:29px}.level{font-size:9px;padding:6px}.age{font-size:9px}.heroValue{font-size:34px}}
.sumgrid{padding:0 16px 14px;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px}.sumgrid .statbox span{display:block;font-size:10px;color:var(--muted);margin-bottom:6px}.sumgrid .statbox b{font-size:17px;font-weight:850;white-space:nowrap}.sumgrid .statbox.big{grid-column:1/-1;display:flex;align-items:center;justify-content:space-between;gap:10px;background:linear-gradient(120deg,#06374a,#061b2d);border-color:#0b6a73}.sumgrid .statbox.big b{font-size:clamp(26px,5vw,36px);letter-spacing:-.04em}.sumgrid .statbox.big small{color:var(--muted);font-size:10px;text-align:right;line-height:1.5}
.filters{display:flex;gap:6px;padding:0 16px 10px;flex-wrap:wrap}.fbtn{background:#06233a;border:1px solid #0b3c56;color:#9cc3df;border-radius:10px;padding:7px 12px;font-size:11px;cursor:pointer}.fbtn.on{color:var(--cyan);border-color:#14708f;background:#07334f}
.tablewrap{overflow-x:auto;padding:0 12px 6px;-webkit-overflow-scrolling:touch}.htable{width:100%;min-width:720px;border-collapse:collapse;font-size:12px}.htable th{text-align:left;color:#7fa6c4;font-weight:600;font-size:10px;letter-spacing:.06em;text-transform:uppercase;padding:8px;border-bottom:1px solid #12405a}.htable td{padding:10px 8px;border-bottom:1px solid #0c2f45;white-space:nowrap}.htable .num{text-align:right;font-variant-numeric:tabular-nums}.htable th.num{text-align:right}
.res{font-size:10px;font-weight:850;padding:4px 7px;border-radius:8px;border:1px solid}.res.win{color:var(--green);background:#053c39;border-color:#087d6e}.res.loss{color:var(--red);background:#3a0d1c;border-color:#7a1f3a}.res.neu{color:#c9d7e3;background:#162a3b;border-color:#2b4a63}
.morebtn{display:block;margin:10px auto 16px;background:#06233a;border:1px solid #0b3c56;color:var(--cyan);border-radius:10px;padding:9px 16px;font-size:12px;cursor:pointer}
.srow{display:grid;grid-template-columns:minmax(0,1.7fr) .45fr .65fr .9fr;gap:8px;padding:10px 17px;border-bottom:1px solid #0c2f45;font-size:11px;align-items:center}.srow.h{color:#7fa6c4;font-size:10px;text-transform:uppercase;letter-spacing:.06em}.srow span:not(:first-child){text-align:right}
.sumgrid .statbox b.green,.sumgrid .statbox.big b.green{color:var(--green)}.sumgrid .statbox b.red,.sumgrid .statbox.big b.red{color:var(--red)}
@media(max-width:760px){.sumgrid{grid-template-columns:repeat(2,minmax(0,1fr))}.sumgrid .statbox b{font-size:15px}}
</style></head><body><div class="app">
<aside class="sidebar"><div class="brand"><div class="logo">J</div><div class="brandname">JustInApps<small>Trading Bot</small></div></div>
<nav class="nav"><a class="active" href="#overview"><span class="navicon">⌂</span>Dashboard</a><a href="#positions"><span class="navicon">▣</span>Positions</a><a href="#history"><span class="navicon">≣</span>History</a><a href="#stats"><span class="navicon">◷</span>Trade Snapshot</a><a href="#settings"><span class="navicon">⚙</span>Settings</a></nav>
<div class="paperbox"><span class="paper"><i class="dot"></i> PAPER TRADING</span><p>No real funds · Demo mode<br>Positions are read-only.</p></div>
<div class="livebox"><span class="liveicon">◴</span><div><b>Live Data</b><small id="sideUpdated">Connecting…</small></div><i class="live-dot"></i></div></aside>
<main class="content" id="overview"><header class="topbar"><div class="greeting"><h1>Good evening, Trader <span aria-hidden="true">👋</span></h1><p>Stay disciplined. Let the strategy work.</p></div><div class="topright"><div class="clock" id="clock">LIVE SNAPSHOT · UTC+8</div><button class="bell" aria-label="Live status">♧</button><span class="paper"><i class="dot"></i> PAPER TRADING</span></div></header>
<div class="layout"><div class="leftcol"><section class="panel hero"><div class="heroMain"><div class="label">PORTFOLIO PNL (NET)</div><div><div id="net" class="heroValue green">—</div><div id="netUsd" class="heroUsd green">Live estimate</div></div><svg class="spark" viewBox="0 0 170 58" preserveAspectRatio="none" aria-label="Current PNL sparkline"><defs><linearGradient id="sparkFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#00e6b2" stop-opacity=".28"/><stop offset="1" stop-color="#00e6b2" stop-opacity="0"/></linearGradient></defs><path d="M0 48 L12 39 L23 43 L34 27 L45 35 L55 24 L67 31 L78 19 L88 29 L99 15 L110 24 L120 13 L130 22 L142 9 L152 15 L170 2 L170 58 L0 58Z" fill="url(#sparkFill)"/><path d="M0 48 L12 39 L23 43 L34 27 L45 35 L55 24 L67 31 L78 19 L88 29 L99 15 L110 24 L120 13 L130 22 L142 9 L152 15 L170 2" fill="none" stroke="#00e6b2" stroke-width="2"/></svg></div><div class="metric"><div class="label">Gross PNL</div><b id="gross" class="green">—</b><small>Before estimated costs</small></div><div class="metric"><div class="label">Avg Net</div><b id="avg" class="green">—</b><small>Per open position</small></div><div class="metric"><div class="label">Active Positions</div><b id="count">—</b><small>Currently followed</small></div></section>
<section class="panel" id="pnl"><div class="sectionhead"><h2>◈ &nbsp; Total PNL · All Time</h2><span class="viewall">REALIZED + OPEN</span></div><div class="sumgrid"><div class="statbox big"><div><span>Total PNL (Net, after costs)</span><b id="tTotal">—</b></div><small id="tTotalSub">—</small></div><div class="statbox"><span>Realized Net</span><b id="tReal">—</b></div><div class="statbox"><span>Net R</span><b id="tNetR">—</b></div><div class="statbox"><span>Win Rate</span><b id="tWr">—</b></div><div class="statbox"><span>Closed Trades</span><b id="tClosed">—</b></div><div class="statbox"><span>Profit Factor</span><b id="tPf">—</b></div><div class="statbox"><span>Max Drawdown</span><b id="tDd" class="red">—</b></div><div class="statbox"><span>Avg Net R / Trade</span><b id="tAvg">—</b></div><div class="statbox"><span>Best Trade</span><b id="tBest" class="green">—</b></div><div class="statbox"><span>Worst Trade</span><b id="tWorst" class="red">—</b></div><div class="statbox"><span>Open PNL (floating)</span><b id="tOpen">—</b></div></div><div class="chartwrap"><svg id="eqChart" class="chartsvg" viewBox="0 0 360 150" role="img" aria-label="Equity curve realized net PNL"><g stroke="#123047" stroke-width="1"><path d="M35 15H350 M35 72H350 M35 130H350"/></g><text id="eqMax" x="2" y="19" fill="#83a8c6" font-size="10"></text><text id="eqMin" x="2" y="133" fill="#83a8c6" font-size="10"></text><path id="eqZero" d="" stroke="#4d7391" stroke-dasharray="3 3" fill="none"/><path id="eqArea" d="" fill="#00e6b222"/><path id="eqLine" d="" fill="none" stroke="#00e6b2" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"/></svg><div class="chartlegend"><span>Trade pertama</span><span id="eqNote">Equity curve · net PNL realized (%)</span><span>Terbaru</span></div></div><div style="height:14px"></div></section>
<section class="panel" id="positions"><div class="sectionhead"><h2>⌖ &nbsp; Active Positions <span id="countTitle">(—)</span></h2><span class="viewall">LIVE · READ ONLY</span></div><div id="cards" class="positions"><div class="empty">Loading live positions…</div></div></section>
<section class="panel" id="history"><div class="sectionhead"><h2>≣ &nbsp; Trade History <span id="histCount">(—)</span></h2><span class="viewall" id="histNote">SEMUA TRADE SELESAI</span></div><div class="filters"><button class="fbtn on" data-f="all">Semua</button><button class="fbtn" data-f="win">Menang</button><button class="fbtn" data-f="loss">Kalah</button></div><div class="tablewrap"><table class="htable"><thead><tr><th>Ditutup</th><th>Pair</th><th>Hasil</th><th class="num">Net R</th><th class="num">Net %</th><th>Strategi</th><th class="num">Entry</th><th class="num">Exit</th></tr></thead><tbody id="histBody"><tr><td colspan="8"><div class="empty">Loading history…</div></td></tr></tbody></table></div><button id="histMore" class="morebtn" style="display:none">Tampilkan lebih banyak</button></section>
<section class="panel" id="stats"><div class="sectionhead"><h2>▥ &nbsp; Portfolio Snapshot</h2><span class="panelhint">CURRENT OPEN POSITIONS</span></div><div style="padding:0 18px 17px;display:grid;grid-template-columns:1fr auto;gap:12px;align-items:center"><div><div class="label">Portfolio PNL (Net)</div><div id="netBottom" class="heroValue green">—</div></div><div style="border-left:1px solid #16415a;padding-left:20px"><div class="label">Active Positions</div><div id="countBottom" style="font-size:27px;font-weight:850;margin-top:6px">—</div></div></div></section></div>
<div class="rightcol"><section class="panel rightpanel"><div class="panelhead"><span class="paneltitle">▧ &nbsp; Market Overview</span><span class="panelhint">FOLLOWED PAIRS</span></div><div id="marketList" class="marketlist"><div class="empty">Waiting for live prices…</div></div></section>
<section class="panel rightpanel"><div class="panelhead"><span class="paneltitle">◈ &nbsp; Performa per Strategi</span><span class="panelhint">TRADE SELESAI</span></div><div id="stratList" style="padding-top:10px"><div class="empty">Belum ada trade selesai</div></div></section>
<section class="panel chartpanel"><div class="panelhead"><span class="paneltitle">♧ &nbsp; Position PNL</span><span class="charttools"><span class="active">LIVE</span></span></div><div class="chartwrap"><svg id="pnlChart" class="chartsvg" viewBox="0 0 360 190" role="img" aria-label="Current PNL by position"><defs><linearGradient id="chartFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#00e6b2" stop-opacity=".25"/><stop offset="1" stop-color="#00e6b2" stop-opacity="0"/></linearGradient></defs><g stroke="#123047" stroke-width="1"><path d="M35 25H350 M35 65H350 M35 105H350 M35 145H350"/><path d="M35 20V155 M115 20V155 M195 20V155 M275 20V155 M350 20V155"/></g><g fill="#83a8c6" font-size="10"><text x="2" y="28">+3%</text><text x="2" y="68">+1%</text><text x="10" y="108">0%</text><text x="2" y="148">-2%</text></g><path id="chartArea" d="M35 105 L80 90 L125 115 L170 60 L215 75 L260 45 L305 68 L350 35 L350 155 L35 155Z" fill="url(#chartFill)"/><path id="chartLine" d="M35 105 L80 90 L125 115 L170 60 L215 75 L260 45 L305 68 L350 35" fill="none" stroke="#00e6b2" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg><div class="chartlegend"><span>Loss ←</span><span>Net PNL per position</span><span>→ Profit</span></div></div></section>
<section class="panel rightpanel"><div class="panelhead"><span class="paneltitle">▥ &nbsp; Trading Stats</span><span class="panelhint">LIVE SNAPSHOT</span></div><div class="statsgrid"><div class="statbox"><span>Open Positions</span><b id="statOpen">—</b></div><div class="statbox"><span>Profitable Now</span><b id="statGreen" class="green">—</b></div><div class="statbox"><span>Best Open PNL</span><b id="statBest" class="green">—</b></div><div class="statbox"><span>Worst Open PNL</span><b id="statWorst" class="red">—</b></div></div></section>
<section class="panel rightpanel" id="settings"><div class="panelhead"><span class="paneltitle">◉ &nbsp; Data & Privacy</span></div><div style="padding:12px 17px 17px;color:#91b5d3;font-size:11px;line-height:1.65">This dashboard shows followed paper-trading positions only. PNL is an estimate after configured costs. No real orders are placed.</div></section></div></div>
<footer class="footer"><span><strong>◈ JustInApps</strong> · Trading Bot</span><span class="footright">ϟ Smarter Trading · Better Results</span></footer></main></div>
<script>
const token=location.pathname.split('/').filter(Boolean).pop();const fmt=(v,d=4)=>Number(v).toLocaleString('en-US',{maximumFractionDigits:d});const pct=v=>(Number(v)>=0?'+':'')+Number(v).toFixed(2)+'%';
function el(tag,cls,txt){const n=document.createElement(tag);if(cls)n.className=cls;if(txt!==undefined)n.textContent=txt;return n}
function setTone(node,v){node.className=node.className.replace(/\bgreen\b|\bred\b/g,'').trim()+' '+(Number(v)>=0?'green':'red')}
function drawPositionChart(positions){const line=document.getElementById('chartLine'),area=document.getElementById('chartArea');if(!positions.length){line.setAttribute('d','M35 105H350');area.setAttribute('d','M35 105H350V155H35Z');line.setAttribute('stroke','#6485a0');return}const vals=positions.map(p=>Number(p.net)||0),max=Math.max(1,...vals.map(Math.abs));const pts=vals.map((v,i)=>{const x=35+(315*(i/(Math.max(1,vals.length-1))));const y=105-(v/max)*65;return [x,y]});if(pts.length===1)pts.push([350,pts[0][1]]);const d=pts.map((p,i)=>(i?'L':'M')+p[0]+' '+p[1]).join(' ');line.setAttribute('d',d);line.setAttribute('stroke',vals.reduce((a,b)=>a+b,0)>=0?'#00e6b2':'#ff496e');area.setAttribute('d',d+' L350 155 L35 155Z');area.setAttribute('fill',vals.reduce((a,b)=>a+b,0)>=0?'url(#chartFill)':'#ff496e18')}

let H={f:'all',n:50,d:null};
const rr=v=>(Number(v)>=0?'+':'')+Number(v).toFixed(2)+'R';
const tfmt=iso=>{if(!iso)return '—';const t=new Date(iso);return isNaN(t)?'—':t.toLocaleString('en-GB',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'Asia/Brunei'})};
const stName=s=>String(s||'-').replace(/_/g,' ');
function put(id,txt,v){const n=document.getElementById(id);if(!n)return;n.textContent=txt;if(v!==undefined)setTone(n,v)}
function drawEquity(eq){const line=document.getElementById('eqLine'),area=document.getElementById('eqArea'),zero=document.getElementById('eqZero');if(!eq||!eq.length){line.setAttribute('d','');area.setAttribute('d','');zero.setAttribute('d','M35 72H350');put('eqMax','');put('eqMin','');document.getElementById('eqNote').textContent='Belum ada trade selesai';return}
const v=[0,...eq],mn=Math.min(0,...v),mx=Math.max(0,...v),span=(mx-mn)||1,X=i=>35+315*(i/Math.max(1,v.length-1)),Y=x=>130-((x-mn)/span)*115;const d=v.map((x,i)=>(i?'L':'M')+X(i).toFixed(1)+' '+Y(x).toFixed(1)).join(' ');const up=v[v.length-1]>=0;line.setAttribute('d',d);line.setAttribute('stroke',up?'#00e6b2':'#ff496e');area.setAttribute('d',d+' L350 130 L35 130Z');area.setAttribute('fill',up?'#00e6b222':'#ff496e1c');zero.setAttribute('d','M35 '+Y(0).toFixed(1)+'H350');put('eqMax',pct(mx));put('eqMin',pct(mn));document.getElementById('eqNote').textContent='Equity curve · '+eq.length+' trade · net PNL realized (%)'}
function renderHistory(){const d=H.d;if(!d)return;const all=d.history||[];const rows=all.filter(r=>H.f==='all'||(H.f==='win'?r.net_r>0:r.net_r<=0));put('histCount','('+rows.length+')');const body=document.getElementById('histBody');body.replaceChildren();
if(!rows.length){const tr=el('tr'),td=el('td');td.colSpan=8;td.append(el('div','empty',all.length?'Tidak ada trade untuk filter ini.':'Belum ada trade selesai. Hasil muncul di sini otomatis saat TP/SL tersentuh.'));tr.append(td);body.append(tr)}
for(const r of rows.slice(0,H.n)){const tr=el('tr');const win=r.net_r>0,neu=r.status==='EXPIRED'||r.status==='AMBIGUOUS';const resTxt=r.status==='AMBIGUOUS'?'AMBIGU':r.status;const c=(txt,cls)=>{const td=el('td',cls||'',txt);return td};tr.append(c(tfmt(r.closed_at)),(()=>{const td=el('td');td.append(el('b','',r.symbol.replace('USDT','')+' '),el('span','tag '+(r.side==='SHORT'?'short':''),r.side),el('span','muted',' '+r.timeframe+'m'));return td})(),(()=>{const td=el('td');td.append(el('span','res '+(win?'win':neu?'neu':'loss'),resTxt));return td})(),c(rr(r.net_r),'num '+(win?'green':'red')),c(pct(r.net_pct),'num '+(win?'green':'red')),c(stName(r.strategy)),c(fmt(r.entry,8),'num'),c(fmt(r.exit,8),'num'));body.append(tr)}
const more=document.getElementById('histMore');more.style.display=rows.length>H.n?'block':'none';more.textContent='Tampilkan lebih banyak ('+(rows.length-H.n)+' lagi)';
document.getElementById('histNote').textContent=d.history_truncated?'MENAMPILKAN '+all.length+' TRADE TERBARU':'SEMUA TRADE SELESAI'}
function renderStrategies(list){const box=document.getElementById('stratList');box.replaceChildren();if(!list||!list.length){box.append(el('div','empty','Belum ada trade selesai'));return}
const h=el('div','srow h');h.append(el('span','','Strategi'),el('span','','Trade'),el('span','','WR'),el('span','','Net R'));box.append(h);
for(const b of list){const row=el('div','srow');const wr=b.n?b.wins/b.n*100:0;row.append(el('span','',stName(b.strategy)),el('span','',String(b.n)),el('span','',wr.toFixed(0)+'%'),el('span',b.net_r>=0?'green':'red',rr(b.net_r)));box.append(row)}}
function renderAll(d){H.d=d;const s=d.summary||{};put('tTotal',pct(s.total_pnl_pct||0),s.total_pnl_pct||0);put('tTotalSub','Realized '+pct(s.net_pct||0)+'\nOpen '+pct(s.open_net_pct||0));put('tReal',pct(s.net_pct||0),s.net_pct||0);put('tNetR',rr(s.net_r||0),s.net_r||0);put('tWr',(s.closed?Number(s.win_rate).toFixed(1)+'% ('+s.wins+'W/'+s.losses+'L)':'—'));put('tClosed',String(s.closed||0));put('tPf',s.profit_factor==null?'—':(s.profit_factor>=999?'∞':Number(s.profit_factor).toFixed(2)));put('tDd',s.closed?'-'+Number(s.max_drawdown_pct).toFixed(2)+'%':'—');put('tAvg',s.closed?rr(s.avg_net_r):'—',s.avg_net_r||0);put('tBest',s.closed?pct(s.best_pct):'—');put('tWorst',s.closed?pct(s.worst_pct):'—');put('tOpen',pct(s.open_net_pct||0),s.open_net_pct||0);document.getElementById('tTotalSub').style.whiteSpace='pre-line';drawEquity(s.equity);renderStrategies(s.by_strategy);renderHistory()}
document.querySelectorAll('.fbtn').forEach(b=>b.addEventListener('click',()=>{H.f=b.dataset.f;H.n=50;document.querySelectorAll('.fbtn').forEach(x=>x.classList.toggle('on',x===b));renderHistory()}));
document.getElementById('histMore').addEventListener('click',()=>{H.n+=50;renderHistory()});
async function load(){try{const r=await fetch('/api/dashboard/'+encodeURIComponent(token),{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);const d=await r.json();for(const id of ['net','netBottom']){const n=document.getElementById(id);n.textContent=pct(d.net);setTone(n,d.net)}document.getElementById('netUsd').textContent='Live estimate · after costs';const gross=document.getElementById('gross');gross.textContent=pct(d.gross);setTone(gross,d.gross);const avg=document.getElementById('avg');avg.textContent=pct(d.avg);setTone(avg,d.avg);const count=String(d.positions.length).padStart(2,'0');['count','countBottom','statOpen'].forEach(id=>document.getElementById(id).textContent=count);document.getElementById('countTitle').textContent='('+count+')';const now=new Date();document.getElementById('clock').textContent=now.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false,timeZone:'Asia/Brunei'})+' (UTC+8)';document.getElementById('sideUpdated').textContent='Updated '+now.toLocaleTimeString('en-GB',{timeZone:'Asia/Brunei'});document.getElementById('updated')?.replaceChildren();const cards=document.getElementById('cards');cards.replaceChildren();if(!d.positions.length)cards.append(el('div','empty','Belum ada posisi aktif. Ikuti sinyal dari Discord untuk melihat posisi di dashboard ini.'));for(const p of d.positions){const card=el('article','position');const head=el('div','poshead');const symbol=p.symbol||'UNKNOWN';const icon=el('div','coin '+(symbol.includes('ETH')||symbol.includes('SUI')?'alt':''),symbol.includes('BTC')?'₿':symbol.includes('ETH')?'◆':symbol.includes('SOL')?'◎':symbol.replace('USDT','').slice(0,1));head.append(icon,el('div','symbol',symbol),el('span','tag '+(p.side==='SHORT'?'short':''),p.side));head.append(el('span','timeframe','· '+p.timeframe+'m'));head.append(el('div','spacer'),el('span','status','OPEN'));card.append(head);const main=el('div','posmain');const left=el('div','');left.append(el('div','pnl '+(p.net>=0?'green':'red'),pct(p.net)),el('div','usd muted','Net PNL · after costs'));const prices=el('div','prices');for(const [label,value] of [['Entry',p.entry],['Last',p.last]]){const row=el('div','price');row.append(el('span','',label),el('b','',fmt(value,8)));prices.append(row)}main.append(left,prices);card.append(main);const levels=el('div','levels');levels.append(el('div','level','◉ SL  '+fmt(p.sl,8)),el('div','level tp','♧ TP  '+fmt(p.tp,8)),el('div','age','◷ '+p.age));card.append(levels);cards.append(card)}const market=document.getElementById('marketList');market.replaceChildren();if(!d.positions.length)market.append(el('div','empty','No followed pairs'));for(const p of d.positions.slice(0,6)){const row=el('div','marketrow');row.append(el('div','marketcoin',p.symbol.includes('BTC')?'₿':p.symbol.includes('ETH')?'◆':p.symbol.includes('SOL')?'◎':p.symbol.slice(0,1)),el('div','marketname',p.symbol),el('div','marketprice',fmt(p.last,8)),el('div','change '+(p.net>=0?'green':'red'),pct(p.net)));market.append(row)}const profitable=d.positions.filter(p=>p.net>0);document.getElementById('statGreen').textContent=profitable.length+' / '+d.positions.length;const sorted=[...d.positions].sort((a,b)=>b.net-a.net);const best=document.getElementById('statBest'),worst=document.getElementById('statWorst');best.textContent=sorted.length?pct(sorted[0].net):'—';worst.textContent=sorted.length?pct(sorted[sorted.length-1].net):'—';setTone(best,sorted.length?sorted[0].net:0);setTone(worst,sorted.length?sorted[sorted.length-1].net:0);drawPositionChart(d.positions);renderAll(d)}catch(e){document.getElementById('sideUpdated').textContent='Connection issue · retrying';console.error(e)}}load();setInterval(load,15000);
</script></body></html>"""


def _dashboard_token(user_id: int) -> str:
    uid = str(int(user_id))
    sig = hmac.new(DASHBOARD_SECRET.encode(), uid.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{uid}.{sig}"


def _valid_dashboard_token(token: str) -> Optional[int]:
    try:
        uid, sig = token.split(".", 1)
        expected = hmac.new(DASHBOARD_SECRET.encode(), uid.encode(), hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(sig, expected):
            return None
        return int(uid)
    except (ValueError, TypeError):
        return None


def _closed_trade_row(rec):
    """Ubah follow yang sudah selesai jadi baris riwayat (PNL dalam % harga dan R). Tidak butuh data pasar."""
    fp = float(rec.get("follow_price") or 0)
    sl = float(rec.get("sl") or 0)
    risk_pct = abs(fp - sl) / fp * 100 if fp else 0.0
    gross_r = float(rec.get("result_r") or 0)
    net_r = float(rec.get("net_r") if rec.get("net_r") is not None else gross_r)
    long = rec.get("direction") == "LONG"
    exit_price = float(rec.get("close_price") or 0)
    status = rec.get("status") or "?"
    if status in ("SL", "AMBIGUOUS"):
        exit_price = sl or exit_price
    elif status == "TP":
        exit_price = float(rec.get("tp") or exit_price)
    return {"signal_id": rec.get("signal_id"), "symbol": rec.get("symbol", "UNKNOWN"),
            "side": "LONG" if long else "SHORT", "timeframe": str(rec.get("timeframe", "?")),
            "strategy": rec.get("strategy") or "-", "status": status,
            "entry": fp, "exit": exit_price, "sl": sl, "tp": float(rec.get("tp") or 0),
            "gross_r": gross_r, "net_r": net_r,
            "gross_pct": gross_r * risk_pct, "net_pct": net_r * risk_pct,
            "opened_at": rec.get("followed_at"), "closed_at": rec.get("closed_at")}


def _history_summary(rows):
    """Ringkasan seluruh trade selesai. rows sudah urut lama -> baru."""
    n = len(rows)
    wins = sum(r["net_r"] > 0 for r in rows)
    gains = sum(r["net_r"] for r in rows if r["net_r"] > 0)
    losses = abs(sum(r["net_r"] for r in rows if r["net_r"] < 0))
    equity, cum, peak, max_dd = [], 0.0, 0.0, 0.0
    for r in rows:
        cum += r["net_pct"]
        peak = max(peak, cum); max_dd = max(max_dd, peak - cum)
        equity.append(round(cum, 4))
    by_strategy = {}
    for r in rows:
        b = by_strategy.setdefault(r["strategy"], {"strategy": r["strategy"], "n": 0, "wins": 0, "net_r": 0.0, "net_pct": 0.0})
        b["n"] += 1; b["wins"] += r["net_r"] > 0; b["net_r"] += r["net_r"]; b["net_pct"] += r["net_pct"]
    return {
        "closed": n, "wins": wins, "losses": n - wins,
        "win_rate": wins / n * 100 if n else 0.0,
        "net_pct": sum(r["net_pct"] for r in rows), "gross_pct": sum(r["gross_pct"] for r in rows),
        "net_r": sum(r["net_r"] for r in rows), "gross_r": sum(r["gross_r"] for r in rows),
        "avg_net_r": sum(r["net_r"] for r in rows) / n if n else 0.0,
        "profit_factor": (gains / losses) if losses else (None if not gains else 999.0),
        "best_pct": max((r["net_pct"] for r in rows), default=0.0),
        "worst_pct": min((r["net_pct"] for r in rows), default=0.0),
        "max_drawdown_pct": max_dd, "equity": equity,
        "by_strategy": sorted(by_strategy.values(), key=lambda x: x["net_r"], reverse=True),
    }


async def _dashboard_data(user_id: int):
    if db is None or market is None:
        raise web.HTTPServiceUnavailable(text="Trading service is not ready")
    recs = await db.user_follows(user_id, HISTORY_LIMIT)
    opened = [r for r in recs if r.get("status") == "OPEN"][:20]
    positions, gross_values, net_values = [], [], []
    for rec in opened:
        live = await live_follow_metrics(rec)
        if not live:
            continue
        entry = float(rec.get("follow_price") or 0)
        side = "LONG" if rec.get("direction") == "LONG" else "SHORT"
        net = float(live["net_pct"])
        gross = float(live["move_pct"])
        net_values.append(net); gross_values.append(gross)
        positions.append({"symbol": rec.get("symbol", "UNKNOWN"), "side": side,
            "timeframe": str(rec.get("timeframe", "?")), "net": net, "gross": gross,
            "entry": entry, "last": float(live["price"]),
            "sl": float(rec.get("sl") or 0), "tp": float(rec.get("tp") or 0),
            "age": f"{max(0, int((datetime.now(timezone.utc).timestamp()-live['age_ts'])/60))}m ago" if live.get("age_ts") else "time n/a"})
    # Riwayat: semua follow yang sudah selesai (TP/SL/Ambigu/Expired), lama -> baru untuk equity curve.
    closed = [_closed_trade_row(r) for r in recs if r.get("status") not in (None, "OPEN")]
    closed.sort(key=lambda r: r.get("closed_at") or "")
    summary = _history_summary(closed)
    summary["open_net_pct"] = sum(net_values)
    summary["total_pnl_pct"] = summary["net_pct"] + sum(net_values)   # realized + floating
    return {"positions": positions, "net": sum(net_values), "gross": sum(gross_values),
            "avg": sum(net_values)/len(net_values) if net_values else 0.0,
            "summary": summary, "history": list(reversed(closed)),
            "history_truncated": len(recs) >= HISTORY_LIMIT}


async def dashboard_home(request):
    # Root domain health/landing page. Personal live data is only exposed by
    # the signed /dashboard/{token} route linked from Discord /trade.
    html = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#07111f"><title>JustJinApps · Trading Dashboard</title><style>*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;background:radial-gradient(ellipse at top,#102c42 0,#07111f 58%);color:#e7f4ff;font:16px Inter,system-ui,sans-serif;padding:24px}.card{max-width:650px;width:100%;padding:clamp(24px,6vw,48px);border:1px solid #1a6571;border-radius:28px;background:linear-gradient(145deg,#102538dd,#0a1726e8);box-shadow:0 24px 80px #0006}.tag{display:inline-block;color:#54e9df;border:1px solid #1b7781;border-radius:999px;padding:7px 12px;font-size:12px;letter-spacing:.12em}h1{font-size:clamp(28px,6vw,44px);margin:20px 0 12px}p{color:#a9bed0;line-height:1.7}.hint{margin-top:28px;padding:16px;border-radius:16px;background:#07111f;border:1px solid #21374a;color:#d8e9f5}.brand{color:#51e6c2;font-weight:700;letter-spacing:.08em;font-size:13px}</style></head><body><main class="card"><div class="brand">JUSTJINAPPS / TRADING</div><div style="margin-top:24px"><span class="tag">PAPER TRADING · READ ONLY</span></div><h1>Live Trading Dashboard</h1><p>Web dashboard server is online. Untuk menjaga privasi, data posisi pribadi tidak ditampilkan di halaman umum ini.</p><div class="hint">Buka Discord dan jalankan <strong>/trade</strong>, lalu gunakan tautan dashboard personal yang dikirim oleh bot.</div></main></body></html>"""
    return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store"})


async def favicon_empty(request):
    return web.Response(status=204)


async def dashboard_page(request):
    if _valid_dashboard_token(request.match_info.get("token", "")) is None:
        raise web.HTTPForbidden(text="Invalid dashboard link")
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def dashboard_api(request):
    user_id = _valid_dashboard_token(request.match_info.get("token", ""))
    if user_id is None:
        raise web.HTTPForbidden(text="Invalid dashboard link")
    return web.json_response(await _dashboard_data(user_id), headers={"Cache-Control": "no-store"})


NOT_FOUND_TEXT = "404 - halaman tidak ada. Buka / untuk status server, atau pakai tautan dashboard dari Discord /trade."


@web.middleware
async def error_pages(request, handler):
    """404 yang jelas (bukan halaman kosong) dan tidak pernah membocorkan traceback."""
    try:
        return await handler(request)
    except web.HTTPNotFound:
        if request.path.startswith("/api/"):
            return web.json_response({"error": "not_found"}, status=404, headers={"Cache-Control": "no-store"})
        return web.Response(text=NOT_FOUND_TEXT, status=404, headers={"Cache-Control": "no-store"})


async def health(request):
    """Cek cepat dari browser/Railway: server web hidup, dan status bot Discord/Redis/Bybit."""
    return web.json_response({
        "ok": True,
        "discord": bot_state["discord"],
        "bot_ready": bot.is_ready(),
        "market": market is not None,
        "database": db is not None,
    }, headers={"Cache-Control": "no-store"})


async def start_dashboard_server():
    """Idempotent. Dipanggil dari main() SEBELUM login Discord, supaya URL Railway selalu hidup."""
    global _dashboard_runner
    if _dashboard_runner is not None:
        return
    app = web.Application(middlewares=[web.normalize_path_middleware(append_slash=False, remove_slash=True), error_pages])
    app.router.add_get("/", dashboard_home)
    app.router.add_get("/dashboard", dashboard_home)  # tanpa token -> landing, bukan 404
    app.router.add_get("/health", health)
    app.router.add_get("/healthz", health)
    app.router.add_get("/favicon.ico", favicon_empty)
    app.router.add_get("/dashboard/{token}", dashboard_page)
    app.router.add_get("/api/dashboard/{token}", dashboard_api)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    try:
        await site.start()
        _dashboard_runner = runner
        log.info("Live dashboard web server listening on 0.0.0.0:%s (routes: / /dashboard /health /dashboard/{token})", port)
    except OSError:
        await runner.cleanup()
        log.exception("Tidak bisa membuka port dashboard %s; pastikan PORT Railway tersedia", port)


def dashboard_url(user_id: int) -> Optional[str]:
    base = DASHBOARD_BASE_URL or ("https://" + os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip().strip("/"))
    if not base or base == "https://":
        return None
    return f"{base}/dashboard/{_dashboard_token(user_id)}"


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
    """Render a reference-inspired, mobile-first Modern Card PNG from the live /trade snapshot."""
    from PIL import Image, ImageDraw, ImageFont

    width = 1100
    pad = 48
    card_gap = 22
    card_h = 420
    shown = list(positions[:12])
    header_h = 168
    portfolio_h = 150
    footer_h = 68
    section_h = 62
    # Compute canvas from actual content so no position card is clipped or skipped.
    height = pad + header_h + 24 + len(shown) * card_h + max(0, len(shown)-1) * card_gap + section_h + portfolio_h + footer_h + pad
    im = Image.new("RGB", (width, height), (5, 13, 27))
    d = ImageDraw.Draw(im)
    try:
        regular = ImageFont.truetype("DejaVuSans.ttf", 24)
        small = ImageFont.truetype("DejaVuSans.ttf", 19)
        tiny = ImageFont.truetype("DejaVuSans.ttf", 16)
        medium = ImageFont.truetype("DejaVuSans-Bold.ttf", 25)
        title_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 31)
        pnl_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 59)
        metric_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 35)
    except OSError:
        regular = small = tiny = medium = title_font = pnl_font = metric_font = ImageFont.load_default()

    bg = (7, 22, 41)
    card = (8, 25, 46)
    inner = (5, 19, 37)
    border = (20, 67, 96)
    white = (239, 247, 255)
    muted = (139, 166, 194)
    green = (0, 224, 166)
    red = (255, 75, 111)
    cyan = (0, 199, 229)

    def rounded(box, radius=24, fill=card, outline=border, w=2):
        d.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=w)

    # Header / identity block
    y = pad
    rounded((pad, y, width-pad, y+header_h), 34, (6, 24, 43), (18, 107, 127), 3)
    d.ellipse((pad+24, y+28, pad+112, y+116), fill=(13, 67, 105), outline=(25, 151, 187), width=3)
    d.text((pad+42, y+48), "JJ", font=title_font, fill=white)
    d.text((pad+132, y+30), "JustJinApps Trading Bot", font=title_font, fill=white)
    d.text((pad+132, y+78), f"/trade  ·  Positions ({len(shown)})", font=regular, fill=muted)
    badge_w = 205
    bx = width-pad-badge_w-24
    rounded((bx, y+30, bx+badge_w, y+78), 13, (4, 49, 67), (0, 143, 175), 2)
    d.text((bx+18, y+43), "PAPER TRADING", font=small, fill=cyan)

    y += header_h + 24
    for idx, pos in enumerate(shown):
        net = float(pos.get("net", 0) or 0)
        gross = float(pos.get("gross", 0) or 0)
        accent = green if net >= 0 else red
        side = str(pos.get("side", "LONG")).upper()
        symbol = str(pos.get("symbol", "UNKNOWN"))
        tf = str(pos.get("timeframe", "?")).replace("m", "")
        # Full position card
        rounded((pad, y, width-pad, y+card_h), 30, card, border, 2)
        # header row
        d.ellipse((pad+28, y+25, pad+88, y+85), fill=(15, 57, 86), outline=(39, 111, 148), width=2)
        coin_mark = symbol[:1] if symbol else "?"
        d.text((pad+46, y+39), coin_mark, font=medium, fill=cyan)
        d.text((pad+108, y+35), symbol, font=title_font, fill=white)
        side_color = green if side == "LONG" else red
        tag_x = pad+108+min(320, int(d.textlength(symbol, font=title_font)))+18
        tag_x = min(tag_x, width-pad-350)
        rounded((tag_x, y+32, tag_x+112, y+78), 13, (7, 55, 55) if side == "LONG" else (65, 25, 47), (0, 124, 112) if side == "LONG" else (145, 43, 69), 2)
        d.text((tag_x+14, y+43), side, font=small, fill=side_color)
        d.text((tag_x+125, y+42), f"· {tf}m", font=regular, fill=muted)
        rounded((width-pad-150, y+31, width-pad-27, y+78), 18, (5, 68, 59), (0, 137, 106), 2)
        d.text((width-pad-128, y+43), "OPEN", font=small, fill=green)

        # PNL hero and entry/last column
        pnl_top = y+108
        rounded((pad+25, pnl_top, pad+535, y+292), 22, inner, (12, 46, 72), 1)
        d.text((pad+47, pnl_top+17), "NET PNL (EST.)", font=small, fill=muted)
        d.text((pad+42, pnl_top+48), f"{net:+.2f}%", font=pnl_font, fill=accent)
        d.text((pad+47, pnl_top+128), f"GROSS  {gross:+.2f}%", font=medium, fill=white)

        rx1, rx2 = pad+558, width-pad-25
        rounded((rx1, pnl_top, rx2, y+292), 22, inner, (12, 46, 72), 1)
        d.text((rx1+24, pnl_top+18), "ENTRY", font=small, fill=muted)
        d.text((rx1+24, pnl_top+49), f"{float(pos.get('entry', 0) or 0):,.10g}", font=metric_font, fill=white)
        d.line((rx1+24, pnl_top+94, rx2-24, pnl_top+94), fill=border, width=2)
        d.text((rx1+24, pnl_top+108), "LAST PRICE", font=small, fill=muted)
        d.text((rx1+24, pnl_top+137), f"{float(pos.get('last', 0) or 0):,.10g}", font=metric_font, fill=white)

        # SL / TP chips and age row
        chip_y = y+315
        rounded((pad+25, chip_y, pad+330, chip_y+58), 16, (55, 19, 39), (119, 40, 64), 2)
        d.text((pad+43, chip_y+17), f"SL  {float(pos.get('sl', 0) or 0):,.10g}", font=small, fill=red)
        rounded((pad+350, chip_y, pad+650, chip_y+58), 16, (4, 48, 48), (0, 111, 94), 2)
        d.text((pad+368, chip_y+17), f"TP  {float(pos.get('tp', 0) or 0):,.10g}", font=small, fill=green)
        age = str(pos.get("age", "time n/a"))
        # Convert Discord-relative timestamp text into compact card text.
        if age.startswith("<t:"):
            age = "FOLLOWED"
        d.text((pad+685, chip_y+18), age[:18], font=small, fill=muted)
        y += card_h + card_gap

    # Portfolio summary panel modeled after the supplied reference.
    if not shown:
        rounded((pad, y, width-pad, y+130), 24, card, border, 2)
        d.text((pad+28, y+28), "NO ACTIVE POSITIONS", font=medium, fill=muted)
        d.text((pad+28, y+72), "Follow a signal to see live PNL here.", font=regular, fill=white)
        y += 130 + 18

    rounded((pad, y, width-pad, y+portfolio_h), 26, card, border, 2)
    d.text((pad+30, y+24), "PORTFOLIO PNL (NET)", font=small, fill=muted)
    portfolio_color = green if total_net >= 0 else red
    d.text((pad+30, y+58), f"{total_net:+.2f}%", font=metric_font, fill=portfolio_color)
    # vertical divider and active count
    divider_x = width-pad-300
    d.line((divider_x, y+22, divider_x, y+portfolio_h-22), fill=border, width=2)
    d.text((divider_x+28, y+25), "ACTIVE POSITIONS", font=small, fill=muted)
    d.text((divider_x+28, y+61), f"{len(shown):02d}", font=metric_font, fill=white)
    d.text((pad+30, y+112), f"Gross {total_gross:+.2f}%   ·   Avg net/trade {avg_net:+.2f}%", font=tiny, fill=muted)

    footer_y = y + portfolio_h + 26
    d.ellipse((pad+2, footer_y+2, pad+42, footer_y+42), outline=(0, 139, 167), width=2)
    d.text((pad+14, footer_y+10), "J", font=small, fill=cyan)
    d.text((pad+56, footer_y+8), "JustJinApps Trading Bot", font=small, fill=muted)
    footer_text = "PAPER TRADING  ·  ROI ESTIMATE 1x  ·  NO REAL ORDERS"
    tw = d.textlength(footer_text, font=tiny)
    d.text((width-pad-tw, footer_y+12), footer_text, font=tiny, fill=cyan)

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
        try:
            await start_services()
        except Exception:
            log.exception("start_services gagal; dicoba lagi di scan berikutnya")
            return
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
    bot_state["discord"] = "ready"
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


@bot.tree.command(name="trade", description="Lihat PnL teks dan buka dashboard visual live")
async def trade_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    if db is None or market is None:
        await interaction.followup.send("Layanan belum siap, coba lagi sebentar.", ephemeral=True)
        return
    recs = await db.user_follows(interaction.user.id, 200)
    opened = [r for r in recs if r.get("status") == "OPEN"][:8]
    lines = []
    net_values, gross_values = [], []
    for rec in opened:
        live = await live_follow_metrics(rec)
        side = "LONG" if rec.get("direction") == "LONG" else "SHORT"
        entry = float(rec.get("follow_price") or 0)
        sl, tp = float(rec.get("sl") or 0), float(rec.get("tp") or 0)
        age_ts = _ts(rec.get("followed_at"))
        age = f"<t:{age_ts}:R>" if age_ts else "waktu tidak tersedia"
        if live:
            net, gross = live["net_pct"], live["move_pct"]
            net_values.append(net); gross_values.append(gross)
            pnl_icon = "🟢" if net >= 0 else "🔴"
            lines.append(f"{pnl_icon} **{rec.get('symbol','UNKNOWN')} · {side} · {rec.get('timeframe','?')}m** — `OPEN`\n"
                f"Net **{net:+.2f}%** · Gross **{gross:+.2f}%**\n"
                f"Entry `{fmt_price(entry)}` → Last `{fmt_price(live['price'])}`\n"
                f"SL `{fmt_price(sl)}` · TP `{fmt_price(tp)}` · {age}")
        else:
            lines.append(f"⚪ **{rec.get('symbol','UNKNOWN')} · {side} · {rec.get('timeframe','?')}m** — `OPEN`\n"
                f"Harga live belum tersedia · Entry `{fmt_price(entry)}` · SL `{fmt_price(sl)}` · TP `{fmt_price(tp)}`")
    net_total = sum(net_values); gross_total = sum(gross_values)
    avg = net_total / len(net_values) if net_values else 0.0
    message = ("**JUSTJINAPPS · TRADE SNAPSHOT**\n"
        f"Mode: `PAPER TRADING` · Posisi aktif: **{len(opened):02d}**\n"
        f"Portfolio Net PNL: **{net_total:+.2f}%** · Gross PNL: **{gross_total:+.2f}%** · Avg Net: **{avg:+.2f}%**\n\n"
        + ("\n\n".join(lines) if lines else "Belum ada posisi aktif. Gunakan tombol **Ikut Trade** pada sinyal untuk mulai memantau posisi."))
    url = dashboard_url(interaction.user.id)
    if url:
        message += f"\n\n🌐 **Dashboard visual live:** {url}\n_Refresh otomatis setiap 15 detik. Siapa pun yang memiliki link ini dapat melihat data posisi milikmu; jangan bagikan jika tidak ingin dibuka orang lain._"
    else:
        message += "\n\n⚠️ Link dashboard belum aktif. Atur `DASHBOARD_BASE_URL` ke domain publik Railway (contoh `https://nama-app.up.railway.app`) dan redeploy."
    await interaction.followup.send(message[:1990], ephemeral=True)


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
    await start_dashboard_server()  # web hidup dulu: URL Railway tidak 404 walau Discord/Redis/Bybit bermasalah
    async with bot:
        bot_state["discord"] = "connecting"
        try:
            await bot.start(DISCORD_TOKEN)
        except discord.LoginFailure:
            bot_state["discord"] = "login_failed"
            log.error("Login Discord GAGAL: DISCORD_TOKEN salah/di-reset. Perbaiki variable di Railway lalu redeploy. "
                      "Web tetap hidup; cek /health.")
            await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
