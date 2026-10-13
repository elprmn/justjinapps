import asyncio
import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
import numpy as np
import pandas as pd

log = logging.getLogger("signalbot.market")

# Host REST Bybit. Jika host pertama diblok (HTTP 403/451) bot otomatis mencoba host berikutnya.
# Bisa diganti lewat env BYBIT_REST_URLS (pisahkan dengan koma).
REST_HOSTS = [h.strip().rstrip("/") for h in os.getenv(
    "BYBIT_REST_URLS", "https://api.bybit.com,https://api.bytick.com").split(",") if h.strip()]
if not REST_HOSTS:
    REST_HOSTS = ["https://api.bybit.com"]
# Opsional: paksa URL WebSocket tertentu. Kosong = diturunkan dari host REST yang sedang berhasil.
WS_URL_OVERRIDE = os.getenv("BYBIT_WS_URL", "").strip()
BLOCKED_STATUSES = (403, 451)
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; signalbot/1.0)", "Accept": "application/json"}


class BybitBlockedError(RuntimeError):
    """Bybit menolak request karena IP/region server (HTTP 403/451)."""
# Strictly limited to short-term timeframes requested by the user.
INTERVALS = {"5": "5", "15": "15", "30": "30"}


def _env_float(name, default):
    try:
        return float(os.getenv(name, str(default)).strip())
    except (TypeError, ValueError):
        return float(default)


# ---------------- Konfigurasi Adaptive Scalping ----------------
TP_R = max(0.5, _env_float("TP_R", 1.5))                  # target awal momentum; wajib diuji dengan backtest
ADX_MIN = _env_float("ADX_MIN", 20)                       # ADX 30m minimal (filter sideways)
PULLBACK_LOOKBACK = int(_env_float("PULLBACK_LOOKBACK", 8))  # candle terakhir yang dicek untuk pullback
MAX_EXTENDED_ATR = _env_float("MAX_EXTENDED_ATR", 1.5)    # anti-chase: maks jarak close dari EMA20 (x ATR)
MIN_SETUP_SCORE = max(0, min(100, int(_env_float("MIN_SETUP_SCORE", 75))))
ANTI_CHASE_ENABLED = os.getenv("ANTI_CHASE_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
CHOP_FILTER_ENABLED = os.getenv("CHOP_FILTER_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
FALSE_BREAKOUT_FILTER_ENABLED = os.getenv("FALSE_BREAKOUT_FILTER_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
SL_BUFFER_ATR = 0.2                                       # buffer SL di luar swing pullback
SL_ATR_MIN, SL_ATR_MAX = 0.8, 2.5                         # batas jarak SL (x ATR)
FEE_PCT = _env_float("FEE_PCT", 0.11)                     # fee pulang-pergi (%)
SLIPPAGE_PCT = max(0.0, _env_float("SLIPPAGE_PCT", 0.04)) # estimasi slippage round-trip (%)
FUNDING_ESTIMATE = os.getenv("FUNDING_ESTIMATE", "1").strip().lower() not in ("0", "false", "no", "off")
MAX_FEE_R = _env_float("MAX_FEE_R", 0.25)                 # tolak setup jika fee > X R (SL terlalu sempit)
MAX_SPREAD_PCT = _env_float("MAX_SPREAD_PCT", 0.10)
BTC_FILTER = os.getenv("BTC_FILTER", "1").strip().lower() not in ("0", "false", "no", "off")
BTC_DUMP_ATR = _env_float("BTC_DUMP_ATR", 1.5)            # BTC 15m gerak > X ATR dalam 3 candle => blok arah berlawanan
NEWS_WINDOW_MIN = int(_env_float("NEWS_WINDOW_MIN", 30))
# Supported strategy names must match the router below; invalid values fail safe to adaptive.
SUPPORTED_STRATEGIES = {
    "breakout_retest", "bb_rsi", "both", "adaptive", "all", "squeeze",
    "stochrsi", "supertrend", "sr", "ema_vwap", "liquidity_sweep", "vwap_reclaim",
}
SIGNAL_STRATEGY = os.getenv("SIGNAL_STRATEGY", "adaptive").strip().lower()
if SIGNAL_STRATEGY not in SUPPORTED_STRATEGIES:
    log.warning("SIGNAL_STRATEGY=%r tidak dikenal; fallback ke adaptive", SIGNAL_STRATEGY)
    SIGNAL_STRATEGY = "adaptive"
ACTIVE_FILTER = os.getenv("ACTIVE_FILTER", "1").strip().lower() not in ("0", "false", "no", "off")
# Eksperimen reversal tidak dimasukkan ke routing default; aktifkan hanya setelah backtest.
ADAPTIVE_INCLUDE_SWEEP = os.getenv("ADAPTIVE_INCLUDE_SWEEP", "0").strip().lower() in ("1", "true", "yes", "on")
ADAPTIVE_INCLUDE_TREND_EXPERIMENTS = os.getenv("ADAPTIVE_INCLUDE_TREND_EXPERIMENTS", "0").strip().lower() in ("1", "true", "yes", "on")
BTC_FILTER_FAIL_CLOSED = os.getenv("BTC_FILTER_FAIL_CLOSED", "1").strip().lower() not in ("0", "false", "no", "off")
SR_HARD_BLOCK = os.getenv("SR_HARD_BLOCK", "1").strip().lower() not in ("0", "false", "no", "off")
REGIME_TREND_ADX = _env_float("REGIME_TREND_ADX", 23)
REGIME_RANGE_ADX = _env_float("REGIME_RANGE_ADX", 20)
SWING_LOOKBACK = max(6, int(_env_float("SWING_LOOKBACK", 12)))
BREAKOUT_LOOKBACK = max(8, int(_env_float("BREAKOUT_LOOKBACK", 20)))
BREAKOUT_RETEST_BARS = max(1, min(4, int(_env_float("BREAKOUT_RETEST_BARS", 3))))
BREAKOUT_MIN_VOLUME = max(0.8, _env_float("BREAKOUT_MIN_VOLUME", 1.1))
BB_RSI_OVERSOLD = min(40.0, max(10.0, _env_float("BB_RSI_OVERSOLD", 35)))
BB_RSI_OVERBOUGHT = max(60.0, min(90.0, _env_float("BB_RSI_OVERBOUGHT", 65)))
BB_MAX_ADX_30M = max(5.0, _env_float("BB_MAX_ADX_30M", 25))
SQUEEZE_PCTL = min(0.5, max(0.05, _env_float("SQUEEZE_PCTL", 0.25)))      # lebar BB dianggap squeeze jika di persentil ini (100 candle)
SQUEEZE_MIN_VOLUME = max(1.0, _env_float("SQUEEZE_MIN_VOLUME", 1.3))      # volume candle breakout vs SMA20
STOCH_OVERSOLD = _env_float("STOCH_OVERSOLD", 25)                         # StochRSI pullback: area jenuh jual (LONG)
STOCH_OVERBOUGHT = _env_float("STOCH_OVERBOUGHT", 75)                     # StochRSI pullback: area jenuh beli (SHORT)
ST_PERIOD = max(5, int(_env_float("ST_PERIOD", 10)))                      # Supertrend ATR period
ST_MULT = max(1.0, _env_float("ST_MULT", 1.5))                            # Supertrend multiplier (>=2 membuat garis terlalu jauh dari harga saat flip, SL melebihi SL_ATR_MAX)
ST_MIN_VOLUME = max(0.8, _env_float("ST_MIN_VOLUME", 1.2))                # volume candle flip vs SMA20
SR_LOOKBACK = max(60, int(_env_float("SR_LOOKBACK", 180)))               # jumlah candle untuk mencari level S/R
SR_MIN_TOUCHES = max(2, int(_env_float("SR_MIN_TOUCHES", 2)))            # minimal sentuhan (pivot) agar level dianggap valid
SR_CONFLUENCE = os.getenv("SR_CONFLUENCE", "1").strip().lower() not in ("0", "false", "no", "off")  # S/R memengaruhi skor semua strategi
ST_MIN_ADX = _env_float("ST_MIN_ADX", 18)                                 # ADX entry-TF minimum agar flip bukan noise sideways


def _parse_blackouts(raw):
    out = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            dt = datetime.fromisoformat(part.replace("Z", "+00:00"))
            out.append(dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc))
        except ValueError:
            log.warning("NEWS_BLACKOUTS: format salah diabaikan: %r (pakai ISO UTC, mis. 2026-10-14T12:30)", part)
    return out


# Jadwal news besar (UTC, ISO, pisah koma) diisi manual lewat env, mis. FOMC/CPI/NFP.
NEWS_BLACKOUTS = _parse_blackouts(os.getenv("NEWS_BLACKOUTS", ""))


def in_news_blackout(now=None):
    now = now or datetime.now(timezone.utc)
    w = timedelta(minutes=NEWS_WINDOW_MIN)
    return any(abs(now - t) <= w for t in NEWS_BLACKOUTS)



def fmt_price(value):
    """Harga ringkas: presisi menyesuaikan besar harga (hemat lebar embed)."""
    try:
        x = float(value)
        dec = 2 if abs(x) >= 1000 else 3 if abs(x) >= 100 else 4 if abs(x) >= 1 else 6 if abs(x) >= 0.01 else 8
        out = f"{x:,.{dec}f}"
        return out.rstrip("0").rstrip(".") if "." in out else out
    except (TypeError, ValueError):
        return str(value)


class BybitMarket:
    """Public Bybit V5 REST market data. No API key and no order execution."""
    def __init__(self):
        self.session = None
        self.sem = asyncio.Semaphore(8)
        self.ticker_cache = {}
        self.ws_connected = False
        self.ws_last_message = None
        self.ws_task = None
        self.host_idx = 0
        self._kline_cache = {}
        self._kline_locks = {}
        self._btc = None
        self._btc_lock = asyncio.Lock()

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=15, connect=8)
        connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=REQUEST_HEADERS)
        self.ws_task = asyncio.create_task(self._ticker_stream_loop(), name="bybit-ticker-websocket")

    async def close(self):
        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
            self.ws_task = None
        self.ws_connected = False
        if self.session:
            await self.session.close()
            self.session = None

    async def get_json(self, path, params=None):
        if self.session is None:
            raise RuntimeError("Market client belum aktif")
        n = len(REST_HOSTS)
        blocked = []
        payload = None
        async with self.sem:
            for offset in range(n):
                host = REST_HOSTS[(self.host_idx + offset) % n]
                async with self.session.get(host + path, params=params) as resp:
                    if resp.status in BLOCKED_STATUSES:
                        blocked.append(f"{host} -> HTTP {resp.status}")
                        continue
                    if resp.status == 429:
                        raise RuntimeError("Bybit rate limit; kurangi MAX_SCAN_SYMBOLS atau interval scan")
                    resp.raise_for_status()
                    payload = await resp.json(content_type=None)
                self.host_idx = (self.host_idx + offset) % n  # ingat host yang berhasil
                break
        if payload is None:
            raise BybitBlockedError(
                "Bybit menolak akses dari IP/region server ini (" + "; ".join(blocked) + "). "
                "Di Railway pindahkan region service ke Singapore (Southeast Asia) lalu redeploy.")
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {payload.get('retMsg')}")
        return payload.get("result", {})

    async def get_tickers(self):
        result = await self.get_json("/v5/market/tickers", {"category": "linear"})
        return result.get("list", [])

    async def get_ticker(self, symbol):
        cached = self.ticker_cache.get(symbol)
        if cached and time.time() - cached.get("_received_at", 0) <= 20:
            return cached
        result = await self.get_json("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        rows = result.get("list", [])
        if not rows:
            raise ValueError("Pair tidak ditemukan pada kategori linear.")
        return rows[0]

    def _ws_url(self):
        if WS_URL_OVERRIDE:
            return WS_URL_OVERRIDE
        host = REST_HOSTS[self.host_idx % len(REST_HOSTS)]
        return host.replace("https://api.", "wss://stream.", 1) + "/v5/public/linear"

    async def _ws_ping(self, ws):
        """Bybit mewajibkan ping JSON {"op":"ping"} setiap ~20 detik."""
        while True:
            await asyncio.sleep(20)
            await ws.send_json({"op": "ping"})

    async def _ticker_stream_loop(self):
        """Subscribe to live Bybit ticker updates; REST remains the fallback."""
        backoff = 5
        while True:
            try:
                # Refresh the universe on reconnect so newly liquid pairs are picked up.
                rows = await self.get_tickers()
                liquid = []
                for row in rows:
                    try:
                        if row.get("symbol", "").endswith("USDT") and float(row.get("turnover24h", 0) or 0) >= 1_000_000:
                            liquid.append(row)
                    except (TypeError, ValueError):
                        continue
                liquid.sort(key=lambda x: float(x.get("turnover24h", 0) or 0), reverse=True)
                symbols = [x["symbol"] for x in liquid[:80]]
                if not symbols:
                    await asyncio.sleep(15)
                    continue
                async with self.session.ws_connect(self._ws_url(), receive_timeout=45) as ws:
                    backoff = 5
                    self.ws_connected = True
                    for start in range(0, len(symbols), 40):
                        await ws.send_json({"op": "subscribe", "args": [f"tickers.{sym}" for sym in symbols[start:start + 40]]})
                    ping_task = asyncio.create_task(self._ws_ping(ws))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                payload = json.loads(msg.data)
                                topic = payload.get("topic", "")
                                data = payload.get("data")
                                if topic.startswith("tickers.") and isinstance(data, dict):
                                    symbol = topic.split(".", 1)[1]
                                    old = self.ticker_cache.get(symbol, {})
                                    merged = {**old, **data, "_received_at": time.time()}
                                    self.ticker_cache[symbol] = merged
                                    self.ws_last_message = time.time()
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE,
                                              aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                                break
                    finally:
                        ping_task.cancel()
                self.ws_connected = False
                # Refresh subscriptions regularly, not continuously reconnecting.
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.ws_connected = False
                raise
            except BybitBlockedError as exc:
                self.ws_connected = False
                log.warning("Bybit memblokir akses; coba lagi %ss: %s", backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 300)
            except Exception as exc:
                self.ws_connected = False
                log.warning("Feed ticker Bybit gagal/terputus; coba lagi %ss: %s", backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    def realtime_status(self):
        age = int(time.time() - self.ws_last_message) if self.ws_last_message else None
        return {"connected": self.ws_connected, "cached_symbols": len(self.ticker_cache), "last_message_age": age}

    async def get_klines(self, symbol, interval, limit=250):
        if interval not in INTERVALS:
            raise ValueError("Timeframe hanya 5, 15, atau 30 menit.")
        result = await self.get_json("/v5/market/kline", {
            "category": "linear", "symbol": symbol, "interval": interval, "limit": min(1000, limit)
        })
        rows = result.get("list", [])
        if not rows:
            raise ValueError("Data candle kosong.")
        rows = list(reversed(rows))
        df = pd.DataFrame(rows, columns=["start", "open", "high", "low", "close", "volume", "turnover"])
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["start"] = pd.to_numeric(df["start"], errors="coerce")
        df = df.dropna().reset_index(drop=True)
        # Always exclude the currently forming candle.
        now_ms = int(time.time() * 1000)
        interval_ms = int(interval) * 60_000
        if len(df) and now_ms < int(df.iloc[-1]["start"]) + interval_ms:
            df = df.iloc[:-1].reset_index(drop=True)
        return df

    async def get_klines_cached(self, symbol, interval, ttl=25):
        """Kline 250 candle dengan cache singkat; mengurangi request REST (15m/30m dipakai ulang antar TF)."""
        key = (symbol, interval)
        hit = self._kline_cache.get(key)
        if hit and time.time() - hit[0] <= ttl:
            return hit[1]
        lock = self._kline_locks.setdefault(key, asyncio.Lock())
        async with lock:
            hit = self._kline_cache.get(key)
            if hit and time.time() - hit[0] <= ttl:
                return hit[1]
            df = await self.get_klines(symbol, interval, 250)
            self._kline_cache[key] = (time.time(), df)
            if len(self._kline_cache) > 400:
                for k, _ in sorted(self._kline_cache.items(), key=lambda kv: kv[1][0])[:100]:
                    self._kline_cache.pop(k, None)
                    self._kline_locks.pop(k, None)
            return df

    async def btc_move_atr(self):
        """Gerak BTC 15m selama 3 candle terakhir dalam satuan ATR (negatif = turun). Dipakai sebagai filter pasar."""
        now = time.time()
        if self._btc and now - self._btc[0] <= 25:
            return self._btc[1]
        async with self._btc_lock:
            if self._btc and time.time() - self._btc[0] <= 25:
                return self._btc[1]
            df = add_indicators(await self.get_klines_cached("BTCUSDT", "15"))
            atr = float(df["atr"].iloc[-1])
            move = float(df["close"].iloc[-1] - df["close"].iloc[-4])
            val = move / atr if atr > 0 else 0.0
            self._btc = (time.time(), val)
            return val


def _supertrend(high, low, close, atr, mult):
    """Supertrend klasik. Mengembalikan (arah +1/-1, garis)."""
    hl2 = ((high + low) / 2.0).to_numpy(); a = atr.to_numpy(); c = close.to_numpy()
    ub, lb = hl2 + mult * a, hl2 - mult * a
    n = len(c); fu, fl = ub.copy(), lb.copy(); d = np.ones(n, dtype=int)
    for i in range(1, n):
        fu[i] = ub[i] if (ub[i] < fu[i - 1] or c[i - 1] > fu[i - 1]) else fu[i - 1]
        fl[i] = lb[i] if (lb[i] > fl[i - 1] or c[i - 1] < fl[i - 1]) else fl[i - 1]
        if d[i - 1] == -1:
            d[i] = 1 if c[i] > fu[i] else -1
        else:
            d[i] = -1 if c[i] < fl[i] else 1
    return d, np.where(d == 1, fl, fu)


def add_indicators(df):
    df = df.copy()
    close = df["close"]
    df["ema9"] = close.ewm(span=9, adjust=False).mean()
    df["ema21"] = close.ewm(span=21, adjust=False).mean()
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    # Stochastic RSI (14,14,3,3), skala 0-100
    rsi_lo, rsi_hi = df["rsi"].rolling(14).min(), df["rsi"].rolling(14).max()
    stoch = (df["rsi"] - rsi_lo) / (rsi_hi - rsi_lo).replace(0, np.nan) * 100
    df["stoch_k"] = stoch.rolling(3).mean()
    df["stoch_d"] = df["stoch_k"].rolling(3).mean()
    prev_close = close.shift(1)
    tr = pd.concat([
        df["high"] - df["low"], (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/14, adjust=False).mean()
    df["atr"] = atr
    st_atr = tr.ewm(alpha=1/ST_PERIOD, adjust=False).mean()
    df["st_dir"], df["st_line"] = _supertrend(df["high"], df["low"], close, st_atr, ST_MULT)
    # ADX (Wilder) untuk filter sideways
    up, down = df["high"].diff(), -df["low"].diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    plus_di = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df["adx"] = dx.ewm(alpha=1/14, adjust=False).mean()
    df["vol_sma20"] = df["volume"].rolling(20).mean()
    # Bollinger Bands (20, 2) for range/reversal scalping.
    df["bb_mid"] = close.rolling(20).mean()
    bb_std = close.rolling(20).std(ddof=0)
    df["bb_upper"] = df["bb_mid"] + 2.0 * bb_std
    df["bb_lower"] = df["bb_mid"] - 2.0 * bb_std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"].replace(0, np.nan)
    # Rolling VWAP; rolling window is appropriate for 24/7 crypto.
    typical = (df["high"] + df["low"] + close) / 3.0
    vol_sum = df["volume"].rolling(24).sum().replace(0, np.nan)
    df["vwap"] = (typical * df["volume"]).rolling(24).sum() / vol_sum
    return df


def _setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Metode Trend Pullback (satu arah). Kembalikan dict setup atau None.

    stage "TRIGGER"  = pullback ke EMA20 + candle konfirmasi (rejection/engulfing) sudah close -> SIGNAL
    stage "PULLBACK" = tren valid, harga sedang pullback ke EMA20, belum ada konfirmasi -> WATCH
    """
    long = direction == "LONG"
    sgn = 1 if long else -1

    def rej(reason):
        if diag is not None:
            diag.setdefault("reasons", []).append(f"{direction}: {reason}")
        return None

    # 1) Tren 30m (wajib): EMA200 + EMA20/50 searah, dan bukan sideways (ADX)
    if long:
        trend30 = m30["close"] > m30["ema200"] and m30["ema20"] > m30["ema50"] and m30["close"] > m30["ema50"]
    else:
        trend30 = m30["close"] < m30["ema200"] and m30["ema20"] < m30["ema50"] and m30["close"] < m30["ema50"]
    if not trend30:
        return rej("tren 30m tidak searah (EMA200/EMA20/EMA50)")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    if adx30 < ADX_MIN:
        return rej(f"ADX 30m {adx30:.0f} < {ADX_MIN:.0f} (sideways)")
    # 2) Konfirmasi 15m (wajib)
    if not ((m15["ema20"] > m15["ema50"]) if long else (m15["ema20"] < m15["ema50"])):
        return rej("EMA20/50 15m belum searah")
    # 3) Struktur TF entry
    e, p = df.iloc[-1], df.iloc[-2]
    seg = df.iloc[-PULLBACK_LOOKBACK:]
    entry_aligned = (e["ema20"] > e["ema50"]) if long else (e["ema20"] < e["ema50"])
    if not entry_aligned:
        return rej("EMA20/50 TF entry tidak searah")
    held = bool((seg["close"] >= seg["ema50"]).all()) if long else bool((seg["close"] <= seg["ema50"]).all())
    if not held:
        return rej("pullback menembus EMA50 (struktur rusak)")
    touched = bool((seg["low"] <= seg["ema20"]).any()) if long else bool((seg["high"] >= seg["ema20"]).any())
    if not touched:
        return rej("belum ada pullback ke EMA20")
    # 4) Anti-chase
    ext = (e["close"] - e["ema20"]) / atr * sgn
    if ext > MAX_EXTENDED_ATR:
        return rej(f"harga sudah lari {ext:.1f}x ATR dari EMA20 (anti-chase)")
    # 5) Filter BTC (bukan untuk BTC sendiri)
    btc_aligned = True
    if not is_btc and btc_move is not None:
        if (long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR):
            return rej(f"BTC 15m bergerak melawan ({btc_move:+.1f} ATR)")
        btc_aligned = (btc_move >= 0) if long else (btc_move <= 0)

    # 6) Trigger candle
    rng = float(e["high"] - e["low"])
    touch_recent = bool((df["low"].iloc[-2:] <= df["ema20"].iloc[-2:]).any()) if long else \
        bool((df["high"].iloc[-2:] >= df["ema20"].iloc[-2:]).any())
    back = e["close"] > e["ema20"] if long else e["close"] < e["ema20"]
    right_color = e["close"] > e["open"] if long else e["close"] < e["open"]
    if long:
        wick = float(min(e["open"], e["close"]) - e["low"])
        engulf = bool(p["close"] < p["open"] and e["close"] > p["open"] and e["open"] <= p["close"])
    else:
        wick = float(e["high"] - max(e["open"], e["close"]))
        engulf = bool(p["close"] > p["open"] and e["close"] < p["open"] and e["open"] >= p["close"])
    rejection = rng > 0 and wick / rng >= 0.4
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    rsi, rsi_prev = float(e["rsi"]), float(p["rsi"])
    if long:
        rsi_ok = 45 <= rsi <= 68 and rsi > rsi_prev and float(seg["rsi"].min()) <= 55
    else:
        rsi_ok = 32 <= rsi <= 55 and rsi < rsi_prev and float(seg["rsi"].max()) >= 45
    trigger = bool(touch_recent and back and right_color and (rejection or engulf) and vol_x >= 1.0 and rsi_ok)

    if not trigger:
        # WATCH hanya jika pullback masih aktif (sentuh EMA20 dalam 3 candle terakhir)
        active = bool((df["low"].iloc[-3:] <= df["ema20"].iloc[-3:]).any()) if long else \
            bool((df["high"].iloc[-3:] >= df["ema20"].iloc[-3:]).any())
        if not active:
            return rej("pullback sudah lewat tanpa konfirmasi")
    stage = "TRIGGER" if trigger else "PULLBACK"

    # 7) SL di luar swing pullback + buffer; TP = TP_R x risiko
    if long:
        sl = float(seg["low"].min()) - SL_BUFFER_ATR * atr
        if price - sl < SL_ATR_MIN * atr:
            sl = price - SL_ATR_MIN * atr
        risk = price - sl
    else:
        sl = float(seg["high"].max()) + SL_BUFFER_ATR * atr
        if sl - price < SL_ATR_MIN * atr:
            sl = price + SL_ATR_MIN * atr
        risk = sl - price
    if risk <= 0 or risk > SL_ATR_MAX * atr:
        return rej(f"pullback terlalu dalam (SL {risk / atr:.1f}x ATR)")
    tp = price + sgn * TP_R * risk
    risk_pct = risk / price * 100
    fee_r = FEE_PCT / risk_pct
    if fee_r > MAX_FEE_R:
        return rej(f"fee memakan {fee_r:.2f}R (SL {risk_pct:.2f}% terlalu sempit untuk scalping)")

    # Skor 0-100 (urutan prioritas, bukan probabilitas menang)
    score = 20                                                   # tren 30m valid
    score += 5 + min(5, max(0.0, adx30 - ADX_MIN) / 2)           # kekuatan tren (ADX)
    score += 10                                                  # 15m searah
    score += 5                                                   # EMA TF entry searah
    score += 10                                                  # pullback tertahan di atas/bawah EMA50
    score += 5 if touch_recent else 0                            # sentuhan EMA20 baru saja
    score += (8 if rejection else 0) + (7 if engulf else 0)      # candle konfirmasi
    score += 10 if vol_x >= 1.5 else 5 if vol_x >= 1.0 else 0    # volume
    score += 10 if rsi_ok else 0                                 # RSI naik/turun dari area pullback
    score += 5 if btc_aligned else 0                             # BTC searah / netral
    score = int(round(min(100, score)))

    reasons = [f"Tren 30m {'↑' if long else '↓'} · ADX {adx30:.0f}", "Pullback EMA20"]
    if trigger:
        kinds = [k for k, ok in (("rejection", rejection), ("engulfing", engulf)) if ok]
        reasons.append(" + ".join(kinds))
        reasons.append(f"Vol {vol_x:.1f}x")
        reasons.append(f"RSI {rsi:.0f}{'↑' if long else '↓'}")
    else:
        reasons.append("menunggu candle konfirmasi")
    return {"direction": direction, "stage": stage, "score": score, "sl": float(sl), "tp": float(tp),
            "rr": TP_R, "risk_pct": risk_pct, "reward_pct": risk_pct * TP_R, "fee_r": fee_r,
            "adx": adx30, "vol_x": vol_x, "rsi": rsi, "reasons": reasons,
            "btc_aligned": btc_aligned, "confirm15": True}


def _momentum_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Breakout/momentum entry pada candle tertutup, selaras tren 30m dan konfirmasi 15m."""
    long = direction == "LONG"
    sgn = 1 if long else -1
    def rej(reason):
        if diag is not None:
            diag.setdefault("reasons", []).append(f"{direction}: {reason}")
        return None
    e = df.iloc[-1]
    prev = df.iloc[-(MOMENTUM_LOOKBACK + 1):-1]
    if len(prev) < MOMENTUM_LOOKBACK:
        return rej("data breakout belum cukup")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    trend30 = (m30["close"] > m30["ema200"] and m30["ema20"] > m30["ema50"] and m30["close"] > m30["ema50"]) if long else \
              (m30["close"] < m30["ema200"] and m30["ema20"] < m30["ema50"] and m30["close"] < m30["ema50"])
    trend15 = (m15["ema20"] > m15["ema50"] and m15["close"] > m15["ema20"]) if long else \
              (m15["ema20"] < m15["ema50"] and m15["close"] < m15["ema20"])
    trend_entry = (e["ema20"] > e["ema50"] and e["close"] > e["ema20"]) if long else \
                  (e["ema20"] < e["ema50"] and e["close"] < e["ema20"])
    if not trend30 or adx30 < ADX_MIN:
        return rej(f"tren 30m/ADX tidak valid ({adx30:.0f})")
    if not trend15:
        return rej("konfirmasi momentum 15m tidak searah")
    if not trend_entry:
        return rej("EMA 5m/15m belum mendukung momentum")
    breakout_level = float(prev["high"].max()) if long else float(prev["low"].min())
    breakout = float(e["close"]) > breakout_level if long else float(e["close"]) < breakout_level
    if not breakout:
        return rej(f"belum breakout range {MOMENTUM_LOOKBACK} candle")
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    if vol_x < MOMENTUM_MIN_VOLUME:
        return rej(f"volume breakout {vol_x:.1f}x < {MOMENTUM_MIN_VOLUME:.1f}x")
    rsi = float(e["rsi"])
    if (long and not 52 <= rsi <= 75) or (not long and not 25 <= rsi <= 48):
        return rej(f"RSI momentum di luar rentang ({rsi:.0f})")
    ext = (float(e["close"]) - float(e["ema20"])) / atr * sgn
    if ext > MAX_EXTENDED_ATR:
        return rej(f"breakout terlalu jauh dari EMA20 ({ext:.1f} ATR)")
    btc_aligned = True
    if not is_btc and btc_move is not None:
        if (long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR):
            return rej(f"BTC 15m bergerak kuat melawan ({btc_move:+.1f} ATR)")
        btc_aligned = (btc_move >= 0) if long else (btc_move <= 0)
    swing = float(prev.tail(5)["low"].min()) if long else float(prev.tail(5)["high"].max())
    sl = swing - 0.15 * atr if long else swing + 0.15 * atr
    risk = price - sl if long else sl - price
    if risk <= 0 or risk > SL_ATR_MAX * atr or risk < 0.6 * atr:
        return rej(f"risiko breakout tidak valid ({risk / atr:.1f} ATR)")
    risk_pct = risk / price * 100
    fee_r = FEE_PCT / risk_pct if risk_pct else 999
    if fee_r > MAX_FEE_R:
        return rej(f"biaya transaksi terlalu besar ({fee_r:.2f}R)")
    tp = price + sgn * TP_R * risk
    score = 25 + min(15, max(0, adx30 - ADX_MIN) * 0.75)
    score += 15 if trend15 else 0
    score += 10 if vol_x >= 1.5 else 7
    score += 15 if (long and 55 <= rsi <= 68) or (not long and 32 <= rsi <= 45) else 8
    score += 10 if btc_aligned else 0
    score += 10 if ext <= 0.8 else 5
    score = int(min(100, round(score)))
    reasons = [f"Momentum breakout {MOMENTUM_LOOKBACK} candle", f"Tren 30m · ADX {adx30:.0f}", f"Volume {vol_x:.1f}x", f"RSI {rsi:.0f}"]
    return {"direction": direction, "stage": "TRIGGER", "score": score, "sl": float(sl), "tp": float(tp),
            "rr": TP_R, "risk_pct": risk_pct, "reward_pct": risk_pct * TP_R, "fee_r": fee_r,
            "adx": adx30, "vol_x": vol_x, "rsi": rsi, "reasons": reasons,
            "btc_aligned": btc_aligned, "confirm15": True, "strategy": "MOMENTUM"}


def _breakout_retest_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Breakout + retest: closed candle must revisit the broken range and hold it."""
    long = direction == "LONG"
    sgn = 1 if long else -1
    def rej(reason):
        if diag is not None:
            diag.setdefault("reasons", []).append(f"{direction}: {reason}")
        return None
    need = BREAKOUT_LOOKBACK + BREAKOUT_RETEST_BARS + 3
    if len(df) < need:
        return rej("data breakout/retest belum cukup")
    e = df.iloc[-1]
    prev_range = df.iloc[-(BREAKOUT_LOOKBACK + BREAKOUT_RETEST_BARS + 1):-BREAKOUT_RETEST_BARS]
    level = float(prev_range["high"].max()) if long else float(prev_range["low"].min())
    recent = df.iloc[-(BREAKOUT_RETEST_BARS + 1):-1]
    # Find a recent closed breakout candle, then a later retest candle (the current candle).
    broke = (recent["close"] > level).any() if long else (recent["close"] < level).any()
    retest = (float(e["low"]) <= level * 1.002 and float(e["close"]) > level) if long else \
             (float(e["high"]) >= level * 0.998 and float(e["close"]) < level)
    if not broke or not retest:
        return rej("belum ada breakout diikuti retest yang bertahan")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    trend30 = (m30["close"] > m30["ema200"] and m30["ema20"] > m30["ema50"] and m30["close"] > m30["ema50"]) if long else \
              (m30["close"] < m30["ema200"] and m30["ema20"] < m30["ema50"] and m30["close"] < m30["ema50"])
    trend15 = (m15["ema20"] > m15["ema50"] and m15["close"] >= m15["ema20"]) if long else \
              (m15["ema20"] < m15["ema50"] and m15["close"] <= m15["ema20"])
    if not trend30 or adx30 < ADX_MIN:
        return rej(f"tren 30m/ADX tidak valid ({adx30:.0f})")
    if not trend15:
        return rej("tren 15m tidak searah")
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    if vol_x < BREAKOUT_MIN_VOLUME:
        return rej(f"volume retest {vol_x:.1f}x < {BREAKOUT_MIN_VOLUME:.1f}x")
    # Close must hold the level with directional candle and avoid chasing an extended move.
    if (long and e["close"] <= e["open"]) or (not long and e["close"] >= e["open"]):
        return rej("candle retest belum mengonfirmasi arah")
    if abs(float(e["close"]) - level) > 1.0 * atr:
        return rej("harga terlalu jauh dari level retest")
    btc_aligned = True
    if not is_btc and btc_move is not None:
        if (long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR):
            return rej(f"BTC bergerak kuat melawan ({btc_move:+.1f} ATR)")
        btc_aligned = (btc_move >= 0) if long else (btc_move <= 0)
    swing = float(df.iloc[-(BREAKOUT_RETEST_BARS + 1):]["low"].min()) if long else \
            float(df.iloc[-(BREAKOUT_RETEST_BARS + 1):]["high"].max())
    sl = min(swing - 0.15 * atr, level - 0.15 * atr) if long else max(swing + 0.15 * atr, level + 0.15 * atr)
    risk = price - sl if long else sl - price
    if risk <= 0 or risk < 0.6 * atr or risk > SL_ATR_MAX * atr:
        return rej(f"jarak SL tidak valid ({risk / atr:.1f} ATR)")
    risk_pct = risk / price * 100
    fee_r = (FEE_PCT / risk_pct) if risk_pct else 999
    if fee_r > MAX_FEE_R:
        return rej(f"biaya transaksi terlalu besar ({fee_r:.2f}R)")
    rsi = float(e["rsi"])
    score = 35 + min(15, max(0, adx30 - ADX_MIN)) + (15 if vol_x >= 1.5 else 8) + (15 if btc_aligned else 5)
    score += 10 if abs(float(e["close"]) - level) <= 0.35 * atr else 5
    score = int(min(100, round(score)))
    return {"direction": direction, "stage": "TRIGGER", "score": score, "sl": float(sl),
            "tp": float(price + sgn * TP_R * risk), "rr": TP_R, "risk_pct": risk_pct,
            "reward_pct": risk_pct * TP_R, "fee_r": fee_r, "adx": adx30, "vol_x": vol_x,
            "rsi": rsi, "reasons": [f"Breakout {BREAKOUT_LOOKBACK} candle + retest", f"Tren 30m · ADX {adx30:.0f}", f"Volume {vol_x:.1f}x", f"Level {level:.8g}"],
            "btc_aligned": btc_aligned, "confirm15": True, "strategy": "BREAKOUT_RETEST"}


def _bb_rsi_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Bollinger + RSI mean reversion; deliberately avoids strong 30m trends."""
    long = direction == "LONG"
    sgn = 1 if long else -1
    def rej(reason):
        if diag is not None:
            diag.setdefault("reasons", []).append(f"{direction}: {reason}")
        return None
    e, p = df.iloc[-1], df.iloc[-2]
    if pd.isna(e["bb_lower"]) or pd.isna(e["bb_upper"]) or pd.isna(e["rsi"]):
        return rej("data Bollinger/RSI belum cukup")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    if adx30 > BB_MAX_ADX_30M:
        return rej(f"ADX 30m {adx30:.0f} terlalu kuat untuk mean reversion")
    # Require previous candle to close outside a band and latest candle to close back inside.
    if long:
        trigger = float(p["close"]) < float(p["bb_lower"]) and float(e["close"]) > float(e["bb_lower"])
        rsi_ok = float(p["rsi"]) <= BB_RSI_OVERSOLD and float(e["rsi"]) > float(p["rsi"])
        candle_ok = float(e["close"]) > float(e["open"])
    else:
        trigger = float(p["close"]) > float(p["bb_upper"]) and float(e["close"]) < float(e["bb_upper"])
        rsi_ok = float(p["rsi"]) >= BB_RSI_OVERBOUGHT and float(e["rsi"]) < float(p["rsi"])
        candle_ok = float(e["close"]) < float(e["open"])
    if not trigger:
        return rej("belum kembali masuk Bollinger Band setelah menyimpang")
    if not rsi_ok or not candle_ok:
        return rej("RSI/candle belum mengonfirmasi reversal")
    btc_aligned = True
    if not is_btc and btc_move is not None:
        if (long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR):
            return rej(f"BTC bergerak kuat melawan ({btc_move:+.1f} ATR)")
        btc_aligned = (btc_move >= 0) if long else (btc_move <= 0)
    # Stop beyond the reversal extreme; target is the BB midline or configured R target, whichever is closer.
    sl = (min(float(p["low"]), float(e["low"])) - 0.15 * atr) if long else \
         (max(float(p["high"]), float(e["high"])) + 0.15 * atr)
    risk = price - sl if long else sl - price
    if risk <= 0 or risk < 0.5 * atr or risk > SL_ATR_MAX * atr:
        return rej(f"jarak SL tidak valid ({risk / atr:.1f} ATR)")
    mid = float(e["bb_mid"])
    target_r = price + sgn * TP_R * risk
    tp = min(mid, target_r) if long else max(mid, target_r)
    reward = (tp - price) * sgn
    if reward <= 0 or reward / risk < 0.7:
        return rej("target ke BB mid terlalu dekat dibanding risiko")
    risk_pct = risk / price * 100
    fee_r = FEE_PCT / risk_pct if risk_pct else 999
    if fee_r > MAX_FEE_R:
        return rej(f"biaya transaksi terlalu besar ({fee_r:.2f}R)")
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    score = 40 + (20 if abs(float(p["rsi"]) - (BB_RSI_OVERSOLD if long else BB_RSI_OVERBOUGHT)) <= 5 else 10)
    score += 15 if vol_x >= 1.0 else 5
    score += 10 if btc_aligned else 0
    score = int(min(100, round(score)))
    return {"direction": direction, "stage": "TRIGGER", "score": score, "sl": float(sl), "tp": float(tp),
            "rr": reward / risk, "risk_pct": risk_pct, "reward_pct": reward / price * 100,
            "fee_r": fee_r, "adx": adx30, "vol_x": vol_x, "rsi": float(e["rsi"]),
            "reasons": ["Bollinger Band re-entry", f"RSI reversal {float(p['rsi']):.0f}→{float(e['rsi']):.0f}", f"ADX 30m {adx30:.0f}", f"Volume {vol_x:.1f}x"],
            "btc_aligned": btc_aligned, "confirm15": True, "strategy": "BB_RSI"}


def _strategy_reject(direction, diag, reason):
    if diag is not None:
        diag.setdefault("reasons", []).append(f"{direction}: {reason}")
    log.debug("Setup rejected direction=%s reason=%s", direction, reason)
    return None


def _risk_levels(direction, price, atr, swing_low, swing_high, rr=None):
    long = direction == "LONG"
    sl = float(swing_low - 0.15 * atr) if long else float(swing_high + 0.15 * atr)
    risk = price - sl if long else sl - price
    if risk <= 0 or risk < 0.45 * atr or risk > SL_ATR_MAX * atr:
        return None
    risk_pct = risk / price * 100 if price else 0.0
    fee_r = (FEE_PCT + SLIPPAGE_PCT) / risk_pct if risk_pct else 999.0
    if fee_r > MAX_FEE_R:
        return None
    r = float(rr or TP_R)
    tp = price + (1 if long else -1) * r * risk
    return sl, tp, risk, risk_pct, fee_r, r


def _ema_vwap_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Trend continuation after EMA20 pullback, confirmed by rolling VWAP and candle close."""
    long = direction == "LONG"; e, p = df.iloc[-1], df.iloc[-2]
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    trend30 = (m30["close"] > m30["ema200"] and m30["ema20"] > m30["ema50"]) if long else (m30["close"] < m30["ema200"] and m30["ema20"] < m30["ema50"])
    trend15 = (m15["ema20"] > m15["ema50"]) if long else (m15["ema20"] < m15["ema50"])
    if not trend30 or not trend15 or adx30 < max(15, REGIME_RANGE_ADX):
        return _strategy_reject(direction, diag, "EMA/VWAP perlu tren 15m/30m searah")
    if pd.isna(e["vwap"]) or pd.isna(e["ema20"]):
        return _strategy_reject(direction, diag, "data VWAP belum cukup")
    touched = (float(p["low"]) <= float(p["ema20"]) * 1.002 and float(e["close"]) > float(e["ema20"])) if long else (float(p["high"]) >= float(p["ema20"]) * .998 and float(e["close"]) < float(e["ema20"]))
    vwap_ok = float(e["close"]) >= float(e["vwap"]) if long else float(e["close"]) <= float(e["vwap"])
    candle_ok = float(e["close"]) > float(e["open"]) if long else float(e["close"]) < float(e["open"])
    if not (touched and vwap_ok and candle_ok):
        return _strategy_reject(direction, diag, "belum ada pullback EMA20 + konfirmasi VWAP")
    if not is_btc and btc_move is not None and ((long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR)):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan setup")
    sw = df.iloc[-8:]
    levels = _risk_levels(direction, price, atr, float(sw["low"].min()), float(sw["high"].max()), TP_R)
    if not levels: return _strategy_reject(direction, diag, "risiko/biaya EMA pullback tidak valid")
    sl,tp,risk,risk_pct,fee_r,rr=levels
    score = min(92, 68 + min(10, max(0, adx30-REGIME_RANGE_ADX)) + (7 if abs(price-float(e["vwap"])) <= .5*atr else 3) + (5 if float(e["volume"]) >= float(e["vol_sma20"] or 0) else 0))
    return {"direction":direction,"stage":"TRIGGER","score":int(score),"sl":sl,"tp":tp,"rr":rr,"risk_pct":risk_pct,"reward_pct":risk_pct*rr,"fee_r":fee_r,"adx":adx30,"vol_x":float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] else 0,"rsi":float(e["rsi"]),"reasons":["EMA20 pullback","VWAP searah","Tren 15m/30m"],"confirm15":True,"strategy":"EMA_PULLBACK_VWAP"}


def _vwap_reclaim_retest_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """VWAP reclaim followed by a retest/hold and a closed confirmation candle."""
    long = direction == "LONG"
    if len(df) < 30 or pd.isna(df["vwap"].iloc[-1]):
        return _strategy_reject(direction, diag, "data VWAP reclaim belum cukup")
    e, p = df.iloc[-1], df.iloc[-2]
    vwap, prev_vwap = float(e["vwap"]), float(p["vwap"]) if pd.notna(p["vwap"]) else float(e["vwap"])
    close, op = float(e["close"]), float(e["open"])
    # Retest candle may wick through VWAP, but must close back on the reclaimed side.
    touched = float(e["low"]) <= vwap + 0.20 * atr if long else float(e["high"]) >= vwap - 0.20 * atr
    held = close >= vwap + 0.05 * atr if long else close <= vwap - 0.05 * atr
    prev_side = float(p["close"]) <= prev_vwap if long else float(p["close"]) >= prev_vwap
    candle_ok = close > op and close >= float(e["low"]) + 0.60 * max(1e-12, float(e["high"])-float(e["low"])) if long else close < op and close <= float(e["low"]) + 0.40 * max(1e-12, float(e["high"])-float(e["low"]))
    # Prefer the retest to occur within the latest two completed candles.
    if not (prev_side and touched and held and candle_ok):
        return _strategy_reject(direction, diag, "belum ada reclaim VWAP lalu retest/hold dengan candle close kuat")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    trend15 = float(m15["ema20"]) > float(m15["ema50"]) if long else float(m15["ema20"]) < float(m15["ema50"])
    trend30 = float(m30["close"]) > float(m30["ema50"]) if long else float(m30["close"]) < float(m30["ema50"])
    if not (trend15 and trend30):
        return _strategy_reject(direction, diag, "VWAP reclaim berlawanan arah tren 15m/30m")
    if not is_btc and btc_move is not None and ((long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR)):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan VWAP reclaim")
    sw = df.iloc[-7:]
    levels = _risk_levels(direction, price, atr, float(sw["low"].min()), float(sw["high"].max()), TP_R)
    if not levels: return _strategy_reject(direction, diag, "risiko/biaya VWAP reclaim tidak valid")
    sl,tp,risk,risk_pct,fee_r,rr=levels
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    score = min(94, 66 + (8 if adx30 >= REGIME_RANGE_ADX else 2) + (6 if vol_x >= 1.2 else 2) + (5 if abs(close-vwap) <= .35*atr else 2))
    return {"direction":direction,"stage":"TRIGGER","score":int(score),"sl":sl,"tp":tp,"rr":rr,"risk_pct":risk_pct,"reward_pct":risk_pct*rr,"fee_r":fee_r,"adx":adx30,"vol_x":vol_x,"rsi":float(e["rsi"]),"reasons":["VWAP reclaim","Retest/hold VWAP","Candle close terkonfirmasi",f"Volume {vol_x:.1f}x"],"confirm15":True,"strategy":"VWAP_RECLAIM_RETEST"}


def _cluster_levels(df, atr, lookback, htf=False, k=3):
    """Level S/R dari pivot high/low (k candle kiri-kanan) yang dikelompokkan dalam toleransi 0.3 ATR.
    Candle terakhir tidak dipakai agar trigger tidak membentuk levelnya sendiri."""
    d = df.iloc[-(lookback + 1):-1]
    if len(d) < 2 * k + 5 or not atr or atr <= 0:
        return []
    hi, lo = d["high"], d["low"]
    ph = hi[hi.rolling(2 * k + 1, center=True).max() == hi]   # tepi window = NaN -> otomatis bukan pivot
    pl = lo[lo.rolling(2 * k + 1, center=True).min() == lo]
    pts = sorted(float(x) for x in list(ph.values) + list(pl.values))
    tol = 0.3 * atr
    groups = []
    for x in pts:
        if groups and x - (sum(groups[-1]) / len(groups[-1])) <= tol:
            groups[-1].append(x)
        else:
            groups.append([x])
    return [{"price": sum(g) / len(g), "touches": len(g), "htf": htf} for g in groups if len(g) >= SR_MIN_TOUCHES]


def _sr_levels(df, atr):
    """Gabungan level S/R entry-TF dan 30m (df.attrs['htf30']). Level yang cocok di dua TF diberi tanda htf."""
    cached = df.attrs.get("_sr")
    if cached is not None and cached[0] == len(df) and cached[1] == round(float(atr), 12):
        return cached[2]
    levels = _cluster_levels(df, atr, SR_LOOKBACK)
    h = df.attrs.get("htf30")
    if h is not None and len(h) > 40 and "atr" in h and pd.notna(h["atr"].iloc[-1]):
        for lv in _cluster_levels(h, float(h["atr"].iloc[-1]), 120, htf=True):
            for own in levels:
                if abs(own["price"] - lv["price"]) <= 0.4 * atr:
                    own["htf"] = True; own["touches"] += lv["touches"]; break
            else:
                levels.append(lv)
    levels.sort(key=lambda x: x["price"])
    df.attrs["_sr"] = (len(df), round(float(atr), 12), levels)
    return levels


def _sr_confluence(c, df, price, atr):
    """Padukan S/R ke setup mana pun: +skor jika entry dekat level pendukung, -skor jika TP terhalang level lawan."""
    if not SR_CONFLUENCE or c.get("strategy") == "SR_BOUNCE":
        return c
    levels = _sr_levels(df, atr)
    if not levels:
        return c
    long = c["direction"] == "LONG"
    tp = float(c["tp"]); note = None; delta = 0
    near = [lv for lv in levels if (lv["price"] <= price + 0.2 * atr and price - lv["price"] <= 0.8 * atr)] if long else \
           [lv for lv in levels if (lv["price"] >= price - 0.2 * atr and lv["price"] - price <= 0.8 * atr)]
    if near:
        best = max(near, key=lambda x: (x["htf"], x["touches"]))
        delta += 6 if best["htf"] else 4
        note = f"Dekat {'support' if long else 'resistance'} {best['touches']}x" + (" (30m)" if best["htf"] else "")
    gap = tp - price if long else price - tp
    block = [lv for lv in levels if (price + 0.3 * atr < lv["price"] < tp - 0.1 * atr)] if long else \
            [lv for lv in levels if (tp + 0.1 * atr < lv["price"] < price - 0.3 * atr)]
    block = [lv for lv in block if abs(lv["price"] - price) < 0.8 * gap]
    if block:
        htf_block = any(lv["htf"] for lv in block)
        delta -= 10 if htf_block else 6
        c["sr_obstructed"] = bool(htf_block and SR_HARD_BLOCK)
        c.setdefault("reasons", []).append(f"TP terhalang {'resistance' if long else 'support'} {block[0]['price']:.8g}")
    if note:
        c.setdefault("reasons", []).append(note)
    if delta:
        c["score"] = int(max(0, min(100, c["score"] + delta)))
        c["sr_adj"] = delta
    return c


def _sr_bounce_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Pantulan di level Support/Resistance yang sudah teruji (>=SR_MIN_TOUCHES pivot): candle menyentuh zona level,
    menolak (wick panjang atau engulfing) dan menutup kembali di sisi level. Target = TP_R atau level lawan terdekat."""
    long = direction == "LONG"; e, p = df.iloc[-1], df.iloc[-2]
    levels = _sr_levels(df, atr)
    if not levels:
        return _strategy_reject(direction, diag, "belum ada level S/R yang teruji")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    down30 = float(m30["close"]) < float(m30["ema200"]) and float(m30["ema20"]) < float(m30["ema50"])
    up30 = float(m30["close"]) > float(m30["ema200"]) and float(m30["ema20"]) > float(m30["ema50"])
    if adx30 >= REGIME_TREND_ADX and ((long and down30) or (not long and up30)):
        return _strategy_reject(direction, diag, "pantulan melawan tren 30m yang kuat")
    o, h, l, cl = float(e["open"]), float(e["high"]), float(e["low"]), float(e["close"])
    rng = h - l
    if rng <= 0:
        return _strategy_reject(direction, diag, "candle tanpa range")
    zone = None
    for lv in sorted(levels, key=lambda x: (x["htf"], x["touches"]), reverse=True):
        L = lv["price"]
        if long and l <= L + 0.25 * atr and l >= L - 0.8 * atr and cl > L and cl > o:
            zone = lv; break
        if not long and h >= L - 0.25 * atr and h <= L + 0.8 * atr and cl < L and cl < o:
            zone = lv; break
    if zone is None:
        return _strategy_reject(direction, diag, "harga belum menyentuh & menolak level S/R")
    L = zone["price"]
    wick = ((min(o, cl) - l) if long else (h - max(o, cl))) / rng
    engulf = (float(p["close"]) < float(p["open"]) and cl > float(p["open"])) if long else \
             (float(p["close"]) > float(p["open"]) and cl < float(p["open"]))
    if wick < 0.4 and not engulf:
        return _strategy_reject(direction, diag, "tidak ada penolakan (wick/engulfing) di level")
    prev_ok = bool((df["close"].iloc[-3:-1] > L - 0.5 * atr).all()) if long else bool((df["close"].iloc[-3:-1] < L + 0.5 * atr).all())
    if not prev_ok:
        return _strategy_reject(direction, diag, "level sudah ditembus candle sebelumnya")
    if _btc_blocks(long, btc_move, is_btc):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan setup")
    extreme = min(l, L - 0.2 * atr) if long else max(h, L + 0.2 * atr)
    levels_rr = _risk_levels(direction, price, atr, extreme, extreme, TP_R)
    if not levels_rr:
        return _strategy_reject(direction, diag, "risiko/biaya SR bounce tidak valid")
    sl, tp, risk, risk_pct, fee_r, rr = levels_rr
    opp = [lv["price"] for lv in levels if lv["price"] > price + 0.3 * atr] if long else [lv["price"] for lv in levels if lv["price"] < price - 0.3 * atr]
    capped = False
    if opp:
        nxt = min(opp) if long else max(opp)
        new_tp = min(tp, nxt - 0.1 * atr) if long else max(tp, nxt + 0.1 * atr)
        capped = new_tp != tp; tp = new_tp
    reward = (tp - price) if long else (price - tp)
    if reward < 1.0 * risk:
        return _strategy_reject(direction, diag, "ruang menuju level berikutnya kurang dari 1R")
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    aligned = (long and up30) or (not long and down30)
    score = min(92, 60 + min(10, zone["touches"] * 2) + (6 if zone["htf"] else 0) + (6 if wick >= 0.55 else 2)
                + (4 if vol_x >= 1.0 else 0) + (4 if aligned else 0))
    return {"direction": direction, "stage": "TRIGGER", "score": int(score), "sl": sl, "tp": float(tp), "rr": reward / risk,
            "risk_pct": risk_pct, "reward_pct": reward / price * 100, "fee_r": fee_r, "adx": adx30, "vol_x": vol_x,
            "rsi": float(e["rsi"]),
            "reasons": [f"{'Support' if long else 'Resistance'} {L:.8g} ({zone['touches']}x{' · 30m' if zone['htf'] else ''})",
                        "Penolakan wick/engulfing", f"Target {'resistance' if long else 'support'} berikutnya" if capped else f"Target {TP_R:g}R"],
            "confirm15": True, "strategy": "SR_BOUNCE"}


def _btc_blocks(long, btc_move, is_btc):
    return (not is_btc and btc_move is not None and
            ((long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR)))


def _stochrsi_pullback_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Tren naik/turun rapi (EMA 9>21>50), harga pullback ke EMA21, lalu StochRSI berbalik dari area jenuh
    searah tren (K memotong D). Metode continuation, bukan tebak pembalikan."""
    long = direction == "LONG"; e, p = df.iloc[-1], df.iloc[-2]
    if pd.isna(e["stoch_k"]) or pd.isna(e["stoch_d"]) or pd.isna(p["stoch_k"]) or pd.isna(p["stoch_d"]):
        return _strategy_reject(direction, diag, "data StochRSI belum cukup")
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    if long:
        trend = e["ema9"] > e["ema21"] > e["ema50"] and m15["ema20"] > m15["ema50"] and m30["close"] > m30["ema50"]
    else:
        trend = e["ema9"] < e["ema21"] < e["ema50"] and m15["ema20"] < m15["ema50"] and m30["close"] < m30["ema50"]
    if not trend or adx30 < 15:
        return _strategy_reject(direction, diag, f"tren EMA 9/21/50 + 15m/30m belum searah (ADX30 {adx30:.0f})")
    k, d, pk, pd_ = float(e["stoch_k"]), float(e["stoch_d"]), float(p["stoch_k"]), float(p["stoch_d"])
    recent = df.iloc[-4:]
    if long:
        extreme = float(recent["stoch_k"].min()) <= STOCH_OVERSOLD
        cross = pk <= pd_ and k > d and k < 60
        touched = float(recent["low"].min()) <= float(e["ema21"]) * 1.003
        candle_ok = float(e["close"]) > float(e["open"]) and float(e["close"]) > float(e["ema21"])
    else:
        extreme = float(recent["stoch_k"].max()) >= STOCH_OVERBOUGHT
        cross = pk >= pd_ and k < d and k > 40
        touched = float(recent["high"].max()) >= float(e["ema21"]) * 0.997
        candle_ok = float(e["close"]) < float(e["open"]) and float(e["close"]) < float(e["ema21"])
    if not extreme:
        return _strategy_reject(direction, diag, "StochRSI belum menyentuh area jenuh saat pullback")
    if not cross:
        return _strategy_reject(direction, diag, "StochRSI K belum memotong D searah tren")
    if not touched or not candle_ok:
        return _strategy_reject(direction, diag, "belum ada pullback ke EMA21 + candle konfirmasi")
    if _btc_blocks(long, btc_move, is_btc):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan setup")
    sw = df.iloc[-6:]
    levels = _risk_levels(direction, price, atr, float(sw["low"].min()), float(sw["high"].max()), TP_R)
    if not levels: return _strategy_reject(direction, diag, "risiko/biaya StochRSI pullback tidak valid")
    sl, tp, risk, risk_pct, fee_r, rr = levels
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    score = min(90, 64 + min(8, max(0, adx30 - 15) / 2) + (6 if vol_x >= 1.0 else 2) + (6 if abs(k - d) >= 5 else 2) + (4 if float(e["adx"]) >= 20 else 0))
    return {"direction": direction, "stage": "TRIGGER", "score": int(score), "sl": sl, "tp": tp, "rr": rr,
            "risk_pct": risk_pct, "reward_pct": risk_pct * rr, "fee_r": fee_r, "adx": adx30, "vol_x": vol_x,
            "rsi": float(e["rsi"]), "reasons": [f"StochRSI {pk:.0f}→{k:.0f} cross searah", "Pullback ke EMA21", "EMA 9/21/50 + tren 15m/30m"],
            "confirm15": True, "strategy": "STOCHRSI_PULLBACK"}


def _supertrend_flip_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Supertrend berbalik arah pada candle terakhir, searah EMA200 + tren 15m/30m, dengan volume dan ADX cukup.
    SL di garis Supertrend baru; setup ditolak jika garis terlalu jauh (SL > SL_ATR_MAX)."""
    long = direction == "LONG"; e, p = df.iloc[-1], df.iloc[-2]
    want, was = (1, -1) if long else (-1, 1)
    if int(e["st_dir"]) != want or int(p["st_dir"]) != was:
        return _strategy_reject(direction, diag, "Supertrend belum berbalik arah di candle terakhir")
    if long:
        trend_ok = e["close"] > e["ema200"] and m15["ema20"] > m15["ema50"] and m30["close"] > m30["ema200"]
        candle_ok = float(e["close"]) > float(e["open"])
    else:
        trend_ok = e["close"] < e["ema200"] and m15["ema20"] < m15["ema50"] and m30["close"] < m30["ema200"]
        candle_ok = float(e["close"]) < float(e["open"])
    if not trend_ok:
        return _strategy_reject(direction, diag, "flip melawan EMA200/tren 15m/30m")
    rng = float(e["high"] - e["low"]); body = abs(float(e["close"]) - float(e["open"]))
    if not candle_ok or rng <= 0 or body / rng < 0.5:
        return _strategy_reject(direction, diag, "candle flip lemah / tidak searah")
    adx = float(e["adx"]) if pd.notna(e["adx"]) else 0.0
    if adx < ST_MIN_ADX:
        return _strategy_reject(direction, diag, f"ADX {adx:.0f} < {ST_MIN_ADX:.0f} (rawan whipsaw sideways)")
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    if vol_x < ST_MIN_VOLUME:
        return _strategy_reject(direction, diag, f"volume flip {vol_x:.1f}x < {ST_MIN_VOLUME:.1f}x")
    if _btc_blocks(long, btc_move, is_btc):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan setup")
    line = float(e["st_line"])
    levels = _risk_levels(direction, price, atr, line, line, TP_R)  # SL = garis Supertrend -/+ 0.15 ATR
    if not levels: return _strategy_reject(direction, diag, "garis Supertrend terlalu jauh / biaya tidak valid")
    sl, tp, risk, risk_pct, fee_r, rr = levels
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    score = min(90, 64 + (8 if vol_x >= 1.6 else 4) + (6 if body / rng >= 0.7 else 2) + min(8, max(0, adx - ST_MIN_ADX) / 2) + (4 if adx30 >= 20 else 0))
    return {"direction": direction, "stage": "TRIGGER", "score": int(score), "sl": sl, "tp": tp, "rr": rr,
            "risk_pct": risk_pct, "reward_pct": risk_pct * rr, "fee_r": fee_r, "adx": adx30, "vol_x": vol_x,
            "rsi": float(e["rsi"]), "reasons": [f"Supertrend flip {'naik' if long else 'turun'}", f"Searah EMA200 · ADX {adx:.0f}", f"Volume {vol_x:.1f}x"],
            "confirm15": True, "strategy": "SUPERTREND_FLIP"}


def _squeeze_breakout_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Bollinger Squeeze Breakout: volatilitas menyempit lalu candle momentum menutup di luar band
    searah tren 15m/30m, dengan lonjakan volume. Metode momentum (bukan counter-trend)."""
    long = direction == "LONG"; e = df.iloc[-1]
    if len(df) < 110 or pd.isna(e["bb_width"]) or pd.isna(e["bb_upper"]):
        return _strategy_reject(direction, diag, "data squeeze belum cukup")
    width = df["bb_width"]
    thr = float(width.iloc[-101:-1].quantile(SQUEEZE_PCTL))
    squeezed = int((width.iloc[-7:-1] <= thr).sum())
    if squeezed < 3:
        return _strategy_reject(direction, diag, f"belum ada squeeze Bollinger ({squeezed}/6 candle sempit)")
    rng = float(e["high"] - e["low"])
    body = abs(float(e["close"]) - float(e["open"]))
    if long:
        broke = float(e["close"]) > float(e["bb_upper"]) and float(e["close"]) > float(e["open"])
        trend_ok = (m15["ema20"] > m15["ema50"] and m30["close"] > m30["ema50"])
    else:
        broke = float(e["close"]) < float(e["bb_lower"]) and float(e["close"]) < float(e["open"])
        trend_ok = (m15["ema20"] < m15["ema50"] and m30["close"] < m30["ema50"])
    if not broke:
        return _strategy_reject(direction, diag, "candle belum menutup di luar Bollinger Band")
    if rng <= 0 or body / rng < 0.55:
        return _strategy_reject(direction, diag, "body candle breakout lemah (banyak sumbu)")
    if not trend_ok:
        return _strategy_reject(direction, diag, "breakout tidak searah tren 15m/30m")
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    if vol_x < SQUEEZE_MIN_VOLUME:
        return _strategy_reject(direction, diag, f"volume breakout {vol_x:.1f}x < {SQUEEZE_MIN_VOLUME:.1f}x")
    if abs(float(e["close"]) - float(e["bb_mid"])) > 3.0 * atr:
        return _strategy_reject(direction, diag, "breakout sudah terlalu extended (jangan kejar)")
    if not is_btc and btc_move is not None and ((long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR)):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan setup")
    sw = df.iloc[-3:]
    levels = _risk_levels(direction, price, atr, float(sw["low"].min()), float(sw["high"].max()), TP_R)
    if not levels: return _strategy_reject(direction, diag, "risiko/biaya squeeze breakout tidak valid")
    sl, tp, risk, risk_pct, fee_r, rr = levels
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    score = min(90, 64 + (8 if vol_x >= 1.8 else 4) + (6 if squeezed >= 5 else 2) + (6 if body / rng >= 0.7 else 2) + (4 if adx30 >= REGIME_RANGE_ADX else 0))
    return {"direction": direction, "stage": "TRIGGER", "score": int(score), "sl": sl, "tp": tp, "rr": rr,
            "risk_pct": risk_pct, "reward_pct": risk_pct * rr, "fee_r": fee_r, "adx": adx30, "vol_x": vol_x,
            "rsi": float(e["rsi"]), "reasons": [f"Squeeze {squeezed}/6 candle", f"Breakout BB + volume {vol_x:.1f}x", "Searah tren 15m/30m"],
            "confirm15": True, "strategy": "SQUEEZE_BREAKOUT"}


def _liquidity_sweep_setup(direction, df, m15, m30, price, atr, btc_move, is_btc, diag):
    """Sweep of prior swing liquidity followed by a close back inside the prior range."""
    long = direction == "LONG"; e = df.iloc[-1]
    prior = df.iloc[-(SWING_LOOKBACK+1):-1]
    if len(prior) < SWING_LOOKBACK: return _strategy_reject(direction, diag, "data swing belum cukup")
    low_level, high_level = float(prior["low"].min()), float(prior["high"].max())
    rng = max(1e-12, float(e["high"]) - float(e["low"]))
    body_ratio = abs(float(e["close"]) - float(e["open"])) / rng
    close_location = (float(e["close"]) - float(e["low"])) / rng
    vol_x = float(e["volume"] / e["vol_sma20"]) if pd.notna(e["vol_sma20"]) and e["vol_sma20"] > 0 else 0.0
    # A valid sweep must pierce liquidity then reclaim it with a meaningful close, not just a long wick.
    reclaim_buffer = 0.05 * atr
    if long:
        swept = float(e["low"]) < low_level
        reclaimed = float(e["close"]) >= low_level + reclaim_buffer
        candle_ok = float(e["close"]) > float(e["open"]) and close_location >= 0.60
    else:
        swept = float(e["high"]) > high_level
        reclaimed = float(e["close"]) <= high_level - reclaim_buffer
        candle_ok = float(e["close"]) < float(e["open"]) and close_location <= 0.40
    trigger = swept and reclaimed and candle_ok and body_ratio >= 0.30 and vol_x >= 0.80
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    if not swept: return _strategy_reject(direction, diag, "belum menyapu swing high/low sebelumnya")
    if not reclaimed: return _strategy_reject(direction, diag, "sweep belum reclaim level dengan buffer 0.05 ATR")
    if not candle_ok or body_ratio < 0.30: return _strategy_reject(direction, diag, "konfirmasi candle sweep lemah (close location/body tidak cukup)")
    if vol_x < 0.80: return _strategy_reject(direction, diag, f"volume sweep terlalu rendah ({vol_x:.1f}x SMA20)")
    if not trigger: return _strategy_reject(direction, diag, "sweep belum terkonfirmasi")
    if adx30 > REGIME_TREND_ADX + 8:
        return _strategy_reject(direction, diag, "tren 30m terlalu kuat untuk reversal sweep")
    if not is_btc and btc_move is not None and ((long and btc_move <= -BTC_DUMP_ATR) or (not long and btc_move >= BTC_DUMP_ATR)):
        return _strategy_reject(direction, diag, "gerak BTC kuat melawan setup")
    levels = _risk_levels(direction, price, atr, float(min(e["low"], low_level)) if long else float(prior["low"].min()), float(prior["high"].max()) if long else float(max(e["high"], high_level)), min(TP_R, 1.25))
    if not levels: return _strategy_reject(direction, diag, "risiko/biaya liquidity sweep tidak valid")
    sl,tp,risk,risk_pct,fee_r,rr=levels
    score = min(90, 70 + (8 if abs(float(e["close"])- (low_level if long else high_level)) <= .35*atr else 3) + (5 if adx30 <= REGIME_RANGE_ADX else 0))
    return {"direction":direction,"stage":"TRIGGER","score":int(score),"sl":sl,"tp":tp,"rr":rr,"risk_pct":risk_pct,"reward_pct":risk_pct*rr,"fee_r":fee_r,"adx":adx30,"vol_x":vol_x,"rsi":float(e["rsi"]),"reasons":["Sweep swing high/low","Reclaim level + buffer 0.05 ATR","Body/close location terkonfirmasi",f"Volume {vol_x:.1f}x"],"confirm15":True,"strategy":"LIQUIDITY_SWEEP"}


def _select_strategy_functions(adx30, trend_up, trend_down):
    """Select a small, testable core by regime; experimental strategies are opt-in."""
    if SIGNAL_STRATEGY == "adaptive":
        if adx30 >= REGIME_TREND_ADX and (trend_up or trend_down):
            methods = [_breakout_retest_setup, _ema_vwap_setup, _vwap_reclaim_retest_setup]
            if ADAPTIVE_INCLUDE_TREND_EXPERIMENTS:
                methods.extend([_stochrsi_pullback_setup, _supertrend_flip_setup])
            return tuple(methods), "TREND"
        if adx30 <= REGIME_RANGE_ADX:
            methods = [_bb_rsi_setup]
            if ADAPTIVE_INCLUDE_SWEEP:
                methods.append(_liquidity_sweep_setup)
            return tuple(methods), "RANGE"
        methods = [_breakout_retest_setup, _bb_rsi_setup, _ema_vwap_setup, _vwap_reclaim_retest_setup] if ACTIVE_FILTER else [_breakout_retest_setup, _bb_rsi_setup]
        if ADAPTIVE_INCLUDE_SWEEP:
            methods.append(_liquidity_sweep_setup)
        return tuple(methods), "TRANSITION"
    methods = {
        "breakout_retest": (_breakout_retest_setup,),
        "bb_rsi": (_bb_rsi_setup,),
        "both": (_breakout_retest_setup, _bb_rsi_setup),
        "ema_vwap": (_ema_vwap_setup,),
        "vwap_reclaim": (_vwap_reclaim_retest_setup,),
        "liquidity_sweep": (_liquidity_sweep_setup,),
        "squeeze": (_squeeze_breakout_setup,),
        "stochrsi": (_stochrsi_pullback_setup,),
        "supertrend": (_supertrend_flip_setup,),
        "sr": (_sr_bounce_setup,),
        "all": (_breakout_retest_setup, _bb_rsi_setup, _ema_vwap_setup, _vwap_reclaim_retest_setup, _squeeze_breakout_setup,
                _liquidity_sweep_setup, _stochrsi_pullback_setup, _supertrend_flip_setup, _sr_bounce_setup),
    }.get(SIGNAL_STRATEGY, (_breakout_retest_setup, _bb_rsi_setup))
    return methods, "MANUAL"


async def analyze_symbol(client: BybitMarket, symbol: str, entry_tf: str = "5", ticker=None, diag=None) -> Optional[dict]:
    """Analisis adaptive scalping. `diag` menyimpan alasan penolakan setup."""
    if entry_tf not in INTERVALS:
        raise ValueError("Timeframe hanya 5, 15, atau 30 menit.")

    def reject(reason):
        if diag is not None:
            diag.setdefault("reasons", []).insert(0, reason)
        log.info("Setup rejected symbol=%s tf=%s reason=%s", symbol, entry_tf, reason)
        return None

    diag_ctx = diag if diag is not None else {"reasons": []}

    if in_news_blackout():
        return reject(f"jendela news besar (±{NEWS_WINDOW_MIN} menit)")
    tfs = list(dict.fromkeys([entry_tf, "15", "30"]))
    data = await asyncio.gather(*(client.get_klines_cached(symbol, tf) for tf in tfs))
    frames = {tf: add_indicators(df) for tf, df in zip(tfs, data)}
    if any(len(df) < 205 for df in frames.values()):
        return reject("data candle belum cukup (butuh 205+ candle)")
    df = frames[entry_tf]
    if entry_tf != "30":
        df.attrs["htf30"] = frames["30"]   # dipakai level S/R 30m
    m15, m30 = frames["15"].iloc[-1], frames["30"].iloc[-1]
    price = float(df["close"].iloc[-1])
    atr = float(df["atr"].iloc[-1]) if pd.notna(df["atr"].iloc[-1]) else 0.0
    if not atr or price <= 0:
        return reject("ATR tidak valid")

    ticker = ticker or await client.get_ticker(symbol)
    bid, ask = float(ticker.get("bid1Price", 0) or 0), float(ticker.get("ask1Price", 0) or 0)
    spread_pct = ((ask - bid) / ((ask + bid) / 2) * 100) if bid > 0 and ask >= bid else 999.0
    turnover = float(ticker.get("turnover24h", 0) or 0)
    if spread_pct > MAX_SPREAD_PCT or turnover < 1_000_000:
        return reject(f"likuiditas kurang (spread {spread_pct:.3f}%, turnover {turnover:,.0f})")
    atr_pct = atr / price * 100
    if atr_pct < 0.05 or atr_pct > 3.0:
        return reject(f"volatilitas di luar batas (ATR {atr_pct:.2f}%)")

    is_btc = symbol == "BTCUSDT"
    btc_move = None
    if BTC_FILTER and not is_btc:
        try:
            btc_move = await client.btc_move_atr()
        except Exception as exc:
            log.warning("Data filter BTC gagal: %s", exc)
            if BTC_FILTER_FAIL_CLOSED:
                return reject("filter BTC tidak tersedia; entry altcoin ditolak demi keamanan")

    # Adaptive regime routing: trend continuation in strong trends, mean reversion/sweep in ranges.
    adx30 = float(m30["adx"]) if pd.notna(m30["adx"]) else 0.0
    trend_up = float(m30["close"]) > float(m30["ema200"]) and float(m30["ema20"]) > float(m30["ema50"])
    trend_down = float(m30["close"]) < float(m30["ema200"]) and float(m30["ema20"]) < float(m30["ema50"])
    strategy_fns, regime = _select_strategy_functions(adx30, trend_up, trend_down)
    cands = [c for fn in strategy_fns for c in
             (fn(d, df, m15, m30, price, atr, btc_move, is_btc, diag_ctx) for d in ("LONG", "SHORT")) if c]
    if not cands:
        rs = diag_ctx.get("reasons", [])
        useful = [r for r in rs if "tren 30m tidak searah" not in r]
        summary = (useful or rs)[:3]
        if diag is not None:
            diag["reasons"] = summary
        log.info("No setup symbol=%s tf=%s regime=%s reasons=%s", symbol, entry_tf, regime, " | ".join(summary) if summary else "no strategy trigger")
        return None
    cands = [_sr_confluence(c, df, price, atr) for c in cands]   # S/R sebagai konfirmasi semua strategi
    # Hard safety gates: avoid chasing stretched entries and trend setups in choppy regimes.
    filtered = []
    ema20 = float(df["ema20"].iloc[-1]) if pd.notna(df["ema20"].iloc[-1]) else price
    vwap_now = float(df["vwap"].iloc[-1]) if pd.notna(df["vwap"].iloc[-1]) else ema20
    extension = min(abs(price-ema20), abs(price-vwap_now)) / atr if atr > 0 else 999.0
    cross_count = int(((df["ema20"] > df["ema50"]) != (df["ema20"].shift(1) > df["ema50"].shift(1))).tail(20).sum())
    close_series = df["close"].tail(12).astype(float)
    net_move = abs(float(close_series.iloc[-1]) - float(close_series.iloc[0]))
    path_move = float(close_series.diff().abs().sum())
    efficiency = net_move / path_move if path_move > 0 else 0.0
    for c in cands:
        strategy = c.get("strategy", "")
        if ANTI_CHASE_ENABLED and extension > MAX_EXTENDED_ATR and strategy not in ("BB_RSI", "LIQUIDITY_SWEEP", "SR_BOUNCE"):
            log.info("Setup rejected symbol=%s tf=%s strategy=%s reason=anti-chase extension %.2f ATR", symbol, entry_tf, strategy, extension)
            if diag is not None: diag.setdefault("reasons", []).append(f"{strategy}: anti-chase ({extension:.2f} ATR dari EMA/VWAP)")
            continue
        trend_setup = strategy in ("BREAKOUT_RETEST", "EMA_PULLBACK_VWAP", "VWAP_RECLAIM_RETEST", "SQUEEZE_BREAKOUT", "STOCHRSI_PULLBACK", "SUPERTREND_FLIP")
        if CHOP_FILTER_ENABLED and trend_setup and adx30 < REGIME_RANGE_ADX and (cross_count >= 4 or efficiency < 0.20):
            log.info("Setup rejected symbol=%s tf=%s strategy=%s reason=choppy market crosses=%d efficiency=%.2f", symbol, entry_tf, strategy, cross_count, efficiency)
            if diag is not None: diag.setdefault("reasons", []).append(f"{strategy}: pasar choppy (EMA cross {cross_count}, efficiency {efficiency:.2f})")
            continue
        # Additional quality points; these are ranking heuristics, not win probabilities.
        q = int(c.get("score", 0))
        aligned = (trend_up if c["direction"] == "LONG" else trend_down)
        if aligned: q += 4
        if float(c.get("vol_x", 0) or 0) >= 1.2: q += 3
        if spread_pct <= MAX_SPREAD_PCT * 0.5: q += 3
        if float(c.get("rr", 0) or 0) >= 1.5: q += 3
        if extension > 0.9: q -= 4
        c["raw_score"] = int(c.get("score", 0))
        c["score"] = max(0, min(100, q))
        c["quality_factors"] = {"htf_aligned": bool(aligned), "volume_x": round(float(c.get("vol_x", 0) or 0), 2),
                                 "spread_pct": round(spread_pct, 4), "rr": round(float(c.get("rr", 0) or 0), 2),
                                 "extension_atr": round(extension, 2), "ema_crosses_20": cross_count, "efficiency_ratio": round(efficiency, 2)}
        filtered.append(c)
    cands = [c for c in filtered if not c.get("sr_obstructed")]
    if not cands:
        return reject("semua setup ditolak filter kualitas (anti-chase/chop/SR); lihat log alasan penolakan")
    # One signal per symbol/timeframe: ranking chooses a single setup, not a vote-count.
    best = max(cands, key=lambda c: (c["stage"] == "TRIGGER", c["score"], c.get("rr", 0)))
    if FALSE_BREAKOUT_FILTER_ENABLED and best.get("strategy") in ("BREAKOUT_RETEST", "SQUEEZE_BREAKOUT"):
        last = df.iloc[-1]
        candle_range = max(1e-12, float(last["high"])-float(last["low"]))
        body_ratio = abs(float(last["close"])-float(last["open"])) / candle_range
        close_loc = (float(last["close"])-float(last["low"])) / candle_range
        vol_x = float(best.get("vol_x", 0) or 0)
        direction_ok = (best["direction"] == "LONG" and close_loc >= 0.60) or (best["direction"] == "SHORT" and close_loc <= 0.40)
        if body_ratio < 0.35 or not direction_ok or vol_x < 0.9:
            return reject(f"false-breakout filter: body={body_ratio:.2f}, close-location={close_loc:.2f}, volume={vol_x:.1f}x")
    if best["score"] < MIN_SETUP_SCORE:
        return reject(f"quality score {best['score']} < MIN_SETUP_SCORE {MIN_SETUP_SCORE} (raw {best.get('raw_score', best['score'])})")
    # Filter ulang memakai estimasi biaya round-trip: fee taker + spread pasar.
    # Ini lebih relevan untuk optimasi net R daripada fee saja.
    funding = ticker.get("fundingRate")
    try:
        funding_pct = float(funding) * 100 if funding not in (None, "") else None
    except (TypeError, ValueError):
        funding_pct = None
    # Conservative estimated costs. Funding is charged only to the side expected to pay
    # at the next funding event; actual holding duration/funding timestamps are not simulated.
    funding_cost_pct = 0.0
    if FUNDING_ESTIMATE and funding_pct is not None:
        rate = funding_pct
        pays_funding = (best["direction"] == "LONG" and rate > 0) or (best["direction"] == "SHORT" and rate < 0)
        if pays_funding:
            funding_cost_pct = abs(rate)
    total_cost_pct = FEE_PCT + spread_pct + SLIPPAGE_PCT + funding_cost_pct
    best["fee_r"] = total_cost_pct / best["risk_pct"] if best.get("risk_pct") else 999.0
    if best["fee_r"] > MAX_FEE_R:
        return reject(f"biaya fee+spread+slippage+funding estimasi memakan {best['fee_r']:.2f}R")
    best.update({
        "symbol": symbol, "timeframe": entry_tf, "entry": price, "candle_start": int(df["start"].iloc[-1]),
        "strategy": best.get("strategy", SIGNAL_STRATEGY.upper()), "regime": regime,
        "spread_pct": spread_pct, "fee_cost_pct": FEE_PCT, "slippage_pct": SLIPPAGE_PCT,
        "funding_pct": funding_pct, "funding_cost_pct": funding_cost_pct, "total_cost_pct": total_cost_pct,
        "turnover24h": turnover, "atr_pct": atr_pct,
        "chart_df": df.tail(150).copy(),  # dipakai chart.py; tidak disimpan ke Redis
    })
    return best
