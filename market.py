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


# ---------------- Konfigurasi metode Trend Pullback ----------------
TP_R = max(0.5, _env_float("TP_R", 1.5))                  # target awal momentum; wajib diuji dengan backtest
ADX_MIN = _env_float("ADX_MIN", 20)                       # ADX 30m minimal (filter sideways)
PULLBACK_LOOKBACK = int(_env_float("PULLBACK_LOOKBACK", 8))  # candle terakhir yang dicek untuk pullback
MAX_EXTENDED_ATR = _env_float("MAX_EXTENDED_ATR", 1.5)    # anti-chase: maks jarak close dari EMA20 (x ATR)
SL_BUFFER_ATR = 0.2                                       # buffer SL di luar swing pullback
SL_ATR_MIN, SL_ATR_MAX = 0.8, 2.5                         # batas jarak SL (x ATR)
FEE_PCT = _env_float("FEE_PCT", 0.11)                     # fee pulang-pergi (%), taker 0.055% x 2
MAX_FEE_R = _env_float("MAX_FEE_R", 0.25)                 # tolak setup jika fee > X R (SL terlalu sempit)
MAX_SPREAD_PCT = _env_float("MAX_SPREAD_PCT", 0.10)
BTC_FILTER = os.getenv("BTC_FILTER", "1").strip().lower() not in ("0", "false", "no", "off")
BTC_DUMP_ATR = _env_float("BTC_DUMP_ATR", 1.5)            # BTC 15m gerak > X ATR dalam 3 candle => blok arah berlawanan
NEWS_WINDOW_MIN = int(_env_float("NEWS_WINDOW_MIN", 30))
# Only the two user-selected strategies are enabled.
SIGNAL_STRATEGY = os.getenv("SIGNAL_STRATEGY", "both").strip().lower()
if SIGNAL_STRATEGY not in ("breakout_retest", "bb_rsi", "both"):
    SIGNAL_STRATEGY = "breakout_retest"
BREAKOUT_LOOKBACK = max(8, int(_env_float("BREAKOUT_LOOKBACK", 20)))
BREAKOUT_RETEST_BARS = max(1, min(4, int(_env_float("BREAKOUT_RETEST_BARS", 3))))
BREAKOUT_MIN_VOLUME = max(0.8, _env_float("BREAKOUT_MIN_VOLUME", 1.1))
BB_RSI_OVERSOLD = min(40.0, max(10.0, _env_float("BB_RSI_OVERSOLD", 35)))
BB_RSI_OVERBOUGHT = max(60.0, min(90.0, _env_float("BB_RSI_OVERBOUGHT", 65)))
BB_MAX_ADX_30M = max(5.0, _env_float("BB_MAX_ADX_30M", 25))


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


def add_indicators(df):
    df = df.copy()
    close = df["close"]
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    prev_close = close.shift(1)
    tr = pd.concat([
        df["high"] - df["low"], (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/14, adjust=False).mean()
    df["atr"] = atr
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


async def analyze_symbol(client: BybitMarket, symbol: str, entry_tf: str = "5", ticker=None, diag=None) -> Optional[dict]:
    """Analisis Trend Pullback. `diag` (dict, opsional) diisi alasan penolakan untuk ditampilkan ke user."""
    if entry_tf not in INTERVALS:
        raise ValueError("Timeframe hanya 5, 15, atau 30 menit.")

    def reject(reason):
        if diag is not None:
            diag.setdefault("reasons", []).insert(0, reason)
        return None

    if in_news_blackout():
        return reject(f"jendela news besar (±{NEWS_WINDOW_MIN} menit)")
    tfs = list(dict.fromkeys([entry_tf, "15", "30"]))
    data = await asyncio.gather(*(client.get_klines_cached(symbol, tf) for tf in tfs))
    frames = {tf: add_indicators(df) for tf, df in zip(tfs, data)}
    if any(len(df) < 205 for df in frames.values()):
        return reject("data candle belum cukup (butuh 205+ candle)")
    df = frames[entry_tf]
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
            log.warning("Filter BTC dilewati (data BTC gagal): %s", exc)

    # Run only the selected strategy, or both user-approved strategies when explicitly configured.
    strategy_fns = {
        "breakout_retest": (_breakout_retest_setup,),
        "bb_rsi": (_bb_rsi_setup,),
        "both": (_breakout_retest_setup, _bb_rsi_setup),
    }[SIGNAL_STRATEGY]
    cands = [c for fn in strategy_fns for c in
             (fn(d, df, m15, m30, price, atr, btc_move, is_btc, diag) for d in ("LONG", "SHORT")) if c]
    if not cands:
        if diag is not None and diag.get("reasons"):
            rs = diag["reasons"]
            useful = [r for r in rs if "tren 30m tidak searah" not in r]  # alasan arah yang paling dekat valid lebih informatif
            diag["reasons"] = (useful or rs)[:2]
        return None
    best = max(cands, key=lambda c: (c["stage"] == "TRIGGER", c["score"]))
    # Filter ulang memakai estimasi biaya round-trip: fee taker + spread pasar.
    # Ini lebih relevan untuk optimasi net R daripada fee saja.
    total_cost_pct = FEE_PCT + spread_pct
    best["fee_r"] = total_cost_pct / best["risk_pct"] if best.get("risk_pct") else 999.0
    if best["fee_r"] > MAX_FEE_R:
        return reject(f"fee + spread memakan {best['fee_r']:.2f}R (batas {MAX_FEE_R:.2f}R)")

    funding = ticker.get("fundingRate")
    try:
        funding_pct = float(funding) * 100 if funding not in (None, "") else None
    except (TypeError, ValueError):
        funding_pct = None
    best.update({
        "symbol": symbol, "timeframe": entry_tf, "entry": price, "candle_start": int(df["start"].iloc[-1]),
        "strategy": best.get("strategy", SIGNAL_STRATEGY.upper()),
        "spread_pct": spread_pct, "turnover24h": turnover, "atr_pct": atr_pct, "funding_pct": funding_pct,
        "chart_df": df.tail(150).copy(),  # dipakai chart.py; tidak disimpan ke Redis
    })
    return best
