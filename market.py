import asyncio
import os
import time
import json
import logging
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


def fmt_price(value):
    try:
        x = float(value)
        return f"{x:,.8f}".rstrip("0").rstrip(".")
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
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    prev_close = close.shift(1)
    tr = pd.concat([
        df["high"] - df["low"], (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()
    df["vol_sma20"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].shift(1).rolling(20).max()
    df["low20"] = df["low"].shift(1).rolling(20).min()
    return df


async def analyze_symbol(client: BybitMarket, symbol: str, entry_tf: str = "5", ticker=None) -> Optional[dict]:
    if entry_tf not in INTERVALS:
        raise ValueError("Timeframe hanya 5, 15, atau 30 menit.")
    # Use only requested short-term TFs. The selected entry TF must be one of these.
    tfs = list(dict.fromkeys([entry_tf, "15", "30"]))
    data = await asyncio.gather(*(client.get_klines(symbol, tf, 250) for tf in tfs))
    frames = {tf: add_indicators(df) for tf, df in zip(tfs, data)}
    if any(len(df) < 205 for df in frames.values()):
        return None

    def last(tf):
        return frames[tf].iloc[-1]

    e, m15, m30 = last(entry_tf), last("15"), last("30")
    price = float(e["close"])
    atr = float(e["atr"]) if pd.notna(e["atr"]) else 0
    if not atr or price <= 0:
        return None

    # Market quality filters: ticker spread, minimum turnover, and ATR sanity.
    ticker = ticker or await client.get_ticker(symbol)
    bid, ask = float(ticker.get("bid1Price", 0) or 0), float(ticker.get("ask1Price", 0) or 0)
    spread_pct = ((ask - bid) / ((ask + bid) / 2) * 100) if bid > 0 and ask >= bid else 999.0
    turnover = float(ticker.get("turnover24h", 0) or 0)
    if spread_pct > 0.15 or turnover < 1_000_000:
        return None
    atr_pct = atr / price * 100
    if atr_pct < 0.03 or atr_pct > 3.0:
        return None

    long_score, short_score = 0, 0
    long_reasons, short_reasons = [], []

    def award(condition, points, side, reason):
        nonlocal long_score, short_score
        if bool(condition):
            if side == "LONG":
                long_score += points
                long_reasons.append(reason)
            else:
                short_score += points
                short_reasons.append(reason)

    # Entry trigger: momentum plus a close beyond a recent 20-candle range.
    award(e["ema20"] > e["ema50"] and price > e["ema20"], 12, "LONG", "Harga di atas EMA20/EMA50")
    award(e["ema20"] < e["ema50"] and price < e["ema20"], 12, "SHORT", "Harga di bawah EMA20/EMA50")
    award(52 <= e["rsi"] <= 70, 8, "LONG", "RSI mendukung momentum bullish")
    award(30 <= e["rsi"] <= 48, 8, "SHORT", "RSI mendukung momentum bearish")
    award(e["macd"] > e["macd_signal"], 8, "LONG", "MACD bullish")
    award(e["macd"] < e["macd_signal"], 8, "SHORT", "MACD bearish")
    award(pd.notna(e["vol_sma20"]) and e["volume"] > e["vol_sma20"] * 1.15, 8, "LONG", "Volume naik di atas rata-rata")
    award(pd.notna(e["vol_sma20"]) and e["volume"] > e["vol_sma20"] * 1.15, 8, "SHORT", "Volume naik di atas rata-rata")
    award(pd.notna(e["high20"]) and price > e["high20"], 15, "LONG", "Breakout high 20 candle")
    award(pd.notna(e["low20"]) and price < e["low20"], 15, "SHORT", "Breakdown low 20 candle")

    # 30m is the trend filter; 15m confirms momentum. Contradiction => NO TRADE.
    bull30 = m30["ema20"] > m30["ema50"] and m30["close"] > m30["ema50"]
    bear30 = m30["ema20"] < m30["ema50"] and m30["close"] < m30["ema50"]
    bull15 = m15["ema20"] > m15["ema50"] and m15["macd"] > m15["macd_signal"]
    bear15 = m15["ema20"] < m15["ema50"] and m15["macd"] < m15["macd_signal"]
    award(bull15, 12, "LONG", "Konfirmasi momentum 15m bullish")
    award(bear15, 12, "SHORT", "Konfirmasi momentum 15m bearish")
    award(bull30, 17, "LONG", "Tren utama 30m bullish")
    award(bear30, 17, "SHORT", "Tren utama 30m bearish")

    if bull30 and bull15 and long_score >= short_score + 15:
        direction, score, reasons = "LONG", min(100, long_score), long_reasons
    elif bear30 and bear15 and short_score >= long_score + 15:
        direction, score, reasons = "SHORT", min(100, short_score), short_reasons
    else:
        return None

    # SL uses ATR and recent structure; ensure nonzero risk and RR >= 1.5.
    if direction == "LONG":
        structural_sl = float(e["low20"]) if pd.notna(e["low20"]) else price - 1.5 * atr
        sl = min(price - 1.2 * atr, structural_sl - 0.1 * atr)
        risk = price - sl
        tp = price + 1.8 * risk
    else:
        structural_sl = float(e["high20"]) if pd.notna(e["high20"]) else price + 1.5 * atr
        sl = max(price + 1.2 * atr, structural_sl + 0.1 * atr)
        risk = sl - price
        tp = price - 1.8 * risk
    if risk <= 0 or abs(price - sl) / price > 0.04:
        return None

    funding = ticker.get("fundingRate")
    try:
        funding_pct = float(funding) * 100 if funding not in (None, "") else None
    except (TypeError, ValueError):
        funding_pct = None
    return {
        "symbol": symbol, "timeframe": entry_tf, "direction": direction,
        "score": int(score), "entry": price, "sl": float(sl), "tp": float(tp),
        "rr": 1.8, "reasons": reasons, "candle_start": int(e["start"]),
        "spread_pct": spread_pct, "turnover24h": turnover, "atr_pct": atr_pct,
        "funding_pct": funding_pct,
    }
