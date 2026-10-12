"""Persistent signal/follow store using Upstash Redis REST API (async via aiohttp)."""
import os, json
from datetime import datetime, timezone
import aiohttp


class UpstashPipeline:
    """Small pipeline wrapper for the Redis commands used by this bot."""
    def __init__(self, client):
        self.client = client
        self.commands = []

    def _add(self, *args):
        self.commands.append([str(a) for a in args])
        return self

    def set(self, *args, **kwargs):
        key, value = args[:2]
        cmd = ["SET", key, value]
        if kwargs.get("ex") is not None: cmd += ["EX", int(kwargs["ex"])]
        if kwargs.get("nx"): cmd += ["NX"]
        return self._add(*cmd)

    def zadd(self, key, mapping):
        cmd = ["ZADD", key]
        for member, score in mapping.items(): cmd += [score, member]
        return self._add(*cmd)

    def sadd(self, key, *members): return self._add("SADD", key, *members)
    def zrem(self, key, *members): return self._add("ZREM", key, *members)
    def delete(self, *keys): return self._add("DEL", *keys)

    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc, tb): return False
    async def execute(self):
        if not self.commands: return []
        return await self.client.execute_pipeline(self.commands)


class UpstashRedisREST:
    """Subset of Redis async API implemented over Upstash's REST pipeline endpoint."""
    def __init__(self, url, token):
        self.url = url.rstrip("/")
        self.token = token
        self.session = None

    async def connect(self):
        timeout = aiohttp.ClientTimeout(total=15, connect=8)
        self.session = aiohttp.ClientSession(timeout=timeout, headers={
            "Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
        await self.ping()

    async def execute_pipeline(self, commands):
        if self.session is None: raise RuntimeError("Upstash REST client belum tersambung")
        async with self.session.post(self.url + "/pipeline", json=commands) as response:
            body = await response.json(content_type=None)
            if response.status >= 400:
                raise RuntimeError(f"Upstash Redis REST HTTP {response.status}: {str(body)[:500]}")
        if not isinstance(body, list): raise RuntimeError(f"Respons Upstash pipeline tidak valid: {str(body)[:300]}")
        results = []
        for item in body:
            if isinstance(item, dict) and item.get("error"):
                raise RuntimeError(f"Upstash Redis command error: {item['error']}")
            results.append(item.get("result") if isinstance(item, dict) else item)
        return results

    async def _cmd(self, *args): return (await self.execute_pipeline([[str(a) for a in args]]))[0]
    async def ping(self): return await self._cmd("PING")
    async def scan(self, cursor=0, match=None, count=100):
        args = ["SCAN", cursor]
        if match is not None: args += ["MATCH", match]
        if count: args += ["COUNT", int(count)]
        return await self._cmd(*args)
    async def get(self, key): return await self._cmd("GET", key)
    async def exists(self, key): return int(await self._cmd("EXISTS", key) or 0)
    async def incr(self, key): return int(await self._cmd("INCR", key))
    async def zrevrange(self, key, start, stop): return await self._cmd("ZREVRANGE", key, start, stop) or []
    async def zcard(self, key): return int(await self._cmd("ZCARD", key) or 0)
    async def zrem(self, key, *members): return await self._cmd("ZREM", key, *members)
    async def zadd(self, key, mapping):
        args = ["ZADD", key]
        for member, score in mapping.items(): args += [score, member]
        return await self._cmd(*args)
    async def set(self, key, value, nx=False, ex=None):
        args = ["SET", key, value]
        if ex is not None: args += ["EX", int(ex)]
        if nx: args += ["NX"]
        return await self._cmd(*args)
    async def delete(self, *keys): return await self._cmd("DEL", *keys)
    async def sadd(self, key, *members): return await self._cmd("SADD", key, *members)
    async def scard(self, key): return int(await self._cmd("SCARD", key) or 0)
    async def smembers(self, key): return await self._cmd("SMEMBERS", key) or []
    async def mget(self, keys):
        if not keys: return []
        return await self._cmd("MGET", *keys) or []
    def pipeline(self, transaction=True): return UpstashPipeline(self)
    async def aclose(self):
        if self.session:
            await self.session.close()
            self.session = None


class Database:
    def __init__(self):
        self.url = os.getenv("UPSTASH_REDIS_REST_URL", "").strip()
        self.token = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
        self.redis = None
        self.prefix = os.getenv("REDIS_PREFIX", "bybit_signal_bot")

    async def connect(self):
        if not self.url or not self.token:
            raise RuntimeError("UPSTASH_REDIS_REST_URL dan UPSTASH_REDIS_REST_TOKEN wajib diisi dari dashboard Upstash Redis.")
        self.redis = UpstashRedisREST(self.url, self.token)
        await self.redis.connect()

    def key(self, suffix): return f"{self.prefix}:{suffix}"

    async def reset_all_data(self):
        """Delete every Redis key in this bot's namespace only; never FLUSHDB/FLUSHALL."""
        if not self.redis:
            raise RuntimeError("Upstash belum terhubung")
        cursor = "0"
        deleted = 0
        pattern = f"{self.prefix}:*"
        while True:
            result = await self.redis.scan(cursor, match=pattern, count=100)
            if not isinstance(result, list) or len(result) != 2:
                raise RuntimeError("Respons SCAN Upstash tidak valid")
            cursor, keys = str(result[0]), result[1] or []
            # Delete in small batches to avoid REST URL/payload limits.
            for start in range(0, len(keys), 50):
                batch = keys[start:start + 50]
                if batch:
                    deleted += int(await self.redis.delete(*batch) or 0)
            if cursor == "0":
                break
        return deleted

    # ------------------------------------------------------------------ sinyal
    async def add_signal(self, s, cooldown_minutes):
        """Simpan sinyal baru. Mengembalikan ID sinyal (int), atau 0 jika duplikat/cooldown."""
        now = datetime.now(timezone.utc)
        candle_key = self.key(f"dedupe:{s['symbol']}:{s['timeframe']}:{s['direction']}:{s.get('candle_start')}")
        if await self.redis.exists(candle_key): return 0
        ids = await self.redis.zrevrange(self.key("signal_ids"), 0, 500)
        cutoff = now.timestamp() - cooldown_minutes * 60
        for sid in ids:
            raw = await self.redis.get(self.key(f"signal:{sid}"))
            if not raw: continue
            old = json.loads(raw)
            if old.get("symbol") == s["symbol"] and old.get("timeframe") == s["timeframe"] and old.get("direction") == s["direction"]:
                try:
                    if datetime.fromisoformat(old["created_at"]).timestamp() > cutoff: return 0
                except (ValueError, TypeError, KeyError): pass
                break
        sid = await self.redis.incr(self.key("next_id"))
        record = {"id": sid, "created_at": now.isoformat(), "symbol": s["symbol"], "timeframe": s["timeframe"],
                  "direction": s["direction"], "score": int(s["score"]), "entry": float(s["entry"]),
                  "stop_loss": float(s["sl"]), "take_profit": float(s["tp"]), "rr": float(s.get("rr") or 0),
                  "status": "OPEN", "outcome": None, "closed_at": None, "candle_start": s.get("candle_start"),
                  "strategy": s.get("strategy", "PULLBACK"), "regime": s.get("regime"), "spread_pct": s.get("spread_pct"),
                  "fee_cost_pct": s.get("fee_cost_pct"), "slippage_pct": s.get("slippage_pct"),
                  "funding_pct": s.get("funding_pct"), "funding_cost_pct": s.get("funding_cost_pct"),
                  "total_cost_pct": s.get("total_cost_pct"), "close_price": None, "result_r": None, "net_r": None, "fee_r": None,
                  "channel_id": None, "message_id": None}
        async with self.redis.pipeline(transaction=True) as p:
            p.set(self.key(f"signal:{sid}"), json.dumps(record, separators=(",", ":")))
            p.zadd(self.key("signal_ids"), {str(sid): sid})
            p.set(candle_key, str(sid), ex=60*60*24*14)
            await p.execute()
        return int(sid)

    async def discard_signal(self, sid):
        """Batalkan sinyal yang gagal terkirim (hapus record + kunci dedupe) supaya bisa dicoba lagi."""
        rec = await self.get_signal(sid)
        if not rec: return
        dedupe = self.key(f"dedupe:{rec['symbol']}:{rec['timeframe']}:{rec['direction']}:{rec.get('candle_start')}")
        async with self.redis.pipeline(transaction=True) as p:
            p.delete(self.key(f"signal:{sid}"))
            p.zrem(self.key("signal_ids"), str(sid))
            p.delete(dedupe)
            await p.execute()

    async def claim_watch(self, s, cooldown_minutes):
        """True jika notifikasi WATCH untuk pair+arah ini boleh dikirim (cooldown atomik via SET NX)."""
        key = self.key(f"watch:{s['symbol']}:{s['direction']}")
        ok = await self.redis.set(key, str(s.get("candle_start") or ""), nx=True, ex=max(60, int(cooldown_minutes) * 60))
        return bool(ok)

    async def get_signal(self, sid):
        raw = await self.redis.get(self.key(f"signal:{sid}"))
        if not raw: return None
        try: return json.loads(raw)
        except json.JSONDecodeError: return None

    async def get_open_signals(self, limit=40):
        return [r for r in await self._recent_records(1000) if r.get("status") == "OPEN"][:limit]

    async def update_signal(self, sid, **changes):
        key = self.key(f"signal:{sid}"); raw = await self.redis.get(key)
        if not raw: return None
        record = json.loads(raw); record.update(changes)
        await self.redis.set(key, json.dumps(record, separators=(",", ":")))
        return record

    async def recent_signals(self, limit=10): return await self._recent_records(limit)

    async def _recent_records(self, limit):
        ids = await self.redis.zrevrange(self.key("signal_ids"), 0, max(0, limit-1))
        out = []
        for sid in ids:
            raw = await self.redis.get(self.key(f"signal:{sid}"))
            if raw:
                try: out.append(json.loads(raw))
                except json.JSONDecodeError: pass
        return out

    async def performance(self):
        """Statistik sinyal. AMBIGUOUS dihitung LOSS (konservatif); net = setelah fee."""
        rows = await self._recent_records(max(1, int(await self.redis.zcard(self.key("signal_ids")))))
        closed = [r for r in rows if r.get("outcome")]
        count = lambda o: sum(r.get("outcome") == o for r in rows)
        tp, sl, amb, exp = count("TP"), count("SL"), count("AMBIGUOUS"), count("EXPIRED")
        gross = sum(float(r.get("result_r") or 0) for r in closed)
        net = sum(float(r.get("net_r") if r.get("net_r") is not None else (r.get("result_r") or 0)) for r in closed)
        by_tf = {}
        for r in closed:
            b = by_tf.setdefault(r["timeframe"], {"n": 0, "win": 0, "net": 0.0})
            n_r = float(r.get("net_r") if r.get("net_r") is not None else (r.get("result_r") or 0))
            b["n"] += 1; b["win"] += n_r > 0; b["net"] += n_r
        return {"total": len(rows), "open": len(rows) - len(closed), "tp": tp, "sl": sl, "ambiguous": amb,
                "expired": exp, "gross_r": gross, "net_r": net, "by_tf": by_tf}

    # ------------------------------------------------------------- follow trade
    async def add_follow(self, sid, user_id, user_name, signal, follow_price):
        """Catat user ikut sinyal. Mengembalikan record, atau None jika user sudah ikut (SET NX atomik)."""
        now = datetime.now(timezone.utc)
        rec = {"signal_id": int(sid), "user_id": int(user_id), "user_name": user_name,
               "followed_at": now.isoformat(), "follow_price": float(follow_price),
               "symbol": signal["symbol"], "timeframe": signal["timeframe"], "direction": signal["direction"],
               "signal_entry": signal["entry"], "sl": signal["stop_loss"], "tp": signal["take_profit"],
               "strategy": signal.get("strategy", "PULLBACK"), "regime": signal.get("regime"),
               "spread_pct": signal.get("spread_pct", 0.0), "fee_cost_pct": signal.get("fee_cost_pct"),
               "slippage_pct": signal.get("slippage_pct"), "funding_pct": signal.get("funding_pct"),
               "funding_cost_pct": signal.get("funding_cost_pct"), "total_cost_pct": signal.get("total_cost_pct"),
               "status": "OPEN", "result_r": None, "net_r": None, "close_price": None, "closed_at": None}
        ok = await self.redis.set(self.key(f"follow:{sid}:{user_id}"), json.dumps(rec, separators=(",", ":")), nx=True)
        if not ok: return None
        async with self.redis.pipeline(transaction=True) as p:
            p.sadd(self.key(f"followers:{sid}"), str(user_id))
            p.zadd(self.key(f"user_follows:{user_id}"), {str(sid): now.timestamp()})
            await p.execute()
        return rec

    async def get_follow(self, sid, user_id):
        raw = await self.redis.get(self.key(f"follow:{sid}:{user_id}"))
        if not raw: return None
        try: return json.loads(raw)
        except json.JSONDecodeError: return None

    async def count_followers(self, sid):
        return int(await self.redis.scard(self.key(f"followers:{sid}")))

    async def followers_of(self, sid):
        uids = await self.redis.smembers(self.key(f"followers:{sid}"))
        if not uids: return []
        raws = await self.redis.mget([self.key(f"follow:{sid}:{u}") for u in uids])
        out = []
        for raw in raws:
            if raw:
                try: out.append(json.loads(raw))
                except json.JSONDecodeError: pass
        return out

    async def update_follow(self, sid, user_id, **changes):
        key = self.key(f"follow:{sid}:{user_id}"); raw = await self.redis.get(key)
        if not raw: return None
        rec = json.loads(raw); rec.update(changes)
        await self.redis.set(key, json.dumps(rec, separators=(",", ":")))
        return rec

    async def user_follows(self, user_id, limit=100):
        """Follow milik user, terbaru dulu."""
        sids = await self.redis.zrevrange(self.key(f"user_follows:{user_id}"), 0, max(0, limit - 1))
        if not sids: return []
        raws = await self.redis.mget([self.key(f"follow:{sid}:{user_id}") for sid in sids])
        out = []
        for raw in raws:
            if raw:
                try: out.append(json.loads(raw))
                except json.JSONDecodeError: pass
        return out

    async def close(self):
        if self.redis: await self.redis.aclose(); self.redis = None
