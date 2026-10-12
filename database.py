"""Redis-backed persistent store: riwayat sinyal + Follow Trade. Requires REDIS_URL on Railway."""
import os, json
from datetime import datetime, timezone


class Database:
    def __init__(self):
        self.url = os.getenv("REDIS_URL", "").strip()
        self.redis = None
        self.prefix = os.getenv("REDIS_PREFIX", "bybit_signal_bot")

    async def connect(self):
        if not self.url:
            raise RuntimeError("REDIS_URL belum diisi. Tambahkan Redis service lalu hubungkan REDIS_URL ke service bot.")
        from redis.asyncio import Redis
        self.redis = Redis.from_url(self.url, decode_responses=True, health_check_interval=30, socket_connect_timeout=10, socket_timeout=10)
        await self.redis.ping()

    def key(self, suffix): return f"{self.prefix}:{suffix}"

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
                  "spread_pct": s.get("spread_pct"), "funding_pct": s.get("funding_pct"),
                  "close_price": None, "result_r": None, "net_r": None, "fee_r": None,
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
