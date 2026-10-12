"""Redis-backed persistent signal store. Requires REDIS_URL on Railway."""
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

    async def add_signal(self, s, cooldown_minutes):
        now = datetime.now(timezone.utc)
        candle_key = self.key(f"dedupe:{s['symbol']}:{s['timeframe']}:{s['direction']}:{s.get('candle_start')}")
        if await self.redis.exists(candle_key): return False
        # Search recent IDs backwards for same pair/timeframe/direction cooldown.
        ids = await self.redis.zrevrange(self.key("signal_ids"), 0, 500)
        cutoff = now.timestamp() - cooldown_minutes * 60
        for sid in ids:
            raw = await self.redis.get(self.key(f"signal:{sid}"))
            if not raw: continue
            old = json.loads(raw)
            if old.get("symbol") == s["symbol"] and old.get("timeframe") == s["timeframe"] and old.get("direction") == s["direction"]:
                try:
                    if datetime.fromisoformat(old["created_at"]).timestamp() > cutoff: return False
                except (ValueError, TypeError, KeyError): pass
                break
        sid = await self.redis.incr(self.key("next_id"))
        record = {"id": sid, "created_at": now.isoformat(), "symbol": s["symbol"], "timeframe": s["timeframe"],
                  "direction": s["direction"], "score": int(s["score"]), "entry": float(s["entry"]),
                  "stop_loss": float(s["sl"]), "take_profit": float(s["tp"]), "status": "OPEN", "outcome": None,
                  "closed_at": None, "candle_start": s.get("candle_start"), "spread_pct": s.get("spread_pct"),
                  "funding_pct": s.get("funding_pct"), "close_price": None, "result_r": None}
        async with self.redis.pipeline(transaction=True) as p:
            p.set(self.key(f"signal:{sid}"), json.dumps(record, separators=(",", ":")))
            p.zadd(self.key("signal_ids"), {str(sid): sid})
            p.set(candle_key, str(sid), ex=60*60*24*14)
            await p.execute()
        return True

    async def claim_watch(self, s, cooldown_minutes):
        """True jika notifikasi WATCH untuk pair+arah ini boleh dikirim (cooldown atomik via SET NX)."""
        key = self.key(f"watch:{s['symbol']}:{s['direction']}")
        ok = await self.redis.set(key, str(s.get("candle_start") or ""), nx=True, ex=max(60, int(cooldown_minutes) * 60))
        return bool(ok)

    async def get_open_signals(self, limit=40):
        return [r for r in await self._recent_records(1000) if r.get("status") == "OPEN"][:limit]

    async def update_signal(self, sid, **changes):
        key = self.key(f"signal:{sid}"); raw = await self.redis.get(key)
        if not raw: return
        record = json.loads(raw); record.update(changes)
        await self.redis.set(key, json.dumps(record, separators=(",", ":")))

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
        rows = await self._recent_records(max(1, int(await self.redis.zcard(self.key("signal_ids")))))
        total = len(rows); tp=sum(r.get("outcome")=="TP" for r in rows); sl=sum(r.get("outcome")=="SL" for r in rows)
        ambiguous=sum(r.get("outcome")=="AMBIGUOUS" for r in rows); expired=sum(r.get("outcome")=="EXPIRED" for r in rows)
        sum_r=sum(float(r.get("result_r") or 0) for r in rows)
        return total,tp,sl,ambiguous,expired,sum_r

    async def close(self):
        if self.redis: await self.redis.aclose(); self.redis = None
