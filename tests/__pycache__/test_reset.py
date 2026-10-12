"""Safety tests for the namespace-scoped Upstash reset."""
import asyncio
import unittest
from database import Database


class FakeRedis:
    def __init__(self):
        self.keys = {
            "bybit_signal_bot:signal:1": "x",
            "bybit_signal_bot:signal_ids": "x",
            "bybit_signal_bot:follow:1:123": "x",
            "other_app:important": "keep",
        }
        self.calls = []

    async def scan(self, cursor=0, match=None, count=100):
        self.calls.append((str(cursor), match))
        matches = sorted(k for k in self.keys if k.startswith("bybit_signal_bot:"))
        return ["0", matches]

    async def delete(self, *keys):
        deleted = 0
        for key in keys:
            if key in self.keys:
                del self.keys[key]
                deleted += 1
        return deleted


class ResetTests(unittest.TestCase):
    def test_reset_deletes_only_own_namespace(self):
        async def run():
            db = Database()
            fake = FakeRedis()
            db.redis = fake
            count = await db.reset_all_data()
            self.assertEqual(count, 3)
            self.assertEqual(fake.keys, {"other_app:important": "keep"})
            self.assertEqual(fake.calls[0][1], "bybit_signal_bot:*")
        asyncio.run(run())

    def test_reset_requires_connected_redis(self):
        async def run():
            db = Database()
            with self.assertRaises(RuntimeError):
                await db.reset_all_data()
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main(verbosity=2)
