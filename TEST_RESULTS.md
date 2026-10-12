# Test results

Executed locally against offline synthetic data and a fake Redis implementation; no live exchange/Discord/Upstash credentials were used.

- `python -m py_compile *.py` — PASS.
- `python -m unittest discover -s tests -v` — PASS, 7 tests.
  - Existing indicator and strategy smoke tests: PASS.
  - Adaptive strategy routing and risk-level sanity: PASS.
  - Reset deletes keys only in the configured bot namespace: PASS.
  - Reset refuses to run without a connected Redis client: PASS.
- ZIP integrity check with `unzip -t` — see build verification.

Not tested: live Discord command sync, Discord message deletion permissions, real Upstash SCAN/DEL behavior against credentials, or live Bybit integration. `/reset` is admin-only and requires a confirmation button. It deletes bot-prefixed Upstash keys and bot-authored messages in the configured alert channel, not all data in the entire Discord server or unrelated Upstash keys.
