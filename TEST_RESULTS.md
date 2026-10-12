# Test report — Adaptive Scalping update

Run date: 2026-10-12 (UTC)

## Tests executed

- `python -m py_compile *.py` — PASS (all project Python modules compile).
- `python -m unittest discover -s tests -v` — PASS, 5 tests:
  - adaptive regime routing for trend/range/transition
  - indicator columns and finite ATR/VWAP on deterministic synthetic candles
  - existing Breakout + Retest and Bollinger + RSI strategy functions run without exceptions on synthetic data
  - new EMA Pullback + VWAP, MACD Divergence, and Liquidity Sweep strategy functions run without exceptions on synthetic data
  - long/short stop and target direction sanity checks
- ZIP archive integrity — checked separately with `unzip -t`.

## Not tested / important limitations

- No historical Bybit or TradingView backtest was run.
- No live Bybit API call, Discord bot runtime, or authenticated Upstash connection was tested.
- The unit tests use deterministic synthetic candles and establish code-path/sanity behavior only; they do not establish profitability, win rate, or execution quality.
- Funding is an estimate for a potential single funding event based on the current rate and side paying; actual funding depends on the position's duration and funding timestamp. Spread/slippage are also estimates.
