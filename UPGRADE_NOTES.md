# Upgrade notes — setup quality filters

Implemented in this build:

1. Bad Setup Prevention Engine: anti-chase extension guard, choppy-market guard for trend strategies, breakout candle-quality/volume guard, and existing liquidity/cost/BTC/S-R gates.
2. VWAP Reclaim + Retest: new `VWAP_RECLAIM_RETEST` strategy. It checks reclaim/hold of rolling VWAP, a strong completed candle, 15m/30m alignment, BTC filter, and risk/cost validity.
3. Refined Liquidity Sweep: requires a sweep of the prior swing, reclaim beyond the level by 0.05 ATR, adequate candle body/close location, and at least 0.8x volume SMA20.
4. Quality scoring and rejection reasons: applies HTF alignment, volume, spread, RR, and extension adjustments; rejects scores below `MIN_SETUP_SCORE`; logs why a setup was rejected and persists `raw_score`/`quality_factors` for accepted signals.

Default controls in `.env.example`:

- `MIN_SETUP_SCORE=75`
- `ANTI_CHASE_ENABLED=1`
- `CHOP_FILTER_ENABLED=1`
- `FALSE_BREAKOUT_FILTER_ENABLED=1`
- `MAX_EXTENDED_ATR=1.5`

The score is a heuristic ranking score, not a probability of winning. No historical backtest or paper-trading validation is included. The bot remains a public-data alert/simulation bot and does not place exchange orders.
