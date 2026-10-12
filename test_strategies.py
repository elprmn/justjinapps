"""Offline smoke/unit tests; these are not a historical profitability backtest."""
import unittest
import numpy as np
import pandas as pd
import market


def frame(seed=7, trend=0.0008, n=260):
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(trend, 0.006, n)))
    op = np.r_[close[0], close[:-1]]
    spread = close * rng.uniform(0.002, 0.008, n)
    high = np.maximum(op, close) + spread
    low = np.minimum(op, close) - spread
    volume = rng.uniform(1000, 5000, n)
    return pd.DataFrame({"start": np.arange(n) * 300000, "open": op, "high": high,
                         "low": low, "close": close, "volume": volume, "turnover": volume*close})


class StrategySmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = market.add_indicators(frame())
        cls.m15 = market.add_indicators(frame(seed=8)).iloc[-1]
        cls.m30 = market.add_indicators(frame(seed=9, trend=0.0012)).iloc[-1]
        cls.price = float(cls.df.close.iloc[-1])
        cls.atr = float(cls.df.atr.iloc[-1])

    def test_indicator_columns(self):
        for col in ("ema20", "ema50", "ema200", "rsi", "atr", "adx", "bb_upper", "bb_lower", "vwap", "macd", "macd_signal", "macd_hist"):
            self.assertIn(col, self.df.columns)
        self.assertGreater(self.atr, 0)
        self.assertTrue(np.isfinite(self.df.vwap.iloc[-1]))

    def test_new_strategy_functions_are_callable(self):
        funcs = (market._ema_vwap_setup, market._macd_divergence_setup, market._liquidity_sweep_setup)
        for fn in funcs:
            for direction in ("LONG", "SHORT"):
                result = fn(direction, self.df, self.m15, self.m30, self.price, self.atr, 0.0, False, {})
                self.assertTrue(result is None or isinstance(result, dict), fn.__name__)

    def test_existing_strategy_functions_are_callable(self):
        for fn in (market._breakout_retest_setup, market._bb_rsi_setup):
            for direction in ("LONG", "SHORT"):
                result = fn(direction, self.df, self.m15, self.m30, self.price, self.atr, 0.0, False, {})
                self.assertTrue(result is None or isinstance(result, dict), fn.__name__)


    def test_adaptive_routing(self):
        methods, regime = market._select_strategy_functions(30, True, False)
        self.assertEqual(regime, "TREND")
        self.assertEqual({fn.__name__ for fn in methods}, {"_breakout_retest_setup", "_ema_vwap_setup"})
        methods, regime = market._select_strategy_functions(10, False, False)
        self.assertEqual(regime, "RANGE")
        self.assertIn(market._bb_rsi_setup, methods)
        self.assertIn(market._liquidity_sweep_setup, methods)
        methods, regime = market._select_strategy_functions(21, False, False)
        self.assertEqual(regime, "TRANSITION")
        self.assertGreaterEqual(len(methods), 2)

    def test_risk_levels_have_valid_direction(self):
        long_levels = market._risk_levels("LONG", 100, 1, 98, 101, 1.5)
        short_levels = market._risk_levels("SHORT", 100, 1, 98, 101, 1.5)
        self.assertIsNotNone(long_levels)
        self.assertIsNotNone(short_levels)
        self.assertLess(long_levels[0], 100); self.assertGreater(long_levels[1], 100)
        self.assertGreater(short_levels[0], 100); self.assertLess(short_levels[1], 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
