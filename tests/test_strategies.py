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
        for col in ("ema20", "ema50", "ema200", "rsi", "atr", "adx", "bb_upper", "bb_lower", "vwap", "bb_width", "stoch_k", "stoch_d", "st_dir", "st_line", "ema9", "ema21"):
            self.assertIn(col, self.df.columns)
        self.assertGreater(self.atr, 0)
        self.assertTrue(np.isfinite(self.df.vwap.iloc[-1]))

    def test_new_strategy_functions_are_callable(self):
        funcs = (market._ema_vwap_setup, market._vwap_reclaim_retest_setup, market._squeeze_breakout_setup, market._liquidity_sweep_setup,
                 market._stochrsi_pullback_setup, market._supertrend_flip_setup, market._sr_bounce_setup)
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
        self.assertEqual({fn.__name__ for fn in methods}, {"_breakout_retest_setup", "_ema_vwap_setup", "_vwap_reclaim_retest_setup"})
        self.assertNotIn(market._sr_bounce_setup, methods)  # eksperimen tidak ikut routing default
        self.assertNotIn(market._stochrsi_pullback_setup, methods)
        self.assertNotIn(market._supertrend_flip_setup, methods)
        methods, regime = market._select_strategy_functions(10, False, False)
        self.assertEqual(regime, "RANGE")
        self.assertIn(market._bb_rsi_setup, methods)
        self.assertNotIn(market._liquidity_sweep_setup, methods)  # opt-in setelah backtest
        self.assertNotIn(market._squeeze_breakout_setup, methods)  # strategi eksperimen
        self.assertFalse(hasattr(market, "_macd_divergence_setup"))
        methods, regime = market._select_strategy_functions(21, False, False)
        self.assertEqual(regime, "TRANSITION")
        self.assertGreaterEqual(len(methods), 2)


    def test_strategy_allowlist_and_manual_routes(self):
        from unittest import mock
        self.assertIn("ema_vwap", market.SUPPORTED_STRATEGIES)
        self.assertIn("liquidity_sweep", market.SUPPORTED_STRATEGIES)
        self.assertIn("vwap_reclaim", market.SUPPORTED_STRATEGIES)
        with mock.patch.object(market, "SIGNAL_STRATEGY", "ema_vwap"):
            methods, regime = market._select_strategy_functions(20, False, False)
            self.assertEqual(regime, "MANUAL")
            self.assertEqual(methods, (market._ema_vwap_setup,))
        with mock.patch.object(market, "SIGNAL_STRATEGY", "liquidity_sweep"):
            methods, regime = market._select_strategy_functions(20, False, False)
            self.assertEqual(regime, "MANUAL")
            self.assertEqual(methods, (market._liquidity_sweep_setup,))
        with mock.patch.object(market, "SIGNAL_STRATEGY", "vwap_reclaim"):
            methods, regime = market._select_strategy_functions(20, False, False)
            self.assertEqual(regime, "MANUAL")
            self.assertEqual(methods, (market._vwap_reclaim_retest_setup,))

    def test_squeeze_breakout_triggers_on_constructed_data(self):
        from unittest import mock
        n = 260
        rng = np.random.default_rng(3)
        close = 100 + np.cumsum(rng.normal(0, 0.35, n))        # volatilitas normal
        close[-40:-1] = close[-41] + rng.normal(0, 0.06, 39)    # squeeze: range sempit
        close[-1] = close[-2] + 0.4                             # candle breakout + volume
        op = np.r_[close[0], close[:-1]]
        high = np.maximum(op, close) + 0.05; low = np.minimum(op, close) - 0.05
        vol = np.full(n, 1000.0); vol[-1] = 3000.0
        df = market.add_indicators(pd.DataFrame({"start": np.arange(n) * 300000, "open": op, "high": high,
                                                  "low": low, "close": close, "volume": vol, "turnover": vol * close}))
        m15 = pd.Series({"ema20": 101.0, "ema50": 100.0, "close": 101.0, "adx": 20.0})
        m30 = pd.Series({"ema20": 101.0, "ema50": 100.0, "ema200": 95.0, "close": 101.0, "adx": 20.0})
        price, atr = float(df.close.iloc[-1]), float(df.atr.iloc[-1])
        with mock.patch.object(market, "MAX_FEE_R", 10.0):  # skala sintetis: abaikan filter fee
            res = market._squeeze_breakout_setup("LONG", df, m15, m30, price, atr, 0.0, False, {})
            self.assertIsNotNone(res)
            self.assertEqual(res["strategy"], "SQUEEZE_BREAKOUT")
            self.assertLess(res["sl"], price); self.assertGreater(res["tp"], price)
            # tanpa lonjakan volume -> ditolak
            vol2 = vol.copy(); vol2[-1] = 1000.0
            df2 = df.copy(); df2["volume"] = vol2; df2 = market.add_indicators(df2[["start", "open", "high", "low", "close", "volume", "turnover"]])
            self.assertIsNone(market._squeeze_breakout_setup("LONG", df2, m15, m30, price, atr, 0.0, False, {}))
            # arah berlawanan tidak boleh memicu
            self.assertIsNone(market._squeeze_breakout_setup("SHORT", df, m15, m30, price, atr, 0.0, False, {}))

    # ---- helper data sintetis untuk StochRSI Pullback & Supertrend Flip
    @staticmethod
    def _mk(close, noise=0.1, vol_last=1000.0, seed=1):
        n = len(close); rng = np.random.default_rng(seed)
        op = np.r_[close[0], close[:-1]]; pad = np.abs(rng.normal(0, noise, n)) + 0.0002 * np.abs(close)
        vol = np.full(n, 1000.0); vol[-1] = vol_last
        return market.add_indicators(pd.DataFrame({"start": np.arange(n) * 300000, "open": op,
            "high": np.maximum(op, close) + pad, "low": np.minimum(op, close) - pad,
            "close": close, "volume": vol, "turnover": vol * close}))

    @staticmethod
    def _htf(up=True):
        b = 100.0
        return pd.Series({"ema20": b + (1 if up else -1), "ema50": b, "ema200": b - (5 if up else -5),
                          "close": b + (1 if up else -1), "adx": 22.0})

    def test_stochrsi_pullback_long_and_short(self):
        from unittest import mock
        rng = np.random.default_rng(1)
        close = 100 + np.cumsum(rng.normal(0.12, 0.25, 260)); base = close[-7]
        close[-6:-1] = base + np.array([-0.3, -0.6, -0.85, -1.0, -1.1]); close[-1] = close[-2] + 0.7
        with mock.patch.object(market, "MAX_FEE_R", 10.0):
            df = self._mk(close, vol_last=1500); e = df.iloc[-1]
            res = market._stochrsi_pullback_setup("LONG", df, self._htf(True), self._htf(True), float(e.close), float(e.atr), 0.0, False, {})
            self.assertIsNotNone(res); self.assertEqual(res["strategy"], "STOCHRSI_PULLBACK")
            self.assertLess(res["sl"], float(e.close)); self.assertGreater(res["tp"], float(e.close))
            # tren 15m/30m berlawanan -> ditolak
            self.assertIsNone(market._stochrsi_pullback_setup("LONG", df, self._htf(False), self._htf(False), float(e.close), float(e.atr), 0.0, False, {}))
            # cermin harga -> SHORT
            dfs = self._mk(2 * close[0] - close, vol_last=1500); es = dfs.iloc[-1]
            rs = market._stochrsi_pullback_setup("SHORT", dfs, self._htf(False), self._htf(False), float(es.close), float(es.atr), 0.0, False, {})
            self.assertIsNotNone(rs); self.assertGreater(rs["sl"], float(es.close)); self.assertLess(rs["tp"], float(es.close))

    def test_supertrend_flip_long_and_rejections(self):
        from unittest import mock
        rng = np.random.default_rng(2)
        close = 100 + np.cumsum(rng.normal(0.15, 0.25, 260)); b = close[-7]
        close[-6:-1] = b - np.linspace(0.3, 0.3 * 6, 5) * 0.9; close[-1] = close[-2] + 1.0
        with mock.patch.object(market, "MAX_FEE_R", 10.0):
            df = self._mk(close, vol_last=2000, seed=2); e = df.iloc[-1]
            self.assertEqual((int(df.st_dir.iloc[-2]), int(df.st_dir.iloc[-1])), (-1, 1))
            res = market._supertrend_flip_setup("LONG", df, self._htf(True), self._htf(True), float(e.close), float(e.atr), 0.0, False, {})
            self.assertIsNotNone(res); self.assertEqual(res["strategy"], "SUPERTREND_FLIP")
            self.assertLess(res["sl"], float(e.close)); self.assertGreater(res["tp"], float(e.close))
            self.assertIsNone(market._supertrend_flip_setup("SHORT", df, self._htf(False), self._htf(False), float(e.close), float(e.atr), 0.0, False, {}))
            low_vol = self._mk(close, vol_last=1000, seed=2)   # tanpa lonjakan volume
            self.assertIsNone(market._supertrend_flip_setup("LONG", low_vol, self._htf(True), self._htf(True), float(e.close), float(e.atr), 0.0, False, {}))

    def test_supertrend_values_sane(self):
        df = self._mk(100 + np.cumsum(np.random.default_rng(5).normal(0.1, 0.3, 260)))
        self.assertTrue(set(df.st_dir.unique()) <= {-1, 1})
        up = df[df.st_dir == 1]; dn = df[df.st_dir == -1]
        self.assertTrue((up.st_line <= up.close + 1e-9).all()); self.assertTrue((dn.st_line >= dn.close - 1e-9).all())

    # ---- Support/Resistance
    @staticmethod
    def _sr_df(last=None, mirror=False, n=260, seed=1):
        rng = np.random.default_rng(seed); t = np.arange(n)
        c = 103 + 3 * np.sin(2 * np.pi * t / 28) + rng.normal(0, 0.12, n)   # range ~100..106
        o = np.r_[c[0], c[:-1]]; h = np.maximum(o, c) + 0.12; l = np.minimum(o, c) - 0.12
        if last: o[-1], h[-1], l[-1], c[-1] = last
        if mirror:
            o, h, l, c = 206 - o, 206 - l, 206 - h, 206 - c
        v = np.full(n, 1000.0); v[-1] = 1500
        return market.add_indicators(pd.DataFrame({"start": t * 300000, "open": o, "high": h, "low": l,
                                                    "close": c, "volume": v, "turnover": v * c}))

    @staticmethod
    def _flat():
        return pd.Series({"ema20": 103.0, "ema50": 103.0, "ema200": 103.0, "close": 103.0, "adx": 15.0})

    def test_sr_levels_detected(self):
        df = self._sr_df(last=(100.9, 101.0, 99.9, 101.0))
        lv = market._sr_levels(df, float(df.atr.iloc[-1]))
        prices = [x["price"] for x in lv]
        self.assertTrue(any(abs(p - 99.8) < 0.6 for p in prices), prices)
        self.assertTrue(any(abs(p - 106.2) < 0.6 for p in prices), prices)

    def test_sr_bounce_long_short_and_rejections(self):
        from unittest import mock
        m = self._flat()
        with mock.patch.object(market, "MAX_FEE_R", 10.0):
            df = self._sr_df(last=(100.9, 101.0, 99.9, 101.0)); atr = float(df.atr.iloc[-1]); price = float(df.close.iloc[-1])
            r = market._sr_bounce_setup("LONG", df, m, m, price, atr, 0.0, False, {})
            self.assertIsNotNone(r); self.assertEqual(r["strategy"], "SR_BOUNCE")
            self.assertLess(r["sl"], price); self.assertGreater(r["tp"], price); self.assertGreaterEqual(r["rr"], 1.0)
            self.assertIsNone(market._sr_bounce_setup("SHORT", df, m, m, price, atr, 0.0, False, {}))
            # cermin -> SHORT di resistance
            dfs = self._sr_df(last=(100.9, 101.0, 99.9, 101.0), mirror=True); atrs = float(dfs.atr.iloc[-1]); ps = float(dfs.close.iloc[-1])
            rs = market._sr_bounce_setup("SHORT", dfs, m, m, ps, atrs, 0.0, False, {})
            self.assertIsNotNone(rs); self.assertGreater(rs["sl"], ps); self.assertLess(rs["tp"], ps)
            # support ditembus (close di bawah level) -> tidak ada bounce
            brk = self._sr_df(last=(100.5, 100.6, 98.4, 98.6)); pb = float(brk.close.iloc[-1])
            self.assertIsNone(market._sr_bounce_setup("LONG", brk, m, m, pb, float(brk.atr.iloc[-1]), 0.0, False, {}))
            # di tengah range (jauh dari level) -> tidak ada bounce
            mid = self._sr_df(last=(102.9, 103.1, 102.8, 103.0)); pm = float(mid.close.iloc[-1])
            self.assertIsNone(market._sr_bounce_setup("LONG", mid, m, m, pm, float(mid.atr.iloc[-1]), 0.0, False, {}))
            # melawan tren 30m kuat -> ditolak
            dn = pd.Series({"ema20": 101.0, "ema50": 103.0, "ema200": 106.0, "close": 100.0, "adx": 30.0})
            self.assertIsNone(market._sr_bounce_setup("LONG", df, m, dn, price, atr, 0.0, False, {}))

    def test_sr_confluence_adjusts_score(self):
        from unittest import mock
        df = self._sr_df(last=(100.6, 100.7, 100.3, 100.5)); atr = float(df.atr.iloc[-1])
        with mock.patch.object(market, "SR_CONFLUENCE", True):
            near = market._sr_confluence({"direction": "LONG", "tp": 101.5, "score": 70, "strategy": "EMA_PULLBACK_VWAP", "reasons": []}, df, 100.5, atr)
            self.assertGreater(near["score"], 70)                      # entry dekat support
            blocked = market._sr_confluence({"direction": "LONG", "tp": 110.0, "score": 70, "strategy": "EMA_PULLBACK_VWAP", "reasons": []}, df, 100.5, atr)
            self.assertTrue(any("terhalang" in r for r in blocked["reasons"]))   # resistance ~106 menghalangi TP
            self.assertLess(blocked["score"], near["score"])
            same = market._sr_confluence({"direction": "LONG", "tp": 110.0, "score": 70, "strategy": "SR_BOUNCE", "reasons": []}, df, 100.5, atr)
            self.assertEqual(same["score"], 70)                        # SR_BOUNCE tidak diberi bonus ganda
        with mock.patch.object(market, "SR_CONFLUENCE", False):
            off = market._sr_confluence({"direction": "LONG", "tp": 110.0, "score": 70, "strategy": "X", "reasons": []}, df, 100.5, atr)
            self.assertEqual(off["score"], 70)

    def test_risk_levels_have_valid_direction(self):
        long_levels = market._risk_levels("LONG", 100, 1, 98, 101, 1.5)
        short_levels = market._risk_levels("SHORT", 100, 1, 98, 101, 1.5)
        self.assertIsNotNone(long_levels)
        self.assertIsNotNone(short_levels)
        self.assertLess(long_levels[0], 100); self.assertGreater(long_levels[1], 100)
        self.assertGreater(short_levels[0], 100); self.assertLess(short_levels[1], 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
