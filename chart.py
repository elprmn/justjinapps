"""Render chart candlestick PNG dengan level Entry / SL / TP untuk embed Discord.

Memakai matplotlib OO API (tanpa pyplot) sehingga aman dipanggil dari thread (asyncio.to_thread).
"""
import io
import os
import threading
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter

from market import fmt_price

CHART_ENABLED = os.getenv("CHART_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
CHART_CANDLES = max(30, min(200, int(os.getenv("CHART_CANDLES", "80"))))
CHART_TZ = os.getenv("CHART_TZ", "Asia/Jakarta").strip() or "UTC"

_render_lock = threading.Lock()

BG, PANEL, GRID, TEXT = "#0f1117", "#151922", "#242a36", "#cfd3dc"
UP, DOWN = "#26a69a", "#ef5350"
ENTRY_C, SL_C, TP_C = "#e0e6f0", "#ff5252", "#00e676"


def _fmt(v):
    """Format harga ringkas untuk label level (presisi menyesuaikan besar harga)."""
    v = float(v)
    dec = 2 if abs(v) >= 1000 else 4 if abs(v) >= 1 else 6 if abs(v) >= 0.01 else 8
    return f"{v:,.{dec}f}".rstrip("0").rstrip(".") if "." in f"{v:,.{dec}f}" else f"{v:,.{dec}f}"


def _tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(CHART_TZ), CHART_TZ
    except Exception:
        return timezone.utc, "UTC"


def render_chart(s: dict) -> bytes:
    """Kembalikan PNG (bytes). `s` = dict hasil analyze_symbol (butuh kunci `chart_df`)."""
    df = s.get("chart_df")
    if df is None or len(df) < 10:
        raise ValueError("Data candle untuk chart tidak tersedia")
    df = df.tail(CHART_CANDLES).reset_index(drop=True)
    n = len(df)
    entry, sl, tp = float(s["entry"]), float(s["sl"]), float(s["tp"])
    is_long = s["direction"] == "LONG"
    tz, tz_name = _tz()

    fig = Figure(figsize=(10, 5.8), dpi=100, facecolor=BG)
    gs = fig.add_gridspec(2, 1, height_ratios=[5, 1], hspace=0.04,
                          left=0.045, right=0.915, top=0.885, bottom=0.075)
    ax = fig.add_subplot(gs[0])
    axv = fig.add_subplot(gs[1], sharex=ax)
    for a in (ax, axv):
        a.set_facecolor(PANEL)
        a.grid(True, color=GRID, linewidth=0.6, alpha=0.8)
        a.tick_params(colors=TEXT, labelsize=8)
        for sp in a.spines.values():
            sp.set_color(GRID)

    # --- candlestick ---
    xs = list(range(n))
    opens, highs, lows, closes = (df[c].astype(float).values for c in ("open", "high", "low", "close"))
    colors = [UP if c >= o else DOWN for o, c in zip(opens, closes)]
    ax.vlines(xs, lows, highs, colors=colors, linewidth=1.0, zorder=3)
    body_bottom = [min(o, c) for o, c in zip(opens, closes)]
    body_h = [max(abs(c - o), (highs.max() - lows.min()) * 0.0008) for o, c in zip(opens, closes)]
    ax.bar(xs, body_h, bottom=body_bottom, width=0.62, color=colors, zorder=4)

    # --- EMA ---
    for col, color, lbl in (("ema20", "#ffb74d", "EMA20"), ("ema50", "#64b5f6", "EMA50")):
        if col in df.columns:
            ax.plot(xs, df[col].astype(float).values, color=color, linewidth=1.1, alpha=0.9, label=lbl, zorder=2)

    # --- zona risk/reward mulai dari candle sinyal ke kanan ---
    pad_right = max(10, n // 6)
    x0, x1 = n - 1, n - 1 + pad_right
    ax.fill_between([x0, x1], entry, tp, color=TP_C, alpha=0.13, zorder=1, linewidth=0)
    ax.fill_between([x0, x1], entry, sl, color=SL_C, alpha=0.13, zorder=1, linewidth=0)
    for price, color, style, lw in ((tp, TP_C, "-", 1.4), (sl, SL_C, "-", 1.4), (entry, ENTRY_C, "--", 1.1)):
        ax.axhline(price, color=color, linestyle=style, linewidth=lw, alpha=0.9, zorder=5)

    # --- label level di sisi kanan ---
    xlim_right = x1 + 0.5
    ax.set_xlim(-1, xlim_right)
    risk_pct = abs(entry - sl) / entry * 100
    reward_pct = abs(tp - entry) / entry * 100
    labels = (
        (tp, f"TP  {_fmt(tp)}  (+{reward_pct:.2f}%)", TP_C),
        (entry, f"ENTRY  {_fmt(entry)}", ENTRY_C),
        (sl, f"SL  {_fmt(sl)}  (-{risk_pct:.2f}%)", SL_C),
    )
    # Label diletakkan DI DALAM area plot (rata kanan) dan digeser agar tidak saling menimpa.
    ymin = min(lows.min(), sl, tp)
    ymax = max(highs.max(), sl, tp)
    pad = (ymax - ymin) * 0.06 or entry * 0.002
    lo, hi = ymin - pad, ymax + pad
    min_gap = (hi - lo) * 0.065
    ordered = sorted(labels, key=lambda t: t[0], reverse=True)
    ys = [t[0] for t in ordered]
    for i in range(1, len(ys)):
        ys[i] = min(ys[i], ys[i - 1] - min_gap)
    if ys[-1] < lo + min_gap * 0.6:  # terlalu ke bawah: geser seluruh label ke atas
        shift = lo + min_gap * 0.6 - ys[-1]
        ys = [y + shift for y in ys]
    for (price, text, color), y in zip(ordered, ys):
        ax.annotate(text, xy=(1.0, y), xycoords=("axes fraction", "data"), xytext=(-5, 0),
                    textcoords="offset points", va="center", ha="right", fontsize=8, fontweight="bold",
                    color=BG, bbox=dict(boxstyle="round,pad=0.25", fc=color, ec="none"), zorder=7)

    # --- sumbu Y: pastikan SL & TP terlihat ---
    ax.set_ylim(lo, hi)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: fmt_price(v)))
    ax.yaxis.tick_right()
    ax.tick_params(labelbottom=False)
    ax.legend(loc="upper left", fontsize=8, frameon=False, labelcolor=TEXT, ncols=2)

    # --- volume ---
    vols = df["volume"].astype(float).values
    axv.bar(xs, vols, width=0.62, color=colors, alpha=0.75)
    axv.set_ylabel("Vol", color=TEXT, fontsize=8)
    axv.yaxis.tick_right()
    axv.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6 else (f"{v/1e3:.0f}K" if v >= 1e3 else f"{v:.0f}")))

    # --- label waktu ---
    step = max(1, n // 8)
    ticks = list(range(0, n, step))
    axv.set_xticks(ticks)
    axv.set_xticklabels([
        datetime.fromtimestamp(int(df.loc[i, "start"]) / 1000, tz).strftime("%H:%M") for i in ticks
    ])
    axv.set_xlim(-1, xlim_right)

    # --- judul ---
    side_color = UP if is_long else DOWN
    tier = s.get("tier")
    tier_txt = {"SIGNAL": "SIGNAL", "WATCH": "WATCH"}.get(tier, "")
    fig.text(0.045, 0.945, f'{s["symbol"]}  ·  {s["direction"]}', color=side_color, fontsize=15, fontweight="bold", va="center")
    sub = f'TF {s["timeframe"]}m  ·  skor {s["score"]}/100  ·  RR 1:{s["rr"]:.2f}'
    if tier_txt:
        sub += f"  ·  {tier_txt}"
    fig.text(0.045, 0.905, sub, color=TEXT, fontsize=9, va="center")
    fig.text(0.915, 0.945, f"Bybit USDT Perp · {tz_name}", color="#7d8590", fontsize=8, ha="right", va="center")

    buf = io.BytesIO()
    with _render_lock:
        fig.savefig(buf, format="png", facecolor=BG)
    return buf.getvalue()
