"""
dashboard.py — CryptoStream real-time analytics dashboard.

Reads directly from Silver and Gold Delta tables via the `deltalake` Python
library. No SparkSession, no JVM. The pipeline writes to Delta, the dashboard
reads from it — fully decoupled.

GOLD SCHEMA CONTRACT (required columns):
    window_start, window_end, symbol, open, high, low, close, volume,
    avg_price, trade_count,
    rsi, macd_approx, signal_approx, macd_histogram, vwap,
    bb_upper, bb_middle, bb_lower

SILVER SCHEMA CONTRACT (required columns):
    window_start, window_end, symbol, open, high, low, close, volume,
    avg_price, trade_count

Run with:
    streamlit run dashboard/app.py

Dependencies:
    pip install streamlit deltalake pandas plotly python-dotenv streamlit-autorefresh
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from deltalake import DeltaTable
from dotenv import load_dotenv
from plotly.subplots import make_subplots

# Optional non-blocking auto-refresh component
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

load_dotenv()

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver/ohlcv_1m")
GOLD_PATH   = os.getenv("GOLD_PATH",   "./data/gold/indicators_1m")
DISPLAY_TZ  = os.getenv("DISPLAY_TZ",  "UTC")  # e.g. "Europe/Paris", "UTC"
REFRESH_MS  = int(os.getenv("REFRESH_MS", "30000"))  # 30 s

# Required columns — fail loud if Gold/Silver writers drift
SILVER_REQUIRED = {"window_start", "symbol", "open", "high", "low", "close",
                   "volume", "avg_price", "trade_count"}
GOLD_REQUIRED   = {"window_start", "symbol", "close",
                   "rsi", "macd_approx", "signal_approx", "macd_histogram",
                   "bb_upper", "bb_middle", "bb_lower", "vwap"}
# NOTE: `macd_approx` = SMA-based approximation of MACD (NOT true EMA12−EMA26).
# Labels in the UI reflect this honestly. Document in README.

# Color palette
CLR_BG, CLR_CARD, CLR_BORDER = "#0a0e1a", "#111827", "#1f2937"
CLR_GREEN, CLR_RED, CLR_YELLOW = "#00ff88", "#ff4466", "#ffd700"
CLR_BLUE, CLR_PURPLE, CLR_TEXT, CLR_MUTED = "#3b82f6", "#a855f7", "#e2e8f0", "#6b7280"

# Indicator warm-up requirements
RSI_WARMUP, MACD_WARMUP, BB_WARMUP = 14, 35, 20

# ─── PAGE ────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="CryptoStream Analytics", page_icon="₿",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');
    .stApp {{ background-color: {CLR_BG}; font-family: 'DM Sans', sans-serif; color: {CLR_TEXT}; }}
    section[data-testid="stSidebar"] {{ background-color: {CLR_CARD}; border-right: 1px solid {CLR_BORDER}; }}
    .metric-card {{ background: {CLR_CARD}; border: 1px solid {CLR_BORDER}; border-radius: 12px; padding: 20px; margin: 6px 0; }}
    .metric-label {{ font-family: 'Space Mono', monospace; font-size: 11px; color: {CLR_MUTED}; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 6px; }}
    .metric-value {{ font-family: 'Space Mono', monospace; font-size: 28px; font-weight: 700; color: {CLR_GREEN}; line-height: 1; }}
    .metric-sub {{ font-size: 12px; color: {CLR_MUTED}; margin-top: 4px; }}
    .section-header {{ font-family: 'Space Mono', monospace; font-size: 13px; color: {CLR_MUTED}; text-transform: uppercase; letter-spacing: 3px; padding: 24px 0 12px 0; border-bottom: 1px solid {CLR_BORDER}; margin-bottom: 16px; }}
    .badge {{ padding: 3px 10px; border-radius: 20px; font-size: 11px; font-family: 'Space Mono', monospace; font-weight: 700; }}
    .badge-green  {{ background: rgba(0,255,136,0.15);  color: {CLR_GREEN};  border: 1px solid {CLR_GREEN}; }}
    .badge-red    {{ background: rgba(255,68,102,0.15); color: {CLR_RED};    border: 1px solid {CLR_RED}; }}
    .badge-yellow {{ background: rgba(255,215,0,0.15);  color: {CLR_YELLOW}; border: 1px solid {CLR_YELLOW}; }}
    .badge-blue   {{ background: rgba(59,130,246,0.15); color: {CLR_BLUE};   border: 1px solid {CLR_BLUE}; }}
    .warm-banner  {{ background: rgba(255,215,0,0.08); border: 1px solid {CLR_YELLOW};
                     border-radius: 8px; padding: 10px 16px; color: {CLR_YELLOW};
                     font-family: 'Space Mono', monospace; font-size: 12px; margin: 8px 0; }}
    #MainMenu, footer, header {{ visibility: hidden; }}
    .block-container {{ padding-top: 1.5rem; padding-bottom: 2rem; }}
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans", color=CLR_TEXT, size=12),
    xaxis=dict(gridcolor=CLR_BORDER, showgrid=True, zeroline=False),
    yaxis=dict(gridcolor=CLR_BORDER, showgrid=True, zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=CLR_BORDER),
    margin=dict(l=10, r=10, t=30, b=10),
)

# ─── DATA LOADERS ────────────────────────────────────────────────────────────
def _localize(df: pd.DataFrame, cols=("window_start", "window_end")) -> pd.DataFrame:
    """Convert UTC timestamp columns to DISPLAY_TZ for clean axis labels."""
    for c in cols:
        if c in df.columns:
            ts = pd.to_datetime(df[c], utc=True)
            df[c] = ts.dt.tz_convert(DISPLAY_TZ) if DISPLAY_TZ != "UTC" else ts
    return df


def _validate_schema(df: pd.DataFrame, required: set, name: str) -> list[str]:
    """Return list of missing columns; never raises (dashboard must keep rendering)."""
    missing = sorted(required - set(df.columns))
    if missing:
        st.error(f"⚠️ {name} schema drift — missing columns: {missing}. "
                 f"Check the writer in your pipeline.")
    return missing


@st.cache_data(ttl=30, show_spinner=False)
def load_silver() -> pd.DataFrame:
    try:
        df = DeltaTable(SILVER_PATH).to_pandas()
    except Exception as e:
        st.error(f"❌ Silver table unreadable at {SILVER_PATH}: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df = _localize(df)
    _validate_schema(df, SILVER_REQUIRED, "Silver")
    return df.sort_values(["symbol", "window_start"]).reset_index(drop=True)


@st.cache_data(ttl=30, show_spinner=False)
def load_gold() -> pd.DataFrame:
    try:
        df = DeltaTable(GOLD_PATH).to_pandas()
    except Exception as e:
        st.warning(f"⚠️ Gold table unreadable at {GOLD_PATH}: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df = _localize(df)
    _validate_schema(df, GOLD_REQUIRED, "Gold")
    return df.sort_values(["symbol", "window_start"]).reset_index(drop=True)


def safe_last(series: pd.Series) -> float:
    """Return last non-NaN value, or NaN if none."""
    s = series.dropna()
    return float(s.iloc[-1]) if not s.empty else float("nan")


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style='padding: 8px 0 24px 0;'>
        <div style='font-family: Space Mono, monospace; font-size: 20px; font-weight: 700; color: {CLR_GREEN};'>₿ CryptoStream</div>
        <div style='font-size: 11px; color: {CLR_MUTED}; letter-spacing: 2px; text-transform: uppercase; margin-top: 4px;'>Analytics Dashboard</div>
    </div>
    """, unsafe_allow_html=True)

    # Auto-refresh — non-blocking
    auto = st.checkbox("Auto-refresh (30s)", value=True)
    if auto:
        if _HAS_AUTOREFRESH:
            st_autorefresh(interval=REFRESH_MS, key="autorefresh")
        else:
            st.caption("⚠️ Install `streamlit-autorefresh` for live refresh. "
                       "Cache TTL still refreshes data on user action.")

    if st.button("🔄 Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ─── LOAD ────────────────────────────────────────────────────────────────────
silver = load_silver()
gold   = load_gold()

if silver.empty:
    st.warning("⚠️ Silver is empty — has the pipeline written any aggregations yet? "
               "Check that the Spark Silver job is running and pointed at "
               f"`{SILVER_PATH}`.")
    st.stop()

# Auto-discover symbols from data (don't hardcode)
SYMBOLS = sorted(silver["symbol"].unique().tolist())

with st.sidebar:
    symbol = st.selectbox("Symbol", SYMBOLS, index=0)
    st.markdown("---")
    st.markdown(f"""
    <div style='font-family: Space Mono, monospace; font-size: 10px; color: {CLR_MUTED};'>
        <div style='margin-bottom: 8px; text-transform: uppercase; letter-spacing: 2px;'>Sources</div>
        <div>🟡 Silver: {SILVER_PATH}</div>
        <div>🥇 Gold:   {GOLD_PATH}</div>
        <div style='margin-top: 12px;'>TZ: {DISPLAY_TZ}</div>
        <div>Refresh: {REFRESH_MS//1000}s</div>
        <div>Last load: {datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")}</div>
        <div style='margin-top: 12px;'>Symbols: {len(SYMBOLS)}</div>
    </div>
    """, unsafe_allow_html=True)

# Filter to selected symbol
sv = silver[silver["symbol"] == symbol].copy()
gd = gold[gold["symbol"] == symbol].copy() if not gold.empty else pd.DataFrame()

# ─── HEADER ──────────────────────────────────────────────────────────────────
n_silver_rows = len(sv)
warmup_msgs = []
if n_silver_rows < BB_WARMUP:
    warmup_msgs.append(f"Bollinger needs {BB_WARMUP} candles ({n_silver_rows}/{BB_WARMUP})")
if n_silver_rows < RSI_WARMUP:
    warmup_msgs.append(f"RSI needs {RSI_WARMUP} candles ({n_silver_rows}/{RSI_WARMUP})")
if n_silver_rows < MACD_WARMUP:
    warmup_msgs.append(f"MACD needs {MACD_WARMUP} candles ({n_silver_rows}/{MACD_WARMUP})")

st.markdown(f"""
<div style='display:flex; align-items:baseline; gap:16px; margin-bottom:8px;'>
    <span style='font-family: Space Mono, monospace; font-size: 32px; font-weight: 700; color: {CLR_GREEN};'>{symbol}</span>
    <span style='font-size: 13px; color: {CLR_MUTED}; font-family: Space Mono, monospace;'>REAL-TIME ANALYTICS · {n_silver_rows} CANDLES</span>
</div>
""", unsafe_allow_html=True)

if warmup_msgs:
    st.markdown(
        f"<div class='warm-banner'>⏳ Warming up — " + " · ".join(warmup_msgs) + "</div>",
        unsafe_allow_html=True,
    )

# ─── ① LATEST PRICES ─────────────────────────────────────────────────────────
st.markdown('<div class="section-header">① Latest Prices — All Symbols</div>', unsafe_allow_html=True)

cols = st.columns(len(SYMBOLS))
for i, sym in enumerate(SYMBOLS):
    sym_data = silver[silver["symbol"] == sym]
    with cols[i]:
        if sym_data.empty:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-label">{sym}</div>
                <div class="metric-value" style="color:{CLR_MUTED};">—</div>
                <div class="metric-sub">No data</div></div>""", unsafe_allow_html=True)
            continue

        latest = sym_data.iloc[-1]
        price  = float(latest["avg_price"])
        prev_p = float(sym_data.iloc[-2]["avg_price"]) if len(sym_data) > 1 else price
        change = price - prev_p
        pct    = (change / prev_p * 100.0) if prev_p else 0.0
        color  = CLR_GREEN if change >= 0 else CLR_RED
        arrow  = "▲" if change >= 0 else "▼"
        ts     = pd.Timestamp(latest["window_start"]).strftime("%H:%M")
        st.markdown(f"""<div class="metric-card">
            <div class="metric-label">{sym}</div>
            <div class="metric-value" style="color:{color};">${price:,.4f}</div>
            <div class="metric-sub" style="color:{color};">{arrow} {pct:+.2f}% · {ts}</div>
        </div>""", unsafe_allow_html=True)

# ─── ② PRICE + BOLLINGER ─────────────────────────────────────────────────────
st.markdown(f'<div class="section-header">② Price + Bollinger Bands — {symbol}</div>', unsafe_allow_html=True)

fig = go.Figure()
if not gd.empty and {"bb_upper", "bb_middle", "bb_lower"}.issubset(gd.columns):
    bb = gd.dropna(subset=["bb_upper", "bb_middle", "bb_lower"])
    if not bb.empty:
        fig.add_trace(go.Scatter(x=bb["window_start"], y=bb["bb_upper"], name="BB Upper",
                                 line=dict(color=CLR_PURPLE, width=1, dash="dot"), opacity=0.6))
        fig.add_trace(go.Scatter(x=bb["window_start"], y=bb["bb_lower"], name="BB Lower",
                                 line=dict(color=CLR_PURPLE, width=1, dash="dot"),
                                 fill="tonexty", fillcolor="rgba(168,85,247,0.05)", opacity=0.6))
        fig.add_trace(go.Scatter(x=bb["window_start"], y=bb["bb_middle"], name="BB Middle (SMA20)",
                                 line=dict(color=CLR_PURPLE, width=1.5)))

fig.add_trace(go.Scatter(x=sv["window_start"], y=sv["avg_price"], name="Avg Price",
                          line=dict(color=CLR_GREEN, width=2)))

# VWAP overlay from Gold (institutional reference price)
if not gd.empty and "vwap" in gd.columns:
    vwap_df = gd.dropna(subset=["vwap"])
    if not vwap_df.empty:
        fig.add_trace(go.Scatter(x=vwap_df["window_start"], y=vwap_df["vwap"],
                                  name="VWAP",
                                  line=dict(color=CLR_BLUE, width=1.5, dash="dash")))

fig.update_layout(**PLOTLY_LAYOUT, height=320,
                  title=dict(text=f"{symbol} — Price + Bollinger + VWAP",
                             font=dict(size=13, color=CLR_MUTED)))
st.plotly_chart(fig, use_container_width=True)

# ─── ③ OHLCV CANDLESTICK ─────────────────────────────────────────────────────
st.markdown(f'<div class="section-header">③ OHLCV Candlestick — {symbol}</div>', unsafe_allow_html=True)

if {"open", "high", "low", "close", "volume"}.issubset(sv.columns):
    fig_c = make_subplots(rows=2, cols=1, shared_xaxes=True,
                          vertical_spacing=0.06, row_heights=[0.75, 0.25])
    fig_c.add_trace(go.Candlestick(
        x=sv["window_start"], open=sv["open"], high=sv["high"],
        low=sv["low"], close=sv["close"], name="OHLC",
        increasing=dict(line=dict(color=CLR_GREEN), fillcolor=CLR_GREEN),
        decreasing=dict(line=dict(color=CLR_RED),   fillcolor=CLR_RED),
    ), row=1, col=1)
    vol_colors = [CLR_GREEN if c >= o else CLR_RED for c, o in zip(sv["close"], sv["open"])]
    fig_c.add_trace(go.Bar(x=sv["window_start"], y=sv["volume"],
                           name="Volume", marker_color=vol_colors, opacity=0.7), row=2, col=1)
    fig_c.update_layout(**PLOTLY_LAYOUT, height=400, xaxis_rangeslider_visible=False,
                         title=dict(text=f"{symbol} — 1-Minute Candles",
                                    font=dict(size=13, color=CLR_MUTED)))
    fig_c.update_yaxes(gridcolor=CLR_BORDER)
    st.plotly_chart(fig_c, use_container_width=True)

# ─── ④ VOLUME + TRADE COUNT ──────────────────────────────────────────────────
st.markdown(f'<div class="section-header">④ Volume & Trade Activity — {symbol}</div>', unsafe_allow_html=True)
c_vol, c_tc = st.columns(2)

with c_vol:
    fig_v = go.Figure(go.Bar(x=sv["window_start"], y=sv["volume"],
                              marker_color=CLR_BLUE, opacity=0.8, name="Volume"))
    fig_v.update_layout(**PLOTLY_LAYOUT, height=260,
                        title=dict(text="Volume per Minute", font=dict(size=13, color=CLR_MUTED)))
    st.plotly_chart(fig_v, use_container_width=True)

with c_tc:
    if "trade_count" in sv.columns:
        fig_t = go.Figure(go.Bar(x=sv["window_start"], y=sv["trade_count"],
                                  marker_color=CLR_YELLOW, opacity=0.8, name="Trade Count"))
        fig_t.update_layout(**PLOTLY_LAYOUT, height=260,
                            title=dict(text="Trade Count per Minute",
                                       font=dict(size=13, color=CLR_MUTED)))
        st.plotly_chart(fig_t, use_container_width=True)

# ─── ⑤ RSI ───────────────────────────────────────────────────────────────────
st.markdown(f'<div class="section-header">⑤ RSI — {symbol}</div>', unsafe_allow_html=True)
c_g, c_h = st.columns([1, 2])

if not gd.empty and "rsi" in gd.columns and not gd["rsi"].dropna().empty:
    rsi_now = safe_last(gd["rsi"])
    rsi_color = CLR_RED if rsi_now >= 70 else (CLR_GREEN if rsi_now <= 30 else CLR_YELLOW)
    zone = "OVERBOUGHT" if rsi_now >= 70 else ("OVERSOLD" if rsi_now <= 30 else "NEUTRAL")

    with c_g:
        fig_g = go.Figure(go.Indicator(
            mode="gauge+number", value=rsi_now,
            number=dict(font=dict(color=rsi_color, family="Space Mono", size=36)),
            gauge=dict(
                axis=dict(range=[0, 100], tickfont=dict(color=CLR_MUTED)),
                bar=dict(color=rsi_color, thickness=0.25),
                bgcolor="rgba(0,0,0,0)", bordercolor=CLR_BORDER,
                steps=[dict(range=[0, 30],   color="rgba(0,255,136,0.15)"),
                       dict(range=[30, 70],  color="rgba(255,215,0,0.10)"),
                       dict(range=[70, 100], color="rgba(255,68,102,0.15)")],
                threshold=dict(line=dict(color=CLR_TEXT, width=2),
                               thickness=0.75, value=rsi_now),
            ),
            title=dict(text=f"RSI(14) · {zone}", font=dict(color=CLR_MUTED, size=12)),
        ))
        fig_g.update_layout(paper_bgcolor="rgba(0,0,0,0)",
                            font=dict(color=CLR_TEXT), height=260,
                            margin=dict(l=20, r=20, t=40, b=10))
        st.plotly_chart(fig_g, use_container_width=True)

    with c_h:
        rsi_df = gd.dropna(subset=["rsi"])
        fig_r = go.Figure()
        fig_r.add_hrect(y0=70, y1=100, fillcolor="rgba(255,68,102,0.08)", line_width=0)
        fig_r.add_hrect(y0=0,  y1=30,  fillcolor="rgba(0,255,136,0.08)",  line_width=0)
        fig_r.add_hline(y=70, line_dash="dot", line_color=CLR_RED,   line_width=1, opacity=0.5)
        fig_r.add_hline(y=30, line_dash="dot", line_color=CLR_GREEN, line_width=1, opacity=0.5)
        fig_r.add_trace(go.Scatter(x=rsi_df["window_start"], y=rsi_df["rsi"],
                                    name="RSI", line=dict(color=CLR_YELLOW, width=2)))
        fig_r.update_layout(**PLOTLY_LAYOUT, height=260,
                             title=dict(text="RSI History",
                                        font=dict(size=13, color=CLR_MUTED)))
        st.plotly_chart(fig_r, use_container_width=True)
else:
    st.info(f"RSI not yet available — needs {RSI_WARMUP} candles per symbol.")

# ─── ⑥ MACD ──────────────────────────────────────────────────────────────────
st.markdown(f'<div class="section-header">⑥ MACD — {symbol}</div>', unsafe_allow_html=True)

if not gd.empty and {"macd_approx", "signal_approx", "macd_histogram"}.issubset(gd.columns):
    md = gd.dropna(subset=["macd_approx"])
    if not md.empty:
        hist_colors = [CLR_GREEN if v >= 0 else CLR_RED
                       for v in md["macd_histogram"].fillna(0)]
        fig_m = make_subplots(rows=2, cols=1, shared_xaxes=True,
                              vertical_spacing=0.06, row_heights=[0.6, 0.4])
        fig_m.add_trace(go.Scatter(x=md["window_start"], y=md["macd_approx"],
                                    name="MACD (SMA12−SMA26 approx)",
                                    line=dict(color=CLR_BLUE, width=2)), row=1, col=1)
        sig = md.dropna(subset=["signal_approx"])
        if not sig.empty:
            fig_m.add_trace(go.Scatter(x=sig["window_start"], y=sig["signal_approx"],
                                        name="Signal (SMA9 of MACD)",
                                        line=dict(color=CLR_YELLOW, width=1.5, dash="dot")),
                             row=1, col=1)
        fig_m.add_trace(go.Bar(x=md["window_start"], y=md["macd_histogram"].fillna(0),
                                name="Histogram", marker_color=hist_colors, opacity=0.8),
                         row=2, col=1)
        fig_m.update_layout(**PLOTLY_LAYOUT, height=340,
                             title=dict(text=f"{symbol} — MACD (SMA approximation, not EMA)",
                                        font=dict(size=13, color=CLR_MUTED)))
        fig_m.update_yaxes(gridcolor=CLR_BORDER)
        st.plotly_chart(fig_m, use_container_width=True)
    else:
        st.info(f"MACD warming up — needs {MACD_WARMUP} candles.")
else:
    st.info("MACD columns not present in Gold table.")

# ─── ⑦ SIGNAL SUMMARY ────────────────────────────────────────────────────────
st.markdown('<div class="section-header">⑦ Signal Summary — All Symbols</div>', unsafe_allow_html=True)


def rsi_zone(rsi: float):
    if pd.isna(rsi):  return "—",          "badge-yellow"
    if rsi >= 70:     return "Overbought", "badge-red"
    if rsi <= 30:     return "Oversold",   "badge-green"
    return "Neutral", "badge-yellow"


def macd_signal_dir(macd: float, sig: float):
    if pd.isna(macd) or pd.isna(sig): return "—", "badge-yellow"
    return ("Bullish ▲", "badge-green") if macd > sig else ("Bearish ▼", "badge-red")


def bb_position(close: float, upper: float, lower: float):
    if pd.isna(close) or pd.isna(upper) or pd.isna(lower): return "—", "badge-yellow"
    if close > upper: return "Above BB ↑",  "badge-red"
    if close < lower: return "Below BB ↓",  "badge-green"
    return "Inside BB", "badge-blue"


def vwap_position(close: float, vwap: float):
    if pd.isna(close) or pd.isna(vwap): return "—", "badge-yellow"
    diff_bps = (close - vwap) / vwap * 10_000  # basis points
    if diff_bps > 5:   return f"Above ↑ ({diff_bps:+.0f}bps)", "badge-green"
    if diff_bps < -5:  return f"Below ↓ ({diff_bps:+.0f}bps)", "badge-red"
    return f"At VWAP ({diff_bps:+.0f}bps)", "badge-blue"


# Build per-symbol latest snapshot — sort first, then take last per group
def latest_per_symbol(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return (df.sort_values("window_start")
              .groupby("symbol", as_index=False)
              .tail(1))


sv_last = latest_per_symbol(silver).set_index("symbol") if not silver.empty else pd.DataFrame()
gd_last = latest_per_symbol(gold).set_index("symbol")   if not gold.empty   else pd.DataFrame()

headers = ["Symbol", "Last Price", "RSI", "RSI Zone", "MACD", "Bollinger", "VWAP"]
hcols = st.columns([1.1, 1.2, 0.8, 1.2, 1.2, 1.2, 1.5])
for col, h in zip(hcols, headers):
    col.markdown(
        f"<div style='font-family:Space Mono,monospace;font-size:10px;color:{CLR_MUTED};"
        f"text-transform:uppercase;letter-spacing:2px;padding-bottom:8px;'>{h}</div>",
        unsafe_allow_html=True,
    )

for sym in SYMBOLS:
    sv_row = sv_last.loc[sym] if sym in sv_last.index else None
    gd_row = gd_last.loc[sym] if sym in gd_last.index else None

    last_price = float(sv_row["avg_price"]) if sv_row is not None else float("nan")
    last_close = float(sv_row["close"])     if sv_row is not None else float("nan")

    def _g(row, k): return float(row[k]) if (row is not None and k in row and pd.notna(row[k])) else float("nan")
    rsi_v  = _g(gd_row, "rsi")
    macd_v = _g(gd_row, "macd_approx")
    sig_v  = _g(gd_row, "signal_approx")
    bb_up  = _g(gd_row, "bb_upper")
    bb_lo  = _g(gd_row, "bb_lower")
    vwap_v = _g(gd_row, "vwap")

    rsi_txt,  rsi_cls  = rsi_zone(rsi_v)
    macd_txt, macd_cls = macd_signal_dir(macd_v, sig_v)
    bb_txt,   bb_cls   = bb_position(last_close, bb_up, bb_lo)
    vwap_txt, vwap_cls = vwap_position(last_close, vwap_v)

    selected = sym == symbol
    name_color = CLR_GREEN if selected else CLR_TEXT
    prefix     = "→ " if selected else "   "

    rcols = st.columns([1.1, 1.2, 0.8, 1.2, 1.2, 1.2, 1.5])
    rcols[0].markdown(f"<div style='font-family:Space Mono,monospace;font-size:13px;font-weight:700;color:{name_color};padding:6px 0;'>{prefix}{sym}</div>", unsafe_allow_html=True)
    rcols[1].markdown(f"<div style='font-family:Space Mono,monospace;font-size:13px;color:{CLR_TEXT};padding:6px 0;'>{'$'+f'{last_price:,.4f}' if not pd.isna(last_price) else '—'}</div>", unsafe_allow_html=True)
    rcols[2].markdown(f"<div style='font-family:Space Mono,monospace;font-size:13px;color:{CLR_YELLOW};padding:6px 0;'>{f'{rsi_v:.1f}' if not pd.isna(rsi_v) else '—'}</div>", unsafe_allow_html=True)
    rcols[3].markdown(f"<span class='badge {rsi_cls}'>{rsi_txt}</span>",   unsafe_allow_html=True)
    rcols[4].markdown(f"<span class='badge {macd_cls}'>{macd_txt}</span>", unsafe_allow_html=True)
    rcols[5].markdown(f"<span class='badge {bb_cls}'>{bb_txt}</span>",     unsafe_allow_html=True)
    rcols[6].markdown(f"<span class='badge {vwap_cls}'>{vwap_txt}</span>", unsafe_allow_html=True)

# ─── FOOTER ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style='margin-top:48px; padding-top:16px; border-top:1px solid {CLR_BORDER};
     font-family:Space Mono,monospace; font-size:10px; color:{CLR_MUTED};
     display:flex; justify-content:space-between;'>
    <span>CryptoStream Analytics · Medallion Pipeline</span>
    <span>Silver → {SILVER_PATH} · Gold → {GOLD_PATH} · TZ {DISPLAY_TZ}</span>
</div>
""", unsafe_allow_html=True)