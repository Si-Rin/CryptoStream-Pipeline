import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from deltalake import DeltaTable

# ── CONFIG PAGE ───────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 Crypto Trading Dashboard")

# ── CHARGER LES DONNÉES ───────────────────────────────────────
@st.cache_data(ttl=30)
def load_silver():
    return DeltaTable("C:/tmp/data/silver/trades").to_pandas()

@st.cache_data(ttl=30)
def load_gold():
    return DeltaTable("C:/tmp/data/gold/trades").to_pandas()

with st.spinner("Chargement des données..."):
    silver = load_silver()
    gold   = load_gold()

# ── CHOISIR LE SYMBOLE ────────────────────────────────────────
symboles = sorted(silver["symbol"].unique())
symbol = st.selectbox("🪙 Choisir une crypto", symboles)

s = silver[silver["symbol"] == symbol].sort_values("window_start")
g = gold[gold["symbol"] == symbol].sort_values("window_start")

# ── PRIX ACTUEL ───────────────────────────────────────────────
st.subheader("💰 Prix Actuel")

col1, col2, col3 = st.columns(3)

latest_price = s["avg_price"].iloc[-1]
prev_price   = s["avg_price"].iloc[-2]
delta        = latest_price - prev_price

col1.metric(
    label=symbol,
    value=f"${latest_price:,.2f}",
    delta=f"{delta:+.2f}"
)
col2.metric(
    label="Volume",
    value=f"{s['volume'].iloc[-1]:,.2f}"
)
col3.metric(
    label="Trades",
    value=f"{s['trade_count'].iloc[-1]}"
)

st.divider()

# ── GRAPHIQUE PRIX ────────────────────────────────────────────
st.subheader("📈 Historique du Prix")

fig_price = go.Figure()
fig_price.add_trace(go.Scatter(
    x=s["window_start"],
    y=s["avg_price"],
    mode="lines",
    name="Prix",
    line=dict(color="blue", width=2)
))
fig_price.update_layout(
    xaxis_title="Temps",
    yaxis_title="Prix ($)",
    height=400
)
st.plotly_chart(fig_price, use_container_width=True)

# ── CANDLESTICK ───────────────────────────────────────────────
st.subheader("🕯️ Bougies Japonaises (OHLCV)")

fig_candle = go.Figure(data=[go.Candlestick(
    x=s["window_start"],
    open=s["open"],
    high=s["high"],
    low=s["low"],
    close=s["close"],
    name="OHLCV"
)])
fig_candle.update_layout(
    xaxis_title="Temps",
    yaxis_title="Prix ($)",
    height=400
)
st.plotly_chart(fig_candle, use_container_width=True)

# ── VOLUME ────────────────────────────────────────────────────
st.subheader("📦 Volume par Minute")

fig_vol = go.Figure()
fig_vol.add_trace(go.Bar(
    x=s["window_start"],
    y=s["volume"],
    name="Volume",
    marker_color="orange"
))
fig_vol.update_layout(height=300)
st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# ── RSI ───────────────────────────────────────────────────────
st.subheader("📊 RSI — Relative Strength Index")

rsi_value = g["rsi"].iloc[-1]

if rsi_value > 70:
    st.error(f"🔴 RSI : {rsi_value:.1f} — SURACHETÉ → Signal de vente possible")
elif rsi_value < 30:
    st.success(f"🟢 RSI : {rsi_value:.1f} — SURVENDU → Signal d'achat possible")
else:
    st.info(f"⚪ RSI : {rsi_value:.1f} — NEUTRE")

# Gauge RSI
fig_gauge = go.Figure(go.Indicator(
    mode="gauge+number",
    value=rsi_value,
    title={"text": "RSI"},
    gauge={
        "axis": {"range": [0, 100]},
        "bar": {"color": "white"},
        "steps": [
            {"range": [0, 30],  "color": "green"},   # Survendu
            {"range": [30, 70], "color": "gray"},    # Neutre
            {"range": [70, 100],"color": "red"},     # Suracheté
        ],
        "threshold": {
            "line": {"color": "white", "width": 4},
            "thickness": 0.75,
            "value": rsi_value
        }
    }
))
fig_gauge.update_layout(height=300)
st.plotly_chart(fig_gauge, use_container_width=True)

# Historique RSI
fig_rsi = go.Figure()
fig_rsi.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["rsi"],
    mode="lines",
    name="RSI",
    line=dict(color="purple", width=2)
))
fig_rsi.add_hline(y=70, line_dash="dash", line_color="red",   annotation_text="Suracheté (70)")
fig_rsi.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Survendu (30)")
fig_rsi.update_layout(
    yaxis=dict(range=[0, 100]),
    height=300
)
st.plotly_chart(fig_rsi, use_container_width=True)
# ── MACD ──────────────────────────────────────────────────────
st.subheader("📉 MACD")

fig_macd = go.Figure()
fig_macd.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["macd"],
    mode="lines",
    name="MACD",
    line=dict(color="blue", width=2)
))
fig_macd.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["signal_line"],
    mode="lines",
    name="Signal",
    line=dict(color="orange", width=2)
))
fig_macd.add_trace(go.Bar(
    x=g["window_start"],
    y=g["histogram"],
    name="Histogramme",
    marker_color=g["histogram"].apply(
        lambda x: "green" if x >= 0 else "red"
    )
))
fig_macd.update_layout(height=350)
st.plotly_chart(fig_macd, use_container_width=True)

st.divider()

# ── BOLLINGER BANDS ───────────────────────────────────────────
st.subheader("📊 Bollinger Bands")

fig_bb = go.Figure()
fig_bb.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["bb_upper"],
    mode="lines",
    name="Bande Haute",
    line=dict(color="red", dash="dash")
))
fig_bb.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["bb_middle"],
    mode="lines",
    name="Moyenne (20)",
    line=dict(color="blue")
))
fig_bb.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["bb_lower"],
    mode="lines",
    name="Bande Basse",
    line=dict(color="green", dash="dash")
))
fig_bb.add_trace(go.Scatter(
    x=g["window_start"],
    y=g["close"],
    mode="lines",
    name="Prix",
    line=dict(color="black", width=1)
))
fig_bb.update_layout(height=400)
st.plotly_chart(fig_bb, use_container_width=True)

st.divider()

# ── SIGNAL SUMMARY ────────────────────────────────────────────
st.subheader("🎯 Signal Summary — Tous les Symboles")

latest = gold.groupby("symbol").last().reset_index()

latest["RSI Zone"] = latest["rsi"].apply(
    lambda x: "🔴 Suracheté" if x > 70
    else ("🟢 Survendu" if x < 30
    else "⚪ Neutre")
)
latest["MACD Signal"] = (latest["macd"] > latest["signal_line"]).map(
    {True: "📈 Haussier", False: "📉 Baissier"}
)
latest["BB Position"] = latest.apply(
    lambda r: "⚠️ Outside" if r["close"] > r["bb_upper"]
    or r["close"] < r["bb_lower"]
    else "✅ Inside",
    axis=1
)

st.dataframe(
    latest[["symbol", "close", "rsi", "RSI Zone", "MACD Signal", "BB Position"]],
    use_container_width=True
)