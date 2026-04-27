# CryptoStream — Real-Time Multi-Exchange Crypto Pipeline

End-to-end data engineering pipeline that ingests live trade data from multiple
crypto exchanges, normalizes it through Kafka, processes it with Spark
Structured Streaming into a Delta Lake medallion architecture, and surfaces
real-time technical indicators through a Streamlit dashboard.

```
WebSockets (Binance / Kraken / Coingecko)
     ↓ normalize to unified schema
Kafka (3 brokers, KRaft, topic-per-symbol)
     ↓ Spark Structured Streaming
Delta Lake — Bronze → Silver → Gold
     ↓ deltalake reader (no JVM)
Streamlit dashboard
```

---

## 1. Use case

Trading desks and quant analysts need a unified, low-latency view of price
action across exchanges that each speak a different symbol convention
(`BTCUSDT` vs `XBT/USD` vs `BTC-USD`). This pipeline:

1. **Unifies symbology** at the edge — one schema, regardless of source.
2. **Persists every trade** durably in Bronze for replay and audit.
3. **Computes OHLCV per minute** (Silver) — the canonical aggregation for charting.
4. **Derives technical indicators** (Gold) — RSI, MACD, Bollinger Bands, VWAP — for signal generation.
5. **Surfaces it live** in a 30-second-refresh dashboard with per-symbol gauges and signal summary.

---

## 2. Architecture

![Pipeline Architecture](docs\Architecture.svg) 

Key design decisions:

- **Normalize before Kafka, not after.** Each exchange's quirky symbol gets mapped to a unified schema in the producer, so Kafka topics, partitioning, and Spark schema are all single-source. Switching exchanges later is a producer change, not a pipeline change.
- **Topic-per-symbol** instead of one big `trades` topic. Lets Spark consume each symbol on its own partition group, enables per-symbol retention, and parallelizes downstream cleanly.
- **3 brokers, RF=3.** Survives one broker loss without data loss. KRaft (no Zookeeper) — fewer moving parts.
- **Watermark + dropDuplicates BEFORE aggregation.** Late trades within 10 min are accepted; later than that are dropped (otherwise infinite state). WebSocket reconnections can replay trades, so we dedupe by `trade_id` (prefixed with `source` to prevent cross-exchange collisions).
- **deltalake (Python) for the dashboard, not Spark.** No JVM in the dashboard process — boots in 1 s instead of 30 s, and the dashboard is fully decoupled from the pipeline.

---

## 3. Data sources

| Source | Protocol | Symbol example | Why |
|---|---|---|---|
| **Binance** | WebSocket spot | `BTCUSDT` | Highest volume, lowest latency public stream |
| **Kraken** | WebSocket | `XBT/USD` | Independent price formation, regulated venue |
| **Coingecko** | REST (polled) + WS | `bitcoin` | Aggregated reference price for sanity checks |

All three are public APIs with no authentication required. Rate limits are
respected by the producer with exponential backoff.

---

## 4. Stack & justifications

| Layer | Technology | Why this and not alternative |
|---|---|---|
| Ingestion | `websocket-client` + `kafka-python` | Lightweight, no extra infra. Considered Debezium — overkill for WebSockets. |
| Broker | **Kafka (KRaft, 3 brokers)** | Industry standard, replayable, partition-per-symbol parallelism. KRaft removes Zookeeper. |
| Stream processing | **Spark Structured Streaming** | Native Kafka source, exactly-once with checkpoints, watermark + windowing primitives. Considered Flink — heavier ops for the same use case at student scale. |
| Storage | **Delta Lake** | ACID on object storage, time travel, schema enforcement, `MERGE` support. Considered Iceberg — equivalent, but Delta has tighter Spark integration and a Python reader (`deltalake`) that lets the dashboard skip the JVM. |
| Orchestration | **Prefect** | Pythonic, modern, simpler than Airflow for streaming-adjacent workloads. Flows visible at `localhost:4200`. |
| Dashboard | **Streamlit + Plotly + deltalake** | Fastest path to a real-time UI in pure Python. `deltalake` reader = no Spark in the dashboard process. |
| Containerization | **Docker Compose** | Single-command bring-up. |

---

## 5. Schema contracts

These are the contracts between layers. **Drift breaks the pipeline silently
unless validated**, so each consumer asserts required columns at startup.

### Bronze (raw trades)
| Column | Type | Note |
|---|---|---|
| `source` | string | `binance` / `kraken` / `coingecko` |
| `symbol` | string | normalized, e.g. `BTCUSDT` |
| `trade_id` | string | prefixed with source: `binance:1234` |
| `price` | double | |
| `qty` | double | |
| `event_time` | timestamp | from the exchange (UTC) |
| `ingest_time` | timestamp | when producer received it |
| `raw_payload` | string | the original JSON (audit trail) |

### Silver (OHLCV 1-minute, per symbol)
`window_start, window_end, symbol, open, high, low, close, volume, avg_price, trade_count`

### Gold (indicators 1-minute, per symbol)
`window_start, window_end, symbol, open, high, low, close, volume, trade_count, avg_price, vwap, rsi, ma12, ma26, macd_approx, signal_approx, macd_histogram, bb_middle, bb_upper, bb_lower`

> **⚠ MACD note:** `macd_approx` / `signal_approx` are **SMA-based approximations**, not true EMA(12,26,9). Spark Window functions don't support recursive computation natively. The approximation tracks the EMA-based MACD closely enough for trend detection but lags by ~1–2 candles on sharp reversals. Document this in any signal-driven downstream work. To upgrade to true EMA, replace the SMA windows in the Gold writer with `ewm(span=N, adjust=False).mean()` inside an `applyInPandas` per `(symbol)` group.

---

## 6. Indicator definitions

| Indicator | Formula | Warm-up |
|---|---|---|
| **RSI(14)** | `100 − 100 / (1 + avg_gain/avg_loss)` over last 14 candles | 14 candles |
| **MACD** | `SMA(12) − SMA(26)` (approx — see note above) | 26 candles |
| **MACD Signal** | `SMA(9)` of MACD | 26 + 9 = 35 candles |
| **MACD Histogram** | `MACD − Signal` | 35 candles |
| **Bollinger Middle** | `SMA(20)` of `close` | 20 candles |
| **Bollinger Upper / Lower** | `Middle ± 2 × stddev(close, 20)` | 20 candles |
| **VWAP** | `Σ(price × qty) / Σ(qty)` per window | 1 candle |

The dashboard renders a yellow warm-up banner until each indicator has enough
candles to be meaningful.

---

## 7. Installation

### Prerequisites
- Docker Desktop (with at least 6 GB allocated to Docker)
- Python 3.10+ and `pip`
- Java 11 (required by Spark; Docker Spark services include it)

### Setup

```bash
git clone <repo-url> CryptoStream-Pipeline
cd CryptoStream-Pipeline

# Python deps for the producer, Spark jobs and dashboard
python -m venv .venv
source .venv/bin/activate         # macOS/Linux
# .venv\Scripts\activate          # Windows

pip install -r requirements.txt

# Configuration
cp .env.example .env              # then edit values
```

### `.env` reference

```ini
# Kafka
CLUSTER_ID=<32-char base64 UUID, see Kafka KRaft docs>
KAFKA_EXTERNAL_HOST=localhost
KAFKA_BROKER=localhost:9092

# Delta paths
BRONZE_PATH=./data/bronze/trades
SILVER_PATH=./data/silver/ohlcv_1m
GOLD_PATH=./data/gold/indicators_1m
CHECKPOINT_PATH=./data/checkpoints

# Symbols to track
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,ADAUSDT

# Dashboard
DISPLAY_TZ=Europe/Paris
REFRESH_MS=30000

# Prefect
PREFECT_API_URL=http://localhost:4200/api
```

---

## 8. Running the pipeline

```bash
# 1) Bring up Kafka (3 brokers, KRaft) + topic creation
docker compose up -d
docker compose ps        # verify all brokers Healthy

# 2) Start the producer (real exchange WebSockets)
python ingestion/producer.py

# 3) Start the streaming jobs
python streaming/bronze.py     # Kafka → Bronze
python streaming/silver.py     # Bronze → Silver (OHLCV)
python streaming/gold.py       # Silver → Gold (indicators)

# Or via Prefect
prefect server start &
python orchestration/deploy.py
# UI at http://localhost:4200

# 4) Launch the dashboard
streamlit run dashboard/app.py
# UI at http://localhost:8501
```

---

## 9. Project structure

```
CryptoStream-Pipeline/
├── docker-compose.yml          # Kafka 3-broker KRaft cluster + topic init
├── .env.example
├── requirements.txt
├── ingestion/
│   ├── producer.py             # Multi-source WebSocket → Kafka
│   ├── normalizer.py           # Symbol mapping, schema unification
│   └── sources/
│       ├── binance.py
│       ├── kraken.py
│       └── coingecko.py
├── streaming/
│   ├── bronze.py               # Kafka → Bronze Delta
│   ├── silver.py               # OHLCV 1m aggregation
│   └── gold.py                 # RSI / MACD / BB / VWAP
├── orchestration/
│   └── deploy.py               # Prefect deployments
├── dashboard/
│   └── app.py                  # Streamlit
├── tests/
│   ├── test_normalizer.py
│   ├── test_aggregations.py
│   ├── test_indicators.py
│   ├── test_dedup.py
│   └── test_schema_contracts.py
├── data/                       # Created at runtime
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
└── docs/
    └── architecture.svg
```

---

## 10. Testing

5 test suites covering the critical correctness paths (per project requirements):

| Test | What it asserts |
|---|---|
| `test_normalizer.py` | `XBT/USD`, `BTC-USD`, `BTCUSDT` all map to canonical `BTCUSDT`; trade_id correctly prefixed with source |
| `test_aggregations.py` | Manually-computed OHLCV from 60 sample trades matches Spark output exactly |
| `test_indicators.py` | RSI, MACD, BB outputs against a hand-validated fixture (golden file) |
| `test_dedup.py` | Same `trade_id` ingested twice produces one Bronze row |
| `test_schema_contracts.py` | Bronze/Silver/Gold writers emit exactly the expected columns and types |

Run with:
```bash
pytest -v tests/
```

---

## 11. Failure scenarios validated (Day 10)

| Scenario | Expected behavior | Status |
|---|---|---|
| Restart one Kafka broker | Producer/consumers reconnect, no data loss | ✅ |
| Kill producer | Spark waits, resumes from offset on producer restart | ✅ |
| Send malformed JSON | Bad row routed to `_corrupt_record`, pipeline keeps running | ✅ |
| Delete dashboard process | Pipeline unaffected, dashboard relaunches with cached Delta | ✅ |
| Cold start (`docker compose down -v`) | Full bring-up resumes producing data within 30 s | ✅ |

---

## 12. Known limits

- **MACD is SMA-approximated**, not true EMA — see Schema contracts §5.
- **Single-node Spark** in this setup. Throughput tested up to ~2 k msg/s per symbol; beyond that the local executor becomes the bottleneck.
- **No backfill from exchanges** — historical data starts at pipeline launch. To backfill, ingest the exchange's REST trade-history endpoint into a separate Bronze partition.
- **Watermark = 10 min.** Trades older than that are dropped silently (visible in Spark metrics, not surfaced in dashboard yet).
- **Coingecko is REST-polled** at 60-second intervals — its data is therefore minute-resolution by design and is used as a sanity reference, not a tick source.

---

## 13. Future work

- True EMA-based MACD via `applyInPandas`.
- Z-score-based anomaly detection on volume spikes (alert layer).
- Cloud deploy: Kafka → MSK, Delta → S3, Spark → EMR Serverless, dashboard → ECS Fargate.
- Feature store split (offline Delta + online Redis) for downstream ML.
- Add Prometheus + Grafana for pipeline-health metrics (lag, throughput, checkpoint freshness).

---

## 14. Team

- **Person A — Platform Track**: Docker, Kafka, Producers, Networking, Reliability, `.env`
- **Person B — Data Track**: Spark, Delta Lake, Transformations, Aggregations, Feature Engineering, Dashboard, Validation
