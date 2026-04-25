"""
Multi-source WebSocket producer → Kafka.
Connects to Binance + Kraken trade streams, normalizes to a unified schema,
then publishes to crypto-trades-<symbol> topics partitioned by symbol.

Unified schema (sent as JSON value):
    { symbol, price, quantity, event_time, trade_id, source }

trade_id is prefixed with source ("binance:12345") to prevent
cross-exchange collisions when Spark deduplicates downstream.
"""
import json
import os
import signal
import sys
import threading
import time
import logging

import websocket
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("producer_ws")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP") 

# Symbols we cover — same 5 on both exchanges for simplicity. Binance uses lowercase, Kraken uppercase with slash.
BINANCE_SYMBOLS = ["btcusdt", "ethusdt", "xrpusdt", "adausdt", "solusdt"]
KRAKEN_SYMBOLS = ["BTC/USD", "ETH/USD", "XRP/USD", "ADA/USD", "SOL/USD"]

# Kraken pair → unified symbol mapping. Binance is already in the desired format.
KRAKEN_MAP = {
    "BTC/USD": "BTCUSDT",
    "ETH/USD": "ETHUSDT",
    "XRP/USD": "XRPUSDT",
    "ADA/USD": "ADAUSDT",
    "SOL/USD": "SOLUSDT"
}

# Stop flag for graceful shutdown
_stop = threading.Event()


def make_producer():
    """One producer, retried on startup until brokers are reachable."""
    while not _stop.is_set():
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",            # wait for in-sync replicas (we have RF=3)
                retries=5,
                linger_ms=20,          # tiny batching, still ~real-time
                request_timeout_ms=10000,
            )
            log.info("Kafka producer ready (bootstrap=%s)", KAFKA_BOOTSTRAP)
            return p
        except KafkaError as e:
            log.warning("Kafka not reachable yet (%s) — retrying in 3s", e)
            time.sleep(3)


def topic_for(symbol: str) -> str:
    return f"crypto-trades-{symbol.lower()}"


def send(producer, msg: dict):
    """Fire-and-forget with error logging. Key=symbol → same partition for same symbol."""
    try:
        producer.send(topic_for(msg["symbol"]), key=msg["symbol"], value=msg)
    except KafkaError as e:
        log.error("Kafka send failed for %s: %s", msg.get("symbol"), e)


# ─── BINANCE ──────────────────────────────────────────────────────────────────
# Combined-stream URL: /stream?streams=btcusdt@trade/ethusdt@trade/...
# Payload shape:
#   {"stream":"btcusdt@trade",
#    "data":{"e":"trade","E":<event_ms>,"s":"BTCUSDT","t":<trade_id>,
#            "p":"<price>","q":"<qty>","T":<trade_time_ms>,...}}
def run_binance(producer):
    streams = "/".join(f"{s}@trade" for s in BINANCE_SYMBOLS)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    def on_message(_ws, raw):
        try:
            payload = json.loads(raw)
            d = payload.get("data") or {}
            if d.get("e") != "trade":
                return
            msg = {
                "symbol":     d["s"],                 # already BTCUSDT-style
                "price":      float(d["p"]),
                "quantity":   float(d["q"]),
                "event_time": int(d["T"]),            # epoch ms (trade time)
                "trade_id":   f"binance:{d['t']}",
                "source":     "binance",
            }
            send(producer, msg)
        except Exception as e:
            log.warning("Binance parse error: %s | raw=%.200s", e, raw)

    def on_error(_ws, err):
        log.warning("Binance WS error: %s", err)

    def on_close(_ws, code, reason):
        log.info("Binance WS closed (%s, %s)", code, reason)

    def on_open(_ws):
        log.info("Binance WS open: streams=%s", streams)

    while not _stop.is_set():
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            # ping every 20s; Binance closes the socket at 24h regardless
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error("Binance loop crash: %s", e)
        if not _stop.is_set():
            log.info("Binance reconnect in 3s")
            time.sleep(3)


# ─── KRAKEN (WS v2) ───────────────────────────────────────────────────────────
# Subscribe message: {"method":"subscribe","params":{"channel":"trade","symbol":[...]}}
# Update payload: {"channel":"trade","type":"update",
#   "data":[{"symbol":"BTC/USD","side":"buy","price":...,"qty":...,
#            "ord_type":"market","trade_id":12345,"timestamp":"2024-..Z"}]}
def run_kraken(producer):
    url = "wss://ws.kraken.com/v2"

    def on_open(ws):
        sub = {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": KRAKEN_SYMBOLS, "snapshot": False}, # no need for initial snapshot since we have Binance and short-term retention
        }
        ws.send(json.dumps(sub))
        log.info("Kraken WS open: subscribed=%s", KRAKEN_SYMBOLS)

    def on_message(_ws, raw):
        try:
            payload = json.loads(raw)
            if payload.get("channel") != "trade" or payload.get("type") != "update":
                return
            for d in payload.get("data", []):
                native = d["symbol"]
                if native not in KRAKEN_MAP:
                    continue
                # Kraken timestamp is RFC3339 (e.g. 2024-03-12T10:11:12.123456Z)
                # Convert to epoch ms for downstream consistency.
                ts = d["timestamp"]
                # cheap parse — strip nanos if present, fall back to time.time on error
                try:
                    from datetime import datetime, timezone
                    if ts.endswith("Z"):
                        ts = ts[:-1] + "+00:00"
                    event_ms = int(datetime.fromisoformat(ts).timestamp() * 1000)
                except Exception:
                    event_ms = int(time.time() * 1000)

                msg = {
                    "symbol":     KRAKEN_MAP[native],
                    "price":      float(d["price"]),
                    "quantity":   float(d["qty"]),
                    "event_time": event_ms,
                    "trade_id":   f"kraken:{d['trade_id']}",
                    "source":     "kraken",
                }
                send(producer, msg)
        except Exception as e:
            log.warning("Kraken parse error: %s | raw=%.200s", e, raw)

    def on_error(_ws, err):
        log.warning("Kraken WS error: %s", err)

    def on_close(_ws, code, reason):
        log.info("Kraken WS closed (%s, %s)", code, reason)

    while not _stop.is_set():
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error("Kraken loop crash: %s", e)
        if not _stop.is_set():
            log.info("Kraken reconnect in 3s")
            time.sleep(3)


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    def shutdown(signum, _frame):
        log.info("Signal %s received — shutting down", signum)
        _stop.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    producer = make_producer()

    threads = [
        threading.Thread(target=run_binance, args=(producer,), name="binance", daemon=True),
        threading.Thread(target=run_kraken,  args=(producer,), name="kraken",  daemon=True),
    ]
    for t in threads:
        t.start()

    try:
        while not _stop.is_set():
            time.sleep(1)
    finally:
        log.info("Flushing producer...")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        log.info("Bye.")
        sys.exit(0)


if __name__ == "__main__":
    main()