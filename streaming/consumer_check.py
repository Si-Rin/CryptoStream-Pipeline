"""
Quick validation consumer (no Spark, no Delta).
Prints a sample message + msg/sec rate per topic.
Use this to confirm the producer is healthy before touching Spark.
"""
import json
import os
import time
from collections import defaultdict

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
TOPICS = [
    "crypto-trades-btcusdt",
    "crypto-trades-ethusdt",
    "crypto-trades-solusdt",
    "crypto-trades-xrpusdt",
    "crypto-trades-adausdt",
]

REQUIRED_FIELDS = {"symbol", "price", "quantity", "event_time", "trade_id", "source"}


def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        auto_offset_reset="latest",
        group_id="validator-cli",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    counts = defaultdict(int)
    invalid = 0
    started = time.time()
    last_report = started
    printed_sample = set()

    print(f"Listening on {len(TOPICS)} topics. Ctrl+C to stop.\n")
    try:
        for record in consumer:
            counts[record.topic] += 1
            v = record.value

            # schema check
            if not isinstance(v, dict) or not REQUIRED_FIELDS.issubset(v):
                invalid += 1
                continue

            # one sample per topic
            if record.topic not in printed_sample:
                printed_sample.add(record.topic)
                print(f"[sample {record.topic} part={record.partition}] {v}")

            # periodic report
            now = time.time()
            if now - last_report >= 5.0: # report every 5s
                elapsed = now - started
                total = sum(counts.values())
                rate = total / elapsed if elapsed > 0 else 0
                print(
                    f"\n--- {elapsed:.0f}s elapsed | total={total} | "
                    f"rate={rate:.1f} msg/s ({rate*60:.0f} msg/min) | invalid={invalid}"
                )
                for t in TOPICS:
                    print(f"    {t}: {counts[t]}")
                last_report = now
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()