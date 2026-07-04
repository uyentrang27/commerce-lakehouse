"""Kafka producer — emit a live stream of order events (SIMULATION).

The batch lane loads marketplace exports on a schedule; this lane models the
*same domain* arriving in real time: one JSON event per order onto a Kafka
topic, which the Spark Structured Streaming job lands into the Bronze zone.
Same lakehouse, two ingestion speeds.

    python streaming/produce_orders.py --count 500 --rate 200

Events look like a raw marketplace webhook — channel-specific codes and status
words, exactly what Silver would later conform (the streaming lane stops at
Bronze here; conforming it is the same code the batch Silver already runs).
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer

CHANNELS = ["amazon", "backmarket"]
# raw status vocab per channel (unconformed on purpose — Bronze keeps it raw)
STATUS = {
    "amazon": ["Pending", "Shipped", "Delivered", "Cancelled", "Returned"],
    "backmarket": ["1", "2", "3", "4", "5"],
}


def make_event(rng: random.Random, spread_s: float = 0.0) -> dict:
    channel = rng.choices(CHANNELS, weights=[0.78, 0.22])[0]  # same skew as batch
    pid = rng.randint(1, 4000)
    sku = f"B0{pid:08d}" if channel == "amazon" else f"SKU{pid:07d}"
    qty = rng.randint(1, 3)
    unit_price = round(rng.uniform(80, 1200), 2)
    # Real events carry now(); --spread-minutes backdates them across a window so
    # the streaming per-minute dashboard has several buckets to draw (demo only).
    ts = datetime.now(UTC)
    if spread_s > 0:
        ts -= timedelta(seconds=rng.uniform(0, spread_s))
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"111-{rng.randint(0, 9_999_999_999):010d}",
        "channel": channel,
        "sku": sku,
        "qty": qty,
        "unit_price": unit_price,
        "currency": "USD" if channel == "amazon" else "EUR",
        "status": rng.choice(STATUS[channel]),
        "event_ts": ts.isoformat(),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--count", type=int, default=500)
    ap.add_argument("--rate", type=float, default=200.0, help="events per second")
    ap.add_argument("--spread-minutes", type=float, default=0.0,
                    help="demo: backdate event_ts across the last N minutes so the "
                         "per-minute streaming dashboard has several buckets")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    spread_s = args.spread_minutes * 60.0

    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        acks="all",  # wait for the broker to persist — no silent loss
        linger_ms=20,
    )

    delay = 1.0 / args.rate if args.rate > 0 else 0
    for i in range(args.count):
        e = make_event(rng, spread_s)
        # key by order_id so all events for an order land in the same partition
        producer.send(args.topic, key=e["order_id"], value=e)
        if delay:
            time.sleep(delay)
        if (i + 1) % 100 == 0:
            print(f"[produce] sent {i + 1}/{args.count}")
    producer.flush()
    producer.close()
    print(f"[produce] done: {args.count} events -> topic '{args.topic}'")


if __name__ == "__main__":
    main()
