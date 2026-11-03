#!/usr/bin/env python3
"""
Replay one day of payment webhook events into a Kafka/Redpanda topic.

The point of this script is that it is NOT a generator. It takes the same
JSON Lines file that part 2 queries in Athena and pushes it through a broker
in the order the events actually reached us (``receivedAt``), compressed in
time. Everything the batch side saw as a column — the 1% redeliveries, the 2%
that arrive up to 10 minutes late — comes out of here as a real ordering
problem for the consumer downstream.

    python producer.py --file data/events-2026-10-01.jsonl
    python producer.py --file ... --fail-rate 0.30      # experiment 3
    python producer.py --file ... --rate 200            # go faster

Why ``receivedAt`` and not ``modifiedDate`` as the replay clock: ``receivedAt``
is processing time, i.e. the moment the event hit our edge. That is precisely
what the network delivers, in that order. ``modifiedDate`` is event time; the
consumer uses it for windowing, and the whole exercise is that the two do not
agree.
"""

import argparse
import hashlib
import json
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

FAILURE_REASONS = [
    "Недостатньо коштів",
    "Невірний CVV",
    "Відмова банку-емітента",
    "Перевищено ліміт",
    "Таймаут 3-D Secure",
]


def parse_ts(s: str) -> float:
    """ISO-8601 with a trailing Z -> POSIX seconds.

    ``datetime.fromisoformat`` only learned to accept the 'Z' suffix in 3.11,
    and this has to run on whatever the lab machine has, so swap it for the
    explicit offset first.
    """
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def load(path: str):
    """Read the file and sort it by processing time.

    The file on S3 is already roughly in receivedAt order, but 'roughly' is not
    good enough: the late events are interleaved, and if we replayed in file
    order the consumer would never see an out-of-order arrival, which is the
    one thing we are here to demonstrate.
    """
    events = []
    with open(path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"skipping malformed line {lineno}: {exc}", file=sys.stderr)
                continue
            events.append((parse_ts(ev["receivedAt"]), ev))
    events.sort(key=lambda pair: pair[0])
    return events


def make_failing(ev: dict, rng: random.Random) -> dict:
    """Turn a terminal event into a failure.

    Keeps the record self-consistent, because the consumer trusts it:
    finalAmount must go to 0 (no money captured) and failureReason must appear
    (it is absent on every non-failure event in the real data).
    """
    ev = dict(ev)
    ev["status"] = "failure"
    ev["finalAmount"] = 0
    ev["failureReason"] = rng.choice(FAILURE_REASONS)
    return ev


def should_fail(invoice_id: str, fail_rate: float) -> bool:
    """Decide per payment, deterministically.

    Hashing the invoiceId rather than calling random() means two runs of the
    producer flip exactly the same payments, so a consumer result is
    reproducible. It also means the decision is stable across the 3 events of
    one payment without having to carry state.
    """
    if fail_rate <= 0:
        return False
    digest = hashlib.sha256(invoice_id.encode()).digest()
    # first 4 bytes as a fraction in [0, 1)
    return int.from_bytes(digest[:4], "big") / 2**32 < fail_rate


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", required=True, help="events.jsonl for one day")
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="payments")
    ap.add_argument("--rate", type=float, default=20.0,
                    help="target payments per second (default 20, as in the brief)")
    ap.add_argument("--speed", type=float, default=None,
                    help="override: event-time seconds per wall second")
    ap.add_argument("--fail-rate", type=float, default=0.0,
                    help="experiment 3: force this share of payments to fail")
    ap.add_argument("--max-payments", type=int, default=None,
                    help="stop after N payments (for a quick smoke test)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)

    print(f"loading {args.file} ...", flush=True)
    events = load(args.file)
    if not events:
        print("no events", file=sys.stderr)
        return 1

    invoices = {ev["invoiceId"] for _, ev in events}
    t_first, t_last = events[0][0], events[-1][0]
    span = max(t_last - t_first, 1e-9)

    # Map event time onto wall time. We want `rate` payments per second, so the
    # whole day has to be squeezed into len(invoices)/rate seconds of real time.
    if args.speed is not None:
        speed = args.speed
    else:
        speed = span / (len(invoices) / args.rate)

    print(f"  events          : {len(events):,}")
    print(f"  payments        : {len(invoices):,}")
    print(f"  event-time span : {span/3600:.2f} h")
    print(f"  speed factor    : {speed:.1f}x  "
          f"(1 wall second = {speed:.0f} event seconds)")
    print(f"  wall duration   : {span/speed/60:.1f} min")
    if args.fail_rate:
        print(f"  forced failures : {args.fail_rate:.0%} of payments")
    print()

    # linger_ms batches sends; without it the client would do a syscall per
    # 300-byte message and the producer, not the sleep, would set the pace.
    # acks='all' + retries is the durable setting: the broker must have the
    # record before the future resolves.
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        linger_ms=50,
        acks="all",
        retries=5,
        compression_type="gzip",
        max_block_ms=30000,
    )

    stats = {"delivered": 0, "failed": 0}

    def on_ok(meta):
        stats["delivered"] += 1

    def on_err(exc):
        stats["failed"] += 1
        if stats["failed"] < 10:
            print(f"delivery failed: {exc}", file=sys.stderr)

    sent = 0
    seen_invoices = set()
    forced = 0
    wall_start = time.monotonic()

    try:
        for ts, ev in events:
            inv = ev["invoiceId"]

            if args.max_payments and inv not in seen_invoices \
                    and len(seen_invoices) >= args.max_payments:
                continue
            seen_invoices.add(inv)

            if ev["status"] in ("success", "failure") and should_fail(inv, args.fail_rate):
                if ev["status"] == "success":
                    forced += 1
                ev = make_failing(ev, rng)

            # Sleep until this event is due. Recomputed against wall_start every
            # time rather than accumulating sleeps, so a slow send does not make
            # the whole replay drift later and later.
            due = (ts - t_first) / speed
            behind = due - (time.monotonic() - wall_start)
            if behind > 0:
                time.sleep(behind)

            # key=invoiceId: kafka-python hashes the key with murmur2, the same
            # function the Java client uses, so all three events of one payment
            # land in the same partition and therefore keep their relative
            # order. Without a key they would be round-robined and `processing`
            # could overtake `success`.
            producer.send(args.topic, key=inv, value=ev) \
                    .add_callback(on_ok).add_errback(on_err)
            sent += 1

            if sent % 5000 == 0:
                elapsed = time.monotonic() - wall_start
                print(f"  sent {sent:>7,} events  "
                      f"{len(seen_invoices):>6,} payments  "
                      f"{elapsed:6.1f}s  "
                      f"{len(seen_invoices)/max(elapsed,1e-9):5.1f} payments/s",
                      flush=True)

    except KeyboardInterrupt:
        print("\ninterrupted — flushing what is already queued")
    except KafkaError as exc:
        print(f"kafka error: {exc}", file=sys.stderr)
        return 1

    producer.flush(timeout=30)
    producer.close(timeout=10)
    elapsed = time.monotonic() - wall_start

    print()
    print(f"sent        : {sent:,} events for {len(seen_invoices):,} payments")
    print(f"delivered   : {stats['delivered']:,}")
    if stats["failed"]:
        print(f"FAILED      : {stats['failed']:,}")
    if forced:
        print(f"forced to failure: {forced:,} payments "
              f"({forced/max(len(seen_invoices),1):.1%})")
    print(f"wall time   : {elapsed:.1f}s "
          f"({len(seen_invoices)/max(elapsed,1e-9):.1f} payments/s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
