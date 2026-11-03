#!/usr/bin/env python3
"""
Replay payment events into Kinesis Data Streams.

Same dataset, same replay clock and the same ~20 payments/s as the local
Redpanda producer in part 3 — so the two pipelines are comparable. What
changes is everything below the API call:

  * PutRecords instead of a Kafka produce: an HTTPS request with up to 500
    records, not a persistent TCP connection with a background sender thread.
  * PartitionKey instead of a message key. Kinesis hashes it with MD5 into a
    128-bit ring and the shard whose hash range covers it gets the record.
    Same effect as Kafka's murmur2 on key: one payment always lands in one
    shard, so its three events keep their order.
  * PARTIAL FAILURES ARE NORMAL. PutRecords returns HTTP 200 with a per-record
    FailedRecordCount; a throttled record is a field in a successful response,
    not an exception. Code that only checks for exceptions silently drops
    data. That retry loop is the single biggest difference from the Kafka
    client, which hides all of this behind its own queue.

    python producer_kinesis.py --file data/events-2026-10-01.jsonl --stream ooad-lab8-payments
"""

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime

import boto3
from botocore.config import Config

# Deliberately duplicated from part 3's producer.py instead of imported: that
# module needs a Kafka client, and a Kinesis producer that cannot run without
# kafka-python installed would be a silly dependency. The two copies must stay
# in step, which the shared sha256 rule guarantees — both pipelines flip
# exactly the same payments, so their results are comparable.
FAILURE_REASONS = [
    "Недостатньо коштів",
    "Невірний CVV",
    "Відмова банку-емітента",
    "Перевищено ліміт",
    "Таймаут 3-D Secure",
]


def should_fail(invoice_id: str, fail_rate: float) -> bool:
    """Deterministic per payment: hash the id rather than roll a die, so two
    runs flip the same payments and one payment's 3 events agree."""
    if fail_rate <= 0:
        return False
    digest = hashlib.sha256(invoice_id.encode()).digest()
    return int.from_bytes(digest[:4], "big") / 2**32 < fail_rate


def make_failing(ev: dict) -> dict:
    """Turn a terminal event into a failure.

    Keeps the record self-consistent, because the consumer trusts it:
    finalAmount must go to 0 (no money captured) and failureReason must appear
    (it is absent on every non-failure event in the real data).

    The reason is DERIVED FROM THE invoiceId, not drawn from an RNG. That is
    not a stylistic choice. ~1% of the events in this dataset are byte-identical
    redeliveries, and a per-call rng.choice() hands the two copies of one event
    two different reasons — which stops them being duplicates at all. Measured:
    with rng.choice the day's duplicate count fell from 1 001 to 920, i.e. the
    failure injection silently destroyed 8% of exactly the thing this lab is
    about detecting. Deriving from the id keeps every copy identical.
    """
    digest = hashlib.sha256(("reason:" + ev["invoiceId"]).encode()).digest()
    ev = dict(ev)
    ev["status"] = "failure"
    ev["finalAmount"] = 0
    ev["failureReason"] = FAILURE_REASONS[digest[0] % len(FAILURE_REASONS)]
    return ev


MAX_RECORDS = 500          # PutRecords hard limit
MAX_BATCH_BYTES = 4 << 20  # 5 MiB limit; stay under it


def parse_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def load(path: str):
    """Read the file and sort it by processing time, exactly as part 3 does,
    so the two pipelines replay the same events in the same order."""
    events = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                ev = json.loads(line)
                events.append((parse_ts(ev["receivedAt"]), ev))
    events.sort(key=lambda p: p[0])
    return events


def put_with_retry(client, stream: str, batch: list, stats: dict,
                   max_attempts: int = 6) -> None:
    """Send one batch, re-sending only the records the service rejected.

    Exponential backoff with a cap. Resending the whole batch on a partial
    failure would duplicate the records that did get through — which the
    consumer would then have to deduplicate, on top of the duplicates already
    in the data.
    """
    pending = batch
    for attempt in range(max_attempts):
        resp = client.put_records(StreamName=stream, Records=pending)
        failed = resp.get("FailedRecordCount", 0)
        stats["sent"] += len(pending) - failed
        if not failed:
            return
        stats["throttled"] += failed
        pending = [rec for rec, res in zip(pending, resp["Records"])
                   if res.get("ErrorCode")]
        sleep = min(2 ** attempt * 0.1, 5.0)
        if attempt == 0:
            codes = {res.get("ErrorCode") for res in resp["Records"]
                     if res.get("ErrorCode")}
            print(f"    {failed} records rejected ({', '.join(sorted(codes))}), "
                  f"retrying in {sleep:.1f}s", file=sys.stderr)
        time.sleep(sleep)

    stats["dropped"] += len(pending)
    print(f"    GIVING UP on {len(pending)} records after {max_attempts} attempts",
          file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", required=True)
    ap.add_argument("--stream", default="ooad-lab8-payments")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--rate", type=float, default=20.0, help="payments per second")
    ap.add_argument("--speed", type=float, default=None)
    ap.add_argument("--max-payments", type=int, default=None)
    ap.add_argument("--fail-rate", type=float, default=0.0,
                    help="force this share of payments to fail, so the SNS "
                         "alert actually has something to fire on")
    ap.add_argument("--batch-interval", type=float, default=0.5,
                    help="flush the buffer at least this often")
    args = ap.parse_args()

    events = load(args.file)
    if not events:
        print("no events", file=sys.stderr)
        return 1

    invoices = {ev["invoiceId"] for _, ev in events}
    t_first, t_last = events[0][0], events[-1][0]
    span = max(t_last - t_first, 1e-9)
    speed = args.speed if args.speed else span / (len(invoices) / args.rate)

    print(f"  stream          : {args.stream} ({args.region})")
    print(f"  events          : {len(events):,}")
    print(f"  payments        : {len(invoices):,}")
    print(f"  speed factor    : {speed:.1f}x")
    print(f"  wall duration   : {span/speed/60:.1f} min")
    if args.fail_rate:
        print(f"  forced failures : {args.fail_rate:.0%} of payments")
    print(flush=True)

    client = boto3.client(
        "kinesis", region_name=args.region,
        # boto3 retries on 5xx and throttling at the HTTP layer; the loop above
        # handles the per-record rejections it cannot see.
        config=Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    )

    stats = {"sent": 0, "throttled": 0, "dropped": 0}
    forced = 0
    # Threshold, not modulo: stats["sent"] grows by a whole batch at a time,
    # so `sent % 5000 == 0` jumps straight over the multiple (4 800 -> 5 300)
    # and the run prints nothing at all for minutes.
    next_report = 5000
    batch, batch_bytes = [], 0
    seen = set()
    wall_start = time.monotonic()
    last_flush = wall_start

    def flush():
        nonlocal batch, batch_bytes, last_flush
        if batch:
            put_with_retry(client, args.stream, batch, stats)
            batch, batch_bytes = [], 0
        last_flush = time.monotonic()

    try:
        for ts, ev in events:
            inv = ev["invoiceId"]
            if args.max_payments and inv not in seen and len(seen) >= args.max_payments:
                continue
            seen.add(inv)

            if ev["status"] in ("success", "failure") and should_fail(inv, args.fail_rate):
                if ev["status"] == "success":
                    forced += 1
                ev = make_failing(ev)

            due = (ts - t_first) / speed
            behind = due - (time.monotonic() - wall_start)
            if behind > 0:
                flush()                 # do not sit on buffered records while sleeping
                time.sleep(behind)

            blob = json.dumps(ev, ensure_ascii=False).encode()
            # Firehose concatenates records as-is; without the newline the whole
            # day arrives as one unparseable line and Athena reads zero rows.
            blob += b"\n"

            if len(batch) >= MAX_RECORDS or batch_bytes + len(blob) > MAX_BATCH_BYTES:
                flush()

            batch.append({"Data": blob, "PartitionKey": inv})
            batch_bytes += len(blob)

            if time.monotonic() - last_flush > args.batch_interval:
                flush()

            if stats["sent"] >= next_report:
                el = time.monotonic() - wall_start
                print(f"  sent {stats['sent']:>7,}  {len(seen):>6,} payments  "
                      f"{el:6.1f}s  {len(seen)/max(el,1e-9):5.1f} payments/s", flush=True)
                while next_report <= stats["sent"]:
                    next_report += 5000

    except KeyboardInterrupt:
        print("\ninterrupted — flushing")
    finally:
        flush()

    el = time.monotonic() - wall_start
    print()
    print(f"sent      : {stats['sent']:,} records for {len(seen):,} payments")
    print(f"throttled : {stats['throttled']:,} (retried)")
    if forced:
        print(f"forced to failure: {forced:,} payments")
    if stats["dropped"]:
        print(f"DROPPED   : {stats['dropped']:,}")
    print(f"wall time : {el:.1f}s ({len(seen)/max(el,1e-9):.1f} payments/s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
