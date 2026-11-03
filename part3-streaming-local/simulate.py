#!/usr/bin/env python3
"""
Run the consumer's windowing logic over a day file WITHOUT a broker.

Same class, same code path as consumer.py — only the transport is replaced by
reading the file in receivedAt order, which is exactly the order the producer
would have pushed it. Useful for two things:

  * checking the window/dedup/lateness logic before Docker is involved,
  * producing the expected numbers for each experiment, so you can tell a real
    difference from a mistake when you run the real thing.

    python simulate.py --file data/events-2026-10-01.jsonl --grace 120
    python simulate.py --file ... --grace 0        # experiment 4
    python simulate.py --file ... --fail-rate 0.3  # experiment 3
"""
import argparse, json, random, sys
from consumer import WindowedFailureRate, parse_ts
from producer import should_fail, make_failing

ap = argparse.ArgumentParser()
ap.add_argument("--file", required=True)
ap.add_argument("--window", type=int, default=60)
ap.add_argument("--grace", type=int, default=120)
ap.add_argument("--threshold", type=float, default=0.10)
ap.add_argument("--min-events", type=int, default=20)
ap.add_argument("--dedup-ttl", type=int, default=None)
ap.add_argument("--fail-rate", type=float, default=0.0)
ap.add_argument("--quiet", action="store_true", help="summary only")
a = ap.parse_args()

rng = random.Random(42)
events = []
with open(a.file, encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if line:
            ev = json.loads(line)
            events.append((parse_ts(ev["receivedAt"]), ev))
events.sort(key=lambda p: p[0])

state = WindowedFailureRate(a.window, a.grace, a.threshold, a.min_events, a.dedup_ttl)

if a.quiet:
    import io, contextlib
    sink = io.StringIO()
    ctx = contextlib.redirect_stdout(sink)
else:
    import contextlib
    ctx = contextlib.nullcontext()

with ctx:
    for _, ev in events:
        if ev["status"] in ("success", "failure") and should_fail(ev["invoiceId"], a.fail_rate):
            ev = make_failing(ev, rng)
        state.add(ev)
        state.close_ready()
        state.evict_seen()
    state.flush_all()

state.summary()
