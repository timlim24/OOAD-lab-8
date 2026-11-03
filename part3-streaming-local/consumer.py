#!/usr/bin/env python3
"""
Event-time windowed failure-rate monitor over the payments topic.

What it does, in the order it does it:

  1. drops duplicate events (the ~1% the webhook sender redelivers),
  2. buckets the survivors into 1-minute windows of EVENT time
     (``modifiedDate``), not of arrival time,
  3. closes a window only once the watermark has moved ``--grace`` seconds past
     its end, so the late 2% still get counted,
  4. prints the failure share per paymentMethod for each closed window, and
     shouts when it goes over ``--threshold``.

    python consumer.py                      # defaults: 60s windows, 120s grace
    python consumer.py --grace 0            # experiment 4
    python consumer.py &; python consumer.py   # experiment 2: two in one group

The watermark is PER PARTITION, combined by taking the MINIMUM. That detail is
not decoration, and getting it wrong is the most instructive bug in this lab.

A single global ``max(modifiedDate)`` looks obviously right and is obviously
wrong as soon as there is more than one partition. The consumer drains
partitions at different speeds — one poll can return 500 records from
partition 0 and none from partitions 1 and 2 — so a global max races ahead on
whichever partition happens to be furthest along, and every record that then
arrives from the slower partitions is "late". Measured on this topic with 3
partitions: a global max rejected 56% of all events as late, with an average
lateness of 6 649 seconds, against a true figure of ~1.7%.

Taking the minimum across partitions fixes it, because the stream as a whole
has only progressed as far as its slowest partition has. This is exactly what
Flink does with per-split watermarks in ``WatermarkStrategy``, and why its API
makes you think about idleness: a partition with no data at all would hold the
minimum at -infinity forever, so one has to be excluded after
``--idle-partition`` seconds of silence.

What a watermark still cannot do is advance while the WHOLE stream is idle,
which is why there is also an inactivity flush at the bottom — without it the
last few windows of a finite replay would never close and you would stare at a
consumer that looks hung.
"""

import argparse
import json
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, ConsumerRebalanceListener
from kafka.errors import KafkaError

TERMINAL = ("success", "failure")


def parse_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


class WindowedFailureRate:
    def __init__(self, window_s: int, grace_s: int, threshold: float,
                 min_events: int, dedup_ttl_s: int | None):
        self.window_s = window_s
        self.grace_s = grace_s
        self.threshold = threshold
        self.min_events = min_events
        self.dedup_ttl_s = dedup_ttl_s

        # window_start -> paymentMethod -> [terminal_count, failure_count]
        self.windows: dict[int, dict[str, list[int]]] = defaultdict(
            lambda: defaultdict(lambda: [0, 0]))

        # The unbounded set the brief asks us to think about. Key is the
        # business identity of an event, not a hash of the bytes: the same
        # (payment, status, moment) IS the same event even if the retry
        # re-serialised the JSON with different key order or spacing.
        # Value is the event time, so --dedup-ttl can evict by age.
        self.seen: dict[tuple, float] = {}

        # partition -> largest event time seen on THAT partition, and when we
        # last saw anything on it (monotonic clock, for the idleness rule).
        self.part_wm: dict[object, float] = {}
        self.part_seen_at: dict[object, float] = {}
        self.idle_partition_s = 10.0

        self.watermark = float("-inf")
        self.closed_up_to = float("-inf")   # end of the most recent closed window

        self.n_total = 0
        self.n_dupes = 0
        self.n_late = 0
        self.n_counted = 0
        self.n_nonterminal = 0
        self.n_alerts = 0
        self.closed_windows = 0
        self.max_seen_size = 0
        self.late_lags: list[float] = []

    # -- watermark ---------------------------------------------------------
    def _recompute_watermark(self) -> None:
        """Global watermark = min over partitions that are not idle.

        Monotonic by construction: each partition's own value only grows, the
        idle set only shrinks the inputs, and the result is clamped against
        the previous value so a partition dropping out can never move it back.
        """
        if not self.part_wm:
            return
        now = time.monotonic()
        # A partition still sitting at -inf has been ASSIGNED but has not
        # delivered its first record yet. It must hold the minimum down: its
        # oldest event could be hours behind whatever the fast partitions are
        # already showing us. Only the idleness rule may remove it.
        active = [wm for p, wm in self.part_wm.items()
                  if now - self.part_seen_at.get(p, now) <= self.idle_partition_s]
        if not active:                      # everything idle: fall back to all
            active = list(self.part_wm.values())
        candidate = min(active)
        if candidate > self.watermark:
            self.watermark = candidate

    # -- ingest ------------------------------------------------------------
    def add(self, ev: dict, partition: object = 0) -> None:
        self.n_total += 1
        ts = parse_ts(ev["modifiedDate"])

        key = (ev["invoiceId"], ev["status"], ev["modifiedDate"])
        if key in self.seen:
            self.n_dupes += 1
            return
        self.seen[key] = ts
        self.max_seen_size = max(self.max_seen_size, len(self.seen))

        w_start = int(ts // self.window_s) * self.window_s
        w_end = w_start + self.window_s

        # Too late: its window was already emitted. In Flink this is the
        # side output; here we only count it, which is the honest minimum —
        # a number that says how much of reality the report is missing.
        if w_end <= self.closed_up_to:
            self.n_late += 1
            self.late_lags.append(self.closed_up_to - ts)
            return

        # Only terminal events carry a success/failure verdict. created and
        # processing are not "not a failure", they are "no answer yet", and
        # putting them in the denominator is exactly the mistake part 2
        # measured: it turns a 6% failure rate into 2%.
        if ev["status"] not in TERMINAL:
            self.n_nonterminal += 1
        else:
            cell = self.windows[w_start][ev.get("paymentMethod", "unknown")]
            cell[0] += 1
            if ev["status"] == "failure":
                cell[1] += 1
            self.n_counted += 1

        if ts > self.part_wm.get(partition, float("-inf")):
            self.part_wm[partition] = ts
        self.part_seen_at[partition] = time.monotonic()

    # -- window lifecycle --------------------------------------------------
    def close_ready(self) -> None:
        """Emit every window whose end + grace is behind the watermark."""
        if self.watermark == float("-inf"):
            return
        deadline = self.watermark - self.grace_s
        for w_start in sorted(w for w in self.windows
                              if w + self.window_s <= deadline):
            self._emit(w_start)

    def flush_all(self) -> None:
        for w_start in sorted(self.windows):
            self._emit(w_start)

    def _emit(self, w_start: int) -> None:
        methods = self.windows.pop(w_start)
        w_end = w_start + self.window_s
        self.closed_up_to = max(self.closed_up_to, w_end)
        self.closed_windows += 1

        total = sum(c[0] for c in methods.values())
        fails = sum(c[1] for c in methods.values())
        overall = 100.0 * fails / total if total else 0.0

        print(f"\n[window {fmt(w_start)}–{fmt(w_end)} UTC]  "
              f"{total:,} terminal, {fails:,} failed, {overall:.2f}% overall "
              f"(watermark {fmt(self.watermark)})")

        for method in sorted(methods, key=lambda m: -methods[m][0]):
            n, f = methods[method]
            pct = 100.0 * f / n if n else 0.0
            flag = ""
            if pct > self.threshold * 100 and n >= self.min_events:
                flag = "  <-- ALERT"
                self.n_alerts += 1
            print(f"    {method:<12} {n:>6,} terminal  {f:>5,} failed  "
                  f"{pct:6.2f}%{flag}")
            if flag:
                print(f"    !! ALERT: {method} failure rate {pct:.2f}% "
                      f"> {self.threshold:.0%} in window "
                      f"{fmt(w_start)}–{fmt(w_end)} ({f}/{n})")

    # -- housekeeping ------------------------------------------------------
    def evict_seen(self) -> int:
        """Drop dedup keys older than the TTL.

        This is the production concern the brief flags: ``self.seen`` grows
        without bound, and at 3 million events a day a Python dict of tuples
        is hundreds of megabytes. A real service puts this in Redis with
        ``SET key 1 EX ttl`` or in Flink keyed state with a state TTL. The TTL
        has to exceed the largest lateness you are willing to deduplicate
        against — 600s in this dataset — or you start letting old duplicates
        back in.
        """
        if not self.dedup_ttl_s or self.watermark == float("-inf"):
            return 0
        cutoff = self.watermark - self.dedup_ttl_s
        stale = [k for k, ts in self.seen.items() if ts < cutoff]
        for k in stale:
            del self.seen[k]
        return len(stale)

    def summary(self) -> None:
        print("\n" + "=" * 72)
        print("SUMMARY")
        print("=" * 72)
        print(f"  events received        : {self.n_total:,}")
        print(f"  duplicates dropped     : {self.n_dupes:,} "
              f"({100.0*self.n_dupes/max(self.n_total,1):.3f}%)")
        print(f"  rejected as LATE       : {self.n_late:,} "
              f"({100.0*self.n_late/max(self.n_total,1):.3f}%)")
        if self.late_lags:
            print(f"      how late they were : "
                  f"min {min(self.late_lags):.0f}s  "
                  f"avg {sum(self.late_lags)/len(self.late_lags):.0f}s  "
                  f"max {max(self.late_lags):.0f}s")
        print(f"  non-terminal (skipped) : {self.n_nonterminal:,}")
        print(f"  counted into windows   : {self.n_counted:,}")
        print(f"  windows closed         : {self.closed_windows:,}")
        print(f"  alerts fired           : {self.n_alerts:,}")
        print(f"  dedup set, final size  : {len(self.seen):,}")
        print(f"  dedup set, peak size   : {self.max_seen_size:,}")
        print(f"  grace period           : {self.grace_s}s")
        if len(self.part_wm) > 1:
            print(f"  per-partition watermark: "
                  + ", ".join(f"p{p}={fmt(wm)}" for p, wm in sorted(self.part_wm.items())))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--topic", default="payments")
    ap.add_argument("--group", default="failure-rate-monitor")
    ap.add_argument("--window", type=int, default=60, help="window size, seconds")
    ap.add_argument("--grace", type=int, default=120,
                    help="wait this long past a window's end before closing it")
    ap.add_argument("--threshold", type=float, default=0.10, help="alert above this share")
    ap.add_argument("--min-events", type=int, default=20,
                    help="do not alert on a cell smaller than this (1 of 3 is not 33 percent)")
    ap.add_argument("--dedup-ttl", type=int, default=None,
                    help="evict dedup keys older than N seconds of event time")
    ap.add_argument("--idle-flush", type=float, default=15.0,
                    help="flush all open windows after this many idle seconds")
    ap.add_argument("--idle-partition", type=float, default=10.0,
                    help="drop a silent partition out of the watermark minimum "
                         "after this many seconds, or one quiet partition "
                         "stalls every window")
    ap.add_argument("--exit-after-idle", type=float, default=0,
                    help="stop after this many idle seconds (0 = run forever). "
                         "A real monitor never exits; this exists so a replay "
                         "experiment finishes on its own and is reproducible.")
    args = ap.parse_args()

    state = WindowedFailureRate(args.window, args.grace, args.threshold,
                                args.min_events, args.dedup_ttl)
    state.idle_partition_s = args.idle_partition

    class Rebalance(ConsumerRebalanceListener):
        """Prints what experiment 2 is about to show you in the Console."""

        def on_partitions_revoked(self, revoked):
            if revoked:
                print(f"[rebalance] revoked:  "
                      f"{sorted(tp.partition for tp in revoked)}", flush=True)
                # Someone else owns these now. Leaving them in the minimum
                # would freeze the watermark at whatever they last reached.
                for tp in revoked:
                    state.part_wm.pop(tp.partition, None)
                    state.part_seen_at.pop(tp.partition, None)

        def on_partitions_assigned(self, assigned):
            print(f"[rebalance] assigned: "
                  f"{sorted(tp.partition for tp in assigned)}", flush=True)
            # Register every assigned partition at -inf BEFORE any record is
            # read. Without this the minimum is taken over the partitions that
            # happened to answer first, the watermark races ahead to whatever
            # the fastest one reached, and everything the slower partitions
            # then deliver is rejected as late. Measured on this topic while
            # catching up from the start of the log: 41% of all events, with
            # an average lateness of 2.5 hours. With it: ~1.7%, matching the
            # offline reference run.
            now = time.monotonic()
            for tp in assigned:
                state.part_wm.setdefault(tp.partition, float("-inf"))
                state.part_seen_at.setdefault(tp.partition, now)

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        # earliest: a brand-new group reads the whole retained log instead of
        # only what arrives after it starts. This is what makes experiment 1
        # work - stop the consumer, restart it, and it resumes from its
        # committed offset with nothing lost.
        auto_offset_reset="earliest",
        # Commit by hand, after the event is folded into a window. With
        # auto-commit the client could commit an offset for a message we have
        # read but not yet processed, and a crash in between would lose it.
        enable_auto_commit=False,
        value_deserializer=lambda b: b,
        max_poll_records=500,
        consumer_timeout_ms=0,
    )
    consumer.subscribe([args.topic], listener=Rebalance())

    running = True

    def stop(signum, frame):
        nonlocal running
        running = False
        print("\n[signal] draining and closing open windows ...", flush=True)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"consuming {args.topic} as group '{args.group}'  "
          f"window={args.window}s grace={args.grace}s "
          f"threshold={args.threshold:.0%}")
    print("Ctrl-C to stop\n", flush=True)

    last_msg = time.monotonic()
    last_commit = time.monotonic()
    idle_flushed = False

    try:
        while running:
            batches = consumer.poll(timeout_ms=1000)

            if not batches:
                idle = time.monotonic() - last_msg
                if idle > args.idle_flush and state.windows and not idle_flushed:
                    print(f"\n[idle {idle:.0f}s] no new events - closing the "
                          f"remaining {len(state.windows)} open window(s). "
                          f"On an endless stream the watermark would do this; "
                          f"on a finite replay somebody has to.", flush=True)
                    state.flush_all()
                    idle_flushed = True
                if args.exit_after_idle and idle > args.exit_after_idle:
                    print(f"\n[idle {idle:.0f}s] --exit-after-idle reached, "
                          f"stopping.", flush=True)
                    running = False
                continue

            last_msg = time.monotonic()
            idle_flushed = False

            for tp, records in batches.items():
                for rec in records:
                    try:
                        ev = json.loads(rec.value)
                    except (json.JSONDecodeError, TypeError) as exc:
                        print(f"bad message at {tp.topic}:{tp.partition}"
                              f"@{rec.offset}: {exc}", file=sys.stderr)
                        continue
                    state.add(ev, partition=tp.partition)

            # Recompute the combined watermark once the whole batch is folded
            # in — a batch usually carries records from several partitions, and
            # the minimum is only meaningful after all of them are accounted
            # for. Then close once per batch rather than once per record:
            # scanning the open windows per record is what would make this
            # consumer the bottleneck.
            state._recompute_watermark()
            state.close_ready()

            now = time.monotonic()
            if now - last_commit > 5.0:
                consumer.commit()
                evicted = state.evict_seen()
                last_commit = now
                if evicted:
                    print(f"[dedup] evicted {evicted:,} keys past TTL, "
                          f"{len(state.seen):,} remain", flush=True)

    except KafkaError as exc:
        print(f"kafka error: {exc}", file=sys.stderr)
        return 1
    finally:
        state.flush_all()
        try:
            consumer.commit()
        except KafkaError as exc:
            print(f"final commit failed: {exc}", file=sys.stderr)
        state.summary()
        # close() leaves the group cleanly so the other members rebalance at
        # once instead of waiting out session.timeout.ms. It can block, and by
        # this point the numbers are already printed, so a failure here must
        # not look like a failed run.
        try:
            consumer.close(autocommit=False)
        except Exception as exc:
            print(f"close() failed: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
