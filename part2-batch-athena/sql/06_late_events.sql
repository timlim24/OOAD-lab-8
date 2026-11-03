-- ============================================================================
-- Step 6 — late events: receivedAt minus modifiedDate
-- ============================================================================
--
-- This is THE event-time vs processing-time question of the whole lab.
--   modifiedDate = EVENT TIME    — when the thing happened in the world
--   receivedAt   = PROCESSING TIME — when our pipeline first saw it
-- The gap is the lateness. A batch job does not care: by morning everything
-- has arrived and lands in the right dt partition anyway. A streaming job
-- cares a great deal — that gap is exactly what the watermark grace period in
-- part 3 has to cover.
--
-- Counted over payment_events (raw), not over the deduplicated view: the
-- question is about events in transit, and a redelivered copy was also an
-- event in transit. The second query shows the same figure after DISTINCT so
-- you can see redelivery does not skew it.
--
WITH lagged AS (
  SELECT date_diff('second',
           from_iso8601_timestamp(modifiedDate),
           from_iso8601_timestamp(receivedAt)) AS lag_s
  FROM lab8.payment_events
)
SELECT
  count(*)                                                   AS total_events,
  count_if(lag_s > 60)                                       AS late_over_60s,
  round(100.0 * count_if(lag_s > 60) / count(*), 3)          AS pct_over_60s,
  count_if(lag_s > 3600)                                     AS late_over_1h,
  round(100.0 * count_if(lag_s > 3600) / count(*), 4)        AS pct_over_1h,
  min(lag_s)                                                 AS min_lag_s,
  round(avg(lag_s), 2)                                       AS avg_lag_s,
  approx_percentile(CAST(lag_s AS double), 0.50)             AS p50_lag_s,
  approx_percentile(CAST(lag_s AS double), 0.99)             AS p99_lag_s,
  max(lag_s)                                                 AS max_lag_s
FROM lagged;

-- Expected:
--   total_events   3 030 197
--   late_over_60s     60 824   (2.007%)   <- matches the stated "about 2% late"
--   late_over_1h           0   (0.0000%)  <- the generator caps lateness at 10 min
--   max_lag_s            600   = exactly 10 minutes
--   p50_lag_s              1   second — the overwhelming majority is instant
-- So: the answer to "which share of events was more than an hour late" is
-- ZERO, and that is a real finding, not a missing result. The tail ends at
-- exactly 600 s, because that is where the generator caps the injected delay.
--
-- This table is what sets the watermark grace period in part 3 — the share of
-- all events that a given grace would already have in hand:
--     grace    0s -> 32.65%      grace  120s -> 98.215%
--     grace   30s -> 97.989%     grace  300s -> 98.880%
--     grace   60s -> 97.993%     grace  600s -> 100.000%
-- The 2-minute grace of part 3 therefore admits 98.2% of events and drops
-- 1.8%; buying the last 1.8% costs 8 more minutes of delay on every alert.


-- Distribution of the lateness. It is bimodal, not a smooth tail: 97.99% of
-- events arrive within 10 seconds, then nothing until the 60s mark, then the
-- injected 2% spread over 1-10 minutes. Expected:
--   a) 0-10s    2 969 260  97.989%
--   b) 10-60s         113   0.004%
--   c) 60-120s      6 735   0.222%
--   d) 2-5min      20 137   0.665%
--   e) 5-10min     33 952   1.120%
SELECT bucket, count(*) AS events,
       round(100.0 * count(*) / sum(count(*)) OVER (), 3) AS pct
FROM (
  SELECT CASE
           WHEN lag_s <= 10   THEN 'a) 0-10s'
           WHEN lag_s <= 60   THEN 'b) 10-60s'
           WHEN lag_s <= 120  THEN 'c) 60-120s'
           WHEN lag_s <= 300  THEN 'd) 2-5min'
           WHEN lag_s <= 600  THEN 'e) 5-10min'
           ELSE                    'f) >10min'
         END AS bucket
  FROM (SELECT date_diff('second',
                 from_iso8601_timestamp(modifiedDate),
                 from_iso8601_timestamp(receivedAt)) AS lag_s
        FROM lab8.payment_events)
)
GROUP BY bucket ORDER BY bucket;


-- Is the dataset partitioned by EVENT time or by PROCESSING time? This decides
-- whether `WHERE dt = '2026-10-05'` is a correct filter for a daily report.
--
-- Expected: the first query returns 0, the second returns 88.
-- So dt is the EVENT day (modifiedDate), and 88 events happened just before
-- midnight but were received after it. Partitioning by event time is the right
-- choice for analytics — a daily report is complete and never changes once the
-- day closes. Had it been partitioned by receivedAt, yesterday's partition
-- would keep growing all night and every daily number would be provisional.
SELECT count(*) AS event_day_differs_from_partition
FROM lab8.payment_events
WHERE date_format(from_iso8601_timestamp(modifiedDate), '%Y-%m-%d') <> dt;

SELECT count(*) AS received_day_differs_from_partition
FROM lab8.payment_events
WHERE date_format(from_iso8601_timestamp(receivedAt), '%Y-%m-%d') <> dt;
