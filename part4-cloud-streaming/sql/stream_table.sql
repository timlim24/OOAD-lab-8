-- ============================================================================
-- Part 4 — Athena over the data that came through the stream
-- ============================================================================
-- Replace <YOUR-BUCKET>. Same SerDe and the same columns as part 2, over a
-- different prefix: this is what Firehose wrote, not what we copied.

CREATE EXTERNAL TABLE IF NOT EXISTS lab8.stream_events (
  invoiceId      string,
  status         string,
  failureReason  string,
  amount         bigint,
  finalAmount    bigint,
  ccy            int,
  reference      string,
  createdDate    string,
  modifiedDate   string,
  receivedAt     string,
  merchantId     string,
  merchantName   string,
  mcc            int,
  customerId     string,
  paymentMethod  string,
  device         string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
STORED AS TEXTFILE
LOCATION 's3://<YOUR-BUCKET>/stream/';

MSCK REPAIR TABLE lab8.stream_events;


-- 1. Did anything arrive at all? (screenshot this one)
SELECT count(*) AS events,
       count(DISTINCT invoiceId) AS payments,
       min(modifiedDate) AS first_event,
       max(modifiedDate) AS last_event
FROM lab8.stream_events;


-- 2. The same failure-rate question as part 3, but answered in batch over the
--    data the stream deposited. Compare these numbers with what the consumer
--    printed: they should agree, because batch and stream are computing the
--    same thing over the same events — only the deadline differs.
WITH dedup AS (
  SELECT DISTINCT invoiceId, status, modifiedDate, paymentMethod
  FROM lab8.stream_events
)
SELECT paymentMethod,
       count_if(status IN ('success','failure'))                AS terminal,
       count_if(status = 'failure')                             AS failures,
       round(100.0 * count_if(status = 'failure')
                   / nullif(count_if(status IN ('success','failure')), 0), 2) AS failure_rate_pct
FROM dedup
GROUP BY paymentMethod
ORDER BY failure_rate_pct DESC;


-- 3. The partition column is PROCESSING time here, not event time.
--    This is the query that proves it, and it is the key difference from
--    part 2. Expect a non-zero count: anything that crossed midnight, plus
--    everything if you replayed an October day today.
SELECT dt,
       min(substr(modifiedDate, 1, 10)) AS earliest_event_day,
       max(substr(modifiedDate, 1, 10)) AS latest_event_day,
       count(*) AS events
FROM lab8.stream_events
GROUP BY dt
ORDER BY dt;


-- 4. End-to-end latency added by the pipeline itself. `receivedAt` is from the
--    original dataset, so this is not a real measurement of Kinesis+Firehose
--    latency — to get that you would stamp the record on the way in. Included
--    to make the point that you cannot measure what you did not record.
SELECT round(avg(date_diff('second',
         from_iso8601_timestamp(modifiedDate),
         from_iso8601_timestamp(receivedAt))), 2) AS avg_dataset_lag_s,
       max(date_diff('second',
         from_iso8601_timestamp(modifiedDate),
         from_iso8601_timestamp(receivedAt))) AS max_dataset_lag_s
FROM lab8.stream_events;


-- 5. How many files did Firehose write, and how big are they? Run in a shell:
--    aws s3 ls s3://<YOUR-BUCKET>/stream/ --recursive --summarize
--    With a 60s/1MB buffer a one-hour replay produces ~60 files. That is the
--    small-files problem: Athena opens each one separately, and at a few
--    thousand files the per-file overhead dominates the query. The fix is the
--    nightly compaction job into Parquet — part 2, step 7.
