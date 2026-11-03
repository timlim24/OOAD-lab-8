-- ============================================================================
-- Step 7 — CTAS: the deduplicated payments as Parquet, partitioned by dt
-- ============================================================================
--
-- Three rules of Athena CTAS that will bite you if you skip them:
--
--  1. The partition columns must be the LAST columns of the SELECT list, in
--     the same order as `partitioned_by`. Athena does not match them by name.
--  2. `external_location` must point at an EMPTY prefix. If a previous run
--     left files there the query fails; drop the table AND delete the files.
--  3. A single CTAS writes at most 100 partitions. We have 31, so we are fine.
--     For a year of data you would need INSERT INTO in chunks.
--
-- We also change the types on the way through. The JSON table had the
-- timestamps as STRING because the Hive reader cannot parse ISO-8601. Parquet
-- has a real TIMESTAMP logical type, so we parse once here and every later
-- query gets to skip from_iso8601_timestamp() entirely. That is the point of a
-- curated layer: pay the parsing cost once, at write time, not on every read.
--
-- Note on CAST(... AS timestamp): from_iso8601_timestamp() returns
-- `timestamp with time zone`. Parquet in Athena stores plain `timestamp`, so
-- the explicit CAST drops the zone. All our data is UTC, so nothing is lost —
-- but the cast must be written out, or the CTAS fails on a type mismatch.
--
DROP TABLE IF EXISTS lab8.payments_parquet;
-- then, outside Athena:
--   aws s3 rm s3://<YOUR-BUCKET>/curated/payments_parquet/ --recursive

CREATE TABLE lab8.payments_parquet
WITH (
  external_location = 's3://<YOUR-BUCKET>/curated/payments_parquet/',
  format            = 'PARQUET',
  write_compression = 'SNAPPY',
  partitioned_by    = ARRAY['dt']
) AS
SELECT
  invoiceId,
  status,
  failureReason,
  amount,
  finalAmount,
  ccy,
  reference,
  CAST(from_iso8601_timestamp(createdDate)  AS timestamp) AS createdDate,
  CAST(from_iso8601_timestamp(modifiedDate) AS timestamp) AS modifiedDate,
  CAST(from_iso8601_timestamp(receivedAt)   AS timestamp) AS receivedAt,
  merchantId,
  merchantName,
  mcc,
  customerId,
  paymentMethod,
  device,
  dt            -- partition column: LAST
FROM lab8.payments;


-- Expected: 1 000 000 rows, 31 partitions.
SELECT count(*) AS rows_written FROM lab8.payments_parquet;
SHOW PARTITIONS lab8.payments_parquet;

-- Compare the footprint on S3:
--   aws s3 ls s3://<YOUR-BUCKET>/payments/                --recursive --summarize
--   aws s3 ls s3://<YOUR-BUCKET>/curated/payments_parquet/ --recursive --summarize
-- Two separate effects are stacked here, and the write-up should keep them
-- apart:
--   * deduplication: 3 030 197 rows -> 1 000 000 rows  (3.03x fewer rows)
--   * encoding:      JSON text -> columnar Parquet + Snappy, which on top of
--     that removes the repeated key names on every row, dictionary-encodes the
--     10 merchant names / 4 methods / 3 devices, and bit-packs the integers.
-- Only the second one is "Parquet"; a CTAS to JSON would already have given
-- you the first.
