-- ============================================================================
-- Step 1 — external table over the raw JSON Lines in S3
-- ============================================================================
--
-- Design notes (be ready to explain these in class):
--
-- * SerDe: org.openx.data.jsonserde.JsonSerDe. It matches JSON keys to columns
--   case-insensitively, so `invoiceId` in the file maps to the `invoiceid`
--   column (Athena lower-cases all identifiers). A key that is absent from a
--   row — `failureReason` on everything that is not a failure — becomes NULL,
--   it is not an error.
--
-- * ignore.malformed.json = true: one broken line does not fail the whole
--   query. In a real pipeline you would instead route those lines to a dead
--   letter prefix, but for a lab this keeps a single bad byte from killing a
--   3 000 000-row scan.
--
-- * The three timestamps are STRING, not TIMESTAMP, on purpose. Athena's Hive
--   TIMESTAMP reader expects 'yyyy-MM-dd HH:mm:ss[.fff]'. Our data is ISO-8601
--   with a 'T' and a trailing 'Z' ("2026-10-01T08:15:22Z"), which that reader
--   returns as NULL — silently. So we keep the raw text and convert in SQL with
--   from_iso8601_timestamp(), which both parses the 'T' and honours the 'Z'
--   as UTC. The Parquet table in step 7 stores them as real timestamps, because
--   by then we have already parsed them once.
--
-- * amount / finalAmount are BIGINT: minor units (kopiyky/cents). Never float —
--   money in a binary floating point type is how you get 0.1 + 0.2 != 0.3 in
--   a ledger.
--
-- * dt is a PARTITION column. It is not in the JSON; Athena reads it from the
--   `dt=2026-10-01` folder name. That is what lets `WHERE dt = '2026-10-05'`
--   skip 30 of 31 files instead of scanning all 1.2 GB.
--
CREATE EXTERNAL TABLE IF NOT EXISTS lab8.payment_events (
  invoiceId      string  COMMENT 'the payment; all its events share it',
  status         string  COMMENT 'created | processing | success | failure',
  failureReason  string  COMMENT 'only present on failed payments',
  amount         bigint  COMMENT 'requested, in minor units',
  finalAmount    bigint  COMMENT 'captured, non-zero only on success',
  ccy            int     COMMENT 'ISO 4217: 980 UAH, 840 USD, 978 EUR',
  reference      string  COMMENT 'merchant order id',
  createdDate    string  COMMENT 'ISO-8601 UTC, when the payment was created',
  modifiedDate   string  COMMENT 'ISO-8601 UTC, EVENT TIME of this status',
  receivedAt     string  COMMENT 'ISO-8601 UTC, PROCESSING TIME at our side',
  merchantId     string,
  merchantName   string,
  mcc            int     COMMENT 'merchant category code',
  customerId     string,
  paymentMethod  string  COMMENT 'card | apple_pay | google_pay | qr',
  device         string  COMMENT 'ios | android | web'
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
STORED AS TEXTFILE
LOCATION 's3://<YOUR-BUCKET>/payments/';


-- Load the 31 Hive-style partitions found under the LOCATION prefix.
-- Without this the table is empty: the partitions exist in S3 but not in the
-- Glue Data Catalog, and Athena only reads partitions the catalog knows about.
MSCK REPAIR TABLE lab8.payment_events;


-- Sanity check. Expected: 3 030 197
SELECT count(*) AS total_events FROM lab8.payment_events;

-- Partitions actually registered. Expected: 31 rows, dt=2026-10-01 .. 10-31
SHOW PARTITIONS lab8.payment_events;
