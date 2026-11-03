-- ============================================================================
-- Step 9 — the numbers the write-up asks for, and the dedup demonstration
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 9.1 Scale: unique invoiceId, duplicates, share of events over an hour late
-- ---------------------------------------------------------------------------
SELECT
  count(*)                     AS total_events,            -- 3 030 197
  count(DISTINCT invoiceId)    AS unique_invoices,          -- 1 000 000
  count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM lab8.payment_events))
                               AS exact_duplicate_rows,     --    30 197
  round(100.0 * (count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM lab8.payment_events)))
              / count(*), 3)   AS duplicate_pct,            --     0.997 %
  count_if(date_diff('second', from_iso8601_timestamp(modifiedDate),
                               from_iso8601_timestamp(receivedAt)) > 3600)
                               AS events_over_1h_late,      --         0
  round(100.0 * count_if(date_diff('second', from_iso8601_timestamp(modifiedDate),
                                             from_iso8601_timestamp(receivedAt)) > 3600)
              / count(*), 4)   AS pct_over_1h_late          --     0.0000 %
FROM lab8.payment_events;


-- How many events does one payment actually produce? The brief says 2-3;
-- in this dataset it is exactly 3 for all 1 000 000 payments
-- (created -> processing -> success|failure).
SELECT n_events, count(*) AS payments
FROM (SELECT invoiceId, count(*) AS n_events
      FROM (SELECT DISTINCT * FROM lab8.payment_events)
      GROUP BY invoiceId)
GROUP BY n_events
ORDER BY n_events;


-- How many copies does a redelivered event arrive in? Exactly two —
-- the original plus one retry. 30 197 events are affected.
SELECT copies, count(*) AS events
FROM (SELECT invoiceId, status, modifiedDate, count(*) AS copies
      FROM lab8.payment_events
      GROUP BY invoiceId, status, modifiedDate)
GROUP BY copies
ORDER BY copies;


-- ---------------------------------------------------------------------------
-- 9.2 What breaks without deduplication
-- ---------------------------------------------------------------------------

-- (a) Counting payments. The raw table has 3.03x too many rows, because every
--     payment is present as its three statuses plus the odd retry.
--     3 030 197 "payments" instead of 1 000 000.
SELECT
  (SELECT count(*) FROM lab8.payment_events) AS looks_like_payments,
  (SELECT count(*) FROM lab8.payments)       AS actual_payments;

-- (b) Failure rate. THIS is the dangerous one, because the number is
--     plausible rather than absurd. On the raw events each payment donates two
--     non-failure rows to the denominator, so the rate is diluted by ~3x:
--     ~2.0% instead of the true ~6.0%. A report that says "card declines are
--     at 2%" when they are at 6% does not look broken — it looks fine, and
--     nobody investigates.
SELECT 'raw events (WRONG)' AS source, paymentMethod, device,
       count(*) AS rows,
       round(100.0 * count_if(status = 'failure') / count(*), 2) AS failure_rate_pct
FROM lab8.payment_events GROUP BY paymentMethod, device
UNION ALL
SELECT 'deduplicated', paymentMethod, device,
       count(*),
       round(100.0 * count_if(status = 'failure') / count(*), 2)
FROM lab8.payments GROUP BY paymentMethod, device
ORDER BY paymentMethod, device, source;

-- (c) Turnover. This one is nearly right by accident and that makes it worse.
--     finalAmount is 0 on created/processing, so the three-row history does
--     not inflate the sum at all. Only the ~1% of redelivered SUCCESS events
--     double-count — a quiet, roughly 1% overstatement of revenue that no
--     sanity check catches.
SELECT
  (SELECT CAST(sum(finalAmount) AS decimal(38,2)) / 100
     FROM lab8.payment_events WHERE status = 'success' AND ccy = 980) AS uah_raw,
  (SELECT CAST(sum(finalAmount) AS decimal(38,2)) / 100
     FROM lab8.payments        WHERE status = 'success' AND ccy = 980) AS uah_dedup;

-- (d) Unique customers / merchant reach: unaffected by the history (COUNT
--     DISTINCT is idempotent), which is exactly why "it worked on my query"
--     is not evidence that a pipeline is deduplicated.
SELECT
  (SELECT count(DISTINCT customerId) FROM lab8.payment_events) AS customers_raw,
  (SELECT count(DISTINCT customerId) FROM lab8.payments)       AS customers_dedup;


-- ---------------------------------------------------------------------------
-- 9.3 Why DISTINCT has to come before row_number()
-- ---------------------------------------------------------------------------
--
-- Take one redelivered terminal event and watch what each version does.
-- Pick a victim first:
SELECT invoiceId, status, modifiedDate, count(*) AS copies
FROM lab8.payment_events
GROUP BY invoiceId, status, modifiedDate
HAVING count(*) > 1
LIMIT 5;

-- Now substitute one of those invoiceIds below.
WITH victim AS (SELECT 'PUT-AN-INVOICE-ID-HERE' AS id)

-- without DISTINCT: rank() returns the duplicate twice
SELECT 'rank, no distinct' AS variant, invoiceId, status, modifiedDate, rk
FROM (
  SELECT invoiceId, status, modifiedDate,
         rank() OVER (PARTITION BY invoiceId ORDER BY from_iso8601_timestamp(modifiedDate) DESC) AS rk
  FROM lab8.payment_events WHERE invoiceId = (SELECT id FROM victim)
) WHERE rk = 1;

-- The full argument, for the write-up:
--
-- 1. row_number() alone does NOT return two rows for an exact duplicate — it
--    numbers them 1 and 2 arbitrarily and `WHERE rn = 1` keeps one. So the
--    naive answer "without DISTINCT you get duplicates in the view" is wrong,
--    and saying it in class will get you asked to demonstrate it.
--
-- 2. What you actually lose is DETERMINISM AND MEANING:
--
--    * Which of the two identical rows wins is undefined. Here they are
--      byte-identical so nobody notices. The moment the retry differs in one
--      field — a re-serialised float, a later receivedAt, an enriched
--      merchantName — the view starts returning different data on identical
--      input, and the Parquet table you build from it in step 7 changes
--      between runs. Non-reproducible output is the bug; the duplicate is
--      just how it gets in.
--
--    * ORDER BY ... DESC with a tie makes the window function's work
--      non-deterministic, so Presto is free to pick either row — and will
--      pick differently depending on split boundaries, i.e. on how many files
--      S3 happened to return. The same query, the same data, a different
--      answer next month.
--
--    * The duplicate count itself becomes unmeasurable. `count(*) - count
--      (DISTINCT *)` is the only honest definition of "how many events were
--      redelivered"; if the view already collapsed them for a different
--      reason you can no longer report that 0.997% figure.
--
-- 3. And DISTINCT and row_number() answer two genuinely different questions,
--    which is the real reason to keep them as separate steps:
--       DISTINCT     -> "the sender told us this twice"      (transport bug)
--       row_number() -> "the payment moved through statuses" (business fact)
--    Collapsing both into one window function hides the fact that one of them
--    is an error you should be alerting on and the other is normal.
