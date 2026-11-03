-- ============================================================================
-- Step 2 — the `payments` view: exactly one row per invoiceId
-- ============================================================================
--
-- Two different problems are being solved here, and the order matters.
--
--   (a) REDELIVERY. ~1% of events arrive twice as byte-identical copies.
--       That is a transport artefact: the webhook sender retried because our
--       200 OK was lost. Those rows carry no new information.
--       -> removed by SELECT DISTINCT.
--
--   (b) EVENT HISTORY. One payment legitimately produces created ->
--       processing -> success|failure. Those rows are all different and all
--       real, but for "how much did merchant X turn over" we want the payment,
--       not its history.
--       -> collapsed by row_number() ... = 1.
--
-- Why DISTINCT must come FIRST: see ../README.md, section "Why DISTINCT before
-- row_number()".
--
-- The tie-breaker on status: modifiedDate has second granularity, so two
-- statuses of a fast payment can in principle land in the same second. Ordering
-- by a status rank after the timestamp makes the view deterministic — the same
-- query returns the same row every time. (Checked on this dataset: 0 invoices
-- actually tie, but a view that is only correct by luck is not correct.)
--
CREATE OR REPLACE VIEW lab8.payments AS
WITH distinct_events AS (
  -- (a) drop byte-identical redeliveries
  SELECT DISTINCT * FROM lab8.payment_events
),
ranked AS (
  -- (b) rank the real history of each payment, newest status first
  SELECT
    invoiceId, status, failureReason, amount, finalAmount, ccy, reference,
    createdDate, modifiedDate, receivedAt,
    merchantId, merchantName, mcc, customerId, paymentMethod, device, dt,
    row_number() OVER (
      PARTITION BY invoiceId
      ORDER BY from_iso8601_timestamp(modifiedDate) DESC,
               CASE status
                 WHEN 'success'    THEN 4
                 WHEN 'failure'    THEN 4   -- both are terminal, never co-occur
                 WHEN 'processing' THEN 2
                 WHEN 'created'    THEN 1
                 ELSE 0
               END DESC
    ) AS rn
  FROM distinct_events
)
SELECT
  invoiceId, status, failureReason, amount, finalAmount, ccy, reference,
  createdDate, modifiedDate, receivedAt,
  merchantId, merchantName, mcc, customerId, paymentMethod, device, dt
FROM ranked
WHERE rn = 1;


-- Verification. Expected: 1 000 000 and 1 000 000 — one row per payment.
SELECT count(*) AS payments, count(DISTINCT invoiceId) AS unique_invoices
FROM lab8.payments;


-- How much did each stage actually remove?
--   3 030 197 raw events
-- - 3 000 000 after DISTINCT   -> 30 197 redelivered copies (0.997%)
-- - 1 000 000 after row_number -> 2 000 000 superseded historical statuses
SELECT
  (SELECT count(*) FROM lab8.payment_events)                        AS raw_events,
  (SELECT count(*) FROM (SELECT DISTINCT * FROM lab8.payment_events)) AS after_distinct,
  (SELECT count(*) FROM lab8.payments)                              AS after_rownumber;
