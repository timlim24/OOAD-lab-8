-- ============================================================================
-- Step 4 — failure rate per payment method and device, highest first
-- ============================================================================
--
-- Read over lab8.payments (one row per payment), never over payment_events.
-- On the raw events every payment contributes 2 non-failure rows (created,
-- processing) plus 1 terminal row, so the same data shows ~2% instead of the
-- true ~6% — the rate is diluted threefold. The comparison is in
-- 09_why_dedup.sql.
--
-- Denominator = all payments of that (method, device). On this dataset every
-- payment reaches a terminal status, so "all payments" and "terminal payments"
-- are the same 1 000 000 rows. In production they are not: payments stuck in
-- `processing` would quietly lower the rate, so the second query below is the
-- one you would actually ship.
--
SELECT
  paymentMethod,
  device,
  count(*)                                                        AS payments,
  count_if(status = 'failure')                                    AS failures,
  round(100.0 * count_if(status = 'failure') / count(*), 2)       AS failure_rate_pct
FROM lab8.payments
GROUP BY paymentMethod, device
ORDER BY failure_rate_pct DESC;

-- Expected: 12 rows. qr/android highest at 6.71%, qr/ios lowest at 5.92%.
-- The spread 5.92..6.71 is inside the noise of a ~6 000-payment cell:
-- a binomial 95% CI at n=6 675, p=0.06 is +/- 0.57pp. Only the `qr` cells are
-- small enough for that to matter — see ../README.md.


-- Production variant: rate over terminal payments only, with a sample-size
-- guard so a cell of 40 payments cannot top the chart at 100%.
SELECT
  paymentMethod,
  device,
  count_if(status IN ('success','failure'))                       AS terminal,
  count_if(status = 'failure')                                    AS failures,
  round(100.0 * count_if(status = 'failure')
              / nullif(count_if(status IN ('success','failure')), 0), 2)
                                                                  AS failure_rate_pct
FROM lab8.payments
GROUP BY paymentMethod, device
HAVING count_if(status IN ('success','failure')) >= 1000
ORDER BY failure_rate_pct DESC;


-- Why the failures happen, for the write-up.
SELECT failureReason, count(*) AS n,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM lab8.payments
WHERE status = 'failure'
GROUP BY failureReason
ORDER BY n DESC;
