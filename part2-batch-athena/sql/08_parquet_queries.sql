-- ============================================================================
-- Step 8 — queries 3, 4, 5 re-run against Parquet
-- ============================================================================
-- Same questions, same expected answers, different storage. Record
-- "Data scanned" and "Run time" from the Athena console for each of the six
-- runs and put them in ../results/benchmark.md.
--
-- Two differences from the JSON versions:
--   * no from_iso8601_timestamp() — the columns are already timestamps;
--   * reading lab8.payments_parquet (a table) instead of lab8.payments
--     (a view over the raw JSON, which re-does DISTINCT + row_number()
--     on every single execution).
-- The second point matters as much as the file format: the view is recomputed
-- from 1.2 GB of JSON every time you query it. The Parquet table is the
-- materialised result of that work.
-- ============================================================================


-- Q3 — turnover by merchant and currency, successful payments only
SELECT
  merchantId, merchantName, ccy,
  CASE ccy WHEN 980 THEN 'UAH' WHEN 840 THEN 'USD' WHEN 978 THEN 'EUR'
           ELSE CAST(ccy AS varchar) END          AS currency,
  count(*)                                        AS payments,
  CAST(sum(finalAmount) AS decimal(38,2)) / 100   AS turnover
FROM lab8.payments_parquet
WHERE status = 'success'
GROUP BY merchantId, merchantName, ccy
ORDER BY currency, turnover DESC;


-- Q4 — failure rate per payment method and device
SELECT
  paymentMethod, device,
  count(*)                                                  AS payments,
  count_if(status = 'failure')                              AS failures,
  round(100.0 * count_if(status = 'failure') / count(*), 2) AS failure_rate_pct
FROM lab8.payments_parquet
GROUP BY paymentMethod, device
ORDER BY failure_rate_pct DESC;


-- Q5 — payments per hour of the day
SELECT hour(createdDate) AS hour_utc, count(*) AS payments
FROM lab8.payments_parquet
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- The two extra wins Parquet gives you, which the three queries above do not
-- show. Run these too; they are the interesting half of the comparison.
-- ---------------------------------------------------------------------------

-- (a) COLUMN PRUNING. This touches 2 of 17 columns. On JSON, Athena must read
--     and parse every byte of every row to find them, so it scans the full
--     1.2 GB. On Parquet it reads 2 column chunks and skips the rest — the
--     ratio here is far better than on the GROUP BYs above.
SELECT status, count(*) FROM lab8.payments_parquet GROUP BY status;
SELECT status, count(*) FROM lab8.payments        GROUP BY status;

-- (b) PARTITION PRUNING. One day out of 31. Both tables are partitioned, so
--     both prune — this is a property of the dt partition, not of Parquet.
--     Worth running to show the students that the two optimisations are
--     independent.
SELECT count(*) FROM lab8.payments_parquet WHERE dt = '2026-10-05';
SELECT count(*) FROM lab8.payments        WHERE dt = '2026-10-05';

-- (c) Same filter without the partition column: a full scan on both. This is
--     the mistake that turns a 3-cent query into a 3-dollar one.
SELECT count(*) FROM lab8.payments_parquet
WHERE date_format(createdDate, '%Y-%m-%d') = '2026-10-05';
