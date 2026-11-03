-- ============================================================================
-- Step 3 — turnover by merchant and currency, successful payments only
-- ============================================================================
--
-- finalAmount, not amount: `amount` is what was requested, `finalAmount` is
-- what was actually captured, and it is zero on everything that is not a
-- success. Summing `amount` would count money that never moved.
--
-- / 100.0 converts minor units to major (kopiyky -> hryvni). CAST to DECIMAL
-- before dividing, not after: DECIMAL(38,0) / 100.0 keeps exact arithmetic,
-- whereas bigint / 100.0 in Presto promotes to DOUBLE and starts losing
-- kopiyky somewhere around the 15th significant digit. At 310 million UAH we
-- are at 11 digits — safe today, wrong on a bigger merchant.
--
-- We do NOT sum across currencies. 980 + 840 + 978 are three different units;
-- adding them needs an FX rate per day, which this dataset does not carry.
--
SELECT
  merchantId,
  merchantName,
  ccy,
  CASE ccy WHEN 980 THEN 'UAH' WHEN 840 THEN 'USD' WHEN 978 THEN 'EUR'
           ELSE CAST(ccy AS varchar) END                       AS currency,
  count(*)                                                     AS payments,
  CAST(sum(finalAmount) AS decimal(38,0)) / 100                AS turnover,
  CAST(avg(finalAmount) AS decimal(38,2)) / 100                AS avg_ticket
FROM lab8.payments
WHERE status = 'success'
GROUP BY merchantId, merchantName, ccy
ORDER BY currency, turnover DESC;

-- Expected (verified locally over all 31 days): 30 rows, 10 merchants x 3 ccy.
-- Largest line: Інтернет-магазин / UAH — 87 115 payments, 310 259 259.21 UAH.
-- Smallest:     Музичний сервіс / USD —  3 762 payments,     372 438.00 USD.
