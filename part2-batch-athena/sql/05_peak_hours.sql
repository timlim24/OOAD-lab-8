-- ============================================================================
-- Step 5 — payments per hour of the day
-- ============================================================================
--
-- Which timestamp? createdDate — the hour the customer started the payment.
-- modifiedDate would answer "when did statuses change", which drifts a few
-- seconds later and smears the boundary; receivedAt would answer "when was our
-- pipeline busy", which is a capacity question, not a demand question.
--
-- Everything here is UTC. Kyiv in October 2026 is UTC+3 until the DST switch
-- on 25 October, UTC+2 after it — so a single fixed offset would be wrong for
-- the month. The local column below is therefore illustrative; the honest fix
-- is AT TIME ZONE 'Europe/Kyiv', shown in the second query.
--
SELECT
  hour(from_iso8601_timestamp(createdDate))        AS hour_utc,
  count(*)                                         AS payments,
  round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_of_day
FROM lab8.payments
GROUP BY 1
ORDER BY 1;

-- Expected shape: a flat night floor of ~6 600/h from 00:00 to 04:00 UTC,
-- a lunch bump at 12:00 UTC (66 304) and the real peak at 18:00 UTC (80 338),
-- which is 21:00 Kyiv. Night trough 04:00 UTC = 6 585. Peak/trough = 12.2x.


-- Same thing in the timezone the customers actually live in. This is the
-- version to put in a report for a Ukrainian merchant.
SELECT
  hour(from_iso8601_timestamp(createdDate) AT TIME ZONE 'Europe/Kyiv') AS hour_kyiv,
  count(*) AS payments
FROM lab8.payments
GROUP BY 1
ORDER BY 1;


-- Peak hour per merchant — the same question, but useful: a coffee shop and a
-- streaming service do not peak together.
SELECT merchantName, hour_utc, payments FROM (
  SELECT merchantName,
         hour(from_iso8601_timestamp(createdDate)) AS hour_utc,
         count(*) AS payments,
         row_number() OVER (PARTITION BY merchantName ORDER BY count(*) DESC) AS rn
  FROM lab8.payments
  GROUP BY merchantName, 2
) WHERE rn = 1
ORDER BY payments DESC;
