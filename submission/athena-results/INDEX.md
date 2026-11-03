# Експортовані результати запитів Athena

Кожен файл — `.csv`, який Athena сама поклала в
`s3://<bucket>/athena-results/{QueryExecutionId}.csv` після виконання запиту.
Це не перемальовані таблиці, а сирий вивід сервісу.

Відповідність `QueryExecutionId` ↔ запит і статистика кожного запуску
(`DataScannedInBytes`, `EngineExecutionTimeInMillis`) — у `benchmark.csv`
відповідної частини репозиторію.

## Частина 2 — батч

| файл | запит |
|---|---|
| `count_raw_events.csv` | COUNT(*) по сирій таблиці — 3 030 197 |
| `partition_count.csv` | кількість партицій — 31 |
| `q2_dedup_verify.csv` | вʼю `payments`: 1 000 000 / 1 000 000 |
| `q2_stage_counts.csv` | що прибрав кожен крок дедуплікації |
| `q3_turnover_json.csv` | **запит 3** — оборот по мерчантах і валютах |
| `q4_failure_json.csv` | **запит 4** — частка відмов по методу й пристрою |
| `q5_hours_json.csv` | **запит 5** — платежі по годинах доби |
| `q6_late_events.csv` | **запит 6** — запізнілі події |
| `q9_scale.csv` | масштаб: унікальні invoiceId, дублікати, частка >1 год |
| `partition_is_event_time.csv` | 0 і 88 — доказ, що `dt` це день події |
| `amount_range.csv` | max(amount) і запас до int32 (268×) |
| `crawled_count.csv` | COUNT(*) по таблиці від Glue Crawler |

## Частина 2 — Parquet, ті самі запити

| файл | запит |
|---|---|
| `parquet_rowcount.csv` | 1 000 000 рядків, 31 партиція |
| `q3_turnover_parquet.csv` | запит 3 по Parquet |
| `q4_failure_parquet.csv` | запит 4 по Parquet |
| `q5_hours_parquet.csv` | запит 5 по Parquet |
| `status_only_json.csv` / `status_only_parquet.csv` | column pruning |
| `partition_prune_json.csv` / `partition_prune_parquet.csv` | partition pruning |
| `no_partition_prune_parquet.csv` | фільтр не по партиції → full scan |

## Частина 4 — дані, що прийшли через Kinesis

| файл | запит |
|---|---|
| `p4_final_verified.csv` | 101 084 події, 33 361 платіж, 1 001 дублікат |
| `p4_partition_proof.csv` | одна партиція `dt=2026-11-03` з подіями 2026-10-01 |
| `p4_failure_by_method.csv` | частка відмов по методу — 34,07–34,43 % |

> Проміжні прогони Частини 4 (до виправлення детермінованої причини відмови
> і до очищення S3 від тестових записів) сюди не включені, щоб у наборі не
> було двох суперечливих відповідей на те саме питання.
