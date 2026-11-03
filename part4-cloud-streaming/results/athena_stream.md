# Athena над даними, що прийшли через стрім

Пайплайн: `producer_kinesis.py` → Kinesis (1 шард) → Firehose → S3 → Glue → Athena.
Реплей: `--rate 150 --fail-rate 0.30`, день `dt=2026-10-01`.

Продюсер:
```
sent      : 101,084 records for 33,361 payments
throttled : 0 (retried)
forced to failure: 9,576 payments
wall time : 222.7s (149.8 payments/s)
```

---

## Запит 1 — чи все доїхало

```sql
SELECT count(*) AS events, count(DISTINCT invoiceId) AS payments,
       count(*) - (SELECT count(*) FROM (SELECT DISTINCT * FROM lab8.stream_events))
         AS duplicate_rows,
       min(modifiedDate) AS first_event, max(modifiedDate) AS last_event
FROM lab8.stream_events;
```

| events | payments | duplicate_rows | first_event | last_event |
|---:|---:|---:|---|---|
| **101 084** | **33 361** | **1 001** | 2026-10-01T00:00:01Z | 2026-10-01T23:59:56Z |

**Облік зійшовся повністю.** Продюсер надіслав 101 084 записи — Athena бачить
101 084. Firehose доставив рівно стільки, скільки Kinesis прийняв
(`DeliveryToS3.Records` = `IncomingRecords`). Нуль втрат на всьому шляху
ноутбук → Kinesis → Firehose → S3.

**1 001 дублікат** — рівно стільки, скільки їх фізично у вихідному файлі
(`count(*) - count(DISTINCT *)` по `day01.jsonl`). Це наскрізний доказ, що
виправлення детермінованої причини відмови працює: через чотири сервіси
структура дублікатів датасету збереглася байт-у-байт.

> Для скриншота: Athena console → Query results. `QueryExecutionId` кожного
> запуску — в [benchmark.csv](benchmark.csv).

---

## Запит 2 — частка відмов, звірка з частиною 3

```sql
WITH dedup AS (
  SELECT DISTINCT invoiceId, status, modifiedDate, paymentMethod
  FROM lab8.stream_events
)
SELECT paymentMethod,
       count_if(status IN ('success','failure')) AS terminal,
       count_if(status = 'failure')              AS failures,
       round(100.0 * count_if(status='failure')
           / nullif(count_if(status IN ('success','failure')),0), 2) AS failure_rate_pct
FROM dedup GROUP BY paymentMethod ORDER BY failure_rate_pct DESC;
```

| paymentMethod | terminal | failures | % (Athena, стрім) | terminal (ч. 3, локально) |
|---|---:|---:|---:|---:|
| apple_pay | 8 412 | 2 896 | 34,43 | 8 412 |
| qr | 700 | 241 | 34,43 | 700 |
| card | 18 406 | 6 336 | 34,42 | 18 406 |
| google_pay | 5 843 | 1 991 | 34,07 | 5 843 |

**Кількість термінальних подій збігається з локальним підрахунком по тому
самому дню до одиниці.** Батч над даними зі стріму і стрім-консюмер з
частини 3 рахують одне й те саме.

Частка 34,1–34,4 % — рівно те, що й має бути: 30 % примусових відмов плюс
природні 6,02 % на решті платежів, тобто `0,30 + 0,70 × 0,0602 = 34,2 %`.

---

## Запит 3 — партиція це **processing time**, а не event time

```sql
SELECT dt AS partition_dt,
       min(substr(modifiedDate,1,10)) AS earliest_event_day,
       max(substr(modifiedDate,1,10)) AS latest_event_day,
       count(*) AS events
FROM lab8.stream_events GROUP BY dt ORDER BY dt;
```

| partition_dt | earliest_event_day | latest_event_day | events |
|---|---|---|---:|
| **2026-11-03** | 2026-10-01 | 2026-10-01 | 101 084 |

**Одна партиція, названа днем запуску реплею, містить цілий інший день
бізнесу.** У [частині 2](../../part2-batch-athena/README.md) ті самі події
лежали у 31 партиції, названих днями, коли вони сталися (перевірено: 0 подій,
де день `modifiedDate` ≠ `dt`).

Причина — у префіксі Firehose: `!{timestamp:yyyy-MM-dd}` підставляє
**approximate arrival time** запису в стрім. Нічого не губиться, але
`WHERE dt = '2026-10-01'` тут поверне нуль рядків, а в частині 2 — правильну
відповідь. Денний звіт по такому `dt` ніколи не дорівнює добі бізнесу.

Полагодити можна двома способами, і обидва — частина 1 у мініатюрі:
dynamic partitioning Firehose із JQ-виразом по `modifiedDate` (+$0,02/ГБ),
або нічний Glue job, що перепартиціонує за часом події — тобто batch-дорога
існує, щоб прибирати за streaming-дорогою.

---

## Файли, які написав Firehose

```bash
aws s3 ls s3://<bucket>/stream/ --recursive --summarize
```

| | |
|---|---:|
| файлів | **41** |
| усього | 42 496 073 Б |
| середній розмір | **1 036 490 Б** |

Середній файл — рівно ~1 МБ, тобто спрацював `BufferingHints.SizeInMBs = 1`,
а не 60-секундний інтервал: при 450 подіях/с по 433 байти мегабайт
набирається за ~5 секунд. Останній, неповний буфер дочекався інтервалу — у
Athena він зʼявився на кілька хвилин пізніше за решту, і доти запит показував
99 785 подій замість 101 084. **Це і є «eventual» у eventual consistency
стріму: дані не загублені, вони ще в буфері.**

41 файл на 100 тисяч подій — це мініатюра **small files problem**. За добу
реального навантаження таких файлів були б тисячі, і Athena витрачала б
більше часу на відкриття файлів, ніж на читання даних. Лікується тим самим
нічним CTAS у Parquet із [частини 2](../../part2-batch-athena/sql/07_ctas_parquet.sql).

---

## Алерт через SNS

Lambda рахує хвилинні вікна в DynamoDB і публікує в SNS при частці відмов
понад 10 %. Приклад із CloudWatch Logs:

```json
{"records": 500, "terminal": 170, "skipped": 0, "cells": 26,
 "alerts": [{"window": 1790847960, "method": "card", "rate": 0.1739, "n": 23},
            {"window": 1790848140, "method": "card", "rate": 0.2,    "n": 20}]}
```

Підтвердження, що алерти реально вийшли назовні (підписки на топік немає,
тож доказ — метрика):

```
AWS/SNS NumberOfMessagesPublished = 1 271
```

> Лічильники вікон у DynamoDB накопичувалися між кількома реплеями, тому
> загальна кількість алертів за годину не є чистим виміром одного прогону.
> Чистий вимір частоти алертів — у [частині 3](../../part3-streaming-local/results/exp3_failures.md):
> 55 на базовому дні проти 326 при 30 % відмов.
