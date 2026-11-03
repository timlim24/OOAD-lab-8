# Скриншот запиту Athena над даними зі стріму

## Запит 1 — чи взагалі щось доїхало

```sql
SELECT count(*) AS events, count(DISTINCT invoiceId) AS payments,
       min(modifiedDate) AS first_event, max(modifiedDate) AS last_event
FROM lab8.stream_events;
```

| events | payments | first_event | last_event |
|---:|---:|---|---|
| | | | |

> **Скриншот консолі Athena сюди.**

## Запит 2 — частка відмов, звірка з частиною 3

| paymentMethod | terminal | failures | % (Athena) | % (консюмер, ч.3) |
|---|---:|---:|---:|---:|
| google_pay | | | | 6,14 |
| apple_pay | | | | 5,94 |
| card | | | | 5,93 |
| qr | | | | 5,57 |

Якщо розбіжність — яка з трьох причин (продюсер зупинено рано / буфер Firehose
не флашнувся / `DROPPED` записи)?

## Запит 3 — партиція це processing time

| dt | earliest_event_day | latest_event_day | events |
|---|---|---|---:|
| | | | |

## Файли, які написав Firehose

```bash
aws s3 ls s3://<bucket>/stream/ --recursive --summarize
```

```
<вставити>
```

Файлів: `____`, середній розмір: `____`.
