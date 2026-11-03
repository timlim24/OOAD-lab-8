# Що завантажувати

Рівно 5 файлів із цієї теки:

| # | Файл | Розмір | Що всередині |
|---|---|---:|---|
| 1 | `1-part1-2-batch.pdf` | 809 КБ | Частина 1 (три питання про архітектуру Uklon) + Частина 2 повністю: запити 2–6 з результатами, повна таблиця оборотів, бенчмарк JSON/Parquet, схема Glue Crawler |
| 2 | `2-part3-streaming-local.pdf` | 425 КБ | Частина 3: опис реалізації + вивід консюмера для всіх 4 експериментів |
| 3 | `3-part4-diagram.png` | 100 КБ | Діаграма хмарного пайплайна |
| 4 | `4-part4-cloud-streaming.pdf` | 450 КБ | Частина 4: порівняння з локальним Redpanda, запити Athena над стрімом, кошторис |
| 5 | `5-athena-results.zip` | 8,4 КБ | **24 експортовані CSV** — сирий вивід Athena для кожного запиту, плюс `INDEX.md` зі звіркою |

Короткий онлайн-текст — у [../SUBMISSION.md](../SUBMISSION.md), розділ
«Короткий текст із відповідями». Посилання на код:
`https://github.com/timlim24/OOAD-lab-8`

## Про CSV замість скриншотів

Завдання дозволяє «screenshots **or** exported CSV». У `5-athena-results.zip`
лежать саме **експортовані CSV** — це не перемальовані таблиці, а файли, які
Athena сама поклала в `s3://<bucket>/athena-results/{QueryExecutionId}.csv`
після виконання кожного запиту.

Відповідність `QueryExecutionId` ↔ запит, разом із `DataScannedInBytes` і
`EngineExecutionTimeInMillis` кожного запуску, — у `benchmark.csv` відповідної
частини репозиторію.

## Як це зібрано

PDF згенеровані з markdown-файлів репозиторію (`markdown` → HTML → Chrome
headless `--print-to-pdf`), тож вони завжди відповідають актуальному коду.
Перезібрати: `make -C .. submission` або повторити команди з історії git.
