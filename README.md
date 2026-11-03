# OOAD Лабораторна 8 — дві дороги даних: batch і streaming

Платіжні вебхуки (≈3 млн подій за жовтень 2026) проходять двома шляхами:
**батчем** через Athena і Glue на AWS, і **стрімом** — локально через Redpanda
та у хмарі через Kinesis.

| Частина | Що там | Статус |
|---|---|---|
| [Частина 1](part1-architecture/README.md) | архітектура даних Uklon: три питання | ✅ готово |
| [Частина 2](part2-batch-athena/README.md) | Athena + Glue Crawler над S3 | ✅ SQL і відповіді; потрібні скриншоти з Athena |
| [Частина 3](part3-streaming-local/README.md) | Redpanda + Python, 4 експерименти | ✅ код і очікувані числа; потрібен запуск у Docker |
| [Частина 4](part4-cloud-streaming/README.md) | Kinesis → Firehose → S3 → Athena + SNS | ✅ код, діаграма, кошторис; потрібен запуск на AWS |

## Ключові цифри (пораховані по всьому датасету)

| | |
|---|---:|
| Подій | 3 030 197 |
| Унікальних `invoiceId` | 1 000 000 |
| Точних дублікатів | 30 197 (0,997 %) |
| Запізнилися > 60 с | 60 824 (2,007 %) |
| Запізнилися > 1 год | **0** |
| Максимальне запізнення | 600 с |
| Частка відмов (після дедуплікації) | 6,02 % |
| Частка відмов (без дедуплікації) | 2,0 % ← помилка в 3× |
| JSON на S3 | 1 260 672 849 Б |
| Parquet | 79 080 980 Б (**15,9× менше**) |
| Ціна запиту: JSON → Parquet | $0,0063 → $0,00005 (**126×**) |

## Швидкий старт

```bash
# Частина 2 — AWS
aws login
cd part2-batch-athena && ./setup.sh && ./setup.sh crawler

# Частина 3 — локально
sudo usermod -aG docker $USER && newgrp docker      # один раз
cd part3-streaming-local
make venv data up topic
make produce          # термінал 1
make consume          # термінал 2

# Частина 3 без Docker — та сама логіка вікон, лише перевірка
make simulate ARGS="--grace 0"

# Частина 4 — хмара
cd part4-cloud-streaming/scripts
./setup.sh you@example.com
# ... потім ОБОВʼЯЗКОВО
./teardown.sh
```

## Що здавати

Див. [SUBMISSION.md](SUBMISSION.md) — короткий текст із відповідями і список
файлів.

## Структура

```
part1-architecture/      відповіді на три питання про архітектуру Uklon
part2-batch-athena/
  sql/                   00..09 — DDL, вʼю, запити, CTAS
  results/               таблиці з відповідями + шаблони для скриншотів
  setup.sh               бакет, копія даних, Athena, Glue Crawler
part3-streaming-local/
  docker-compose.yml     Redpanda + Console
  producer.py            реплей дня в топік
  consumer.py            дедуп + віконна частка відмов + алерти
  simulate.py            та сама логіка без брокера
  results/               по файлу на експеримент
part4-cloud-streaming/
  diagram.svg            діаграма пайплайна
  producer_kinesis.py    PutRecords з обробкою часткових відмов
  lambda/alert.py        віконний алерт через SNS
  scripts/setup.sh       7 ресурсів + 2 IAM-ролі
  scripts/teardown.sh    видалити все
  sql/stream_table.sql   Athena над даними зі стріму
```

## Чому все перевірено локально

Усі числа в цих README пораховані на повному датасеті через DuckDB
(частина 2) і прогоном класу `WindowedFailureRate` із
[consumer.py](part3-streaming-local/consumer.py) напряму на файлі
(частина 3, [simulate.py](part3-streaming-local/simulate.py)). Це дає еталон,
з яким можна звірити вивід Athena й консюмера — щоб відрізнити справжню
розбіжність від власної помилки.
