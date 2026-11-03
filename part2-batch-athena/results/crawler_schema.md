# Шлях Б — схема, яку вивів Glue Crawler

```bash
./setup.sh crawler
aws glue get-crawler --name ooad-lab8-payments-crawler --query 'Crawler.State' --output text
aws glue get-table --database-name lab8_crawled --name payments \
    --query 'Table.{cols:StorageDescriptor.Columns, parts:PartitionKeys}' --output table
```

> **Вставити сюди фактичний вивід `get-table` (або скриншот із консолі Glue).**

## Порівняння схем

| Поле | Наша (`01_create_table.sql`) | Crawler | Збіг? |
|---|---|---|---|
| `invoiceid` | string | | |
| `status` | string | | |
| `failurereason` | string | | |
| `amount` | bigint | | |
| `finalamount` | bigint | | |
| `ccy` | int | | |
| `reference` | string | | |
| `createddate` | string | | |
| `modifieddate` | string | | |
| `receivedat` | string | | |
| `merchantid` | string | | |
| `merchantname` | string | | |
| `mcc` | int | | |
| `customerid` | string | | |
| `paymentmethod` | string | | |
| `device` | string | | |
| `dt` | **партиція** string | | |

SerDe, який обрав crawler: `__________`
(очікувано `org.openx.data.jsonserde.JsonSerDe` або `org.apache.hive.hcatalog.data.JsonSerDe`)

Кількість партицій, які він зареєстрував: `____` (очікувано 31)

## Відповіді на три питання — у [../README.md](../README.md#шлях-б--glue-crawler)
