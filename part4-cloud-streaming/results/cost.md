# Фактична вартість прогону

> Cost Explorer оновлюється із затримкою до доби. Звіряти наступного дня:
> `aws ce get-cost-and-usage --time-period Start=<день>,End=<день+1> \
>    --granularity DAILY --metrics UnblendedCost --group-by Type=DIMENSION,Key=SERVICE`

## Оцінка (прайс-лист us-east-1, 1 година, 101 084 записи)

| Сервіс | Оцінка |
|---|---:|
| Kinesis shard-hour | $0,0150 |
| Kinesis PUT units | $0,0014 |
| Firehose (0,505 ГБ після округлення до 5 КБ) | $0,0147 |
| S3 | $0,0011 |
| Lambda | $0,0008 |
| DynamoDB | $0,0054 |
| Athena | $0,0003 |
| CloudWatch Logs | $0,0025 |
| **Разом** | **≈ $0,041** |

## Факт

| Сервіс | $ |
|---|---:|
| Amazon Kinesis | |
| Amazon Data Firehose | |
| Amazon S3 | |
| AWS Lambda | |
| Amazon DynamoDB | |
| Amazon Athena | |
| AmazonCloudWatch | |
| **Разом** | |

## Чи все видалено

```bash
aws kinesis list-streams --region us-east-1
aws firehose list-delivery-streams --region us-east-1
aws lambda list-functions --query 'Functions[?starts_with(FunctionName,`ooad-lab8`)].FunctionName'
aws dynamodb list-tables
```

```
<вставити вивід — має бути порожньо>
```
