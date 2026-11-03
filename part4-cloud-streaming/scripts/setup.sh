#!/usr/bin/env bash
# =============================================================================
# Part 4 — build the cloud streaming pipeline.
#
#   Kinesis Data Stream ─┬─> Firehose ──> S3 (dt=…) ──> Glue/Athena
#                        └─> Lambda ──> DynamoDB counters ──> SNS alert
#
#   ./setup.sh you@example.com      # the address the SNS alert goes to
#   ./setup.sh                      # no subscription: alerts land in the
#                                   # Lambda log and the SNS NumberOfMessages
#                                   # Published metric, nowhere else
#
# Idempotent: re-running skips what already exists.
# REMEMBER TO RUN ./teardown.sh — a shard costs $0.015/hour whether you use it
# or not.
# =============================================================================
set -euo pipefail

EMAIL="${1:-}"
REGION="${REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

STREAM="${STREAM:-ooad-lab8-payments}"
BUCKET="${BUCKET:-ooad-lab8-payments-${ACCOUNT_ID}}"
PREFIX="stream"
FIREHOSE="${FIREHOSE:-ooad-lab8-to-s3}"
TOPIC="${TOPIC:-ooad-lab8-alerts}"
TABLE="${TABLE:-ooad-lab8-windows}"
FN="${FN:-ooad-lab8-alerting}"
LAMBDA_ROLE="ooad-lab8-lambda-role"
FIREHOSE_ROLE="ooad-lab8-firehose-role"
DB="${DB:-lab8}"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

echo "account : ${ACCOUNT_ID}"
echo "region  : ${REGION}"
echo "bucket  : s3://${BUCKET}/${PREFIX}/"
echo "alerts  : ${EMAIL:-<no subscription — Lambda log only>}"

# -----------------------------------------------------------------------------
say "1/8  S3 bucket"
# -----------------------------------------------------------------------------
if ! aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  if [ "${REGION}" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
  else
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
      --create-bucket-configuration "LocationConstraint=${REGION}"
  fi
else
  echo "exists"
fi

# -----------------------------------------------------------------------------
say "2/8  Kinesis data stream (1 shard)"
# -----------------------------------------------------------------------------
# One shard takes 1 MiB/s or 1000 records/s of writes. We send ~60 records/s,
# so one shard is ~6% utilised. PROVISIONED, not ON_DEMAND: on-demand has no
# shard-hour charge but a much higher per-GB price and a 4x higher minimum, and
# for a lab that runs for an hour the shard hour ($0.015) is cheaper.
if ! aws kinesis describe-stream-summary --stream-name "${STREAM}" \
        --region "${REGION}" >/dev/null 2>&1; then
  aws kinesis create-stream --stream-name "${STREAM}" --shard-count 1 \
      --region "${REGION}"
  echo "waiting for ACTIVE ..."
  aws kinesis wait stream-exists --stream-name "${STREAM}" --region "${REGION}"
else
  echo "exists"
fi
STREAM_ARN="$(aws kinesis describe-stream-summary --stream-name "${STREAM}" \
    --region "${REGION}" --query 'StreamDescriptionSummary.StreamARN' --output text)"
echo "${STREAM_ARN}"

# -----------------------------------------------------------------------------
say "3/8  SNS topic + email subscription"
# -----------------------------------------------------------------------------
TOPIC_ARN="$(aws sns create-topic --name "${TOPIC}" --region "${REGION}" \
    --query TopicArn --output text)"
echo "${TOPIC_ARN}"
if [ -n "${EMAIL}" ]; then
  aws sns subscribe --topic-arn "${TOPIC_ARN}" --protocol email \
      --notification-endpoint "${EMAIL}" --region "${REGION}" >/dev/null
  echo "CONFIRM THE SUBSCRIPTION IN YOUR MAILBOX — an unconfirmed subscription"
  echo "silently receives nothing, and you will think the alert never fired."
else
  echo "no subscription: sns:Publish still succeeds and is still billed, the"
  echo "message simply has nowhere to go. Evidence that an alert fired is then"
  echo "the Lambda log line and the SNS NumberOfMessagesPublished metric:"
  echo "  aws logs tail /aws/lambda/${FN} --follow --region ${REGION}"
fi

# -----------------------------------------------------------------------------
say "4/8  DynamoDB table for the window counters"
# -----------------------------------------------------------------------------
if ! aws dynamodb describe-table --table-name "${TABLE}" --region "${REGION}" >/dev/null 2>&1; then
  aws dynamodb create-table \
      --table-name "${TABLE}" \
      --attribute-definitions AttributeName=pk,AttributeType=S \
      --key-schema AttributeName=pk,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      --region "${REGION}" >/dev/null
  aws dynamodb wait table-exists --table-name "${TABLE}" --region "${REGION}"
  # TTL so the counters clean themselves up; otherwise the table keeps every
  # minute of every day forever.
  aws dynamodb update-time-to-live --table-name "${TABLE}" \
      --time-to-live-specification "Enabled=true,AttributeName=expires_at" \
      --region "${REGION}" >/dev/null
else
  echo "exists"
fi

# -----------------------------------------------------------------------------
say "5/8  IAM role for Lambda"
# -----------------------------------------------------------------------------
if ! aws iam get-role --role-name "${LAMBDA_ROLE}" >/dev/null 2>&1; then
  aws iam create-role --role-name "${LAMBDA_ROLE}" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{
      "Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},
      "Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name "${LAMBDA_ROLE}" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  aws iam put-role-policy --role-name "${LAMBDA_ROLE}" --policy-name inline \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
      {\"Effect\":\"Allow\",\"Action\":[\"kinesis:DescribeStream\",
        \"kinesis:DescribeStreamSummary\",\"kinesis:GetRecords\",
        \"kinesis:GetShardIterator\",\"kinesis:ListShards\",
        \"kinesis:ListStreams\",\"kinesis:SubscribeToShard\"],
       \"Resource\":\"${STREAM_ARN}\"},
      {\"Effect\":\"Allow\",\"Action\":[\"dynamodb:UpdateItem\",\"dynamodb:GetItem\"],
       \"Resource\":\"arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${TABLE}\"},
      {\"Effect\":\"Allow\",\"Action\":\"sns:Publish\",\"Resource\":\"${TOPIC_ARN}\"}]}"
  echo "waiting 15s for IAM to propagate"
  sleep 15
else
  echo "exists"
fi
LAMBDA_ROLE_ARN="$(aws iam get-role --role-name "${LAMBDA_ROLE}" --query Role.Arn --output text)"

# -----------------------------------------------------------------------------
say "6/8  Lambda function + event source mapping"
# -----------------------------------------------------------------------------
TMP="$(mktemp -d)"
cp "$(dirname "$0")/../lambda/alert.py" "${TMP}/alert.py"
(cd "${TMP}" && zip -q alert.zip alert.py)

if aws lambda get-function --function-name "${FN}" --region "${REGION}" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "${FN}" \
      --zip-file "fileb://${TMP}/alert.zip" --region "${REGION}" >/dev/null
  aws lambda wait function-updated --function-name "${FN}" --region "${REGION}"
  aws lambda update-function-configuration --function-name "${FN}" \
      --environment "Variables={WINDOW_TABLE=${TABLE},TOPIC_ARN=${TOPIC_ARN},THRESHOLD=0.10,MIN_EVENTS=20,WINDOW_S=60}" \
      --region "${REGION}" >/dev/null
else
  aws lambda create-function --function-name "${FN}" \
      --runtime python3.13 --handler alert.handler \
      --role "${LAMBDA_ROLE_ARN}" \
      --zip-file "fileb://${TMP}/alert.zip" \
      --timeout 60 --memory-size 256 \
      --environment "Variables={WINDOW_TABLE=${TABLE},TOPIC_ARN=${TOPIC_ARN},THRESHOLD=0.10,MIN_EVENTS=20,WINDOW_S=60}" \
      --region "${REGION}" >/dev/null
  aws lambda wait function-active --function-name "${FN}" --region "${REGION}"
fi
rm -rf "${TMP}"

if ! aws lambda list-event-source-mappings --function-name "${FN}" \
        --region "${REGION}" --query 'EventSourceMappings[0].UUID' \
        --output text 2>/dev/null | grep -qv '^None$'; then
  # BatchSize 500 / MaxBatchingWindow 10s: wait up to 10 seconds to fill a
  # batch instead of invoking on every arriving record. Fewer invocations,
  # fewer DynamoDB round trips, 10 more seconds of alerting latency.
  aws lambda create-event-source-mapping \
      --function-name "${FN}" \
      --event-source-arn "${STREAM_ARN}" \
      --starting-position LATEST \
      --batch-size 500 \
      --maximum-batching-window-in-seconds 10 \
      --region "${REGION}" >/dev/null
else
  echo "mapping exists"
fi

# -----------------------------------------------------------------------------
say "7/8  IAM role for Firehose"
# -----------------------------------------------------------------------------
if ! aws iam get-role --role-name "${FIREHOSE_ROLE}" >/dev/null 2>&1; then
  aws iam create-role --role-name "${FIREHOSE_ROLE}" \
    --assume-role-policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{
      \"Effect\":\"Allow\",\"Principal\":{\"Service\":\"firehose.amazonaws.com\"},
      \"Action\":\"sts:AssumeRole\",
      \"Condition\":{\"StringEquals\":{\"sts:ExternalId\":\"${ACCOUNT_ID}\"}}}]}" >/dev/null
  aws iam put-role-policy --role-name "${FIREHOSE_ROLE}" --policy-name inline \
    --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
      {\"Effect\":\"Allow\",\"Action\":[\"s3:AbortMultipartUpload\",
        \"s3:GetBucketLocation\",\"s3:GetObject\",\"s3:ListBucket\",
        \"s3:ListBucketMultipartUploads\",\"s3:PutObject\"],
       \"Resource\":[\"arn:aws:s3:::${BUCKET}\",\"arn:aws:s3:::${BUCKET}/*\"]},
      {\"Effect\":\"Allow\",\"Action\":[\"kinesis:DescribeStream\",
        \"kinesis:GetShardIterator\",\"kinesis:GetRecords\",
        \"kinesis:ListShards\"],\"Resource\":\"${STREAM_ARN}\"},
      {\"Effect\":\"Allow\",\"Action\":[\"logs:PutLogEvents\",
        \"logs:CreateLogStream\",\"logs:CreateLogGroup\"],\"Resource\":\"*\"}]}"
  echo "waiting 15s for IAM to propagate"
  sleep 15
else
  echo "exists"
fi
FIREHOSE_ROLE_ARN="$(aws iam get-role --role-name "${FIREHOSE_ROLE}" --query Role.Arn --output text)"

# -----------------------------------------------------------------------------
say "8/8  Firehose delivery stream -> S3"
# -----------------------------------------------------------------------------
# The prefix uses !{timestamp:...}, which Firehose fills from the record's
# APPROXIMATE ARRIVAL TIME in the stream — that is PROCESSING time.
#
# This is the single most important difference from part 2, where dt was the
# EVENT day. Here a payment that happened at 23:59:58 and arrived at 00:00:03
# lands in TOMORROW's partition. A daily report built on this `dt` is therefore
# never exactly a day of business. Fixing it means either dynamic partitioning
# with a JQ expression over modifiedDate (Firehose can do it, +$0.02/GB) or a
# nightly Glue job that repartitions by event time — which is precisely the
# batch road from part 1 existing to clean up after the streaming road.
if ! aws firehose describe-delivery-stream --delivery-stream-name "${FIREHOSE}" \
        --region "${REGION}" >/dev/null 2>&1; then
  aws firehose create-delivery-stream \
    --delivery-stream-name "${FIREHOSE}" \
    --delivery-stream-type KinesisStreamAsSource \
    --kinesis-stream-source-configuration \
      "KinesisStreamARN=${STREAM_ARN},RoleARN=${FIREHOSE_ROLE_ARN}" \
    --extended-s3-destination-configuration "{
      \"RoleARN\":\"${FIREHOSE_ROLE_ARN}\",
      \"BucketARN\":\"arn:aws:s3:::${BUCKET}\",
      \"Prefix\":\"${PREFIX}/dt=!{timestamp:yyyy-MM-dd}/\",
      \"ErrorOutputPrefix\":\"${PREFIX}-errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/\",
      \"BufferingHints\":{\"SizeInMBs\":1,\"IntervalInSeconds\":60},
      \"CompressionFormat\":\"UNCOMPRESSED\",
      \"CloudWatchLoggingOptions\":{\"Enabled\":true,
        \"LogGroupName\":\"/aws/kinesisfirehose/${FIREHOSE}\",
        \"LogStreamName\":\"S3Delivery\"}}" \
    --region "${REGION}" >/dev/null
  echo "waiting for ACTIVE ..."
  for _ in $(seq 1 30); do
    st="$(aws firehose describe-delivery-stream --delivery-stream-name "${FIREHOSE}" \
          --region "${REGION}" --query 'DeliveryStreamDescription.DeliveryStreamStatus' \
          --output text)"
    [ "$st" = "ACTIVE" ] && break
    sleep 5
  done
else
  echo "exists"
fi

cat <<SUMMARY

=============================================================================
Pipeline is up.

  stream   : ${STREAM}
  firehose : ${FIREHOSE}  ->  s3://${BUCKET}/${PREFIX}/dt=YYYY-MM-DD/
  lambda   : ${FN}
  table    : ${TABLE}
  topic    : ${TOPIC_ARN}

Next:
  1. Confirm the SNS subscription in your mailbox.
  2. Replay the data:
       python ../producer_kinesis.py --file ../../part3-streaming-local/data/events-2026-10-01.jsonl \\
              --stream ${STREAM} --rate 100
  3. Wait ~2 minutes for the first Firehose buffer to flush, then:
       aws s3 ls s3://${BUCKET}/${PREFIX}/ --recursive | head
  4. Create the Athena table:
       sed "s|<YOUR-BUCKET>|${BUCKET}|" ../sql/stream_table.sql
  5. Watch the Lambda:
       aws logs tail /aws/lambda/${FN} --follow --region ${REGION}

WHEN YOU ARE DONE:  ./teardown.sh
A shard costs \$0.015/hour whether or not anything is flowing through it.
=============================================================================
SUMMARY
