#!/usr/bin/env bash
# =============================================================================
# Part 4 — delete everything setup.sh created.
#
#   ./teardown.sh          # keeps the S3 data so Athena still works
#   ./teardown.sh --all    # also deletes the S3 prefix
#
# The expensive things are the Kinesis shard ($0.015/h = $11/month) and, to a
# much smaller degree, the DynamoDB table. Firehose and SNS cost nothing when
# idle. Run this the moment you have your screenshots.
# =============================================================================
set -uo pipefail     # not -e: a missing resource must not stop the cleanup

REGION="${REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

STREAM="${STREAM:-ooad-lab8-payments}"
BUCKET="${BUCKET:-ooad-lab8-payments-${ACCOUNT_ID}}"
FIREHOSE="${FIREHOSE:-ooad-lab8-to-s3}"
TOPIC="${TOPIC:-ooad-lab8-alerts}"
TABLE="${TABLE:-ooad-lab8-windows}"
FN="${FN:-ooad-lab8-alerting}"
LAMBDA_ROLE="ooad-lab8-lambda-role"
FIREHOSE_ROLE="ooad-lab8-firehose-role"

step() { printf '\n\033[1m-- %s\033[0m\n' "$*"; }

step "event source mappings"
for uuid in $(aws lambda list-event-source-mappings --function-name "${FN}" \
                --region "${REGION}" --query 'EventSourceMappings[].UUID' \
                --output text 2>/dev/null); do
  [ "${uuid}" = "None" ] && continue
  echo "deleting mapping ${uuid}"
  aws lambda delete-event-source-mapping --uuid "${uuid}" --region "${REGION}" >/dev/null
done

step "lambda function"
aws lambda delete-function --function-name "${FN}" --region "${REGION}" 2>/dev/null \
  && echo deleted || echo "not found"

step "firehose delivery stream"
aws firehose delete-delivery-stream --delivery-stream-name "${FIREHOSE}" \
    --region "${REGION}" 2>/dev/null && echo deleted || echo "not found"

step "kinesis data stream  <-- this is the one that costs money"
aws kinesis delete-stream --stream-name "${STREAM}" --enforce-consumer-deletion \
    --region "${REGION}" 2>/dev/null && echo deleted || echo "not found"

step "dynamodb table"
aws dynamodb delete-table --table-name "${TABLE}" --region "${REGION}" >/dev/null 2>&1 \
  && echo deleted || echo "not found"

step "sns topic"
TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT_ID}:${TOPIC}"
aws sns delete-topic --topic-arn "${TOPIC_ARN}" --region "${REGION}" 2>/dev/null \
  && echo deleted || echo "not found"

step "iam roles"
for role in "${LAMBDA_ROLE}" "${FIREHOSE_ROLE}"; do
  # Inline policies and attached managed policies must go before the role.
  for p in $(aws iam list-role-policies --role-name "${role}" \
               --query 'PolicyNames[]' --output text 2>/dev/null); do
    aws iam delete-role-policy --role-name "${role}" --policy-name "${p}"
  done
  for arn in $(aws iam list-attached-role-policies --role-name "${role}" \
                 --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
    aws iam detach-role-policy --role-name "${role}" --policy-arn "${arn}"
  done
  aws iam delete-role --role-name "${role}" 2>/dev/null \
    && echo "deleted ${role}" || echo "${role} not found"
done

step "cloudwatch log groups"
for lg in "/aws/lambda/${FN}" "/aws/kinesisfirehose/${FIREHOSE}"; do
  aws logs delete-log-group --log-group-name "${lg}" --region "${REGION}" 2>/dev/null \
    && echo "deleted ${lg}" || true
done

if [ "${1:-}" = "--all" ]; then
  step "s3 data"
  aws s3 rm "s3://${BUCKET}/stream/" --recursive
  aws s3 rm "s3://${BUCKET}/stream-errors/" --recursive
else
  echo
  echo "S3 data kept at s3://${BUCKET}/stream/ so Athena still works."
  echo "Pass --all to delete it too."
fi

step "verify nothing is left running"
echo -n "kinesis streams : "; aws kinesis list-streams --region "${REGION}" \
  --query 'StreamNames' --output text 2>/dev/null || echo "-"
echo -n "firehose        : "; aws firehose list-delivery-streams --region "${REGION}" \
  --query 'DeliveryStreamNames' --output text 2>/dev/null || echo "-"
echo
echo "Done."
