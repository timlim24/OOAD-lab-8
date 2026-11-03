#!/usr/bin/env bash
# =============================================================================
# Part 2 — provision everything on AWS: bucket, data copy, Athena workgroup,
#          Glue crawler. Idempotent: safe to re-run.
#
#   ./setup.sh            # bucket + copy + athena setup
#   ./setup.sh crawler    # additionally create and run the Glue crawler
#
# Prerequisites: `aws login` (or valid credentials) and region us-east-1.
# =============================================================================
set -euo pipefail

REGION="${REGION:-us-east-1}"
SRC="s3://ucu-ooad-lab8-payments/payments/"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${BUCKET:-ooad-lab8-payments-${ACCOUNT_ID}}"
DB="${DB:-lab8}"
CRAWLER_ROLE="${CRAWLER_ROLE:-AWSGlueServiceRole-ooad-lab8}"
CRAWLER_NAME="${CRAWLER_NAME:-ooad-lab8-payments-crawler}"

echo "account  : ${ACCOUNT_ID}"
echo "region   : ${REGION}"
echo "bucket   : s3://${BUCKET}"
echo

# -----------------------------------------------------------------------------
# 1. Bucket
# -----------------------------------------------------------------------------
if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "[1/4] bucket already exists"
else
  echo "[1/4] creating bucket"
  # us-east-1 is the one region where create-bucket must NOT carry a
  # LocationConstraint; every other region requires it.
  if [ "${REGION}" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
  else
    aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
      --create-bucket-configuration "LocationConstraint=${REGION}"
  fi
fi

# -----------------------------------------------------------------------------
# 2. Copy the dataset.
#    S3-to-S3 inside one region: the bytes never leave AWS and never touch this
#    laptop, so there is no data-transfer charge. Cross-region would cost
#    $0.02/GB — which is why the brief insists on us-east-1.
#    1.2 GB / ~3500 objects... actually 31 objects, so this is quick.
# -----------------------------------------------------------------------------
echo "[2/4] syncing dataset (idempotent, skips what is already there)"
aws s3 sync "${SRC}" "s3://${BUCKET}/payments/" \
    --source-region "${REGION}" --region "${REGION}" --no-progress
aws s3 ls "s3://${BUCKET}/payments/" --recursive --summarize | tail -3

# -----------------------------------------------------------------------------
# 3. Athena: query-result location + database
# -----------------------------------------------------------------------------
echo "[3/4] configuring Athena"
aws athena update-work-group --work-group primary --region "${REGION}" \
  --configuration-updates \
  "ResultConfigurationUpdates={OutputLocation=s3://${BUCKET}/athena-results/}" \
  >/dev/null 2>&1 || echo "      (could not update workgroup — set the result location in the console)"

aws athena start-query-execution --region "${REGION}" \
  --query-string "CREATE DATABASE IF NOT EXISTS ${DB}" \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query QueryExecutionId --output text

# -----------------------------------------------------------------------------
# 4. Render the SQL with the real bucket name
# -----------------------------------------------------------------------------
echo "[4/4] rendering sql/ -> sql.rendered/ with bucket ${BUCKET}"
mkdir -p sql.rendered
for f in sql/*.sql; do
  sed "s|<YOUR-BUCKET>|${BUCKET}|g" "$f" > "sql.rendered/$(basename "$f")"
done

echo
echo "Done. Run sql.rendered/*.sql in the Athena console in order."
echo "Result location: s3://${BUCKET}/athena-results/"

# -----------------------------------------------------------------------------
# Optional: Glue crawler (Way B)
# -----------------------------------------------------------------------------
if [ "${1:-}" = "crawler" ]; then
  echo
  echo "=== Glue crawler ==="

  if ! aws iam get-role --role-name "${CRAWLER_ROLE}" >/dev/null 2>&1; then
    echo "creating role ${CRAWLER_ROLE}"
    aws iam create-role --role-name "${CRAWLER_ROLE}" \
      --assume-role-policy-document '{
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow",
                      "Principal":{"Service":"glue.amazonaws.com"},
                      "Action":"sts:AssumeRole"}]}' >/dev/null
    aws iam attach-role-policy --role-name "${CRAWLER_ROLE}" \
      --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
    # The managed policy covers Glue itself; the crawler still needs to read
    # our specific bucket.
    aws iam put-role-policy --role-name "${CRAWLER_ROLE}" \
      --policy-name "read-${BUCKET}" \
      --policy-document "{
        \"Version\":\"2012-10-17\",
        \"Statement\":[{\"Effect\":\"Allow\",
                        \"Action\":[\"s3:GetObject\",\"s3:ListBucket\"],
                        \"Resource\":[\"arn:aws:s3:::${BUCKET}\",
                                      \"arn:aws:s3:::${BUCKET}/*\"]}]}"
    echo "waiting 15s for IAM propagation"
    sleep 15
  fi

  aws glue create-database --database-input "Name=${DB}_crawled" >/dev/null 2>&1 || true

  aws glue create-crawler \
    --name "${CRAWLER_NAME}" \
    --role "${CRAWLER_ROLE}" \
    --database-name "${DB}_crawled" \
    --targets "{\"S3Targets\":[{\"Path\":\"s3://${BUCKET}/payments/\"}]}" \
    --region "${REGION}" >/dev/null 2>&1 \
    || echo "crawler already exists"

  aws glue start-crawler --name "${CRAWLER_NAME}" --region "${REGION}"
  echo "crawler started; poll with:"
  echo "  aws glue get-crawler --name ${CRAWLER_NAME} --query 'Crawler.State' --output text"
  echo "then inspect the schema it inferred:"
  echo "  aws glue get-table --database-name ${DB}_crawled --name payments \\"
  echo "      --query 'Table.{cols:StorageDescriptor.Columns, parts:PartitionKeys}' --output table"
fi
