#!/usr/bin/env bash
# =============================================================================
# Run one Athena statement and print its result together with the two numbers
# the write-up needs: Data scanned and Run time.
#
#   ./run_query.sh "SELECT count(*) FROM lab8.payment_events"
#   ./run_query.sh -f sql.rendered/03_turnover.sql      # first statement in file
#   ./run_query.sh -l label "SELECT ..."                # tag it in benchmark.csv
#
# Appends a row to results/benchmark.csv so the comparison table builds itself.
# =============================================================================
set -euo pipefail

REGION="${REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${BUCKET:-ooad-lab8-payments-${ACCOUNT_ID}}"
OUT="s3://${BUCKET}/athena-results/"
LABEL=""
ROWS="${ROWS:-40}"

while getopts "f:l:" opt; do
  case "$opt" in
    f) QUERY="$(cat "$OPTARG")" ;;
    l) LABEL="$OPTARG" ;;
    *) exit 1 ;;
  esac
done
shift $((OPTIND - 1))
QUERY="${QUERY:-${1:-}}"

if [ -z "${QUERY}" ]; then
  echo "usage: $0 [-l label] 'SQL'   |   $0 -f file.sql" >&2
  exit 1
fi

QID="$(aws athena start-query-execution \
        --query-string "${QUERY}" \
        --result-configuration "OutputLocation=${OUT}" \
        --region "${REGION}" \
        --query QueryExecutionId --output text)"

while true; do
  read -r STATE REASON <<<"$(aws athena get-query-execution --query-execution-id "${QID}" \
      --region "${REGION}" \
      --query 'QueryExecution.Status.[State,StateChangeReason]' --output text)"
  case "${STATE}" in
    SUCCEEDED) break ;;
    FAILED|CANCELLED)
      echo "query ${STATE}: ${REASON}" >&2
      echo "  ${QUERY}" | head -5 >&2
      exit 1 ;;
  esac
  sleep 1
done

read -r SCANNED MILLIS TOTALMS <<<"$(aws athena get-query-execution \
    --query-execution-id "${QID}" --region "${REGION}" \
    --query 'QueryExecution.Statistics.[DataScannedInBytes,EngineExecutionTimeInMillis,TotalExecutionTimeInMillis]' \
    --output text)"

printf '\n\033[1m%s\033[0m\n' "${LABEL:-query} — scanned $(numfmt --to=iec-i --suffix=B "${SCANNED}" 2>/dev/null || echo "${SCANNED}B"), engine ${MILLIS}ms, total ${TOTALMS}ms"

TMP="$(mktemp)"
aws s3 cp "${OUT}${QID}.csv" "${TMP}" --region "${REGION}" --quiet 2>/dev/null || true
if [ -s "${TMP}" ]; then
  column -s, -t "${TMP}" 2>/dev/null | head -n "${ROWS}" || head -n "${ROWS}" "${TMP}"
  TOTAL_ROWS=$(( $(wc -l < "${TMP}") - 1 ))
  [ "${TOTAL_ROWS}" -gt "${ROWS}" ] && echo "... (${TOTAL_ROWS} rows total)"
fi
rm -f "${TMP}"

if [ -n "${LABEL}" ]; then
  mkdir -p results
  [ -f results/benchmark.csv ] || echo "label,data_scanned_bytes,engine_ms,total_ms,query_id" > results/benchmark.csv
  echo "${LABEL},${SCANNED},${MILLIS},${TOTALMS},${QID}" >> results/benchmark.csv
fi
