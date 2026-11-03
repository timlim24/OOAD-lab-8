"""
Kinesis -> 1-minute event-time failure rate -> SNS alert.

The hard part of this function is the part that is NOT in it: STATE.

A Lambda invocation sees one batch of records and then dies. A one-minute
window of event time does not fit inside a batch — the events of minute T
arrive spread over several batches, several invocations, and (because Kinesis
shards are read in parallel) several concurrent executions. So a windowed
aggregation in Lambda has to keep its counters somewhere outside the process.

Here that is a DynamoDB table with atomic ADD, which is correct but costs a
round trip per window per batch. Flink would keep exactly the same counters in
keyed state, in memory, next to the data, and checkpoint them together with
the stream offsets. That difference — "where does the window live" — is the
whole reason managed stream processors exist, and it is the honest answer to
"why not just use Lambda for everything".

What this function deliberately does NOT do, and part 3's consumer does:
  * no watermark. It cannot have one: a watermark is a property of the whole
    stream, and this function only ever sees a fragment of it.
  * no late-event rejection. A record for a window closed ten minutes ago is
    simply added, and the alert for that window may fire a second time.
  * no exactly-once. Kinesis event source mapping is at-least-once; on a retry
    the whole batch is reprocessed, so counters can be double-added.
Those three gaps are not bugs to fix here — they are what you are buying when
you pay for Managed Flink.
"""

import base64
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

TABLE = os.environ["WINDOW_TABLE"]
TOPIC_ARN = os.environ["TOPIC_ARN"]
THRESHOLD = float(os.environ.get("THRESHOLD", "0.10"))
MIN_EVENTS = int(os.environ.get("MIN_EVENTS", "20"))
WINDOW_S = int(os.environ.get("WINDOW_S", "60"))
TTL_DAYS = int(os.environ.get("TTL_DAYS", "2"))

ddb = boto3.client("dynamodb")
sns = boto3.client("sns")

TERMINAL = ("success", "failure")


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def handler(event, context):
    # ---- fold this batch down before touching DynamoDB ----------------------
    # One ADD per (window, method) in the batch, not one per record. A batch of
    # 500 records typically covers 2-3 minutes and 4 methods, so this turns
    # ~500 writes into ~12.
    local = defaultdict(lambda: [0, 0])     # (window_start, method) -> [n, fails]
    parsed = skipped = 0

    for rec in event["Records"]:
        try:
            payload = base64.b64decode(rec["kinesis"]["data"])
            ev = json.loads(payload)
        except Exception:
            skipped += 1
            continue

        status = ev.get("status")
        if status not in TERMINAL:
            continue

        try:
            ts = parse_ts(ev["modifiedDate"])
        except Exception:
            skipped += 1
            continue

        w = int(ts // WINDOW_S) * WINDOW_S
        cell = local[(w, ev.get("paymentMethod", "unknown"))]
        cell[0] += 1
        if status == "failure":
            cell[1] += 1
        parsed += 1

    alerts = []
    expires = int(time.time()) + TTL_DAYS * 86400

    for (w, method), (n, fails) in local.items():
        # ADD is atomic and commutative, so concurrent invocations reading
        # different shards can update the same window without a lock and
        # without losing counts.
        resp = ddb.update_item(
            TableName=TABLE,
            Key={"pk": {"S": f"{w}#{method}"}},
            UpdateExpression="ADD terminal :n, failures :f SET expires_at = :e",
            ExpressionAttributeValues={
                ":n": {"N": str(n)},
                ":f": {"N": str(fails)},
                ":e": {"N": str(expires)},
            },
            ReturnValues="UPDATED_NEW",
        )
        attrs = resp["Attributes"]
        total = int(attrs["terminal"]["N"])
        failed = int(attrs["failures"]["N"])
        rate = failed / total if total else 0.0

        if total >= MIN_EVENTS and rate > THRESHOLD:
            # Fire once per window+method. The conditional write is the guard:
            # it succeeds only if `alerted` is not already set, so the second
            # invocation to cross the threshold gets a
            # ConditionalCheckFailedException and stays quiet. Without it a
            # busy window alerts on every batch.
            try:
                ddb.update_item(
                    TableName=TABLE,
                    Key={"pk": {"S": f"{w}#{method}"}},
                    UpdateExpression="SET alerted = :t",
                    ConditionExpression="attribute_not_exists(alerted)",
                    ExpressionAttributeValues={":t": {"BOOL": True}},
                )
            except ddb.exceptions.ConditionalCheckFailedException:
                continue

            window_utc = datetime.fromtimestamp(w, tz=timezone.utc)
            msg = (
                f"Payment failure rate above {THRESHOLD:.0%}\n\n"
                f"  window        : {window_utc:%Y-%m-%d %H:%M:%S} UTC "
                f"(+{WINDOW_S}s)\n"
                f"  paymentMethod : {method}\n"
                f"  failure rate  : {rate:.1%}  ({failed} of {total})\n"
            )
            sns.publish(
                TopicArn=TOPIC_ARN,
                Subject=f"[lab8] {method} failures {rate:.0%}",
                Message=msg,
            )
            alerts.append({"window": w, "method": method,
                           "rate": round(rate, 4), "n": total})

    print(json.dumps({
        "records": len(event["Records"]),
        "terminal": parsed,
        "skipped": skipped,
        "cells": len(local),
        "alerts": alerts,
    }, ensure_ascii=False))

    return {"alerts": len(alerts)}
