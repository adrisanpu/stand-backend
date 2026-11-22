"""One bounded resolver operation per async invocation, with scheduled outbox repair."""
import json
import time

import boto3

from stand_common.kivly_resolver import MAX_INLINE_DELAY_SECONDS, process, recover


def _handle(dynamo, body):
    if body.get("kind") == "recovery":
        recover(dynamo, body.get("cursor"))
        return
    identifier = body.get("requestId")
    if not isinstance(identifier, str) or not identifier.startswith("registration:"):
        raise ValueError("Invalid registration message")
    process(dynamo, identifier)


def lambda_handler(event, context):
    dynamo = boto3.resource("dynamodb")
    if "Records" in event:
        # Legacy SQS deliveries still in flight during migration.
        failures = []
        for record in event["Records"]:
            if context is not None and context.get_remaining_time_in_millis() < 30000:
                failures.append({"itemIdentifier": record["messageId"]})
                continue
            try:
                _handle(dynamo, json.loads(record["body"]))
            except Exception:
                failures.append({"itemIdentifier": record["messageId"]})
        return {"batchItemFailures": failures}
    if "requestId" in event or event.get("kind") == "recovery":
        delay = event.get("delaySeconds")
        if isinstance(delay, int) and 0 < delay <= MAX_INLINE_DELAY_SECONDS:
            time.sleep(delay)
        # Failures stay pending in the outbox and are retried by the scheduled sweep.
        _handle(dynamo, event)
        return {"ok": True}
    recover(dynamo)
    return {"ok": True}
