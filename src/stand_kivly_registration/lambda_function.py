"""One bounded resolver operation per queue message, with scheduled outbox repair."""
import json

import boto3

from stand_common.kivly_resolver import process, recover


def lambda_handler(event, context):
    dynamo = boto3.resource("dynamodb")
    if "Records" not in event:
        recover(dynamo)
        return {"ok": True}
    failures = []
    for record in event["Records"]:
        if context is not None and context.get_remaining_time_in_millis() < 30000:
            failures.append({"itemIdentifier": record["messageId"]})
            continue
        try:
            body = json.loads(record["body"])
            if body.get("kind") == "recovery":
                recover(dynamo, body.get("cursor"))
            else:
                identifier = body.get("requestId")
                if not isinstance(identifier, str) or not identifier.startswith("registration:"):
                    raise ValueError("Invalid registration message")
                process(dynamo, identifier)
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
