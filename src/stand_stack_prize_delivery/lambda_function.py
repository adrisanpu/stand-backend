"""Recover the durable outbox and deliver STACK awards without a browser."""
import json
import os
import time

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from stand_common import kivly_registration as registration
from stand_common import stack_prizes as prizes
from stand_common.reward_email import build_message
from stand_common.utils import is_valid_email, is_valid_phone, log

dynamo = boto3.resource("dynamodb")


class DeliveryFailure(Exception):
    def __init__(self, stage, error):
        super().__init__(getattr(error, "code", "DeliveryUnavailable"))
        self.code = getattr(error, "code", "DeliveryUnavailable")
        self.http_status = getattr(error, "http_status", None)
        self.retry_after = getattr(error, "retry_after", None)
        self.stage = stage


def dispatch():
    page = prizes.table(dynamo).query(
        IndexName="pending-work", KeyConditionExpression=Key("work").eq("pending") & Key("dueAt").lte(int(time.time())),
        Limit=100,
    )
    for row in page.get("Items", []):
        if not prizes.enqueue(row):
            continue
        try:
            prizes.table(dynamo).update_item(
                Key={"gameId": row["gameId"], "id": row["id"]},
                UpdateExpression="SET dueAt = :later",
                ConditionExpression="#w = :pending", ExpressionAttributeNames={"#w": "work"},
                ExpressionAttributeValues={":later": int(time.time()) + 300, ":pending": "pending"},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise


def grant(row):
    meta = {"gameId": row["gameId"], "ownerUserId": row["ownerUserId"]}
    program = prizes.upgrade_program(row["program"])
    conditions = {"completed", "top_finish", "score"} if row["condition"] == "finish" else {row["condition"]}
    existing_records = prizes.records(dynamo, row["gameId"])
    for winner in row["winners"][:5]:
        player = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
            Key={"gameId": row["gameId"], "playerId": winner["playerId"]}, ConsistentRead=True,
        ).get("Item")
        if not player:
            raise prizes.PrizeError("AwardPlayerMissing")
        if (player.get("type") or {}).get("STACK", {}).get("attempt", {}).get("status") == "excluded":
            continue
        if ("top_final" in conditions
                and "prizeProgram" not in (player.get("type") or {}).get("STACK", {}).get("attempt", {})):
            continue
        for rule in program["rules"]:
            if rule["condition"] in conditions and not (
                rule["condition"].startswith("top_")
                and (winner.get("rank") is None or winner["rank"] > rule["topX"])
            ) and (rule["condition"] != "score" or prizes.score_matches(rule, winner.get("score"))):
                prizes.allocate_rule(
                    dynamo, meta, player, program, rule, row["id"], winner.get("rank"), existing_records,
                    score=winner.get("score"),
                )
    if len(row["winners"]) > 5:
        prizes.table(dynamo).update_item(
            Key={"gameId": row["gameId"], "id": row["id"]},
            UpdateExpression="SET winners = :remaining, dueAt = :now",
            ExpressionAttributeValues={":remaining": row["winners"][5:], ":now": 0},
        )
        return
    prizes.table(dynamo).update_item(
        Key={"gameId": row["gameId"], "id": row["id"]},
        UpdateExpression="SET #s = :done REMOVE #w, dueAt",
        ExpressionAttributeNames={"#s": "state", "#w": "work"}, ExpressionAttributeValues={":done": "confirmed"},
    )


def finish_close(row):
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
    ).get("Item")
    if not meta:
        raise prizes.PrizeError("GameNotFound")
    prizes.close(dynamo, meta, [])
    current = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
    )["Item"]
    if current.get("stackPrizeState") == "closed":
        prizes.table(dynamo).update_item(
            Key={"gameId": row["gameId"], "id": row["id"]},
            UpdateExpression="REMOVE #w, dueAt", ExpressionAttributeNames={"#w": "work"},
        )


def deliver(row):
    key = {"gameId": row["gameId"], "id": row["id"]}
    if row["state"] == "sending":
        # An interrupted earn may have succeeded; never issue it again.
        prizes.table(dynamo).update_item(
            Key=key, UpdateExpression="SET #s = :s REMOVE #w, dueAt",
            ExpressionAttributeNames={"#s": "state", "#w": "work"}, ExpressionAttributeValues={":s": "reconciliation_required"},
        )
        return
    if row["state"] != "queued":
        return
    try:
        conn = registration.connection(dynamo, row["ownerUserId"])
    except (registration.RegistrationError, registration.KivlyError) as exc:
        raise DeliveryFailure("connection", exc) from None
    if not conn or conn["locationId"] != row["prize"]["locationId"]:
        raise DeliveryFailure("connection", prizes.PrizeError("KivlyConnectionChanged"))
    try:
        client = registration._client(conn)
    except (registration.RegistrationError, registration.KivlyError) as exc:
        raise DeliveryFailure("connection", exc) from None
    if os.environ.get("KIVLY_EXTERNAL_WRITES_ENABLED", "true").lower() != "true":
        log("stack_prize_delivery_read_only", {"stage": "earn", "result": "skipped"})
        try:
            prizes.table(dynamo).update_item(
                Key=key, UpdateExpression="SET dueAt = :later",
                ConditionExpression="#s = :queued", ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={":later": int(time.time()) + 3600, ":queued": "queued"},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        return
    try:
        prizes.table(dynamo).update_item(
            Key=key, UpdateExpression="SET #s = :sending REMOVE beforeIds, lastError",
            ConditionExpression="#s = :queued", ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":sending": "sending", ":queued": "queued"},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return
        raise
    stage = "earn"
    failure = None
    try:
        result = client.earn_reward(
            conn["locationId"], conn["ownerId"], row["customerId"], row["prize"]["rewardId"],
        )
        if not isinstance(result, dict) or result.get("success") is not True:
            raise registration.KivlyError("KivlyEarnNotConfirmed")
        state, redemption_id = "confirmed", None
    except (registration.KivlyError, prizes.PrizeError) as exc:
        failure = exc
        log("stack_prize_reconciliation_required", {
            "stage": stage, "code": getattr(exc, "code", str(exc)),
            "httpStatus": getattr(exc, "http_status", None),
        })
        state, redemption_id = "reconciliation_required", None
    update = "SET #s = :s, redemptionId = :r REMOVE #w, dueAt"
    values = {":s": state, ":r": redemption_id}
    if failure:
        update = "SET #s = :s, redemptionId = :r, lastError = :e, lastErrorStage = :stage, lastErrorHttpStatus = :http REMOVE #w, dueAt"
        values.update({
            ":e": getattr(failure, "code", "KivlyRewardReconciliationRequired"),
            ":stage": stage,
            ":http": getattr(failure, "http_status", None),
        })
    else:
        if row.get("deliveryChannel") == "email":
            update = "SET #s = :s, emailState = :pending, #w = :pending, dueAt = :now REMOVE redemptionId, lastError, lastErrorStage, lastErrorHttpStatus"
            values = {":s": state, ":pending": "pending", ":now": 0}
        else:
            update = "SET #s = :s REMOVE redemptionId, #w, dueAt, lastError, lastErrorStage, lastErrorHttpStatus"
            values = {":s": state}
    prizes.table(dynamo).update_item(
        Key=key, UpdateExpression=update,
        ExpressionAttributeNames={"#s": "state", "#w": "work"},
        ExpressionAttributeValues=values,
    )


def email_status(key, state, error=None, attempts=0, retry=False):
    update = "SET emailState = :state, emailError = :error"
    values = {":state": state, ":error": error}
    if retry:
        update += ", dueAt = :later"
        values[":later"] = int(time.time()) + 60 * max(1, attempts)
    else:
        update += " REMOVE #w, dueAt"
    prizes.table(dynamo).update_item(
        Key=key, UpdateExpression=update,
        **({} if retry else {"ExpressionAttributeNames": {"#w": "work"}}),
        ExpressionAttributeValues=values,
    )
    if error:
        log("stack_reward_email_failed", {"code": error, "state": state})


def notify(row):
    key = {"gameId": row["gameId"], "id": row["id"]}
    if row.get("emailState") == "sending":
        # SES may have accepted an interrupted request; never resend automatically.
        email_status(key, "reconciliation_required", "RewardEmailUncertain")
        return
    if row.get("emailState") != "pending":
        return
    if (os.environ.get("STACK_REWARD_EMAIL_ENABLED", "false").lower() != "true"
            or os.environ.get("KIVLY_EXTERNAL_WRITES_ENABLED", "true").lower() != "true"):
        prizes.table(dynamo).update_item(
            Key=key, UpdateExpression="SET dueAt = :later",
            ExpressionAttributeValues={":later": int(time.time()) + 3600},
        )
        return
    attempts = int(row.get("emailAttempts", 0)) + 1
    try:
        prizes.table(dynamo).update_item(
            Key=key, UpdateExpression="SET emailState = :sending, emailAttempts = :attempts",
            ConditionExpression="emailState = :pending",
            ExpressionAttributeValues={":sending": "sending", ":pending": "pending", ":attempts": attempts},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return
        raise
    player = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
        Key={"gameId": row["gameId"], "playerId": row["playerId"]}, ConsistentRead=True,
    ).get("Item") or {}
    email = player.get("email")
    if not player.get("emailVerifiedAt") or not is_valid_email(email or ""):
        email_status(key, "failed", "VerifiedEmailRequired")
        return
    sender = os.environ.get("EMAIL_SENDER_ADDRESS")
    if not sender:
        email_status(key, "failed", "RewardEmailUnavailable")
        return
    try:
        conn = registration.connection(dynamo, row["ownerUserId"])
        if not conn or conn["locationId"] != row["prize"]["locationId"]:
            email_status(key, "failed", "KivlyConnectionChanged")
            return
        customer = registration._client(conn).find_customer(conn["locationId"], conn["ownerId"], email)
    except (registration.RegistrationError, registration.KivlyError) as exc:
        email_status(key, "pending" if attempts < 3 else "failed",
                     getattr(exc, "code", "KivlyUnavailable"), attempts, retry=attempts < 3)
        return
    if not customer or customer.get("id") != row["customerId"] or customer.get("is_active") is not True:
        email_status(key, "failed", "KivlyCustomerInvalid")
        return
    phone = customer.get("phone")
    if not is_valid_phone(phone):
        email_status(key, "failed", "KivlyPhoneRequired")
        return
    message = build_message(sender, email, player.get("username") or "", row["prize"], phone)
    try:
        result = boto3.client(
            "ses", region_name="us-east-1",
            config=Config(retries={"mode": "standard", "total_max_attempts": 1},
                          connect_timeout=5, read_timeout=10),
        ).send_raw_email(
            Source=sender, Destinations=[email], RawMessage={"Data": message},
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        throttled = code in ("Throttling", "ThrottlingException", "TooManyRequestsException")
        rejected = throttled or code in (
            "MessageRejected", "MailFromDomainNotVerifiedException", "ConfigurationSetDoesNotExistException",
            "ConfigurationSetSendingPausedException", "AccountSendingPausedException",
            "AccessDenied", "AccessDeniedException", "InvalidParameterValue", "ValidationError",
        )
        if not rejected or exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) >= 500:
            email_status(key, "reconciliation_required", "RewardEmailUncertain")
            return
        retry = throttled and attempts < 3
        email_status(key, "pending" if retry else "failed", "RewardEmailRejected", attempts, retry=retry)
        return
    except BotoCoreError:
        email_status(key, "reconciliation_required", "RewardEmailUncertain")
        return
    if not isinstance(result.get("MessageId"), str) or not result["MessageId"]:
        email_status(key, "reconciliation_required", "RewardEmailUncertain")
        return
    email_status(key, "accepted")


def lambda_handler(event, context):
    if "Records" not in event:
        dispatch()
        return {"ok": True}
    failed = []
    # FIFO stops a customer group at its first failure.
    for index, record in enumerate(event["Records"]):
        row = None
        try:
            key = json.loads(record["body"])
            row = prizes.table(dynamo).get_item(Key=key, ConsistentRead=True).get("Item")
            if row and row.get("work") == "pending":
                if row.get("kind") == "close":
                    finish_close(row)
                elif row.get("kind") == "grant":
                    grant(row)
                elif row.get("state") == "confirmed" and row.get("deliveryChannel") == "email":
                    notify(row)
                else:
                    deliver(row)
        except (ClientError, registration.KivlyError, registration.RegistrationError, prizes.PrizeError, DeliveryFailure) as exc:
            stage = getattr(exc, "stage", None)
            http_status = getattr(exc, "http_status", None)
            log("stack_prize_delivery_failed", {
                "code": getattr(exc, "code", str(exc)), "stage": stage, "httpStatus": http_status,
            })
            if row and row.get("state") == "queued":
                current = prizes.table(dynamo).get_item(
                    Key={"gameId": row["gameId"], "id": row["id"]}, ConsistentRead=True,
                ).get("Item")
                if current and current.get("state") == "queued":
                    prizes.table(dynamo).update_item(
                        Key={"gameId": row["gameId"], "id": row["id"]},
                        UpdateExpression="SET dueAt = :later, lastError = :error, lastErrorStage = :stage, lastErrorHttpStatus = :http",
                        ExpressionAttributeValues={
                            ":later": int(time.time()) + max(60, getattr(exc, "retry_after", None) or 300),
                            ":error": getattr(exc, "code", "DeliveryUnavailable"),
                            ":stage": stage or "delivery",
                            ":http": http_status,
                        },
                    )
                    continue
            failed = [{"itemIdentifier": r["messageId"]} for r in event["Records"][index:]]
            break
    return {"batchItemFailures": failed}
