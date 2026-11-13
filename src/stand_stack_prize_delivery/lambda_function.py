"""Recover the durable outbox and deliver STACK awards without a browser."""
import json
import os
import time
from urllib.parse import urlencode

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from stand_common import kivly_registration as registration
from stand_common import stack_prizes as prizes
from stand_common import game_lifecycle
from stand_common import game_forms
from stand_common.reward_email import build_reward_message
from stand_common.utils import _iso_now, is_valid_email, issue_prize_token, log

dynamo = boto3.resource("dynamodb")


class DeliveryFailure(Exception):
    def __init__(self, stage, error):
        super().__init__(getattr(error, "code", "DeliveryUnavailable"))
        self.code = getattr(error, "code", "DeliveryUnavailable")
        self.http_status = getattr(error, "http_status", None)
        self.retry_after = getattr(error, "retry_after", None)
        self.stage = stage


class RewardEmailAssetError(Exception):
    pass


def _pause_if_archived(row):
    game = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
    ).get("Item")
    if game and not any(game.get(key) for key in ("archivedAt", "deletionStartedAt", "deletedAt")):
        return False
    try:
        prizes.table(dynamo).update_item(
            Key={"gameId": row["gameId"], "id": row["id"]},
            UpdateExpression="SET dueAt = :later",
            ConditionExpression="#w = :pending",
            ExpressionAttributeNames={"#w": "work"},
            ExpressionAttributeValues={":later": int(time.time()) + 300, ":pending": "pending"},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    return True


def _award_player(dynamo, row):
    return dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
        Key={"gameId": row["gameId"], "playerId": row["playerId"]}, ConsistentRead=True,
    ).get("Item") or {}


def _pause_for_quiz(row):
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
    ).get("Item") or {}
    if not meta or not game_forms.pending(meta, _award_player(dynamo, row), "prize"):
        return False
    prizes.table(dynamo).update_item(
        Key={"gameId": row["gameId"], "id": row["id"]},
        UpdateExpression="SET dueAt = :later",
        ExpressionAttributeValues={":later": int(time.time()) + 60},
    )
    return True


def _valid_customer_id(value):
    return (isinstance(value, str) and bool(value.strip()) and value == value.strip()
            and value != registration.PENDING_CUSTOMER_ID)


def _confirmed_membership(player, row):
    membership = (player.get("kivly") or {}).get("membership") or {}
    return (
        _valid_customer_id(row.get("customerId"))
        and membership.get("locationId") == row["prize"]["locationId"]
        and membership.get("customerId") == row["customerId"]
        and _valid_customer_id(membership.get("customerId"))
    )


def _block_award(key, row, reason):
    update = "SET #s = :blocked"
    values = {":blocked": "connection_required", ":queued": "queued"}
    if (row.get("prize") or {}).get("source") == "kivly":
        update += ", emailState = :email_blocked, emailError = :error"
        values[":email_blocked"] = "blocked"
        values[":error"] = reason
    update += " REMOVE #w, dueAt"
    try:
        prizes.prize_inventory.update_delivery(dynamo, row, "connection_required", {
            "Key": key, "UpdateExpression": update, "ConditionExpression": "#s = :queued",
            "ExpressionAttributeNames": {"#s": "state", "#w": "work"}, "ExpressionAttributeValues": values,
        })
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def _customer_by_confirmed_id(client, conn, row, player):
    if not _confirmed_membership(player, row):
        return None
    try:
        customer = client.get_customer(conn["locationId"], conn["ownerId"], row["customerId"])
    except registration.KivlyError as exc:
        if getattr(exc, "http_status", None) == 404 or getattr(exc, "code", "") in (
            "KivlyCustomerNotFound", "KivlyCustomerInvalid",
        ):
            return None
        raise
    if (not isinstance(customer, dict) or customer.get("id") != row["customerId"]
            or customer.get("is_active") is not True):
        return None
    return customer


def _selected_logo(dynamo, owner, *, required=True):
    users_table = os.environ.get("USERS_TABLE")
    bucket = os.environ.get("USER_MEDIA_BUCKET")
    if not users_table or not bucket:
        if not required:
            return None
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    user = dynamo.Table(users_table).get_item(
        Key={"userId": owner}, ConsistentRead=True, ProjectionExpression="media",
    ).get("Item")
    media = user.get("media") if isinstance(user, dict) else None
    logo = media.get("logo") if isinstance(media, dict) else None
    items = logo.get("items") if isinstance(logo, dict) else None
    selected_id = logo.get("selectedId") if isinstance(logo, dict) else None
    selected = next((item for item in items if isinstance(item, dict)
                     and item.get("id") == selected_id), None) if isinstance(items, list) else None
    if selected is None and not required:
        return None
    key = selected.get("key") if selected else None
    if (not isinstance(owner, str) or not owner or not isinstance(key, str)
            or not key.startswith(f"users/{owner}/logos/") or len(key) > 1024):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    try:
        size = response.get("ContentLength")
        if (response.get("ContentType") != "image/png" or type(size) is not int
                or not 0 < size <= 5 * 1024 * 1024 or body is None):
            raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
        data = body.read(5 * 1024 * 1024 + 1)
    finally:
        if body is not None:
            body.close()
    if (not isinstance(data, bytes) or len(data) != size
            or not data.startswith(b"\x89PNG\r\n\x1a\n")):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    return data


def _stand_prize_image(dynamo, row, prize):
    key = prize.get("imageKey")
    if not key:
        return None
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
        ProjectionExpression="gameId, ownerUserId",
    ).get("Item")
    if (not meta or meta.get("gameId") != row["gameId"]
            or meta.get("ownerUserId") != row["ownerUserId"]):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    if prizes.image_key(prize, meta) != key:
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    bucket = os.environ.get("USER_MEDIA_BUCKET")
    if not bucket:
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    try:
        size = response.get("ContentLength")
        if (response.get("ContentType") != "image/png" or type(size) is not int
                or not 0 < size <= 5 * 1024 * 1024 or body is None):
            raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
        data = body.read(5 * 1024 * 1024 + 1)
    finally:
        if body is not None:
            body.close()
    if (not isinstance(data, bytes) or len(data) != size
            or not data.startswith(b"\x89PNG\r\n\x1a\n")):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    return data


def _email_branding(dynamo, row, conn, context, customer):
    if (context.location_id != conn["locationId"]
            or context.owner_id != conn["ownerId"]):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
    ).get("Item")
    if (not meta or meta.get("ownerUserId") != row["ownerUserId"]
            or meta.get("gameType") != "STACK"):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    program = meta.get("stackPrizeProgram") or prizes.legacy_program(meta)
    origin = prizes.resolved_storefront(program, conn)
    if not origin:
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    wallet_url = origin + "/add-to-wallet?" + urlencode({
        "customer_id": customer["id"], "location_id": conn["locationId"],
    })
    logo_png = _selected_logo(dynamo, row["ownerUserId"])
    return context.location_name, wallet_url, logo_png


def _kivly_wallet_link(dynamo, row, conn, context, customer):
    if context.location_id != conn["locationId"] or context.owner_id != conn["ownerId"]:
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": row["gameId"]}, ConsistentRead=True,
    ).get("Item")
    if not meta or meta.get("ownerUserId") != row["ownerUserId"] or meta.get("gameType") != "STACK":
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    program = meta.get("stackPrizeProgram") or prizes.legacy_program(meta)
    origin = prizes.resolved_storefront(program, conn)
    if not origin or customer.get("id") != row.get("customerId"):
        raise RewardEmailAssetError("RewardEmailAssetsUnavailable")
    return origin + "/add-to-wallet?" + urlencode({
        "customer_id": customer["id"], "location_id": conn["locationId"],
    })


def dispatch():
    page = prizes.table(dynamo).query(
        IndexName="pending-work", KeyConditionExpression=Key("work").eq("pending") & Key("dueAt").lte(int(time.time())),
        Limit=100,
    )
    for row in page.get("Items", []):
        if _pause_if_archived(row):
            continue
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
    if row.get("closure"):
        conditions = {"top_final", "top_finish"}
    existing_records = prizes.records(dynamo, row["gameId"])
    for winner in row["winners"][:5]:
        player = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
            Key={"gameId": row["gameId"], "playerId": winner["playerId"]}, ConsistentRead=True,
        ).get("Item")
        if not player:
            raise prizes.PrizeError("AwardPlayerMissing")
        if (player.get("type") or {}).get("STACK", {}).get("attempt", {}).get("status") == "excluded":
            continue
        if ("top_final" in conditions and not row.get("closure")
                and "prizeProgram" not in (player.get("type") or {}).get("STACK", {}).get("attempt", {})):
            continue
        player_conditions = conditions
        if row.get("closure") and "prizeProgram" not in (player.get("type") or {}).get("STACK", {}).get("attempt", {}):
            player_conditions = conditions | {"completed", "score"}
        for rule in program["rules"]:
            if rule["condition"] in player_conditions and not (
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
    if row.get("cycle") and row["cycle"] != meta.get("closeCycle"):
        game_lifecycle.complete(dynamo, row)
        return
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
        prizes.prize_inventory.update_delivery(dynamo, row, "reconciliation_required", {
            "Key": key, "UpdateExpression": "SET #s = :s REMOVE #w, dueAt",
            "ExpressionAttributeNames": {"#s": "state", "#w": "work"},
            "ExpressionAttributeValues": {":s": "reconciliation_required"},
        })
        return
    if row["state"] != "queued":
        return
    if _pause_for_quiz(row):
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
    player = _award_player(dynamo, row)
    if not _confirmed_membership(player, row):
        _block_award(key, row, "KivlyMembershipRequired")
        return
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
        customer = _customer_by_confirmed_id(client, conn, row, player)
    except registration.KivlyError as exc:
        raise DeliveryFailure("customer_lookup", exc) from None
    if not customer:
        _block_award(key, row, "KivlyCustomerInvalid")
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
    if _pause_if_archived(row):
        try:
            prizes.table(dynamo).update_item(
                Key=key, UpdateExpression="SET #s = :queued",
                ConditionExpression="#s = :sending",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={":queued": "queued", ":sending": "sending"},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        return
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
        update = "SET #s = :s, emailState = :pending, #w = :pending, dueAt = :now REMOVE emailError, redemptionId, lastError, lastErrorStage, lastErrorHttpStatus"
        values = {":s": state, ":pending": "pending", ":now": 0}
    prizes.prize_inventory.update_delivery(dynamo, row, state, {
        "Key": key, "UpdateExpression": update,
        "ExpressionAttributeNames": {"#s": "state", "#w": "work"},
        "ExpressionAttributeValues": values,
    })


def email_status(key, state, error=None, attempts=0, retry=False, message_id=None):
    update = "SET emailState = :state, emailError = :error"
    values = {":state": state, ":error": error}
    if attempts:
        update += ", emailAttempts = :attempts"
        values[":attempts"] = attempts
    if message_id:
        update += ", emailMessageId = :message, emailAcceptedAt = :accepted"
        values[":message"] = message_id
        values[":accepted"] = _iso_now()
        update += " REMOVE emailError"
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
    if _pause_for_quiz(row):
        return
    if os.environ.get("REWARD_EMAIL_ENABLED", os.environ.get(
            "STACK_REWARD_EMAIL_ENABLED", "false")).lower() != "true":
        prizes.table(dynamo).update_item(
            Key=key, UpdateExpression="SET dueAt = :later",
            ExpressionAttributeValues={":later": int(time.time()) + 3600},
        )
        return
    attempts = int(row.get("emailAttempts", 0)) + 1
    player = _award_player(dynamo, row)
    email = player.get("email")
    if (not player.get("verifiedAt") or player.get("verificationMethod") != "email-code"
            or not is_valid_email(email or "")):
        email_status(key, "failed", "VerifiedEmailRequired")
        return
    sender = os.environ.get("EMAIL_SENDER_ADDRESS")
    if not sender:
        email_status(key, "failed", "RewardEmailUnavailable")
        return
    try:
        prize = row.get("prize") or {}
        source = prize.get("source")
        expected_state = row.get("state")
        links = []
        qr_value = None
        if source == "kivly":
            if expected_state != "confirmed":
                email_status(key, "failed", "KivlyRewardNotConfirmed")
                return
            conn = registration.connection(dynamo, row["ownerUserId"])
            if not conn or conn["locationId"] != prize.get("locationId"):
                email_status(key, "failed", "KivlyConnectionChanged")
                return
            client = registration._client(conn)
            customer = _customer_by_confirmed_id(client, conn, row, player)
            if not customer:
                email_status(key, "failed", "KivlyCustomerInvalid")
                return
            wallet_url = _kivly_wallet_link(dynamo, row, conn, client.get_context(), customer)
            links.append(("Kivly Wallet", wallet_url))
        elif source == "stand":
            if expected_state not in ("available", "delivered"):
                email_status(key, "failed", "RewardNotAvailable")
                return
            token = issue_prize_token(
                row["gameId"], row["playerId"], "a", os.environ["PLAYER_TOKEN_SECRET"],
                ttl_seconds=315360000, award_id=row["id"],
            )
            qr_value = token
            logo_png = _selected_logo(dynamo, row["ownerUserId"], required=False)
            prize_image_png = _stand_prize_image(dynamo, row, prize)
        else:
            email_status(key, "failed", "RewardSourceUnsupported")
            return
        message = build_reward_message(
            sender, email, player.get("username") or "", prize,
            qr_value=qr_value, wallet_links=links,
            **({"logo_png": logo_png, "prize_image_png": prize_image_png}
               if source == "stand" else {}),
        )
    except (registration.RegistrationError, registration.KivlyError) as exc:
        retry = attempts < 3
        email_status(key, "pending" if retry else "failed",
                     getattr(exc, "code", "KivlyUnavailable"), attempts, retry=retry)
        return
    except (RewardEmailAssetError, KeyError, TypeError, ValueError):
        email_status(key, "failed", "RewardEmailAssetsUnavailable")
        return
    try:
        prizes.table(dynamo).update_item(
            Key=key, UpdateExpression="SET emailState = :sending, emailAttempts = :attempts",
            ConditionExpression="emailState = :pending AND #s = :expected",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={
                ":sending": "sending", ":pending": "pending", ":attempts": attempts,
                ":expected": expected_state,
            },
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return
        raise
    if _pause_if_archived(row):
        try:
            prizes.table(dynamo).update_item(
                Key=key, UpdateExpression="SET emailState = :pending",
                ConditionExpression="emailState = :sending",
                ExpressionAttributeValues={":pending": "pending", ":sending": "sending"},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        return
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
    email_status(key, "accepted", message_id=result["MessageId"])


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
                if _pause_if_archived(row):
                    continue
                elif row.get("kind") == "lifecycle":
                    game_lifecycle.finish(dynamo, row)
                elif row.get("kind") == "close":
                    finish_close(row)
                elif row.get("kind") == "grant":
                    grant(row)
                elif row.get("emailState") in ("pending", "sending"):
                    notify(row)
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
