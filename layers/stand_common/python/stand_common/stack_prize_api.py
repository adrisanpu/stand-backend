"""Owner endpoints for the STACK prize program and final closure."""
import os
import uuid

import boto3
from botocore.exceptions import ClientError
from stand_common import kivly_registration as registration
from stand_common import stack_prizes as prizes
from stand_common.prizes import staged_prize_image_key
from stand_common.utils import _get_claims, _read_json_body, _resp, _json_sanitize, get_game_type_blob


def handle(event, method, dynamo, raffles_enabled, read_image):
    try:
        owner = _get_claims(event).get("sub")
        if not owner:
            return _resp(401, {"ok": False, "error": "Unauthorized"})
        body = (event.get("queryStringParameters") or {}) if method == "GET" else _read_json_body(event)
        if not isinstance(body, dict) or not isinstance(body.get("gameId"), str):
            raise prizes.PrizeError("InvalidRequest", 400)
        games = dynamo.Table(os.environ["GAMES_TABLE"])
        meta = games.get_item(Key={"gameId": body["gameId"]}, ConsistentRead=True).get("Item")
        if not meta or meta.get("ownerUserId") != owner:
            raise prizes.PrizeError("Forbidden", 403)
        if meta.get("gameType") != "STACK":
            raise prizes.PrizeError("UnsupportedGameType", 400)
        if method == "GET":
            return response(dynamo, meta)
        if method == "POST":
            if body.get("action") == "retry_email":
                award_id = body.get("awardId")
                if not isinstance(award_id, str) or not award_id.startswith("award_") or len(award_id) > 80:
                    raise prizes.PrizeError("InvalidAward", 400)
                if os.environ.get("STACK_REWARD_EMAIL_ENABLED", "false").lower() != "true":
                    raise prizes.PrizeError("RewardEmailUnavailable", 503)
                key = {"gameId": meta["gameId"], "id": award_id}
                try:
                    prizes.table(dynamo).update_item(
                        Key=key, UpdateExpression="SET emailState = :pending, emailAttempts = :zero, #w = :pending, dueAt = :zero REMOVE emailError",
                        ConditionExpression="#s = :confirmed AND deliveryChannel = :email AND emailState = :failed",
                        ExpressionAttributeNames={"#s": "state", "#w": "work"},
                        ExpressionAttributeValues={":pending": "pending", ":zero": 0, ":confirmed": "confirmed",
                                                   ":email": "email", ":failed": "failed"},
                    )
                except ClientError as exc:
                    if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                        raise prizes.PrizeError("RewardEmailNotRetryable") from None
                    raise
                prizes.enqueue({**key})
                return response(dynamo, meta)
            if body.get("action") is not None:
                raise prizes.PrizeError("InvalidAction", 400)
            excluded = body.get("excludePlayerIds", [])
            if (not isinstance(excluded, list) or len(excluded) > 100
                    or any(type(pid) is not int or pid < 1 for pid in excluded)):
                raise prizes.PrizeError("InvalidExcludedPlayers", 400)
            try:
                prizes.close(dynamo, meta, excluded)
            except prizes.PrizeError as exc:
                if exc.code != "StackClosingStarted":
                    raise
            current = games.get_item(Key={"gameId": meta["gameId"]}, ConsistentRead=True)["Item"]
            return response(dynamo, current)
        if method != "PUT":
            return _resp(405, {"ok": False, "error": "MethodNotAllowed"})
        if meta.get("stackPrizeState", "open") != "open":
            raise prizes.PrizeError("StackClosing")
        if "stackPrizeProgram" not in meta and any(
            get_game_type_blob(p, "STACK").get("attempt", {}).get("status") == "started"
            for p in prizes.players(dynamo, meta["gameId"])
        ):
            raise prizes.PrizeError("ActiveLegacyAttempts")
        conn = registration.connection(dynamo, owner)
        raw = body.get("program")
        if not isinstance(raw, dict) or not isinstance(raw.get("catalog"), list):
            raise prizes.PrizeError("InvalidStackPrizeProgram", 400)
        rewards = []
        if isinstance(raw, dict) and any(p.get("source") == "kivly" for p in raw.get("catalog", [])
                                        if isinstance(p, dict)):
            if not conn or not registration.account_game_settings(meta, conn)["enabled"]:
                raise prizes.PrizeError("KivlyConnectionRequired")
            rewards = registration._client(conn).list_rewards(conn["locationId"], conn["ownerId"])
        # Promote verified PNGs to immutable keys; interrupted saves leave only unreferenced objects.
        if isinstance(raw, dict) and isinstance(raw.get("catalog"), list):
            raw = {**raw, "catalog": [dict(p) if isinstance(p, dict) else p for p in raw["catalog"]]}
            for p in raw["catalog"]:
                if not isinstance(p, dict):
                    continue
                key = p.get("imageKey")
                if key and staged_prize_image_key(owner, meta["gameId"], "ranking", key):
                    image = read_image(key)
                    committed = f"games/{owner}/{meta['gameId']}/ranking/{uuid.uuid4()}.png"
                    boto3.client("s3").put_object(
                        Bucket=os.environ["USER_MEDIA_BUCKET"], Key=committed,
                        Body=image, ContentType="image/png",
                    )
                    p["imageKey"] = committed
        program = prizes.validate_program(raw, meta, conn, rewards, raffles_enabled(owner))
        stock = prizes.updated_stock(meta, program, prizes.records(dynamo, meta["gameId"]))
        # Keep existing participants on their legacy flow; new attempts opt in at start.
        prizes.transact(dynamo, [prizes.game_update(
            meta, ", stackPrizeProgram = :program, stackPrizeStock = :stock",
            {":program": program, ":stock": stock},
        )])
        return _resp(200, _json_sanitize({
            "ok": True, "program": prizes.program_view(prizes.effective_program(dynamo, meta, program), meta),
        }))
    except prizes.PrizeError as exc:
        return _resp(exc.status, {"ok": False, "error": exc.code})
    except registration.ERRORS as exc:
        status, payload = registration.error_response(exc)
        return _resp(status, payload)
    except ValueError:
        return _resp(400, {"ok": False, "error": "InvalidStackPrizeImage"})


def response(dynamo, meta):
    pending = []
    if meta.get("stackPrizeState") == "closing":
        pending = [{"playerId": int(p["playerId"]), "name": p.get("username", "")}
                   for p in prizes.players(dynamo, meta["gameId"])
                   if get_game_type_blob(p, "STACK").get("attempt", {}).get("status") == "started"]
    rows = prizes.records(dynamo, meta["gameId"])
    awards = [{"id": r["id"], "playerId": r["playerId"], "name": r["prize"]["name"],
               "source": r["prize"]["source"], "condition": r["condition"], "state": r["state"],
               "deliveryChannel": r.get("deliveryChannel", "wallet"),
               "emailState": r.get("emailState"), "emailError": r.get("emailError"),
               "lastError": r.get("lastError"), "lastErrorStage": r.get("lastErrorStage"),
               "lastErrorHttpStatus": r.get("lastErrorHttpStatus")}
              for r in rows if r["id"].startswith("award_")]
    program = prizes.effective_program(dynamo, meta)
    stock = meta.get("stackPrizeStock") or prizes.initial_stock(program, rows)
    return _resp(200, _json_sanitize({
        "ok": True, "program": prizes.program_view(program, meta),
        "enabled": "stackPrizeProgram" in meta, "state": meta.get("stackPrizeState", "open"),
        "emailAvailable": os.environ.get("STACK_REWARD_EMAIL_ENABLED", "false").lower() == "true",
        "pendingPlayers": pending, "awards": awards,
        "inventory": [{"prizeId": pid, **value} for pid, value in stock.items()],
        "allocations": [{"playerId": r["playerId"], "ruleId": r["ruleId"],
                         "awarded": r.get("awarded", []), "state": r["state"]}
                        for r in rows if r.get("kind") == "allocation"],
    }))
