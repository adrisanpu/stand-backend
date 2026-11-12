"""Owner endpoints for the STACK prize program and final closure."""
import os
import uuid

import boto3
from botocore.exceptions import ClientError
from stand_common import kivly_registration as registration
from stand_common import stack_prizes as prizes
from stand_common import game_lifecycle, reward_conditions, prize_inventory
from stand_common.prizes import staged_prize_image_key
from stand_common.utils import _get_claims, _read_json_body, _resp, _json_sanitize, get_game_type_blob


def release_unused_reservations(dynamo, meta, program, stock, rows):
    selected = {allocation["prizeId"] for rule in program["rules"]
                for group in rule["groups"] for allocation in group["allocations"]}
    removed = {prize["id"] for prize in program["catalog"]} - selected
    if not removed:
        return program, stock
    consumed = {
        prize["id"]: max(
            stock[prize["id"]]["total"] - stock[prize["id"]]["remaining"]
            if stock[prize["id"]]["total"] is not None else 0,
            sum(row.get("id", "").startswith("award_")
                and row.get("prize", {}).get("id") == prize["id"] for row in rows),
        ) for prize in program["catalog"]
    }
    if prize_inventory.enabled():
        identities = {p["id"]: prize_inventory.prize_id(p) for p in program["catalog"]}
        for pid in {identities[local_id] for local_id in removed} - {None}:
            assigned = prize_inventory.event_usage(dynamo, meta["ownerUserId"], pid).get(
                meta["gameId"], {}).get("assigned", 0)
            # Movement history survives deleted awards and can predate local stock counters.
            missing = max(0, assigned - sum(consumed[local_id] for local_id in identities if identities[local_id] == pid))
            local_id = next(p["id"] for p in program["catalog"] if p["id"] in removed and identities[p["id"]] == pid)
            consumed[local_id] += missing
    catalog = []
    stock = {**stock}
    for prize in program["catalog"]:
        if prize["id"] in removed:
            quantity = consumed[prize["id"]]
            prize = {**prize, "stockTotal": quantity}
            stock[prize["id"]] = {"total": quantity, "remaining": 0}
        catalog.append(prize)
    return {**program, "catalog": catalog}, stock


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
                        ConditionExpression="emailState = :failed",
                        ExpressionAttributeNames={"#w": "work"},
                        ExpressionAttributeValues={":pending": "pending", ":zero": 0, ":failed": "failed"},
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
            if excluded:
                raise prizes.PrizeError("PendingAttemptsMustFinish")
            current = game_lifecycle.change_active(dynamo, meta, False)
            return response(dynamo, current)
        if method != "PUT":
            return _resp(405, {"ok": False, "error": "MethodNotAllowed"})
        if meta.get("archivedAt") or not meta.get("isActive", True) or meta.get("stackPrizeState", "open") != "open":
            raise prizes.PrizeError("StackClosing")
        if "stackPrizeProgram" not in meta and any(
            get_game_type_blob(p, "STACK").get("attempt", {}).get("status") == "started"
            for p in prizes.players(dynamo, meta["gameId"])
        ):
            raise prizes.PrizeError("ActiveLegacyAttempts")
        conn = registration.connection(dynamo, owner)
        conditions_only = body.get("action") == "rules"
        existing = prizes.upgrade_program(prizes.effective_program(dynamo, meta))
        if conditions_only:
            if set(body) != {"gameId", "action", "expectedRevision", "rules"}:
                raise prizes.PrizeError("InvalidStackPrizeRules", 400)
            if body["expectedRevision"] != reward_conditions.revision(existing):
                raise prizes.PrizeError("RewardConditionsChanged")
            raw = {**_json_sanitize(existing), "rules": body["rules"]}
        elif body.get("action") == "catalog":
            if set(body) != {"gameId", "action", "program"} or not isinstance(body["program"], dict):
                raise prizes.PrizeError("InvalidStackPrizeProgram", 400)
            raw = {**body["program"], "rules": _json_sanitize(existing["rules"])}
        else:
            if body.get("action") is not None:
                raise prizes.PrizeError("InvalidAction", 400)
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
        users = dynamo.Table(os.environ["USERS_TABLE"])
        shared_catalog = (users.get_item(Key={"userId": owner}, ConsistentRead=True).get("Item") or {}).get("sharedPrizeCatalog") or []
        program = prizes.validate_program(raw, meta, conn, rewards, raffles_enabled(owner), shared_catalog)
        rows = prizes.records(dynamo, meta["gameId"])
        stock = prizes.updated_stock(meta, program, rows)
        if conditions_only:
            program, stock = release_unused_reservations(dynamo, meta, program, stock, rows)
        # Keep existing participants on their legacy flow; new attempts opt in at start.
        active_condition = "isActive = :active" if "isActive" in meta else "attribute_not_exists(isActive)"
        prizes.transact(dynamo, [prizes.game_update(
            meta, ", stackPrizeProgram = :program, stackPrizeStock = :stock",
            {":program": program, ":stock": stock, **({":active": True} if "isActive" in meta else {})},
            extra=" AND " + active_condition + " AND attribute_not_exists(archivedAt)",
        )])
        return _resp(200, _json_sanitize({
            "ok": True, "program": prizes.program_view(prizes.effective_program(dynamo, meta, program), meta),
            "programRevision": reward_conditions.revision(prizes.effective_program(dynamo, meta, program)),
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
               "emailState": prizes.email_delivery_state(r), "emailError": r.get("emailError"),
               "emailAcceptedAt": r.get("emailAcceptedAt"),
               "lastError": r.get("lastError"), "lastErrorStage": r.get("lastErrorStage"),
               "lastErrorHttpStatus": r.get("lastErrorHttpStatus")}
              for r in rows if r["id"].startswith("award_")]
    program = prizes.effective_program(dynamo, meta)
    stock = meta.get("stackPrizeStock") or prizes.initial_stock(program, rows)
    general = prize_inventory.capacity(dynamo, meta["ownerUserId"], program["catalog"], meta)
    return _resp(200, _json_sanitize({
        "ok": True, "program": prizes.program_view(program, meta),
        "programRevision": reward_conditions.revision(prizes.upgrade_program(program)),
        "enabled": "stackPrizeProgram" in meta,
        "state": "closing" if game_lifecycle.state(meta) == "closing" else meta.get("stackPrizeState", "open"),
        "emailAvailable": os.environ.get("STACK_REWARD_EMAIL_ENABLED", "false").lower() == "true",
        "pendingPlayers": pending, "awards": awards,
        "inventory": [{"prizeId": pid, **value} for pid, value in stock.items()],
        "globalInventory": [{"prizeId": pid, "inventoryPrizeId": value["prizeId"],
                             "remaining": value["remaining"], "reason": value["reason"]}
                            for pid, value in general.items()] if general is not None else None,
        "allocations": [{"playerId": r["playerId"], "ruleId": r["ruleId"],
                         "awarded": r.get("awarded", []), "state": r["state"],
                         "inventoryReasons": r.get("inventoryReasons", {})}
                        for r in rows if r.get("kind") == "allocation"],
    }))
