import os
import random
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import log, _json_sanitize, _iso_now, normalize_email, issue_player_token
from stand_common.email_verification import (
    VerificationError, RegistrationError, registration_values, verification_table,
    require_verified_registration, complete_registration, identity_id,
)

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
PLAYER_TOKEN_SECRET = os.environ.get("PLAYER_TOKEN_SECRET", "stand-dev-player-token-v1")

# ========= AWS =========
dynamo_r = boto3.resource("dynamodb")

games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)

def _code4() -> int:
    return random.randint(1000, 9999)

def _get_game_meta(game_id: str) -> dict | None:
    resp = games_table.get_item(Key={"gameId": game_id})
    return resp.get("Item")

def _ensure_game_counters(game_id: str):
    """
    Inicializa counters si no existen. Idempotente (compat con games antiguos).
    """
    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression=(
            "SET playersCount = if_not_exists(playersCount, :z), "
            "validatedCount = if_not_exists(validatedCount, :z)"
        ),
        ExpressionAttributeValues={":z": 0},
    )

def _reserve_player_slot(game_id: str, max_players: int, now_iso: str) -> bool:
    """
    Incrementa playersCount de forma atómica SOLO si no excede maxPlayers.
    Devuelve True si reserva OK, False si límite alcanzado.
    """
    try:
        games_table.update_item(
            Key={"gameId": game_id},
            UpdateExpression="ADD playersCount :one SET lastJoinAt = :now, updatedAt = :now",
            ConditionExpression="attribute_not_exists(playersCount) OR playersCount < :max",
            ExpressionAttributeValues={":one": 1, ":now": now_iso, ":max": int(max_players)},
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise

def _rollback_player_slot(game_id: str, now_iso: str):
    """
    Rollback best-effort del contador si algo falla tras reservar plaza.
    """
    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression="ADD playersCount :neg SET updatedAt = :now",
        ExpressionAttributeValues={":neg": -1, ":now": now_iso},
    )


def _get_last_player_id(game_id: str) -> int:
    """
    Query the highest existing ID and retain the high-water mark after removals.
    """
    resp = gp_table.query(
        KeyConditionExpression=Key("gameId").eq(game_id),
        ProjectionExpression="playerId",
        ScanIndexForward=False,
        Limit=1,
        ConsistentRead=True,
    )
    items = resp.get("Items") or []
    latest_id = int(items[0]["playerId"]) if items else 0
    game = games_table.get_item(
        Key={"gameId": game_id},
        ProjectionExpression="lastPlayerId",
        ConsistentRead=True,
    ).get("Item") or {}
    return max(latest_id, int(game.get("lastPlayerId", 0)))


def _put_player(item: dict) -> None:
    # Conditional protects races on playerId within same partition
    gp_table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(playerId)",
    )




# ========= Assigners registry =========
# Import here (not top) to avoid circular imports
from assigners.empareja2 import assign_empareja2
from assigners.generic import assign_generic
from assigners.l3tras import assign_l3tras

ASSIGNERS = {
    "EMPAREJA2": assign_empareja2,
    "L3TRAS": assign_l3tras,
}


def _find_existing_player_by_email(game_id: str, email: str) -> dict | None:
    email_norm = normalize_email(email)
    try:
        kwargs = {
            "KeyConditionExpression": Key("gameId").eq(game_id),
            "ProjectionExpression": "gameId, playerId, email, #username, joinedAt, validated, validationCode, #type",
            "ExpressionAttributeNames": {"#type": "type", "#username": "username"},
            "ConsistentRead": True,
        }
        while True:
            page = gp_table.query(**kwargs)
            for it in page.get("Items") or []:
                if normalize_email(it.get("email") or "") == email_norm:
                    return it
            lek = page.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
    except ClientError as e:
        log("assign_email_query_failed", {"errorType": type(e).__name__, "gameId": game_id})
        raise
    return None


WEB_ASSIGN_GAME_TYPES = frozenset({"EMPAREJA2", "T1MER", "STACK", "RULET4"})


def _empareja2_web_assignment(player: dict) -> dict:
    from assigners.empareja2 import _character_image_url
    blob = (player.get("type") or {}).get("EMPAREJA2") or {}
    cname = blob.get("characterName")
    partner = blob.get("partnerCharacterName")
    code = player.get("validationCode")
    return {
        "playerId": player.get("playerId"),
        "characterId": blob.get("characterId"),
        "characterName": cname,
        "partnerCharacterName": partner,
        "characterImageUrl": _character_image_url(cname) if cname else None,
        "partnerCharacterImageUrl": _character_image_url(partner) if partner else None,
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
    }


def _t1mer_web_assignment(player: dict) -> dict:
    code = player.get("validationCode")
    display = (player.get("username") or "").strip()
    if not display:
        email = (player.get("email") or "").strip()
        display = email.split("@", 1)[0] if email else f"Jugador {player.get('playerId', '')}"
    blob = (player.get("type") or {}).get("T1MER") or {}
    score_raw = blob.get("score")
    timer_raw = blob.get("timer")
    out = {
        "playerId": player.get("playerId"),
        "displayName": display,
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
    }
    if score_raw is not None:
        out["score"] = int(score_raw) if not isinstance(score_raw, Decimal) else int(score_raw)
    if timer_raw is not None:
        out["timer"] = float(timer_raw) if not isinstance(timer_raw, Decimal) else float(timer_raw)
    return out


def _web_assignment(game_type: str, player: dict) -> dict:
    if game_type == "RULET4":
        from stand_common.rulet4_gameplay import public_assignment
        return public_assignment(player, _t1mer_web_assignment(player)["displayName"])
    if game_type == "EMPAREJA2":
        return _empareja2_web_assignment(player)
    if game_type == "STACK":
        from stand_common.stack import public_assignment
        return public_assignment(player, gp_table)
    return _t1mer_web_assignment(player)


def _web_player_token(player: dict, game_id: str) -> str:
    pid = player.get("playerId")
    return issue_player_token(game_id, int(pid) if not isinstance(pid, Decimal) else int(pid), PLAYER_TOKEN_SECRET)


def _verified_email(event, game_id, values):
    receipt = event.get("email_verification")
    if not isinstance(receipt, str):
        raise VerificationError("VerificationRequired", 403)
    table = verification_table(dynamo_r)
    record = require_verified_registration(table, PLAYER_TOKEN_SECRET, game_id, values, receipt)
    return table, record


def _find_verified_player(table, game_id, email):
    mapping = table.get_item(
        Key={"id": identity_id(PLAYER_TOKEN_SECRET, game_id, email)}, ConsistentRead=True,
    ).get("Item")
    if not mapping:
        return _find_existing_player_by_email(game_id, email)
    player = gp_table.get_item(
        Key={"gameId": game_id, "playerId": int(mapping["playerId"])}, ConsistentRead=True,
    ).get("Item")
    if not player or normalize_email(player.get("email") or "") != email:
        log("verified_player_mapping_invalid", {"gameId": game_id})
        raise VerificationError("VerificationUnavailable", 503)
    return player


def _complete_verified_player(table, record, game_id, player_id):
    verified_at = datetime.fromtimestamp(int(record["verifiedAt"]), timezone.utc).isoformat().replace("+00:00", "Z")
    gp_table.update_item(
        Key={"gameId": game_id, "playerId": int(player_id)},
        UpdateExpression="SET emailVerifiedAt = if_not_exists(emailVerifiedAt, :at), emailVerificationMethod = :method",
        ConditionExpression="attribute_exists(playerId)",
        ExpressionAttributeValues={":at": verified_at, ":method": "email-code"},
    )
    complete_registration(table, record, player_id)


def _existing_web_response(table, record, game_id, game_type, player):
    _complete_verified_player(table, record, game_id, player["playerId"])
    return _json_sanitize({
        "ok": True, "created_new": False, "already_joined": True,
        "gameId": game_id, "gameType": game_type,
        "playerToken": _web_player_token(player, game_id),
        "assignment": _web_assignment(game_type, player),
    })


def _put_verified_player(table, record, player, max_players, now):
    """Commit identity, player and capacity together; a lost response is safely recoverable."""
    game_id = player["gameId"]
    challenge_id = record["id"].split(":", 1)[1]
    transactions = [
        {"Update": {
            "TableName": games_table.name, "Key": {"gameId": game_id},
            "UpdateExpression": "SET lastJoinAt = :now, updatedAt = :now ADD playersCount :one",
            "ConditionExpression": (
                "attribute_exists(gameId) AND (attribute_not_exists(playersCount) OR playersCount < :max) "
                "AND (attribute_not_exists(isActive) OR isActive = :active)"
            ),
            "ExpressionAttributeValues": {":now": now, ":one": 1, ":max": max_players, ":active": True},
        }},
        {"Put": {
            "TableName": gp_table.name, "Item": player,
            "ConditionExpression": "attribute_not_exists(playerId)",
        }},
        {"Put": {
            "TableName": table.name,
            "Item": {"id": identity_id(PLAYER_TOKEN_SECRET, game_id, player["email"]), "playerId": player["playerId"]},
            "ConditionExpression": "attribute_not_exists(id)",
        }},
        {"ConditionCheck": {
            "TableName": table.name, "Key": {"id": record["recipientId"]},
            "ConditionExpression": "challengeId = :cid",
            "ExpressionAttributeValues": {":cid": challenge_id},
        }},
        {"ConditionCheck": {
            "TableName": table.name, "Key": {"id": record["id"]},
            "ConditionExpression": "attribute_exists(verifiedAt) AND expiresAt > :now AND attempts < :max",
            "ExpressionAttributeValues": {":now": int(time.time()), ":max": 5},
        }},
    ]
    try:
        table.meta.client.transact_write_items(TransactItems=transactions)
    except Exception as exc:
        response = getattr(exc, "response", {})
        if response.get("Error", {}).get("Code") == "TransactionCanceledException":
            reasons = response.get("CancellationReasons") or []
            failed = {index for index, reason in enumerate(reasons) if reason.get("Code") == "ConditionalCheckFailed"}
            if 3 in failed:
                raise VerificationError("VerificationSuperseded", 409) from exc
            if 4 in failed:
                raise VerificationError("VerificationExpired", 410) from exc
            if 2 in failed:
                return "exists"
            if 0 in failed:
                current = games_table.get_item(Key={"gameId": game_id}, ConsistentRead=True).get("Item")
                if not current:
                    raise VerificationError("GameNotFound", 404) from exc
                if not current.get("isActive", True):
                    raise VerificationError("GameInactive", 403) from exc
                return "full"
            if 1 in failed:
                return "collision"
        raise
    return "created"


def _handle_web_assign(event: dict) -> dict:
    game_id = (event or {}).get("game_id", "").strip()

    if not game_id:
        return {"ok": False, "reason": "missing_game_id"}

    meta = _get_game_meta(game_id)
    if not meta:
        return {"ok": False, "reason": "game_not_found"}
    if not meta.get("isActive", True):
        return {"ok": False, "reason": "game_inactive"}

    game_type = (meta.get("gameType") or "UNKNOWN").upper()
    if game_type not in WEB_ASSIGN_GAME_TYPES:
        return {"ok": False, "reason": "unsupported_game_type", "gameType": game_type}
    try:
        values = registration_values(meta, event)
    except RegistrationError as exc:
        return {"ok": False, "reason": str(exc)}
    email = values.get("email", "")
    phone = values.get("phone")
    username = values.get("username")

    verified_table, verified_record = _verified_email(event, game_id, values) if email else (None, None)
    existing = _find_verified_player(verified_table, game_id, email) if email else None
    if existing:
        return _existing_web_response(verified_table, verified_record, game_id, game_type, existing)

    max_players_raw = meta.get("maxPlayers") or 9999
    max_players = int(max_players_raw) if not isinstance(max_players_raw, Decimal) else int(max_players_raw)
    now = _iso_now()
    if not email:
        try:
            _ensure_game_counters(game_id)
        except Exception as e:
            log("assign_ensure_counters_failed", {"error": repr(e), "gameId": game_id})
        if not _reserve_player_slot(game_id, max_players, now):
            return {"ok": False, "reason": "player_limit_reached", "maxPlayers": max_players}

    for _ in range(6):
        new_pid = int(_get_last_player_id(game_id)) + 1
        base_item = {
            "gameId": game_id,
            "playerId": new_pid,
            "joinSource": "web",
            "joinedAt": now,
            "validated": False,
            "validationCode": _code4(),
        }
        if email:
            base_item["email"] = email
            base_item["emailVerifiedAt"] = datetime.fromtimestamp(
                int(verified_record["verifiedAt"]), timezone.utc,
            ).isoformat().replace("+00:00", "Z")
            base_item["emailVerificationMethod"] = "email-code"
        if phone:
            base_item["phone"] = phone
        if username:
            base_item["username"] = username

        assigner = ASSIGNERS.get(game_type, assign_generic)
        patch = assigner({
            "gameId": game_id,
            "playerId": new_pid,
            "gameType": game_type,
            "gameMeta": meta,
            "now": now,
            "dynamo_r": dynamo_r,
            "log_fn": log,
            "validationCode": base_item["validationCode"],
        })
        if patch:
            base_item.update(patch)
        t = base_item.get("type") or {}
        for blob in t.values():
            if isinstance(blob, dict):
                blob["raffleEligible"] = False

        try:
            if email:
                outcome = _put_verified_player(verified_table, verified_record, base_item, max_players, now)
                if outcome == "collision":
                    continue
                if outcome == "full":
                    return {"ok": False, "reason": "player_limit_reached", "maxPlayers": max_players}
                if outcome == "exists":
                    # Another confirmation won the same identity; recover it without reserving another slot.
                    winner = _find_verified_player(verified_table, game_id, email)
                    if not winner:
                        raise VerificationError("VerificationUnavailable", 503)
                    return _existing_web_response(verified_table, verified_record, game_id, game_type, winner)
                complete_registration(verified_table, verified_record, new_pid)
            else:
                _put_player(base_item)
            log("assign_web_ok", {"gameId": game_id, "playerId": new_pid, "gameType": game_type})
            return _json_sanitize({
                "ok": True,
                "created_new": True,
                "gameId": game_id,
                "gameType": game_type,
                "playerToken": _web_player_token(base_item, game_id),
                "assignment": _web_assignment(game_type, base_item),
            })
        except ClientError as e:
            if email:
                raise
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                continue
            if not email:
                try:
                    _rollback_player_slot(game_id, now)
                except Exception as re:
                    log("assign_playersCount_rollback_failed", {"error": repr(re), "gameId": game_id})
            return {"ok": False, "reason": "put_failed", "detail": str(e)}

    try:
        _rollback_player_slot(game_id, now)
    except Exception as re:
        log("assign_playersCount_rollback_failed", {"error": repr(re), "gameId": game_id})
    return {"ok": False, "reason": "race_condition_retry_exhausted"}


def lambda_handler(event, context):
    try:
        source = ((event or {}).get("source") or "").strip().lower()
        if source == "web":
            return _handle_web_assign(event)
        return {"ok": False, "reason": "unsupported_source"}
    except VerificationError as e:
        return {"ok": False, "reason": e.code}
    except Exception as e:
        log("assign_internal_error", {"errorType": type(e).__name__})
        return {"ok": False, "reason": "internal_error", "detail": repr(e)}
