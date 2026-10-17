import os
import json
import random
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import log, _json_sanitize, _iso_now, get_game_type_blob, normalize_email, issue_player_token
from stand_common.game_access import resolve_game_access
from stand_common.player_access import (
    ACCESS_LINK_FAILED_TEXT,
    access_link_message,
    access_link_ttl_seconds,
    issue_access_link,
)
from stand_common.email_verification import (
    VerificationError, RegistrationError, registration_values, verification_table,
    require_verified_registration, complete_registration, identity_id,
)

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "instagram-sender")
QUIZ_QUEUE_URL = os.environ.get("QUIZ_QUEUE_URL", "")
PLAYER_TOKEN_SECRET = os.environ.get("PLAYER_TOKEN_SECRET", "stand-dev-player-token-v1")


# Optional GSI
GSI_INSTAGRAM_PSID = os.environ.get("GSI_INSTAGRAM_PSID", "gsi-instagramPSID")

# ========= AWS =========
dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
sqs_client = boto3.client("sqs")

games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)

def _code4() -> int:
    return random.randint(1000, 9999)

def _send_bulk_messages(messages):
    """
    messages: [{ "psid": "...", "text": "..." }, { "psid": "...", "image_url": "..." }, ...]
    """
    if not messages:
        return

    payload = {"messages": messages}
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("assign_send_bulk_error", {"error": repr(e), "count": len(messages)})

def _send_single_dm(psid: str, text: str):
    if not psid:
        return
    _send_bulk_messages([{"psid": psid, "text": text}])


def _raffle_required_follows(meta: dict) -> list[str]:
    """List of handles (no @) for raffle follow requirement."""
    follows = (meta or {}).get("raffleRequiredFollows") or []
    if not isinstance(follows, list):
        follows = []
    return [str(h).strip().lower().lstrip("@") for h in follows if h and str(h).strip()]


def _raffle_follow_messages(psid: str, meta: dict) -> list[dict]:
    handles = _raffle_required_follows(meta)
    if not handles or not psid:
        return []
    return [
        {"psid": psid, "text": "⚠️ Para participar en el sorteo, sigue estas cuentas:"},
        {"psid": psid, "template": "follow_accounts", "handles": handles},
    ]


def _send_code_then_raffle(psid: str, code_text: str, meta: dict) -> None:
    messages = [{"psid": psid, "text": code_text}]
    messages.extend(_raffle_follow_messages(psid, meta))
    _send_bulk_messages(messages)


def _is_mobile_play(meta: dict) -> bool:
    return resolve_game_access(meta)["playMode"] == "mobile"


def _quiz_completed(player: dict, game_type: str) -> bool:
    return bool(get_game_type_blob(player or {}, game_type).get("quizCompleted"))


def _send_access_link_then_raffle(psid: str, game_id: str, player_id, meta: dict, intro_text: str | None = None) -> bool:
    """Mobile play: (optional intro) + personal one-use link + raffle follows, for the existing player."""
    messages = [{"psid": psid, "text": intro_text}] if intro_text else []
    ttl = access_link_ttl_seconds()
    try:
        url = issue_access_link(gp_table, game_id, int(player_id), PLAYER_TOKEN_SECRET, ttl_seconds=ttl)
    except Exception as e:
        # Never log the link/token itself.
        log("assign_access_link_failed", {"error": type(e).__name__, "gameId": game_id, "playerId": player_id})
        messages.append({"psid": psid, "text": ACCESS_LINK_FAILED_TEXT})
        _send_bulk_messages(messages)
        return False
    messages.append(access_link_message(psid, url, ttl))
    messages.extend(_raffle_follow_messages(psid, meta))
    _send_bulk_messages(messages)
    return True


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
    Query descending by SK to get the last assigned playerId.
    Returns 0 if none.
    """
    resp = gp_table.query(
        KeyConditionExpression=Key("gameId").eq(game_id),
        ProjectionExpression="playerId",
        ScanIndexForward=False,
        Limit=1,
        ConsistentRead=True,
    )
    items = resp.get("Items") or []
    if not items:
        return 0
    pid = items[0].get("playerId")
    return int(pid) if not isinstance(pid, Decimal) else int(pid)

def _find_existing_player_by_psid(game_id: str, psid: str) -> dict | None:
    """
    Preferred: query GSI instagramPSID (PK) + gameId (SK).
    Fallback: query the partition and search (OK for MVP; partitions are small).
    """
    # Try GSI first
    try:
        resp = gp_table.query(
            IndexName=GSI_INSTAGRAM_PSID,
            KeyConditionExpression=Key("instagramPSID").eq(psid) & Key("gameId").eq(game_id),
            Limit=1,
        )
        items = resp.get("Items") or []
        return items[0] if items else None
    except ClientError as e:
        # Index might not exist yet -> fallback
        log("assign_psid_gsi_unavailable_fallback", {"error": str(e), "index": GSI_INSTAGRAM_PSID})

    # Fallback: query partition and scan in-memory (no Dynamo Scan)
    try:
        kwargs = {
            "KeyConditionExpression": Key("gameId").eq(game_id),
            "ProjectionExpression": "gameId, playerId, instagramPSID, instagramUsername, instagramProfilePicUrl, joinedAt, validated, validationCode, type",
        }
        while True:
            page = gp_table.query(**kwargs)
            for it in page.get("Items") or []:
                if (it.get("instagramPSID") or "") == psid:
                    return it
            lek = page.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
    except ClientError as e:
        log("assign_psid_fallback_query_failed", {"error": str(e)})

    return None

def _put_player(item: dict) -> None:
    # Conditional protects races on playerId within same partition
    gp_table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(playerId)",
    )


def _enqueue_quiz_start(game_id: str, psid: str, delay_seconds: int = 3):
    """Send a quiz_start event to SQS with a short delay so the welcome DM
    has time to be delivered before the quiz intro arrives."""
    if not QUIZ_QUEUE_URL:
        log("assign_quiz_queue_missing", {"gameId": game_id})
        return
    try:
        sqs_client.send_message(
            QueueUrl=QUIZ_QUEUE_URL,
            MessageBody=json.dumps({
                "kind": "quiz_start",
                "gameId": game_id,
                "psid": [psid],
            }, ensure_ascii=False),
            DelaySeconds=delay_seconds,
        )
    except Exception as e:
        log("assign_quiz_enqueue_error", {"error": repr(e), "gameId": game_id})


# Code tail por juego (solo para requiresValidation = True). Fallback para INFOCARDS/UNKNOWN.
CODE_TAIL_TEMPLATES = {
    "EMPAREJA2": "🎟️ Tu código: {code}\n\nCuando encuentres a tu pareja, introducid los dos códigos en la pantalla para validar.",
    "CUPIDO": "🎟️ Tu código: {code}\n\nCuando encuentres a tu pareja, introducid los dos códigos en la pantalla para validar.",
    "SEMAFORO": "🎟️ Tu código: {code}\n\nCuando os encontréis, introducid los dos códigos en la pantalla para validar.",
    "L3TRAS": "🎟️ Tu código: {code}\n\nReuníos y juntad todos los códigos en la pantalla para formar la palabra.",
    "T1MER": "🎟️ Tu código: {code}\n\nIntrodúcelo en la pantalla para activar tu temporizador.",
    "RULET4": "🎟️ Tu código: {code}\n\nIntrodúcelo en la pantalla para activar la ruleta.",
}
CODE_TAIL_DEFAULT = "🎟️ Tu código: {code}\n\nVe a la pantalla, introdúcelo y ¡a jugar! 🚀"


def _code_tail(code, game_type: str = "", letter: str | None = None) -> str:
    """Mensaje del código de validación; texto adaptado al tipo de juego (requiresValidation = True)."""
    key = (game_type or "").upper().strip()
    if key == "L3TRAS" and letter:
        return (
            f"🎟️ Tu código: {code}\n"
            f"🔤 Tu letra: {letter}\n\n"
            "Reuníos y juntad todos los códigos en la pantalla para formar la palabra."
        )
    template = CODE_TAIL_TEMPLATES.get(key, CODE_TAIL_DEFAULT)
    return template.format(code=code)


# Empareja2: aviso cuando hay quiz (el código llega tras responder)
EMPAREJA2_QUIZ_NOTICE = "Para conseguir tu código de validación necesitas responder unas preguntas."
# Mobile play: instructions point to the personal link, never to the master screen
EMPAREJA2_MOBILE_NOTICE = "Abre tu enlace personal para ver tu código y validar con tu pareja desde el móvil."
MOBILE_QUIZ_NOTICE = "Para conseguir tu enlace personal para jugar necesitas responder unas preguntas."


def _requires_validation(meta: dict, game_type: str) -> bool:
    """True if game requires validation code; False for no-validation games (e.g. INFOCARDS)."""
    # Prefer top-level requiresValidation; fallback for legacy games without the field
    if "requiresValidation" in (meta or {}):
        val = meta.get("requiresValidation")
    else:
        val = None
    if val is None:
        val = False if (game_type or "").upper() == "INFOCARDS" else True
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes")
    return bool(val)


def _quiz_completed_message(meta: dict, game_type: str) -> str:
    """Message to send when no validation; from blob or default."""
    blob = get_game_type_blob(meta or {}, game_type)
    msg = (blob.get("quizCompletedMessage") or "").strip()
    return msg if msg else "¡Gracias! Ya estás participando en el sorteo."


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
            "ProjectionExpression": "gameId, playerId, email, instagramUsername, instagramProfilePicUrl, instagramPSID, joinedAt, validated, validationCode, #type",
            "ExpressionAttributeNames": {"#type": "type"},
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


WEB_ASSIGN_GAME_TYPES = frozenset({"EMPAREJA2", "T1MER", "STACK"})


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
    display = (player.get("instagramUsername") or "").strip()
    if not display:
        email = (player.get("email") or "").strip()
        display = email.split("@", 1)[0] if email else f"Jugador {player.get('playerId', '')}"
    blob = (player.get("type") or {}).get("T1MER") or {}
    score_raw = blob.get("score")
    timer_raw = blob.get("timer")
    pic = (player.get("instagramProfilePicUrl") or "").strip() or None
    out = {
        "playerId": player.get("playerId"),
        "displayName": display,
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
        "profilePicUrl": pic,
    }
    if score_raw is not None:
        out["score"] = int(score_raw) if not isinstance(score_raw, Decimal) else int(score_raw)
    if timer_raw is not None:
        out["timer"] = float(timer_raw) if not isinstance(timer_raw, Decimal) else float(timer_raw)
    return out


def _web_assignment(game_type: str, player: dict) -> dict:
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
    # Instagram-channel games register only via DM (keeps PSID identity and quiz gating).
    if resolve_game_access(meta)["joinChannel"] != "web":
        return {"ok": False, "reason": "web_join_disabled", "gameType": game_type}

    try:
        values = registration_values(meta, {**event, "instagram": event.get("instagram_username")})
    except RegistrationError as exc:
        return {"ok": False, "reason": str(exc)}
    email = values.get("email", "")
    ig_handle = values.get("instagram")
    phone = values.get("phone")

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
        display_name = f"@{ig_handle}" if ig_handle else (email.split("@", 1)[0] if email else f"Jugador {new_pid}")
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
        if ig_handle:
            base_item["instagramUsername"] = f"@{ig_handle}"
        if phone:
            base_item["phone"] = phone

        assigner = ASSIGNERS.get(game_type, assign_generic)
        patch, _, _ = assigner({
            "gameId": game_id,
            "playerId": new_pid,
            "psid": None,
            "username_at": display_name,
            "display_name": display_name,
            "gameType": game_type,
            "gameMeta": meta,
            "now": now,
            "dynamo_r": dynamo_r,
            "lambda_client": lambda_client,
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
        source = ((event or {}).get("source") or "instagram").strip().lower()
        if source == "web":
            return _handle_web_assign(event)
        return _handle_instagram_assign(event)
    except VerificationError as e:
        return {"ok": False, "reason": e.code}
    except Exception as e:
        log("assign_internal_error", {"errorType": type(e).__name__})
        return {"ok": False, "reason": "internal_error", "detail": repr(e)}


def _handle_instagram_assign(event: dict) -> dict:
    """
    Expects dispatcher event:
    {
      "psid": "USER_PSID",
      "username_at": "@usuario" | None,
      "instagram_profile_pic_url": "https://..." | None,
      "game_id": "K9R4QM",
      "raw_event": {...}
    }
    """
    try:
        psid = (event or {}).get("psid")
        username_at = (event or {}).get("username_at")
        profile_pic_raw = (event or {}).get("instagram_profile_pic_url")
        profile_pic_url = (str(profile_pic_raw).strip() if profile_pic_raw else "") or None
        game_id = (event or {}).get("game_id")

        # 1) sanity
        if not psid or not game_id:
            log("assign_bad_event", {"reason": "missing psid or game_id", "event": event})
            return {"ok": False, "reason": "missing psid or game_id"}

        if not username_at:
            _send_single_dm(
                psid,
                "Necesito tu usuario de Instagram para continuar 😅\n"
                "Responde con tu @usuario exactamente como aparece en Instagram."
            )
            log("assign_missing_username", {"psid": psid, "gameId": game_id})
            return {"ok": False, "reason": "missing username_at"}

        # 2) load game meta
        meta = _get_game_meta(game_id)
        if not meta:
            _send_single_dm(psid, "Ese juego no existe o ya no está disponible.")
            return {"ok": False, "reason": "game_not_found"}

        if not meta.get("isActive", True):
            _send_single_dm(psid, "Este juego ya no está activo.")
            return {"ok": False, "reason": "game_inactive"}

        game_type = (meta.get("gameType") or "UNKNOWN").upper()
        max_players_raw = meta.get("maxPlayers") or 9999
        max_players = int(max_players_raw) if not isinstance(max_players_raw, Decimal) else int(max_players_raw)

        # 3) idempotency: if already joined, do not create again
        existing = _find_existing_player_by_psid(game_id, psid)
        if existing:
            pid = existing.get("playerId")
            already_in = "Ya estás dentro ✅\nContinúa jugando! 🚀"
            quiz_enabled = bool(meta.get("quizOrder"))
            if _is_mobile_play(meta):
                # Same PSID player: re-issue a personal link (no new player, no attempt consumed).
                if quiz_enabled and not _quiz_completed(existing, game_type):
                    _send_single_dm(psid, already_in)
                    _enqueue_quiz_start(game_id, psid)
                else:
                    _send_access_link_then_raffle(psid, game_id, pid, meta, intro_text=already_in)
            elif quiz_enabled:
                _send_single_dm(psid, already_in)
                _enqueue_quiz_start(game_id, psid)
            else:
                _send_single_dm(psid, already_in)
                if _requires_validation(meta, game_type):
                    code = existing.get("validationCode")
                    if code is not None:
                        letter = None
                        if game_type == "L3TRAS":
                            letter = (((existing.get("type") or {}).get("L3TRAS") or {}).get("letter") or None)
                        _send_code_then_raffle(psid, _code_tail(code, game_type, letter=letter), meta)
                else:
                    _send_single_dm(psid, _quiz_completed_message(meta, game_type))
            log("assign_already_joined", {"gameId": game_id, "psid": psid, "playerId": pid})
            return _json_sanitize({
                "ok": True,
                "created_new": False,
                "already_joined": True,
                "gameId": game_id,
                "gameType": game_type,
                "player": existing,
            })

        # 4) counters + enforce maxPlayers (atómico, sin contar en gp_table)
        attempts = 6
        now = _iso_now()

        # compat: games antiguos pueden no tener counters
        try:
            _ensure_game_counters(game_id)
        except Exception as e:
            log("assign_ensure_counters_failed", {"error": repr(e), "gameId": game_id})

        reserved = _reserve_player_slot(game_id, max_players, now)
        if not reserved:
            _send_single_dm(
                psid,
                "Este juego ha alcanzado el límite de jugadores 😅\n"
                "Pide al organizador que amplíe el límite (plan 24h) o cree otra partida."
            )
            log("assign_limit_reached_atomic", {"gameId": game_id, "max": max_players})
            return {"ok": False, "reason": "player_limit_reached", "maxPlayers": max_players}

        # 5) allocate playerId + write (retry on race)
        for _ in range(attempts):
            last_pid = _get_last_player_id(game_id)
            new_pid = int(last_pid) + 1

            base_item = {
                "gameId": game_id,
                "playerId": new_pid,
                "instagramPSID": psid,
                "instagramUsername": username_at,
                "joinedAt": now,
                "validated": False,
                "validationCode": _code4(),
            }
            if profile_pic_url:
                base_item["instagramProfilePicUrl"] = profile_pic_url

            assigner = ASSIGNERS.get(game_type, assign_generic)
            ctx = {
                "gameId": game_id,
                "playerId": new_pid,
                "psid": psid,
                "username_at": username_at,
                "gameType": game_type,
                "gameMeta": meta,
                "now": now,
                "dynamo_r": dynamo_r,
                "lambda_client": lambda_client,
                "log_fn": log,
                "validationCode": base_item["validationCode"],
            }

            # assigner contract: (patch, welcome_header, extra_messages)
            patch, welcome_header, extra_messages = assigner(ctx)

            # merge patch into base_item BEFORE writing
            if patch:
                for k, v in patch.items():
                    base_item[k] = v
            if game_type == "STACK":
                base_item["type"]["STACK"]["raffleEligible"] = False

            try:
                _put_player(base_item)

                quiz_enabled = bool(meta.get("quizOrder"))
                mobile = _is_mobile_play(meta)

                if mobile:
                    # Mobile play: same images/assignment/quiz order; finish with a personal link.
                    if extra_messages:
                        _send_bulk_messages(extra_messages)
                    if game_type == "EMPAREJA2":
                        time.sleep(2)
                    if quiz_enabled:
                        tail = MOBILE_QUIZ_NOTICE
                    else:
                        tail = EMPAREJA2_MOBILE_NOTICE if game_type == "EMPAREJA2" else ""
                    intro = "\n\n".join(p.strip() for p in (welcome_header or "", tail) if p and p.strip()) or None
                    if quiz_enabled:
                        if intro:
                            _send_single_dm(psid, intro)
                        _enqueue_quiz_start(game_id, psid)
                    else:
                        _send_access_link_then_raffle(psid, game_id, new_pid, meta, intro_text=intro)
                elif game_type == "EMPAREJA2":
                    # 1) Image first, 2) wait so image is delivered first, 3) single message: welcome + (quiz notice or code)
                    if extra_messages:
                        _send_bulk_messages(extra_messages)
                    time.sleep(2)
                    tail = EMPAREJA2_QUIZ_NOTICE if quiz_enabled else _code_tail(base_item["validationCode"], game_type)
                    if welcome_header:
                        msg = welcome_header + "\n\n" + tail
                        if quiz_enabled:
                            _send_single_dm(psid, msg)
                        else:
                            _send_code_then_raffle(psid, msg, meta)
                    if quiz_enabled:
                        _enqueue_quiz_start(game_id, psid)
                else:
                    # Generic flow: extra_messages, welcome_header, then quiz or code
                    if extra_messages:
                        _send_bulk_messages(extra_messages)
                    if welcome_header:
                        _send_single_dm(psid, welcome_header)
                    if quiz_enabled:
                        _enqueue_quiz_start(game_id, psid)
                    else:
                        if _requires_validation(meta, game_type):
                            letter = None
                            if game_type == "L3TRAS":
                                letter = (((base_item.get("type") or {}).get("L3TRAS") or {}).get("letter") or None)
                            _send_code_then_raffle(
                                psid,
                                _code_tail(base_item["validationCode"], game_type, letter=letter),
                                meta,
                            )
                        else:
                            _send_single_dm(psid, _quiz_completed_message(meta, game_type))
                log("assign_ok", {
                    "gameId": game_id,
                    "psid": psid,
                    "username_at": username_at,
                    "gameType": game_type,
                    "playerId": new_pid,
                    "created_new": True,
                    "quiz_enabled": quiz_enabled,
                })

                safe = _json_sanitize(base_item)
                return {
                    "ok": True,
                    "created_new": True,
                    "gameId": game_id,
                    "gameType": game_type,
                    "player": safe,
                    "quizEnabled": quiz_enabled,
                }

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    # race on playerId -> retry (NO rollback, already reserved slot)
                    continue

                # rollback reserva (best-effort)
                try:
                    _rollback_player_slot(game_id, now)
                except Exception as re:
                    log("assign_playersCount_rollback_failed", {"error": repr(re), "gameId": game_id})

                log("assign_put_failed", {"error": str(e), "gameId": game_id})
                return {"ok": False, "reason": "put_failed", "detail": str(e)}
        # si agotamos retries por race: rollback reserva
        try:
            _rollback_player_slot(game_id, now)
        except Exception as re:
            log("assign_playersCount_rollback_failed", {"error": repr(re), "gameId": game_id})

        log("assign_race_exhausted", {"gameId": game_id, "psid": psid})
        return {"ok": False, "reason": "race_condition_retry_exhausted"}


    except Exception as e:
        log("assign_internal_error", {"error": repr(e)})
        return {"ok": False, "reason": "internal_error", "detail": repr(e)}
