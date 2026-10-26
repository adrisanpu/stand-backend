import os
import json
import random
import uuid
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.types import TypeSerializer
from stand_common.stack import game_config as stack_game_config, validate_config as validate_stack_config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import (
    log,
    _json_sanitize,
    _resp,
    _iso_now,
    _parse_iso,
    _get_claims,
    _read_json_body,
    set_game_type_blob,
    JOIN_CODE_ALPHABET,
    JOIN_CODE_LENGTH,
    is_valid_join_code,
    normalize_participant_fields,
)
from stand_common.game_access import (
    GameAccessError,
    access_for_create,
    access_for_update,
    resolve_game_access,
)
from stand_common.email_verification import identity_id
from stand_common.prizes import (
    MAX_PRIZE_IMAGE_BYTES,
    allowed_prize_slots,
    serialize_prize_catalog,
    staged_prize_image_key,
    validate_png,
    validate_prize_catalog,
)

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
USERS_TABLE = os.environ.get("USERS_TABLE", "stand-prod-user-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
EMAIL_VERIFICATION_TABLE = os.environ.get("EMAIL_VERIFICATION_TABLE", "")
PLAYER_TOKEN_SECRET = os.environ.get("PLAYER_TOKEN_SECRET", "stand-dev-player-token-v1")
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "stand-prod-catalog-table")
USER_MEDIA_BUCKET = os.environ.get("USER_MEDIA_BUCKET", "")

# GSI names (recommended)
GSI_OWNER = os.environ.get("GSI_OWNER", "gsi-ownerUserId")
GSI_GAMENAME = os.environ.get("GSI_GAMENAME", "gsi-gameName")  # optional; only if you created it

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")
games_table = dynamodb.Table(GAMES_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
gp_table = dynamodb.Table(GAMEPLAYER_TABLE)
catalog_table = dynamodb.Table(CATALOG_TABLE)
prize_s3 = boto3.client("s3")

# ========= CONST =========
SUPPORTED_GAME_TYPES = {"EMPAREJA2", "T1MER", "RULET4", "L3TRAS", "SEMAFORO", "INFOCARDS", "IMPOSTOR", "STACK"}

JOIN_CODE_MAX_ATTEMPTS = 10

def _get_query_params(event):
    return event.get("queryStringParameters") or {}

def _is_user_pro(user_item: dict) -> bool:
    # ponytail: dev stack skips billing limits so QA can create multiple games without Stripe
    if os.environ.get("STAGE") == "dev":
        return True
    if not user_item:
        return False
    plan = (user_item.get("plan") or "FREE").upper().strip()
    if plan == "FREE":
        return False
    au = _parse_iso(user_item.get("activeUntil"))
    if not au:
        return False
    return au > datetime.now(timezone.utc)

def _get_user(user_id: str):
    resp = users_table.get_item(Key={"userId": user_id})
    return resp.get("Item")

def _new_join_code(length: int = JOIN_CODE_LENGTH) -> str:
    return "".join(random.choice(JOIN_CODE_ALPHABET) for _ in range(length))

def _public_game_shape(item: dict, include_prize_catalog: bool = True) -> dict:
    # Safe shape for clients/players
    shape = {
        "gameId": item.get("gameId"),
        "gameName": item.get("gameName"),
        "gameType": item.get("gameType"),
        "createdAt": item.get("createdAt"),
        "maxPlayers": item.get("maxPlayers"),
        "participantFields": normalize_participant_fields(item.get("gameType") or "", item.get("participantFields")),
    }
    shape = {k: v for k, v in shape.items() if v is not None}
    # Always the effective (legacy-resolved) pair, so clients never guess defaults.
    shape.update(resolve_game_access(item))
    if include_prize_catalog and "prizeCatalog" in item:
        shape["prizeCatalog"] = _serialize_prize_catalog(item)
    return shape


def _serialize_prize_catalog(item: dict) -> dict:
    return serialize_prize_catalog(
        item.get("prizeCatalog"),
        item.get("ownerUserId") or "",
        item.get("gameId") or "",
        USER_MEDIA_BUCKET,
        prize_s3.generate_presigned_url,
    )


def _get_game(game_id: str):
    return games_table.get_item(Key={"gameId": game_id}).get("Item")


def _read_verified_prize_image(key: str) -> bytes:
    try:
        head = prize_s3.head_object(Bucket=USER_MEDIA_BUCKET, Key=key)
        size = head.get("ContentLength")
        if type(size) is not int or size < 1 or size > MAX_PRIZE_IMAGE_BYTES:
            raise ValueError("Prize image must be between 1 byte and 5 MB.")
        response = prize_s3.get_object(Bucket=USER_MEDIA_BUCKET, Key=key)
        body = response["Body"]
        try:
            data = bytearray()
            while len(data) <= MAX_PRIZE_IMAGE_BYTES:
                chunk = body.read(min(64 * 1024, MAX_PRIZE_IMAGE_BYTES + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
        finally:
            close = getattr(body, "close", None)
            if close:
                close()
        if len(data) != size:
            raise ValueError("Prize image size does not match the uploaded object.")
        image = bytes(data)
        validate_png(image)
        return image
    except ClientError as exc:
        code = (exc.response.get("Error") or {}).get("Code")
        if code in {"NoSuchKey", "NotFound", "404"}:
            raise ValueError("Prize image upload was not found.") from exc
        log("Prize image object verification failed", {"key": key, "error": str(exc)})
        raise RuntimeError("Could not verify prize image upload.") from exc


def _handle_prize_upload_url(event):
    if not USER_MEDIA_BUCKET:
        return _resp(500, {"error": "Missing USER_MEDIA_BUCKET env var"})
    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"error": "Invalid JSON body"})
    game_id = body.get("gameId")
    slot = body.get("slot")
    file_name = body.get("fileName")
    content_type = body.get("contentType")
    size_bytes = body.get("sizeBytes")
    if not isinstance(game_id, str) or not is_valid_join_code(game_id.strip()):
        return _resp(400, {"error": "Invalid gameId."})
    game_id = game_id.strip()
    if not isinstance(slot, str) or slot not in {"validation", "ranking", "raffle"}:
        return _resp(400, {"error": "slot must be validation, ranking, or raffle."})
    if not isinstance(file_name, str) or not file_name.strip():
        return _resp(400, {"error": "fileName is required."})
    if content_type != "image/png":
        return _resp(400, {"error": "contentType must be image/png."})
    if type(size_bytes) is not int or size_bytes < 1 or size_bytes > MAX_PRIZE_IMAGE_BYTES:
        return _resp(400, {"error": "Prize image must be between 1 byte and 5 MB."})

    item = _get_game(game_id)
    if not item:
        return _resp(404, {"error": "GameNotFound", "gameId": game_id})
    if (item.get("ownerUserId") or "") != owner_user_id:
        return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})
    raffle_enabled = _is_user_pro(_get_user(owner_user_id))
    if slot not in allowed_prize_slots(item.get("gameType") or "", raffle_enabled):
        return _resp(400, {"error": f"The {slot} prize is not supported for this game or plan."})

    key = f"games/staged/{owner_user_id}/{game_id}/{slot}/{uuid.uuid4()}.png"
    try:
        upload_url = prize_s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": USER_MEDIA_BUCKET,
                "Key": key,
                "ContentType": "image/png",
                "Tagging": "staged=true",
            },
            ExpiresIn=3600,
        )
    except ClientError as exc:
        log("Prize image upload URL creation failed", {"gameId": game_id, "error": str(exc)})
        return _resp(500, {"error": "UploadUrlCreationFailed"})
    return _resp(200, {"ok": True, "key": key, "uploadUrl": upload_url})

def _quiz_enabled_from_game(game_item: dict) -> bool:
    order = game_item.get("quizOrder") or []
    return isinstance(order, list) and len(order) > 0

def _raffle_winner_row(winner) -> dict | None:
    if not isinstance(winner, dict):
        return None
    username = winner.get("username")
    if not (isinstance(username, str) and username.strip()):
        # Winners stored before the Web-only migration only kept their historical display name here.
        username = winner.get("instagramUsername")
    row = {
        "playerId": winner.get("playerId"),
        "username": username.strip() if isinstance(username, str) and username.strip() else None,
        "wonAt": winner.get("wonAt"),
    }
    if winner.get("validationCode") is not None:
        row["validationCode"] = winner.get("validationCode")
    return row


def _raffle_winners_from_game(game_item: dict) -> list:
    winners = game_item.get("raffleWinners") or []
    if not isinstance(winners, list):
        return []
    return [row for row in (_raffle_winner_row(w) for w in winners) if row is not None]


def _get_player_prize_delivered(game_id: str, player_id: int, game_type: str) -> bool:
    """Read type.<gameType>.prizeDelivered for a specific player, default False."""
    try:
        resp = gp_table.get_item(Key={"gameId": game_id, "playerId": int(player_id)})
    except ClientError as e:
        log("GetPlayerFailed", {"error": str(e), "gameId": game_id, "playerId": player_id})
        return False
    it = resp.get("Item") or {}
    t = it.get("type") or {}
    blob = t.get((game_type or "").upper()) or {}
    return bool(blob.get("prizeDelivered", False))


EMPAREJA2_CATALOG_ID = "EMPAREJA2#CHARACTERS#v1"
_empareja2_pair_count_cache = None


def _empareja2_catalog_pair_count() -> int:
    """Count distinct pair groups in empareja2 character catalog. Cached per Lambda invocation."""
    global _empareja2_pair_count_cache
    if _empareja2_pair_count_cache is not None:
        return _empareja2_pair_count_cache
    items = []
    kwargs = {"KeyConditionExpression": Key("catalogId").eq(EMPAREJA2_CATALOG_ID)}
    while True:
        resp = catalog_table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    seen = set()
    for it in items:
        g = it.get("pairGroupId") or it.get("pairNumericId")
        if g is not None:
            seen.add(str(int(g)) if isinstance(g, Decimal) else str(g).strip())
    _empareja2_pair_count_cache = len(seen) if seen else 0
    return _empareja2_pair_count_cache

def _get_path(event):
    return (
        event.get("rawPath")
        or event.get("path")
        or event.get("requestContext", {}).get("http", {}).get("path")
        or ""
    )

def _load_default_quiz_from_catalog(game_type: str, game_id: str):
    """
    Returns (quizOrder, quizQuestions) or (None, None)
    """
    # TODO: now hardcoded to use always v1
    catalog_id = f"{game_type}#QUIZ#v1"

    items = []
    last = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("catalogId").eq(catalog_id),
        }
        if last:
            kwargs["ExclusiveStartKey"] = last

        resp = catalog_table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last = resp.get("LastEvaluatedKey")
        if not last:
            break

    if not items:
        return None, None

    # ordenar por orderIndex
    items.sort(key=lambda x: int(x.get("orderIndex", 0)))

    quiz_order = []
    quiz_questions = {}

    for it in items:
        qid = it.get("questionId")
        text = it.get("text")
        options = it.get("options") or []

        if not qid or not text or not options:
            continue

        quiz_order.append(qid)

        quiz_questions[qid] = {
            "text": text,
            "options": [
                {
                    "title": o["title"],
                    "payload": f"{game_id}_{qid}_{o['answerId']}",
                }
                for o in options
            ],
        }

    if not quiz_order:
        return None, None

    return quiz_order, quiz_questions


# ---------------- GET ----------------
def _handle_get(event):
    """
    GET /v1/game (AUTH REQUIRED for all cases)

    Supports (priority):
      1) ?gameId=XXXXXX   -> fetch by PK (owner only)
      2) ?gameName=...    -> fetch by GSI_GAMENAME (owner only, if exists)
      3) ?gameType=TYPE   -> list user's games by type
      4) (no params)      -> list all user's games
    """
    params = _get_query_params(event)
    body = _read_json_body(event) or {}

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    game_id   = (params.get("gameId")   or body.get("gameId")   or "").strip()
    game_name = (params.get("gameName") or body.get("gameName") or "").strip()
    game_type_raw = (params.get("gameType") or body.get("gameType") or "").strip()

    # ---------------- 1) gameId ----------------
    if game_id:
        try:
            resp = games_table.get_item(Key={"gameId": game_id})
            item = resp.get("Item")
        except ClientError as e:
            log("GetItem failed", {"error": str(e)})
            return _resp(500, {"error": "GetItemFailed", "detail": str(e)})

        if not item:
            return _resp(404, {"error": "GameNotFound", "gameId": game_id})

        # owner only
        if (item.get("ownerUserId") or "") != owner_user_id:
            return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})

        if not item.get("isActive", True):
            return _resp(403, {"error": "GameInactive", "gameId": game_id})

        game_shape = _public_game_shape(item, include_prize_catalog=False)
        payload = {"ok": True, "game": game_shape}
        if "prizeCatalog" in item:
            payload["prizeCatalog"] = _serialize_prize_catalog(item)
        if (item.get("gameType") or "").upper() == "EMPAREJA2":
            payload["settings"] = {"empareja2": (item.get("type") or {}).get("EMPAREJA2") or {}}
            payload["empareja2CatalogPairCount"] = _empareja2_catalog_pair_count()
        elif (item.get("gameType") or "").upper() == "L3TRAS":
            payload["settings"] = {"l3tras": (item.get("type") or {}).get("L3TRAS") or {}}
        elif (item.get("gameType") or "").upper() == "IMPOSTOR":
            payload["settings"] = {"impostor": (item.get("type") or {}).get("IMPOSTOR") or {}}
        elif (item.get("gameType") or "").upper() == "STACK":
            payload["game"]["type"] = {"STACK": stack_game_config(item)}
        return _resp(200, payload)

    # ---------------- 2) gameName (optional) ----------------
    if game_name:
        try:
            q = games_table.query(
                IndexName=GSI_GAMENAME,
                KeyConditionExpression=Key("gameName").eq(game_name),
                Limit=5,
            )
            items = q.get("Items", []) or []
        except ClientError as e:
            log("Query by gameName failed", {"error": str(e)})
            return _resp(500, {"error": "QueryFailed", "detail": str(e)})
        except Exception as e:
            log("Query by gameName error", {"error": repr(e)})
            return _resp(500, {"error": "GameNameIndexError", "detail": repr(e)})

        # filtra por owner
        items = [it for it in items if (it.get("ownerUserId") or "") == owner_user_id]

        if not items:
            return _resp(404, {"error": "GameNotFound", "gameName": game_name})

        item = items[0]
        if not item.get("isActive", True):
            return _resp(403, {"error": "GameInactive", "gameName": game_name})

        game_id = item.get("gameId")
        game_type = (item.get("gameType") or "").upper() or "UNKNOWN"

        # ✅ Cache counters en games_table (de assign/validate)
        player_count = item.get("playersCount", 0)
        validated_count = item.get("validatedCount", 0)

        # sanitiza Decimal -> int
        try:
            if isinstance(player_count, Decimal):
                player_count = int(player_count)
            else:
                player_count = int(player_count)
        except Exception:
            player_count = 0

        try:
            if isinstance(validated_count, Decimal):
                validated_count = int(validated_count)
            else:
                validated_count = int(validated_count)
        except Exception:
            validated_count = 0

        payload = {
            "ok": True,
            "game": _public_game_shape(item, include_prize_catalog=False),
            "playerCount": player_count,
            "validatedCount": validated_count,
            "raffleWinners": _raffle_winners_from_game(item),
            "quiz": {
                "enabled": _quiz_enabled_from_game(item),
                "questionsCount": len(item.get("quizOrder") or []) if _quiz_enabled_from_game(item) else 0
            }
        }
        if "prizeCatalog" in item:
            payload["prizeCatalog"] = _serialize_prize_catalog(item)
        # enrich raffleWinners with prizeDelivered flag per player
        try:
            game_type = (item.get("gameType") or "").upper()
            winners_with_flags = []
            for w in payload.get("raffleWinners") or []:
                pid = w.get("playerId")
                delivered = False
                if pid is not None:
                    delivered = _get_player_prize_delivered(game_id, int(pid), game_type)
                row = dict(w)
                row["prizeDelivered"] = delivered
                winners_with_flags.append(row)
            payload["raffleWinners"] = winners_with_flags
        except Exception as e:
            log("RaffleWinnersPrizeDeliveredEnrichFailed", {"error": repr(e), "gameId": game_id})
        if game_type == "EMPAREJA2":
            payload["settings"] = {"empareja2": (item.get("type") or {}).get("EMPAREJA2") or {}}
            payload["empareja2CatalogPairCount"] = _empareja2_catalog_pair_count()
        elif game_type == "IMPOSTOR":
            payload["settings"] = {"impostor": (item.get("type") or {}).get("IMPOSTOR") or {}}
        elif game_type == "STACK":
            payload["game"]["type"] = {"STACK": stack_game_config(item)}
        return _resp(200, payload)
    
    # ---------------- 3) & 4) list user's games ----------------
    game_type = game_type_raw.upper() if game_type_raw else None
    if game_type and game_type not in SUPPORTED_GAME_TYPES:
        return _resp(400, {
            "error": f"Unsupported gameType '{game_type_raw}'.",
            "supportedGameTypes": sorted(SUPPORTED_GAME_TYPES),
        })

    try:
        q = games_table.query(
            IndexName=GSI_OWNER,
            KeyConditionExpression=Key("ownerUserId").eq(owner_user_id),
        )
        items = q.get("Items") or []
    except ClientError as e:
        log("Owner query failed", {"error": str(e)})
        return _resp(500, {"error": "OwnerQueryFailed", "detail": str(e)})

    if game_type:
        items = [it for it in items if (it.get("gameType") or "").upper() == game_type]

    items.sort(key=lambda x: (x.get("createdAt") or ""), reverse=True)

    return _resp(200, {
        "ok": True,
        "ownerUserId": owner_user_id,
        "gameType": game_type,
        "count": len(items),
        "games": [_public_game_shape(it) for it in items],
    })


def _player_admin_game(event):
    params = _get_query_params(event)
    body = _read_json_body(event)
    if not isinstance(body, dict):
        body = {}
    game_id_raw = params.get("gameId") or body.get("gameId")
    if not isinstance(game_id_raw, str) or not game_id_raw.strip():
        return None, _resp(400, {"error": "Missing required field 'gameId'."})
    game_id = game_id_raw.strip()

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return None, _resp(401, {"error": "Missing sub claim (unauthorized)"})

    try:
        item = games_table.get_item(Key={"gameId": game_id}, ConsistentRead=True).get("Item")
    except ClientError as exc:
        log("PlayerAdminGetGameFailed", {"error": str(exc), "gameId": game_id})
        return None, _resp(500, {"error": "GetItemFailed"})
    if not item:
        return None, _resp(404, {"error": "GameNotFound", "gameId": game_id})
    if (item.get("ownerUserId") or "") != owner_user_id:
        return None, _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})
    return (game_id, owner_user_id, item), None


def _handle_get_players(event):
    game, error = _player_admin_game(event)
    if error:
        return error
    game_id, _, _ = game
    players = []
    kwargs = {
        "KeyConditionExpression": Key("gameId").eq(game_id),
        "ProjectionExpression": "playerId, #username, instagramUsername",
        "ExpressionAttributeNames": {"#username": "username"},
    }
    try:
        while True:
            response = gp_table.query(**kwargs)
            for item in response.get("Items") or []:
                player_id = int(item["playerId"])
                username = item.get("username") or item.get("instagramUsername")
                players.append({
                    "playerId": player_id,
                    "username": username.strip() if isinstance(username, str) and username.strip() else f"Jugador {player_id}",
                })
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
    except (ClientError, KeyError, TypeError, ValueError) as exc:
        log("PlayerAdminListFailed", {"error": str(exc), "gameId": game_id})
        return _resp(500, {"error": "PlayerListFailed"})
    return _resp(200, {"ok": True, "players": players})


def _serialize_dynamodb_values(values):
    serializer = TypeSerializer()
    return {key: serializer.serialize(value) for key, value in values.items()}


def _handle_delete_player(event):
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"error": "Invalid JSON body"})
    player_id_raw = body.get("playerId")
    if isinstance(player_id_raw, bool):
        return _resp(400, {"error": "playerId must be a positive integer."})
    try:
        player_id = int(player_id_raw)
        if player_id <= 0 or str(player_id_raw).strip() != str(player_id):
            raise ValueError
    except (TypeError, ValueError):
        return _resp(400, {"error": "playerId must be a positive integer."})

    game, error = _player_admin_game(event)
    if error:
        return error
    game_id, owner_user_id, game_item = game

    try:
        player_response = gp_table.get_item(
            Key={"gameId": game_id, "playerId": player_id},
            ConsistentRead=True,
        )
    except ClientError as exc:
        log("PlayerAdminGetPlayerFailed", {"error": str(exc), "gameId": game_id, "playerId": player_id})
        return _resp(500, {"error": "GetPlayerFailed"})
    player = player_response.get("Item")
    if not player:
        return _resp(404, {"error": "PlayerNotFound", "playerId": player_id})

    identity_mapping = None
    if player.get("email"):
        if not EMAIL_VERIFICATION_TABLE:
            log("PlayerAdminIdentityTableMissing", {"gameId": game_id, "playerId": player_id})
            return _resp(500, {"error": "PlayerIdentityCleanupUnavailable"})
        identity_key = identity_id(PLAYER_TOKEN_SECRET, game_id, player["email"])
        try:
            identity_mapping = dynamodb.Table(EMAIL_VERIFICATION_TABLE).get_item(
                Key={"id": identity_key},
                ConsistentRead=True,
            ).get("Item")
        except ClientError as exc:
            log("PlayerAdminGetIdentityFailed", {"error": str(exc), "gameId": game_id, "playerId": player_id})
            return _resp(500, {"error": "PlayerIdentityCleanupUnavailable"})

    existing_winners = game_item.get("raffleWinners")
    existing_winners = existing_winners if isinstance(existing_winners, list) else []
    remaining_winners = [
        winner for winner in existing_winners
        if not isinstance(winner, dict) or str(winner.get("playerId")) != str(player_id)
    ]
    try:
        player_count = int(game_item.get("playersCount", 0))
        validated_count = int(game_item.get("validatedCount", 0))
        last_player_id = max(int(game_item.get("lastPlayerId", 0)), player_id)
    except (TypeError, ValueError, OverflowError) as exc:
        log("PlayerAdminInvalidGameCounters", {"error": str(exc), "gameId": game_id})
        return _resp(500, {"error": "InvalidGameCounters"})

    update_expression = (
        "SET playersCount = :playersCount, raffleWinners = :winners, "
        "lastPlayerId = :lastPlayerId, updatedAt = :now"
    )
    values = {
        ":playersCount": max(0, player_count - 1),
        ":winners": remaining_winners,
        ":lastPlayerId": last_player_id,
        ":now": _iso_now(),
        ":owner": owner_user_id,
    }
    conditions = ["ownerUserId = :owner", "attribute_exists(gameId)"]
    if "playersCount" in game_item:
        conditions.append("playersCount = :expectedPlayersCount")
        values[":expectedPlayersCount"] = game_item["playersCount"]
    else:
        conditions.append("attribute_not_exists(playersCount)")
    if "raffleWinners" in game_item:
        conditions.append("raffleWinners = :expectedWinners")
        values[":expectedWinners"] = game_item["raffleWinners"]
    else:
        conditions.append("attribute_not_exists(raffleWinners)")
    if player.get("validated"):
        update_expression += ", validatedCount = :validatedCount"
        values[":validatedCount"] = max(0, validated_count - 1)
        if "validatedCount" in game_item:
            conditions.append("validatedCount = :expectedValidatedCount")
            values[":expectedValidatedCount"] = game_item["validatedCount"]
        else:
            conditions.append("attribute_not_exists(validatedCount)")

    transaction = [
        {
            "Update": {
                "TableName": GAMES_TABLE,
                "Key": _serialize_dynamodb_values({"gameId": game_id}),
                "UpdateExpression": update_expression,
                "ConditionExpression": " AND ".join(conditions),
                "ExpressionAttributeValues": _serialize_dynamodb_values(values),
            }
        },
        {
            "Delete": {
                "TableName": GAMEPLAYER_TABLE,
                "Key": _serialize_dynamodb_values({"gameId": game_id, "playerId": player_id}),
                "ConditionExpression": "attribute_exists(gameId) AND attribute_exists(playerId)",
            }
        },
    ]
    if identity_mapping and str(identity_mapping.get("playerId")) == str(player_id):
        transaction.append({
            "Delete": {
                "TableName": EMAIL_VERIFICATION_TABLE,
                "Key": _serialize_dynamodb_values({"id": identity_key}),
                "ConditionExpression": "playerId = :removedPlayerId",
                "ExpressionAttributeValues": _serialize_dynamodb_values({":removedPlayerId": player_id}),
            }
        })
    try:
        dynamodb.meta.client.transact_write_items(TransactItems=transaction)
    except ClientError as exc:
        code = (exc.response.get("Error") or {}).get("Code")
        reasons = exc.response.get("CancellationReasons") or []
        conditional_failure = code == "TransactionCanceledException" and any(
            reason.get("Code") == "ConditionalCheckFailed" for reason in reasons
        )
        if conditional_failure:
            return _resp(409, {"error": "PlayerChanged", "detail": "Refresh the player list and retry."})
        log("PlayerAdminDeleteFailed", {"error": str(exc), "gameId": game_id, "playerId": player_id})
        return _resp(500, {"error": "PlayerDeleteFailed"})

    return _resp(200, {"ok": True, "playerId": player_id})


# ---------------- POST ----------------
def _handle_post(event):
    """
    Create game (auth required).
    body: { "gameType": "...", "gameName": "..."? }
    - gameId is a 6-char alphanumeric join code (PK).
    - FREE plan: max 1 game (uses GSI_OWNER).
    - Optional uniqueness check for gameName if you created GSI_GAMENAME.
    """
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"error": "Invalid JSON body"})

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    game_type_raw = (body.get("gameType") or "").strip()
    game_name = (body.get("gameName") or "").strip()

    if not game_type_raw:
        return _resp(400, {"error": "Missing required field 'gameType'."})

    game_type = game_type_raw.upper()
    if game_type not in SUPPORTED_GAME_TYPES:
        return _resp(400, {
            "error": f"Unsupported gameType '{game_type_raw}'.",
            "supportedGameTypes": sorted(SUPPORTED_GAME_TYPES),
        })

    if ("playMode" in body and body["playMode"] is None) or (
        "joinChannel" in body and body["joinChannel"] is None
    ):
        return _resp(400, {"error": "playMode must be 'mobile' and joinChannel must be 'web'."})
    try:
        access = access_for_create(game_type, body.get("playMode"), body.get("joinChannel"))
    except GameAccessError as exc:
        return _resp(400, {"error": str(exc)})

    try:
        participant_fields = normalize_participant_fields(game_type, body.get("participantFields"))
    except ValueError as exc:
        return _resp(400, {"error": str(exc)})

    stack_config = None
    if game_type == "STACK":
        settings = body.get("settings", {})
        if not isinstance(settings, dict):
            return _resp(400, {"error": "settings must be an object."})
        if "stack" in settings and not isinstance(settings["stack"], dict):
            return _resp(400, {"error": "settings.stack must be an object."})
        try:
            stack_config = validate_stack_config(settings.get("stack"))
        except ValueError as exc:
            return _resp(400, {"error": str(exc)})

    # Plan enforcement: FREE => only 1 game
    user_item = _get_user(owner_user_id)
    is_pro = _is_user_pro(user_item)

    if not is_pro:
        try:
            q = games_table.query(
                IndexName=GSI_OWNER,
                KeyConditionExpression=Key("ownerUserId").eq(owner_user_id),
                Limit=1,
                ProjectionExpression="gameId",
            )
        except ClientError as e:
            log("Owner query failed", {"error": str(e)})
            return _resp(500, {"error": "OwnerQueryFailed", "detail": str(e)})

        if q.get("Items"):
            return _resp(403, {
                "error": "FREE plan limit reached",
                "detail": "On FREE you can only create 1 game. Upgrade to create more."
            })

    # Optional: enforce unique gameName (ONLY if you created gsi-gameName)
    if game_name:
        try:
            existing = games_table.query(
                IndexName=GSI_GAMENAME,
                KeyConditionExpression=Key("gameName").eq(game_name),
                Limit=1,
                ProjectionExpression="gameId",
            )
            if existing.get("Items"):
                return _resp(409, {"error": "Game name already exists.", "gameName": game_name})
        except ClientError as e:
            log("GameName uniqueness check failed", {"error": str(e)})
            return _resp(500, {"error": "GameNameCheckFailed", "detail": str(e)})

    now = _iso_now()
    max_players = 25 if not is_pro else 9999

    # Generate unique join code (gameId) with retries
    game_id = None
    last_err = None

    for _ in range(JOIN_CODE_MAX_ATTEMPTS):
        candidate = _new_join_code()
        requires_validation = (game_type or "").upper() != "INFOCARDS"
        item = {
            "gameId": candidate,
            "ownerUserId": owner_user_id,
            "gameType": game_type,
            "gameName": game_name or None,
            "isActive": True,
            "maxPlayers": max_players,
            "playersCount": 0,
            "validatedCount": 0,
            "raffleWinners": [],
            "requiresValidation": requires_validation,
            "playMode": access["playMode"],
            "joinChannel": access["joinChannel"],
            "createdAt": now,
            "updatedAt": now,
        }
        item = {k: v for k, v in item.items() if v is not None}

        quiz_order, quiz_questions = _load_default_quiz_from_catalog(game_type, candidate)

        if quiz_order:
            item["quizOrder"] = quiz_order
            item["quizQuestions"] = quiz_questions

        if game_type in ("EMPAREJA2", "T1MER", "STACK"):
            item["participantFields"] = participant_fields
        if game_type == "STACK":
            item["type"] = {"STACK": stack_config}
            item["requiresValidation"] = False
        
        try:
            games_table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(gameId)",
            )
            game_id = candidate
            return _resp(201, {"message": "Game created successfully", "game": item})
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ConditionalCheckFailedException":
                # collision -> retry
                last_err = e
                continue
            last_err = e
            log("Create failed", {"error": str(e)})
            return _resp(500, {"error": "CreateFailed", "detail": str(e)})

    log("Join code generation exhausted", {"error": str(last_err) if last_err else "unknown"})
    return _resp(500, {
        "error": "GameIdGenerationFailed",
        "detail": "Unable to generate unique join code. Retry."
    })

# ---------------- PUT ----------------

def _handle_put(event):
    """
    Update game (owner only).
    body: { "gameId": "XXXXXX", "isActive": true/false?, "settings": { "empareja2": { "numPairs": N?, "maxValidations": M? } }? }
    - isActive: optional boolean. If provided, updates isActive and updatedAt.
    - settings.empareja2: optional. If provided, merges into type.EMPAREJA2 (numPairs: int >= 1 or None, maxValidations: int >= 0 or None).
    - settings.l3tras: optional. Merges into type.L3TRAS (objectiveWord, always stored uppercase).
    """
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"error": "Invalid JSON body"})

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    game_id = (body.get("gameId") or "").strip()
    if not game_id:
        return _resp(400, {"error": "Missing required field 'gameId'."})
    prize_catalog_supplied = "prizeCatalog" in body
    raw_prize_catalog = body.get("prizeCatalog")

    is_active = body.get("isActive", None)
    settings = body.get("settings") or {}
    if not isinstance(settings, dict):
        return _resp(400, {"error": "settings must be an object."})
    if "stack" in settings and not isinstance(settings["stack"], dict):
        return _resp(400, {"error": "settings.stack must be an object."})
    empareja2_settings = settings.get("empareja2")
    prize_update = settings.get("prizeDelivered")
    l3tras_settings = settings.get("l3tras")
    impostor_settings = settings.get("impostor")
    stack_settings = settings.get("stack")
    participant_fields = body.get("participantFields")
    join_channel = body.get("joinChannel")
    play_mode = body.get("playMode")

    if ("playMode" in body and play_mode is None) or (
        "joinChannel" in body and join_channel is None
    ):
        return _resp(400, {"error": "playMode must be 'mobile' and joinChannel must be 'web'."})

    # At least one update
    if (
        is_active is None
        and not empareja2_settings
        and not prize_update
        and not l3tras_settings
        and not impostor_settings
        and stack_settings is None
        and participant_fields is None
        and join_channel is None
        and play_mode is None
        and not prize_catalog_supplied
    ):
        return _resp(400, {"error": "Provide at least one game update, including 'prizeCatalog'."})

    now = _iso_now()

    try:
        resp = games_table.get_item(Key={"gameId": game_id})
        item = resp.get("Item")
    except ClientError as e:
        log("GetItem failed on PUT", {"error": str(e)})
        return _resp(500, {"error": "GetItemFailed", "detail": str(e)})

    if not item:
        return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
    if (item.get("ownerUserId") or "") != owner_user_id:
        return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})

    prepared_prize_catalog = None
    prepared_prize_response = None
    staged_upload_keys = []
    if prize_catalog_supplied:
        existing_catalog = item.get("prizeCatalog")
        raffle_enabled = (
            _is_user_pro(_get_user(owner_user_id))
            if (
                isinstance(raw_prize_catalog, dict) and "raffle" in raw_prize_catalog
                or isinstance(existing_catalog, dict) and "raffle" in existing_catalog
            )
            else False
        )
        try:
            prepared_prize_catalog = validate_prize_catalog(
                raw_prize_catalog,
                item.get("gameType") or "",
                raffle_enabled,
                owner_user_id,
                game_id,
                existing_catalog=existing_catalog,
            )
            for slot, prize in prepared_prize_catalog.items():
                image_key = prize["imageKey"]
                if image_key is None:
                    continue
                image_data = _read_verified_prize_image(image_key)
                if staged_prize_image_key(owner_user_id, game_id, slot, image_key):
                    committed_key = f"games/{owner_user_id}/{game_id}/{slot}/{uuid.uuid4()}.png"
                    prize_s3.put_object(
                        Bucket=USER_MEDIA_BUCKET,
                        Key=committed_key,
                        Body=image_data,
                        ContentLength=len(image_data),
                        ContentType="image/png",
                    )
                    staged_upload_keys.append(image_key)
                    prize["imageKey"] = committed_key
            prepared_prize_response = serialize_prize_catalog(
                prepared_prize_catalog,
                owner_user_id,
                game_id,
                USER_MEDIA_BUCKET,
                prize_s3.generate_presigned_url,
            )
        except ValueError as exc:
            return _resp(400, {"error": str(exc)})
        except RuntimeError as exc:
            log("Prize catalog image verification failed", {"gameId": game_id, "error": str(exc)})
            return _resp(500, {"error": "PrizeImageVerificationFailed", "detail": str(exc)})

    stack_config = None
    if stack_settings is not None:
        if (item.get("gameType") or "").upper() != "STACK":
            return _resp(400, {"error": "settings.stack is only valid for STACK games."})
        try:
            stack_config = validate_stack_config(stack_settings, base=stack_game_config(item))
        except ValueError as exc:
            return _resp(400, {"error": str(exc)})
    access = None
    if join_channel is not None or play_mode is not None:
        try:
            access = access_for_update(item, play_mode, join_channel)
        except GameAccessError as exc:
            return _resp(400, {"error": str(exc)})
    if participant_fields is not None:
        try:
            participant_fields = normalize_participant_fields(item.get("gameType") or "", participant_fields)
        except ValueError as exc:
            return _resp(400, {"error": str(exc)})

    if stack_config is not None:
        set_game_type_blob(
            games_table, game_id, "STACK", stack_config, updated_at=now,
            condition_expression="attribute_exists(gameId) AND ownerUserId = :owner",
            condition_values={":owner": owner_user_id},
        )

    if participant_fields is not None:
        try:
            games_table.update_item(
                Key={"gameId": game_id},
                UpdateExpression="SET participantFields = :pf, updatedAt = :u",
                ExpressionAttributeValues={":pf": participant_fields, ":u": now, ":owner": owner_user_id},
                ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
            log("Update participantFields failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    if access is not None:
        # Single write: playMode and joinChannel are always persisted together.
        try:
            games_table.update_item(
                Key={"gameId": game_id},
                UpdateExpression="SET playMode = :pm, joinChannel = :jc, updatedAt = :u",
                ExpressionAttributeValues={
                    ":pm": access["playMode"], ":jc": access["joinChannel"], ":u": now,
                    ":owner": owner_user_id, ":gt": item.get("gameType"),
                },
                ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner AND gameType = :gt",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
            log("Update game access failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    if isinstance(is_active, bool):
        try:
            games_table.update_item(
                Key={"gameId": game_id},
                UpdateExpression="SET isActive = :a, updatedAt = :u",
                ExpressionAttributeValues={
                    ":a": is_active,
                    ":u": now,
                    ":owner": owner_user_id,
                },
                ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
            log("Update isActive failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    if empareja2_settings is not None and isinstance(empareja2_settings, dict):
        num_pairs = empareja2_settings.get("numPairs")
        max_validations = empareja2_settings.get("maxValidations")
        if num_pairs is not None:
            try:
                n = int(num_pairs)
                if n < 1:
                    return _resp(400, {"error": "settings.empareja2.numPairs must be >= 1 or omitted."})
                num_pairs = n
            except (TypeError, ValueError):
                return _resp(400, {"error": "settings.empareja2.numPairs must be a positive integer or omitted."})
        if max_validations is not None:
            try:
                m = int(max_validations)
                if m < 0:
                    return _resp(400, {"error": "settings.empareja2.maxValidations must be >= 0 or omitted."})
                max_validations = m
            except (TypeError, ValueError):
                return _resp(400, {"error": "settings.empareja2.maxValidations must be a non-negative integer or omitted."})
        current_blob = (item.get("type") or {}).get("EMPAREJA2") or {}
        merged = {**current_blob}
        if num_pairs is not None:
            merged["numPairs"] = num_pairs
        if max_validations is not None:
            merged["maxValidations"] = max_validations
        try:
            set_game_type_blob(
                games_table,
                game_id,
                "EMPAREJA2",
                merged,
                updated_at=now,
                condition_expression="attribute_exists(gameId) AND ownerUserId = :owner",
                condition_values={":owner": owner_user_id},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
            log("Update type.EMPAREJA2 failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    if l3tras_settings is not None and isinstance(l3tras_settings, dict) and l3tras_settings:
        if (item.get("gameType") or "").upper() != "L3TRAS":
            return _resp(400, {"error": "settings.l3tras is only valid for L3TRAS games."})
        current_blob = (item.get("type") or {}).get("L3TRAS") or {}
        merged = {**current_blob}
        if "objectiveWord" in l3tras_settings and l3tras_settings["objectiveWord"] is not None:
            raw = str(l3tras_settings["objectiveWord"]).strip()
            letters_only = "".join(c for c in raw if c.isalpha())
            if not letters_only:
                return _resp(400, {"error": "settings.l3tras.objectiveWord must contain at least one letter."})
            if len(letters_only) > 40:
                return _resp(400, {"error": "settings.l3tras.objectiveWord must be at most 40 letters."})
            merged["objectiveWord"] = letters_only.upper()
        if "validationTimeLimitSeconds" in l3tras_settings and l3tras_settings["validationTimeLimitSeconds"] is not None:
            try:
                tls = int(l3tras_settings["validationTimeLimitSeconds"])
            except Exception:
                return _resp(400, {"error": "settings.l3tras.validationTimeLimitSeconds must be an integer."})
            if tls < 15 or tls > 3600:
                return _resp(
                    400,
                    {"error": "settings.l3tras.validationTimeLimitSeconds must be between 15 and 3600 (seconds)."},
                )
            merged["validationTimeLimitSeconds"] = tls
        merged.pop("normalize", None)
        try:
            set_game_type_blob(
                games_table,
                game_id,
                "L3TRAS",
                merged,
                updated_at=now,
                condition_expression="attribute_exists(gameId) AND ownerUserId = :owner",
                condition_values={":owner": owner_user_id},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
            log("Update type.L3TRAS failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    if impostor_settings is not None and isinstance(impostor_settings, dict) and impostor_settings:
        if (item.get("gameType") or "").upper() != "IMPOSTOR":
            return _resp(400, {"error": "settings.impostor is only valid for IMPOSTOR games."})
        current_blob = (item.get("type") or {}).get("IMPOSTOR") or {}
        merged = {**current_blob}
        if "maxPlayers" in impostor_settings and impostor_settings["maxPlayers"] is not None:
            try:
                mp = int(impostor_settings["maxPlayers"])
            except Exception:
                return _resp(400, {"error": "settings.impostor.maxPlayers must be an integer."})
            if mp < 3 or mp > 25:
                return _resp(400, {"error": "settings.impostor.maxPlayers must be between 3 and 25."})
            merged["maxPlayers"] = mp
            try:
                games_table.update_item(
                    Key={"gameId": game_id},
                    UpdateExpression="SET maxPlayers = :m, updatedAt = :u",
                    ExpressionAttributeValues={":m": mp, ":u": now, ":owner": owner_user_id},
                    ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner",
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
                log("Update maxPlayers failed", {"error": str(e)})
                return _resp(500, {"error": "UpdateFailed", "detail": str(e)})
        if "words" in impostor_settings and impostor_settings["words"] is not None:
            words = impostor_settings["words"]
            if not isinstance(words, list):
                return _resp(400, {"error": "settings.impostor.words must be an array of strings."})
            normalized = []
            for w in words:
                s = str(w).strip()
                if s:
                    normalized.append(s)
            if not normalized:
                return _resp(400, {"error": "settings.impostor.words must contain at least one word."})
            merged["words"] = normalized[:250]
        try:
            set_game_type_blob(
                games_table,
                game_id,
                "IMPOSTOR",
                merged,
                updated_at=now,
                condition_expression="attribute_exists(gameId) AND ownerUserId = :owner",
                condition_values={":owner": owner_user_id},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
            log("Update type.IMPOSTOR failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    # settings.prizeDelivered: { gameType, playerId, delivered: bool }
    if prize_update is not None and isinstance(prize_update, dict):
        p_game_type = (prize_update.get("gameType") or "").upper()
        p_player_id = prize_update.get("playerId")
        delivered_flag = prize_update.get("delivered")
        if not p_game_type or p_player_id is None or not isinstance(delivered_flag, bool):
            return _resp(400, {"error": "settings.prizeDelivered requires 'gameType', 'playerId' and boolean 'delivered'."})
        try:
            pid_int = int(p_player_id)
        except Exception:
            return _resp(400, {"error": "settings.prizeDelivered.playerId must be numeric."})
        try:
            # Three separate updates: DynamoDB does not allow overlapping paths in one expression
            # (e.g. [type] and [type, T1MER], or [type, T1MER] and [type, T1MER, prizeDelivered])
            gp_table.update_item(
                Key={"gameId": game_id, "playerId": pid_int},
                UpdateExpression="SET #type = if_not_exists(#type, :tinit)",
                ExpressionAttributeNames={"#type": "type"},
                ExpressionAttributeValues={":tinit": {}},
            )
            gp_table.update_item(
                Key={"gameId": game_id, "playerId": pid_int},
                UpdateExpression="SET #type.#g = if_not_exists(#type.#g, :ginit)",
                ExpressionAttributeNames={"#type": "type", "#g": p_game_type},
                ExpressionAttributeValues={":ginit": {}},
            )
            gp_table.update_item(
                Key={"gameId": game_id, "playerId": pid_int},
                UpdateExpression="SET #type.#g.prizeDelivered = :d",
                ExpressionAttributeNames={"#type": "type", "#g": p_game_type},
                ExpressionAttributeValues={":d": delivered_flag},
            )
        except ClientError as e:
            log("UpdatePrizeDeliveredFailed", {"error": str(e), "gameId": game_id, "playerId": pid_int})
            return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

    prize_image_cleanup_pending = False
    if prize_catalog_supplied:
        try:
            latest_item = _get_game(game_id)
        except ClientError as e:
            log("GetItem failed before prize catalog update", {"error": str(e), "gameId": game_id})
            return _resp(500, {"error": "PrizeCatalogReadFailed"})
        if not latest_item:
            return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
        if (latest_item.get("ownerUserId") or "") != owner_user_id:
            return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})

        expression_values = {
            ":catalog": prepared_prize_catalog,
            ":updated": _iso_now(),
            ":owner": owner_user_id,
            ":gameType": latest_item.get("gameType"),
        }
        expression = "SET prizeCatalog = :catalog, updatedAt = :updated"
        expression_names = {}
        if (
            (latest_item.get("gameType") or "").upper() == "EMPAREJA2"
            and "validation" in prepared_prize_catalog
        ):
            current_type = latest_item.get("type")
            if current_type is not None and not isinstance(current_type, dict):
                return _resp(500, {"error": "InvalidStoredGameTypeConfig"})
            current_type = current_type or {}
            empareja2 = current_type.get("EMPAREJA2")
            if empareja2 is not None and not isinstance(empareja2, dict):
                return _resp(500, {"error": "InvalidStoredEmpareja2Config"})
            validation_limit = prepared_prize_catalog["validation"]["limit"]
            if isinstance(empareja2, dict):
                expression_names.update({
                    "#type": "type",
                    "#gameType": "EMPAREJA2",
                    "#maxValidations": "maxValidations",
                })
                if validation_limit is None:
                    expression += " REMOVE #type.#gameType.#maxValidations"
                else:
                    expression += " , #type.#gameType.#maxValidations = :maxValidations"
                    expression_values[":maxValidations"] = validation_limit
            elif validation_limit is not None:
                if isinstance(latest_item.get("type"), dict):
                    expression += ", #type.#gameType = :gameConfig"
                    expression_names.update({"#type": "type", "#gameType": "EMPAREJA2"})
                else:
                    expression += ", #type = :type"
                    expression_names["#type"] = "type"
                if isinstance(latest_item.get("type"), dict):
                    expression_values[":gameConfig"] = {"maxValidations": validation_limit}
                else:
                    expression_values[":type"] = {"EMPAREJA2": {"maxValidations": validation_limit}}

        condition = "attribute_exists(gameId) AND ownerUserId = :owner AND gameType = :gameType"
        previous_updated = latest_item.get("updatedAt")
        if previous_updated is None:
            condition += " AND attribute_not_exists(updatedAt)"
        else:
            condition += " AND updatedAt = :previousUpdated"
            expression_values[":previousUpdated"] = previous_updated
        update_kwargs = {
            "Key": {"gameId": game_id},
            "UpdateExpression": expression,
            "ConditionExpression": condition,
            "ExpressionAttributeValues": expression_values,
        }
        if expression_names:
            update_kwargs["ExpressionAttributeNames"] = expression_names
        try:
            games_table.update_item(**update_kwargs)
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code")
            if code == "ConditionalCheckFailedException":
                return _resp(409, {"error": "PrizeCatalogChanged", "gameId": game_id})
            log("Prize catalog update failed", {"error": str(e), "gameId": game_id})
            return _resp(500, {"error": "PrizeCatalogUpdateFailed"})

        for key in staged_upload_keys:
            for attempt in range(2):
                try:
                    prize_s3.delete_object_tagging(Bucket=USER_MEDIA_BUCKET, Key=key)
                    break
                except Exception as e:
                    if attempt == 1:
                        prize_image_cleanup_pending = True
                        log("Committed prize image staging tag cleanup failed", {
                            "gameId": game_id, "key": key, "error": str(e),
                        })

    # Return current state
    try:
        resp = games_table.get_item(Key={"gameId": game_id})
        attrs = resp.get("Item") or item
    except Exception:
        attrs = item
    if isinstance(attrs, dict):
        attrs = {**attrs, **resolve_game_access(attrs)}
    payload = {"message": "Game updated successfully", "game": attrs}
    if prize_catalog_supplied:
        attrs = {**attrs, "prizeCatalog": prepared_prize_catalog}
        if (
            (attrs.get("gameType") or "").upper() == "EMPAREJA2"
            and "validation" in prepared_prize_catalog
        ):
            current_type = dict(attrs.get("type") or {})
            empareja2 = dict(current_type.get("EMPAREJA2") or {})
            validation_limit = prepared_prize_catalog["validation"]["limit"]
            if validation_limit is None:
                empareja2.pop("maxValidations", None)
            else:
                empareja2["maxValidations"] = validation_limit
            current_type["EMPAREJA2"] = empareja2
            attrs["type"] = current_type
        payload["prizeCatalog"] = prepared_prize_response
        # The public catalog is returned at the response top level, never as unsigned S3 data.
        attrs = dict(attrs)
        attrs.pop("prizeCatalog", None)
        payload["game"] = attrs
    if prize_image_cleanup_pending:
        payload["warnings"] = ["PrizeImageCleanupPending"]
    return _resp(200, payload)

# ---------------- DELETE ----------------

def _handle_delete(event):
    """
    Delete game metadata (owner only).
    body: { "gameId": "XXXXXX" }
    Note: no per-game tables exist anymore.
    """
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"error": "Invalid JSON body"})

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    game_id = (body.get("gameId") or "").strip()
    if not game_id:
        return _resp(400, {"error": "Missing required field 'gameId'."})

    try:
        games_table.delete_item(
            Key={"gameId": game_id},
            ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner",
            ExpressionAttributeValues={":owner": owner_user_id},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
        log("Delete failed", {"error": str(e)})
        return _resp(500, {"error": "DeleteFailed", "detail": str(e)})

    return _resp(200, {"message": "Game deleted successfully", "gameId": game_id})

# ---------------- Router ----------------

def lambda_handler(event, context):
    method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "POST")
    )

    try:
        if method == "OPTIONS":
            return _resp(204, "")

        path = _get_path(event).rstrip("/")
        if path.endswith("/v1/game/players"):
            if method == "GET":
                return _handle_get_players(event)
            if method == "DELETE":
                return _handle_delete_player(event)
            return _resp(405, {"error": f"Method {method} not allowed"})

        if method == "GET":
            return _handle_get(event)

        if method == "POST":
            if _get_path(event).rstrip("/").endswith("/v1/game/prizes"):
                action = ((_get_query_params(event).get("action") or "").strip().lower())
                if action != "upload-url":
                    return _resp(400, {"error": "Unsupported prize action."})
                return _handle_prize_upload_url(event)
            return _handle_post(event)

        if method == "PUT":
            return _handle_put(event)

        if method == "DELETE":
            return _handle_delete(event)

        return _resp(405, {"error": f"Method {method} not allowed"})

    except Exception as e:
        log("UnhandledError", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
