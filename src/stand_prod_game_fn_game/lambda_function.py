import os
import json
import random
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import log, _json_sanitize, _resp, _iso_now, _parse_iso, _get_claims, _read_json_body, set_game_type_blob

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
USERS_TABLE = os.environ.get("USERS_TABLE", "stand-prod-user-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "stand-prod-catalog-table")

# GSI names (recommended)
GSI_OWNER = os.environ.get("GSI_OWNER", "gsi-ownerUserId")
GSI_GAMENAME = os.environ.get("GSI_GAMENAME", "gsi-gameName")  # optional; only if you created it

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")
games_table = dynamodb.Table(GAMES_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
gp_table = dynamodb.Table(GAMEPLAYER_TABLE)
catalog_table = dynamodb.Table(CATALOG_TABLE)

# ========= CONST =========
SUPPORTED_GAME_TYPES = {"EMPAREJA2", "T1MER", "RULET4", "L3TRAS", "SEMAFORO", "INFOCARDS"}

# 6-char alphanumeric join code (Crockford-style base32 without I, L, O to avoid confusion)
JOIN_CODE_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTUVWXYZ"
JOIN_CODE_LENGTH = 6
JOIN_CODE_MAX_ATTEMPTS = 10

def _get_query_params(event):
    return event.get("queryStringParameters") or {}

def _is_user_pro(user_item: dict) -> bool:
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

def _public_game_shape(item: dict) -> dict:
    # Safe shape for clients/players
    return {
        "gameId": item.get("gameId"),
        "gameName": item.get("gameName"),
        "gameType": item.get("gameType"),
        "createdAt": item.get("createdAt"),
        "maxPlayers": item.get("maxPlayers"),
    }

def _quiz_enabled_from_game(game_item: dict) -> bool:
    order = game_item.get("quizOrder") or []
    return isinstance(order, list) and len(order) > 0

def _raffle_winners_from_game(game_item: dict) -> list:
    # si lo guardas en games_table, ideal.
    winners = game_item.get("raffleWinners") or []
    return winners if isinstance(winners, list) else []


def _raffle_required_follows_from_game(game_item: dict) -> list:
    """List of Instagram handles (no @) required to participate in raffle."""
    follows = game_item.get("raffleRequiredFollows") or []
    if not isinstance(follows, list):
        return []
    return [str(h).strip().lower().lstrip("@") for h in follows if h and str(h).strip()]


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

        payload = {"ok": True, "game": _public_game_shape(item)}
        if (item.get("gameType") or "").upper() == "EMPAREJA2":
            payload["settings"] = {"empareja2": (item.get("type") or {}).get("EMPAREJA2") or {}}
            payload["empareja2CatalogPairCount"] = _empareja2_catalog_pair_count()
        elif (item.get("gameType") or "").upper() == "L3TRAS":
            payload["settings"] = {"l3tras": (item.get("type") or {}).get("L3TRAS") or {}}
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
            "game": _public_game_shape(item),
            "playerCount": player_count,
            "validatedCount": validated_count,
            "raffleWinners": _raffle_winners_from_game(item),
            "raffleRequiredFollows": _raffle_required_follows_from_game(item),
            "quiz": {
                "enabled": _quiz_enabled_from_game(item),
                "questionsCount": len(item.get("quizOrder") or []) if _quiz_enabled_from_game(item) else 0
            }
        }
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
            "createdAt": now,
            "updatedAt": now,
        }
        item = {k: v for k, v in item.items() if v is not None}

        quiz_order, quiz_questions = _load_default_quiz_from_catalog(game_type, candidate)

        if quiz_order:
            item["quizOrder"] = quiz_order
            item["quizQuestions"] = quiz_questions
        
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
    if body is None:
        return _resp(400, {"error": "Invalid JSON body"})

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    game_id = (body.get("gameId") or "").strip()
    if not game_id:
        return _resp(400, {"error": "Missing required field 'gameId'."})

    is_active = body.get("isActive", None)
    settings = body.get("settings") or {}
    empareja2_settings = settings.get("empareja2")
    raffle_settings = settings.get("raffle")
    prize_update = settings.get("prizeDelivered")
    l3tras_settings = settings.get("l3tras")

    # At least one update
    if is_active is None and not empareja2_settings and not raffle_settings and not prize_update and not l3tras_settings:
        return _resp(400, {"error": "Provide at least one of 'isActive', 'settings.empareja2', 'settings.raffle', 'settings.l3tras', or 'settings.prizeDelivered'."})

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

    if raffle_settings is not None and isinstance(raffle_settings, dict):
        required_follows = raffle_settings.get("requiredFollows")
        if required_follows is not None:
            if not isinstance(required_follows, list):
                return _resp(400, {"error": "settings.raffle.requiredFollows must be an array of strings or omitted."})
            normalized = []
            for h in required_follows:
                if h is None:
                    continue
                s = str(h).strip().lower().lstrip("@")
                if s and s not in normalized:
                    normalized.append(s)
            try:
                games_table.update_item(
                    Key={"gameId": game_id},
                    UpdateExpression="SET raffleRequiredFollows = :r, updatedAt = :u",
                    ExpressionAttributeValues={
                        ":r": normalized,
                        ":u": now,
                        ":owner": owner_user_id,
                    },
                    ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner",
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
                log("Update raffleRequiredFollows failed", {"error": str(e)})
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

    # Return current state
    try:
        resp = games_table.get_item(Key={"gameId": game_id})
        attrs = resp.get("Item") or item
    except Exception:
        attrs = item
    return _resp(200, {"message": "Game updated successfully", "game": attrs})

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

        if method == "GET":
            return _handle_get(event)

        if method == "POST":
            return _handle_post(event)

        if method == "PUT":
            return _handle_put(event)

        if method == "DELETE":
            return _handle_delete(event)

        return _resp(405, {"error": f"Method {method} not allowed"})

    except Exception as e:
        log("UnhandledError", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
