import os
import json
import random
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

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
SUPPORTED_GAME_TYPES = {"EMPAREJA2", "T1MER", "RULET4", "L3TRAS", "SEMAFORO"}

# numeric 6 digits 
JOIN_CODE_ALPHABET = "0123456789"
JOIN_CODE_LENGTH = 6
JOIN_CODE_MAX_ATTEMPTS = 10

HEADERS = {"Content-Type": "application/json"}

def log(msg, obj=None):
    if obj is not None:
        print(json.dumps({"msg": msg, "data": obj}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))

def _resp(status, body):
    if not isinstance(body, str):
        body = json.dumps(_json_sanitize(body), ensure_ascii=False)
    return {
        "statusCode": int(status),
        "headers": HEADERS,
        "body": body,
        "isBase64Encoded": False,
    }

def _json_sanitize(obj):
    """
    Convierte Decimals de DynamoDB a int/float para que json.dumps no falle.
    Mantiene dict/list recursivamente.
    """
    if isinstance(obj, Decimal):
        # si es entero -> int, si no -> float
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_sanitize(v) for v in obj]
    return obj

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _get_claims(event: dict) -> dict:
    rc = (event or {}).get("requestContext") or {}
    auth = rc.get("authorizer") or {}
    jwt = auth.get("jwt") or {}
    return jwt.get("claims") or auth.get("claims") or {}

def _read_json_body(event):
    raw = event.get("body") or "{}"
    try:
        return json.loads(raw) if isinstance(raw, str) else (raw or {})
    except Exception:
        return None

def _get_query_params(event):
    return event.get("queryStringParameters") or {}

def _parse_iso_dt(s: str):
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _is_user_pro(user_item: dict) -> bool:
    if not user_item:
        return False
    plan = (user_item.get("plan") or "FREE").upper().strip()
    if plan == "FREE":
        return False
    au = _parse_iso_dt(user_item.get("activeUntil"))
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

        return _resp(200, {"ok": True, "game": _public_game_shape(item)})

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

        return _resp(200, {
            "ok": True,
            "game": _public_game_shape(item),
            "playerCount": player_count,
            "validatedCount": validated_count,
            "raffleWinners": _raffle_winners_from_game(item),
            "quiz": {
                "enabled": _quiz_enabled_from_game(item),
                "questionsCount": len(item.get("quizOrder") or []) if _quiz_enabled_from_game(item) else 0
            }
        })
    
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
    - gameId is a 6-char base32 join code (PK).
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

    now = _now_iso()
    max_players = 25 if not is_pro else 9999

    # Generate unique join code (gameId) with retries
    game_id = None
    last_err = None

    for _ in range(JOIN_CODE_MAX_ATTEMPTS):
        candidate = _new_join_code()
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
    Update isActive (owner only).
    body: { "gameId": "XXXXXX", "isActive": true/false }
    """
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"error": "Invalid JSON body"})

    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    if not owner_user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    game_id = (body.get("gameId") or "").strip()
    is_active = body.get("isActive", None)

    if not game_id:
        return _resp(400, {"error": "Missing required field 'gameId'."})
    if not isinstance(is_active, bool):
        return _resp(400, {"error": "Field 'isActive' must be a boolean."})

    now = _now_iso()

    try:
        resp = games_table.update_item(
            Key={"gameId": game_id},
            UpdateExpression="SET isActive = :a, updatedAt = :u",
            ExpressionAttributeValues={
                ":a": is_active,
                ":u": now,
                ":owner": owner_user_id,
            },
            ConditionExpression="attribute_exists(gameId) AND ownerUserId = :owner",
            ReturnValues="ALL_NEW",
        )
        attrs = resp.get("Attributes") or {}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return _resp(404, {"error": "Game not found (or not owner)", "gameId": game_id})
        log("Update failed", {"error": str(e)})
        return _resp(500, {"error": "UpdateFailed", "detail": str(e)})

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
