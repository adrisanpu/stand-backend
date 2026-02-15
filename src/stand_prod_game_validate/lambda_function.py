import os
import json
import base64
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# ===== ENV =====
GAMES_TABLE = os.environ.get("GAMES_TABLE", "")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "")
GAME_TABLE = os.environ.get("GAME_TABLE", "")

QUIZ_LAMBDA_NAME = os.environ.get("QUIZ_LAMBDA_NAME", "")  # optional
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "")

# S3 (private bucket + presigned urls)
CHAR_BUCKET = os.environ.get("CHAR_BUCKET", "")            # e.g. "empareja2-characters"
CHAR_URL_TTL = int(os.environ.get("CHAR_URL_TTL", "120"))  # seconds

dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
s3 = boto3.client("s3")

games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)

HEADERS = {"Content-Type": "application/json"}

# ===== Logging / JSON =====

def log(msg, obj=None):
    if obj is not None:
        print(json.dumps({"msg": msg, "data": obj}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))

def _json_sanitize(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_sanitize(v) for v in obj]
    return obj

def _resp(code: int, body):
    if not isinstance(body, str):
        body = json.dumps(_json_sanitize(body), ensure_ascii=False)
    return {"statusCode": int(code), "headers": HEADERS, "body": body}

def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# ===== Parsing =====

def _parse_http_event(event):
    method = (
        (event or {}).get("httpMethod")
        or (event or {}).get("requestContext", {}).get("http", {}).get("method", "GET")
    )

    if method == "OPTIONS":
        # you said you don't need CORS; return 204 anyway
        return method, "", []

    raw = (event or {}).get("body")
    if raw and (event or {}).get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except Exception as e:
            log("body_base64_decode_error", {"error": repr(e)})
            raw = None

    body = {}
    if raw:
        try:
            body = json.loads(raw)
        except Exception as e:
            log("body_json_parse_error", {"error": repr(e), "raw_sample": str(raw)[:200]})
            body = {}

    qs = (event or {}).get("queryStringParameters") or {}

    game_id = (body.get("gameId") or qs.get("gameId") or "").strip()

    codes_raw = body.get("codes")
    if codes_raw is None:
        codes_raw = qs.get("codes")

    codes = []
    if isinstance(codes_raw, list):
        codes = [str(c).strip() for c in codes_raw if str(c).strip()]
    elif isinstance(codes_raw, str):
        parts = codes_raw.replace(" ", "").split(",")
        codes = [p for p in parts if p]

    return method, game_id, codes

def to_int_code(code_str: str):
    try:
        return int(str(code_str).strip())
    except Exception:
        return None

# ===== S3 presign =====

def presign_character_png(character_name: str) -> str | None:
    """
    Returns a temporary URL to a PRIVATE S3 object: <characterName>.png
    """
    if not CHAR_BUCKET or not character_name:
        return None

    key = f"{character_name}.png"
    try:
        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": CHAR_BUCKET, "Key": key},
            ExpiresIn=CHAR_URL_TTL,
        )
    except Exception as e:
        log("presign_error", {"error": repr(e), "bucket": CHAR_BUCKET, "key": key})
        return None

# ===== Side-effects (IG, Quiz) =====

def send_bulk(messages):
    if not messages:
        return
    if not IG_SENDER_LAMBDA:
        log("send_bulk_skipped_missing_IG_SENDER_LAMBDA", {"count": len(messages)})
        return
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"messages": messages}, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("send_bulk_error", {"error": repr(e), "count": len(messages)})

def invoke_quiz(game_id: str, psids: list[str]):
    if not QUIZ_LAMBDA_NAME or not psids:
        return
    try:
        payload = {"kind": "quiz_start", "gameId": game_id, "psid": psids}
        lambda_client.invoke(
            FunctionName=QUIZ_LAMBDA_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("quiz_invoke_error", {"error": repr(e), "target": QUIZ_LAMBDA_NAME})

# ===== Dynamo helpers =====

def get_game_meta(game_id: str):
    resp = games_table.get_item(Key={"gameId": game_id})
    return resp.get("Item")

def query_players_by_code(game_id: str, code_int: int):
    """
    Query partition (gameId) and filter by validationCode.
    No table scans; cheap because partitions are small.
    """
    resp = gp_table.query(
        KeyConditionExpression=Key("gameId").eq(game_id),
        FilterExpression=Attr("validationCode").eq(Decimal(code_int)),
    )
    return resp.get("Items") or []

def set_validated(game_id: str, player_id: int) -> bool:
    now = _iso_now()
    try:
        gp_table.update_item(
            Key={"gameId": game_id, "playerId": int(player_id)},
            UpdateExpression="SET validated = :v, validatedAt = :t",
            ExpressionAttributeValues={":v": True, ":t": now, ":f": False},
            ConditionExpression="attribute_not_exists(validated) OR validated = :f",
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise

def inc_validated_count(game_id: str, n: int):
    """
    Incremento atómico del contador validatedCount en games_table.
    """
    now = _iso_now()
    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression="ADD validatedCount :n SET lastValidatedAt = :t, updatedAt = :t",
        ExpressionAttributeValues={":n": int(n), ":t": now},
    )
    return now

# ===== Feature gating via type.<game> =====

def is_quiz_required(player_item: dict, game_type_upper: str) -> bool:
    """
    Quiz is optional by plan.
    If not present => NOT required.
    Reads ONLY from type.<game>.quizRequired
    """
    t = player_item.get("type") or {}
    g = t.get(game_type_upper) or {}
    return bool(g.get("quizRequired", False))

def is_quiz_completed(player_item: dict, game_type_upper: str) -> bool:
    """
    Reads ONLY from type.<game>.quizCompleted
    Missing => False
    """
    t = player_item.get("type") or {}
    g = t.get(game_type_upper) or {}
    return bool(g.get("quizCompleted", False))

# ===== Validators registry =====
from validators.empareja2 import validate_empareja2
from validators.t1mer import validate_single_code_t1mer
from validators.rulet4 import validate_single_code_rulet4
from validators.generic import validate_generic
from validators.semaforo import validate_semaforo

VALIDATORS = {
    "EMPAREJA2": validate_empareja2,
    "T1MER": validate_single_code_t1mer,
    "RULET4": validate_single_code_rulet4,
    "SEMAFORO": validate_semaforo,
}

# ===== Entry =====

def lambda_handler(event, context):
    try:
        method, game_id, codes = _parse_http_event(event)

        if method == "OPTIONS":
            return {"statusCode": 204, "headers": HEADERS, "body": ""}

        if not game_id:
            return _resp(400, {"valid": False, "error": "MissingGameId", "message": "Falta gameId."})
        if not codes:
            return _resp(400, {"valid": False, "error": "MissingCodes", "message": "Falta 'codes'."})

        meta = get_game_meta(game_id)
        if not meta:
            return _resp(404, {"valid": False, "error": "GameNotFound", "message": "Ese juego no existe."})
        if not meta.get("isActive", True):
            return _resp(403, {"valid": False, "error": "GameInactive", "message": "Este juego ya no está activo."})

        game_type = (meta.get("gameType") or "UNKNOWN").upper()

        ctx = {
            "gameId": game_id,
            "codes": codes,
            "gameType": game_type,
            "gameMeta": meta,

            # helpers
            "log": log,
            "to_int_code": to_int_code,
            "query_players_by_code": query_players_by_code,
            "set_validated": set_validated,

            # s3
            "presign_character_png": presign_character_png,

            # feature gating via type.<game>
            "is_quiz_required": is_quiz_required,
            "is_quiz_completed": is_quiz_completed,

            # side effects
            "send_bulk": send_bulk,
            "invoke_quiz": invoke_quiz,
            "inc_validated_count": inc_validated_count,
        }

        validator = VALIDATORS.get(game_type, validate_generic)
        result = validator(ctx)

        log("validate_result", {
            "gameId": game_id,
            "gameType": game_type,
            "codes": codes,
            "valid": result.get("valid"),
            "reason": result.get("reason"),
        })

        return _resp(200, result)

    except ClientError as e:
        log("validate_dynamo_error", {"error": str(e)})
        return _resp(500, {"valid": False, "error": "DynamoError", "detail": str(e)})
    except Exception as e:
        log("validate_internal_error", {"error": repr(e)})
        return _resp(500, {"valid": False, "error": "internal_error", "detail": repr(e)})
