"""
stand_common.utils — shared utilities for all Stand Lambda functions.

Import from handlers:
    from stand_common.utils import log, _resp, _iso_now, _parse_iso, _get_claims, \
        _get_http_method, _read_json_body, _json_sanitize, _as_int, HEADERS, \
        get_game_type_blob, set_game_type_blob
"""
import base64
import json
from datetime import datetime, timezone
from decimal import Decimal

HEADERS = {"Content-Type": "application/json"}


def log(msg, obj=None):
    """Structured JSON logger. Sanitizes obj so DynamoDB Decimals never crash json.dumps."""
    if obj is not None:
        print(json.dumps({"msg": msg, "data": _json_sanitize(obj)}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))


def _json_sanitize(obj):
    """Convert DynamoDB Decimals to int/float recursively so json.dumps never raises."""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_sanitize(v) for v in obj]
    return obj


def _resp(status: int, body):
    """Build an API Gateway HTTP response. Sanitizes body if it is not already a string."""
    if not isinstance(body, str):
        body = json.dumps(_json_sanitize(body), ensure_ascii=False)
    return {
        "statusCode": int(status),
        "headers": HEADERS,
        "body": body,
    }


def _iso_now() -> str:
    """Return the current UTC time as an ISO-8601 string ending in Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(s) -> datetime | None:
    """
    Parse an ISO-8601 string (with optional trailing Z) into a timezone-aware datetime.
    Returns None if s is None, not a string, or cannot be parsed.
    """
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _get_claims(event: dict) -> dict:
    """Extract Cognito JWT claims from an API Gateway authorizer context."""
    rc = (event or {}).get("requestContext") or {}
    auth = rc.get("authorizer") or {}
    jwt = auth.get("jwt") or {}
    return jwt.get("claims") or auth.get("claims") or {}


def _get_http_method(event: dict) -> str:
    """Return the HTTP method from an API Gateway v1 or v2 event, uppercased."""
    method = (
        (event or {}).get("httpMethod")
        or (event or {}).get("requestContext", {}).get("http", {}).get("method", "")
    )
    return (method or "").upper()


def _read_json_body(event: dict):
    """
    Parse the JSON body from an API Gateway event.
    Handles base64-encoded bodies (isBase64Encoded=true).
    Returns a dict on success, None if the body is invalid JSON, {} if empty.
    """
    raw = (event or {}).get("body") or ""
    if raw and (event or {}).get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except Exception:
            return None
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else (raw or {})
    except Exception:
        return None


def _as_int(x) -> int | None:
    """Safely convert a DynamoDB Decimal or other numeric value to int. Returns None on failure."""
    try:
        return int(x)
    except Exception:
        return None


# ---------- Game table type blob (stand-prod-game-table) ----------
# Same pattern as stand-prod-gameplayer-table: generic "type" map keyed by game type.


def get_game_type_blob(game_item: dict, game_type_upper: str) -> dict:
    """
    Return the type-specific blob for a game from the game item.
    game_item: item from stand-prod-game-table (get_item/query).
    game_type_upper: e.g. "INFOCARDS", "EMPAREJA2".
    """
    t = (game_item or {}).get("type") or {}
    return t.get((game_type_upper or "").upper()) or {}


def set_game_type_blob(table, game_id: str, game_type_upper: str, payload: dict, *, updated_at: str | None = None, condition_expression: str | None = None, condition_values: dict | None = None):
    """
    Write the type-specific blob for a game in stand-prod-game-table.
    table: boto3 dynamodb Table resource (games_table).
    game_id: PK of the game.
    game_type_upper: e.g. "INFOCARDS".
    payload: dict to store under type.<game_type_upper>.
    updated_at: optional ISO string to set updatedAt.
    condition_expression: optional e.g. "attribute_exists(gameId)".
    condition_values: optional dict merged into ExpressionAttributeValues for the condition.
    """
    g = (game_type_upper or "").upper()
    key = {"gameId": game_id}

    # 1) Ensure the type map exists (single path [type]); avoids "invalid document path" when type is missing.
    kwargs1 = {
        "Key": key,
        "UpdateExpression": "SET #type = if_not_exists(#type, :empty)",
        "ExpressionAttributeNames": {"#type": "type"},
        "ExpressionAttributeValues": {":empty": {}},
    }
    if condition_expression:
        kwargs1["ConditionExpression"] = condition_expression
    if condition_values:
        kwargs1["ExpressionAttributeValues"] = {**kwargs1["ExpressionAttributeValues"], **condition_values}
    table.update_item(**kwargs1)

    # 2) Set the game-type blob (single path [type, <g>]); no overlap with first call.
    names = {"#type": "type", "#g": g}
    values = {":payload": payload}
    expr = "SET #type.#g = :payload"
    if updated_at is not None:
        expr += ", updatedAt = :now"
        values[":now"] = updated_at
    table.update_item(
        Key=key,
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )
