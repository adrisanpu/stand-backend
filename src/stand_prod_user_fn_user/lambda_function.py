import os
import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _iso_now, _parse_iso, _get_claims, _get_http_method

dynamodb = boto3.resource("dynamodb")
USERS_TABLE = os.environ.get("USERS_TABLE", "")

def _is_expired(active_until: str | None) -> bool:
    if not active_until:
        return False
    try:
        return datetime.now(timezone.utc) > _parse_iso(active_until)
    except Exception:
        return False

def _normalize_user(item: dict) -> dict:
    # Garantiza forma consistente en respuesta
    user = dict(item or {})
    user.setdefault("plan", "FREE")
    user.setdefault("activeUntil", None)
    return user

def handle_bootstrap(event):
    log("handle_bootstrap:start")

    if not USERS_TABLE:
        return _resp(500, {"error": "Missing USERS_TABLE env var"})

    claims = _get_claims(event)
    user_id = claims.get("sub")
    email = claims.get("email") or claims.get("cognito:username")

    if not user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    table = dynamodb.Table(USERS_TABLE)
    now = _iso_now()

    try:
        existing = table.get_item(Key={"userId": user_id}).get("Item")
        if existing:
            # (Opcional) si existe y está expirado, lo corregimos aquí también
            user = _normalize_user(existing)
            if user.get("plan") == "EVENT_24H" and _is_expired(user.get("activeUntil")):
                table.update_item(
                    Key={"userId": user_id},
                    UpdateExpression="SET #plan=:p, #updatedAt=:now REMOVE #activeUntil",
                    ExpressionAttributeNames={"#plan": "plan", "#updatedAt": "updatedAt", "#activeUntil": "activeUntil"},
                    ExpressionAttributeValues={":p": "FREE", ":now": now},
                )
                user["plan"] = "FREE"
                user["activeUntil"] = None
            return _resp(200, {"user": user})

        # Nuevo user
        user = {
            "userId": user_id,
            "email": email,
            "plan": "FREE",
            "createdAt": now,
            "updatedAt": now,
        }
        table.put_item(
            Item=user,
            ConditionExpression="attribute_not_exists(userId)",
        )
        return _resp(200, {"user": _normalize_user(user)})

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            existing = table.get_item(Key={"userId": user_id}).get("Item")
            return _resp(200, {"user": _normalize_user(existing)})
        log("handle_bootstrap:dynamodb_error", e.response)
        return _resp(500, {"error": "Internal error"})
    except Exception as e:
        log("handle_bootstrap:exception", str(e))
        return _resp(500, {"error": "Internal error"})

def handle_me(event):
    log("handle_me:start")

    if not USERS_TABLE:
        return _resp(500, {"error": "Missing USERS_TABLE env var"})

    claims = _get_claims(event)
    user_id = claims.get("sub")
    if not user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    table = dynamodb.Table(USERS_TABLE)
    now = _iso_now()

    try:
        item = table.get_item(Key={"userId": user_id}).get("Item")
        if not item:
            return _resp(200, {"user": {"userId": user_id, "plan": "FREE", "activeUntil": None}})

        user = _normalize_user(item)

        # Si expiró, lo corregimos EN DB para que TODO el backend lo vea bien
        if user.get("plan") == "EVENT_24H" and _is_expired(user.get("activeUntil")):
            table.update_item(
                Key={"userId": user_id},
                UpdateExpression="SET #plan=:p, #updatedAt=:now REMOVE #activeUntil",
                ExpressionAttributeNames={"#plan": "plan", "#updatedAt": "updatedAt", "#activeUntil": "activeUntil"},
                ExpressionAttributeValues={":p": "FREE", ":now": now},
            )
            user["plan"] = "FREE"
            user["activeUntil"] = None

        return _resp(200, {"user": user})

    except Exception as e:
        log("handle_me:exception", str(e))
        return _resp(500, {"error": "Internal error"})

def lambda_handler(event, context):
    method = _get_http_method(event)

    if method == "OPTIONS":
        return _resp(200, {"ok": True})
    if method == "POST":
        return handle_bootstrap(event)
    if method == "GET":
        return handle_me(event)

    return _resp(405, {"error": f"Method {method} not allowed"})
