import os
import json
import re
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _iso_now, _parse_iso, _get_claims, _get_http_method, _read_json_body

dynamodb = boto3.resource("dynamodb")
USERS_TABLE = os.environ.get("USERS_TABLE", "")
USER_MEDIA_BUCKET = os.environ.get("USER_MEDIA_BUCKET", "")
s3 = boto3.client("s3")
PHOTO_TYPES = {"image/png", "image/jpeg", "image/webp"}
MAX_PHOTO_SIZE = 5 * 1024 * 1024

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
    user.pop("kivlyConnection", None)
    user.setdefault("plan", "FREE")
    user.setdefault("activeUntil", None)
    key = user.get("profilePhotoKey")
    user["profilePicUrl"] = (
        s3.generate_presigned_url(
            "get_object", Params={"Bucket": USER_MEDIA_BUCKET, "Key": key}, ExpiresIn=3600,
        ) if key and USER_MEDIA_BUCKET else None
    )
    return user


def handle_update_profile(event):
    claims = _get_claims(event)
    user_id = claims.get("sub")
    if not user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})
    if not USERS_TABLE:
        return _resp(500, {"error": "Missing USERS_TABLE env var"})
    try:
        body = _read_json_body(event)
        if not isinstance(body, dict) or set(body) != {"name", "profilePhotoKey"}:
            return _resp(400, {"error": "Only name and profilePhotoKey are accepted."})
        name, key = body["name"], body["profilePhotoKey"]
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 100:
            return _resp(400, {"error": "Name must contain 1 to 100 characters."})
        if key is not None:
            prefix = f"users/{user_id}/profile-photos/"
            if not isinstance(key, str) or not key.startswith(prefix) or not re.fullmatch(
                r"[a-f0-9-]{36}\.(png|jpg|webp)", key[len(prefix):],
            ):
                return _resp(400, {"error": "Invalid profile photo key."})
            if not USER_MEDIA_BUCKET:
                return _resp(500, {"error": "Missing USER_MEDIA_BUCKET env var"})
            try:
                photo = s3.head_object(Bucket=USER_MEDIA_BUCKET, Key=key)
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                    return _resp(400, {"error": "Profile photo has not been uploaded."})
                raise
            if photo.get("ContentType") not in PHOTO_TYPES or not 0 < photo.get("ContentLength", 0) <= MAX_PHOTO_SIZE:
                return _resp(400, {"error": "Invalid profile photo type or size."})

        now = _iso_now()
        table = dynamodb.Table(USERS_TABLE)
        result = table.update_item(
            Key={"userId": user_id},
            UpdateExpression=(
                "SET #name=:name, #photo=:photo, #updated=:now, "
                "#created=if_not_exists(#created,:now), #plan=if_not_exists(#plan,:free), "
                "#email=if_not_exists(#email,:email)"
            ),
            ExpressionAttributeNames={
                "#name": "name", "#photo": "profilePhotoKey", "#updated": "updatedAt",
                "#created": "createdAt", "#plan": "plan", "#email": "email",
            },
            ExpressionAttributeValues={
                ":name": name.strip(), ":photo": key, ":now": now,
                ":free": "FREE", ":email": claims.get("email"),
            },
            ReturnValues="ALL_NEW",
        )
        return _resp(200, {"user": _normalize_user(result["Attributes"])})
    except (ValueError, TypeError):
        return _resp(400, {"error": "Invalid profile request."})
    except Exception as error:
        log("update_profile:exception", str(error))
        return _resp(500, {"error": "Could not save account profile."})

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
            return _resp(200, {"user": _normalize_user({
                "userId": user_id, "email": claims.get("email"), "plan": "FREE",
            })})

        user = _normalize_user(item)
        user["email"] = user.get("email") or claims.get("email")

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
    if method == "PUT":
        return handle_update_profile(event)

    return _resp(405, {"error": f"Method {method} not allowed"})
