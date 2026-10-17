import os
import re
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from stand_common.utils import _get_claims, _get_http_method, _read_json_body, _resp, _iso_now, log

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

USERS_TABLE = os.environ.get("USERS_TABLE", "")
USER_MEDIA_BUCKET = os.environ.get("USER_MEDIA_BUCKET", "")
# Dark UI (oscuro) → white letters; light UI (claro) → black letters
DEFAULT_STAND_LOGO_DARK_KEY = os.environ.get(
    "DEFAULT_STAND_LOGO_DARK_KEY", "defaults/logo/logo_stand_letters_white.png"
)
DEFAULT_STAND_LOGO_LIGHT_KEY = os.environ.get(
    "DEFAULT_STAND_LOGO_LIGHT_KEY", "defaults/logo/logo_stand_letters_black.png"
)
DEFAULT_STAND_AVATAR_IDLE_KEY = os.environ.get("DEFAULT_STAND_AVATAR_IDLE_KEY", "defaults/avatar/stand-idle.mp4")
DEFAULT_STAND_AVATAR_WIN_KEY = os.environ.get("DEFAULT_STAND_AVATAR_WIN_KEY", "defaults/avatar/stand-win.mp4")
DEFAULT_STAND_AVATAR_LOSE_KEY = os.environ.get("DEFAULT_STAND_AVATAR_LOSE_KEY", "defaults/avatar/stand-lose.mp4")

MAX_FILES_PER_REQUEST = 10
MAX_LOGO_SIZE = 5 * 1024 * 1024
MAX_AVATAR_SIZE = 30 * 1024 * 1024
PRESIGNED_TTL = 3600

LOGO_ALLOWED_TYPES = {"image/png", "image/jpeg", "image/webp", "image/svg+xml"}
AVATAR_ALLOWED_TYPES = {"video/mp4", "video/webm"}
AVATAR_STATES = {"idle", "win", "lose"}


def _empty_media() -> dict:
    return {
        "logo": {"items": [], "selectedId": None},
        "avatar": {"items": [], "selectedByState": {"idle": None, "win": None, "lose": None}},
    }


def _ensure_media_shape(item: dict | None) -> dict:
    media = dict((item or {}).get("media") or {})
    logo = dict(media.get("logo") or {})
    avatar = dict(media.get("avatar") or {})
    logo.setdefault("items", [])
    logo.setdefault("selectedId", None)
    avatar.setdefault("items", [])
    avatar_selected = dict(avatar.get("selectedByState") or {})
    avatar_selected.setdefault("idle", None)
    avatar_selected.setdefault("win", None)
    avatar_selected.setdefault("lose", None)
    avatar["selectedByState"] = avatar_selected
    media["logo"] = logo
    media["avatar"] = avatar
    return media


def _load_user_with_media(table, user_id: str) -> tuple[dict, dict]:
    item = table.get_item(Key={"userId": user_id}).get("Item")
    if not item:
        now = _iso_now()
        item = {
            "userId": user_id,
            "plan": "FREE",
            "createdAt": now,
            "updatedAt": now,
            "media": _empty_media(),
        }
        table.put_item(Item=item)
    media = _ensure_media_shape(item)
    return item, media


def _guess_extension(content_type: str) -> str:
    mapping = {
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/webp": "webp",
        "image/svg+xml": "svg",
        "video/mp4": "mp4",
        "video/webm": "webm",
    }
    return mapping.get(content_type, "bin")


def _sanitize_file_name(file_name: str) -> str:
    cleaned = "".join(c for c in file_name.strip().lower() if c.isalnum() or c in ("-", "_", "."))
    return cleaned[:80] or "file"


def _normalize_state(raw_state: str | None) -> str:
    state = (raw_state or "").strip().lower()
    if state not in AVATAR_STATES:
        raise ValueError("Avatar state must be one of: idle, win, lose.")
    return state


def _presign_get_url(key: str) -> str | None:
    if not USER_MEDIA_BUCKET:
        return None
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": USER_MEDIA_BUCKET, "Key": key},
            ExpiresIn=PRESIGNED_TTL,
        )
    except ClientError as e:
        log("presign_get_failed", {"key": key, "error": str(e)})
        return None


def _upsert_media_item(items: list[dict], new_item: dict) -> list[dict]:
    filtered = [it for it in items if (it.get("id") or "") != new_item["id"]]
    filtered.insert(0, new_item)
    return filtered[:100]


def _handle_upload_url(event, user_id: str):
    if not USER_MEDIA_BUCKET:
        return _resp(500, {"error": "Missing USER_MEDIA_BUCKET env var"})

    body = _read_json_body(event) or {}
    files = body.get("files") or []
    if not isinstance(files, list) or not files:
        return _resp(400, {"error": "Missing required field 'files' (non-empty list)."})
    if len(files) > MAX_FILES_PER_REQUEST:
        return _resp(400, {"error": f"Too many files. Maximum {MAX_FILES_PER_REQUEST}."})

    upload_urls = []
    for file_info in files:
        kind = (file_info.get("kind") or "").strip().lower()
        file_name = _sanitize_file_name(file_info.get("fileName") or "")
        content_type = (file_info.get("contentType") or "").strip().lower()
        size_bytes = int(file_info.get("sizeBytes") or 0)
        display_name = _sanitize_file_name(file_info.get("name") or file_name.rsplit(".", 1)[0])

        if kind not in {"logo", "avatar"}:
            return _resp(400, {"error": "kind must be 'logo' or 'avatar'."})
        if not content_type:
            return _resp(400, {"error": "contentType is required."})

        state = None
        if kind == "logo":
            if content_type not in LOGO_ALLOWED_TYPES:
                return _resp(400, {"error": "Unsupported logo content type.", "allowedTypes": sorted(LOGO_ALLOWED_TYPES)})
            if size_bytes > MAX_LOGO_SIZE:
                return _resp(400, {"error": "Logo file too large (max 5MB)."})
            ext = _guess_extension(content_type)
            key = f"users/{user_id}/logos/{uuid.uuid4()}-{display_name}.{ext}"
        else:
            if content_type not in AVATAR_ALLOWED_TYPES:
                return _resp(400, {"error": "Unsupported avatar content type.", "allowedTypes": sorted(AVATAR_ALLOWED_TYPES)})
            if size_bytes > MAX_AVATAR_SIZE:
                return _resp(400, {"error": "Avatar file too large (max 30MB)."})
            state = _normalize_state(file_info.get("state"))
            ext = _guess_extension(content_type)
            key = f"users/{user_id}/avatars/{display_name}__{state}__{uuid.uuid4()}.{ext}"

        media_id = str(uuid.uuid4())
        try:
            put_url = s3.generate_presigned_url(
                "put_object",
                Params={"Bucket": USER_MEDIA_BUCKET, "Key": key, "ContentType": content_type},
                ExpiresIn=PRESIGNED_TTL,
            )
        except ClientError as e:
            log("presign_put_failed", {"key": key, "error": str(e)})
            return _resp(500, {"error": "PresignFailed", "detail": str(e)})

        upload_urls.append(
            {
                "id": media_id,
                "kind": kind,
                "state": state,
                "key": key,
                "url": put_url,
                "expiresIn": PRESIGNED_TTL,
                "meta": {
                    "id": media_id,
                    "kind": kind,
                    "state": state,
                    "name": display_name,
                    "fileName": file_name,
                    "contentType": content_type,
                    "sizeBytes": size_bytes,
                    "key": key,
                },
            }
        )

    return _resp(200, {"ok": True, "uploadUrls": upload_urls})


def _handle_register(event, table, user_id: str):
    body = _read_json_body(event) or {}
    items = body.get("items") or []
    if not isinstance(items, list) or not items:
        return _resp(400, {"error": "Missing required field 'items' (non-empty list)."})

    db_user, media = _load_user_with_media(table, user_id)
    now = _iso_now()
    for item in items:
        kind = (item.get("kind") or "").strip().lower()
        if kind not in {"logo", "avatar"}:
            return _resp(400, {"error": "kind must be 'logo' or 'avatar'."})
        media_item = {
            "id": (item.get("id") or "").strip(),
            "kind": kind,
            "name": (item.get("name") or "file").strip(),
            "fileName": (item.get("fileName") or "").strip(),
            "contentType": (item.get("contentType") or "").strip(),
            "sizeBytes": int(item.get("sizeBytes") or 0),
            "key": (item.get("key") or "").strip(),
            "createdAt": now,
        }
        if not media_item["id"] or not media_item["key"]:
            return _resp(400, {"error": "Each item must include id and key."})
        if kind == "avatar":
            media_item["state"] = _normalize_state(item.get("state"))
            media["avatar"]["items"] = _upsert_media_item(media["avatar"]["items"], media_item)
        else:
            media["logo"]["items"] = _upsert_media_item(media["logo"]["items"], media_item)

    table.update_item(
        Key={"userId": user_id},
        UpdateExpression="SET #media=:media, #updatedAt=:updatedAt",
        ExpressionAttributeNames={"#media": "media", "#updatedAt": "updatedAt"},
        ExpressionAttributeValues={":media": media, ":updatedAt": now},
    )
    return _resp(200, {"ok": True, "count": len(items), "userId": db_user["userId"]})


def _handle_select(event, table, user_id: str):
    body = _read_json_body(event) or {}
    kind = (body.get("kind") or "").strip().lower()
    if kind == "appearance":
        appearance = body.get("appearance")
        if not isinstance(appearance, dict) or appearance.get("themeMode") not in {"dark", "light"}:
            return _resp(400, {"error": "Invalid appearance themeMode."})
        colors = appearance.get("colors")
        if colors is not None and (
            not isinstance(colors, dict) or set(colors) != {"primary", "accent"}
            or any(not isinstance(value, str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", value)
                   for value in colors.values())
        ):
            return _resp(400, {"error": "Appearance colors must be primary/accent hex colors."})
        _, media = _load_user_with_media(table, user_id)
        media["appearance"] = {"colors": colors, "themeMode": appearance["themeMode"]}
        table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET #media=:media, #updatedAt=:updatedAt",
            ExpressionAttributeNames={"#media": "media", "#updatedAt": "updatedAt"},
            ExpressionAttributeValues={":media": media, ":updatedAt": _iso_now()},
        )
        return _resp(200, {"ok": True})
    media_id = (body.get("mediaId") or "").strip()
    if kind not in {"logo", "avatar"}:
        return _resp(400, {"error": "kind must be 'logo' or 'avatar'."})
    if not media_id:
        return _resp(400, {"error": "mediaId is required."})

    _, media = _load_user_with_media(table, user_id)
    now = _iso_now()
    if kind == "logo":
        found = any((it.get("id") or "") == media_id for it in media["logo"]["items"])
        if not found:
            return _resp(404, {"error": "MediaNotFound"})
        media["logo"]["selectedId"] = media_id
    else:
        state = _normalize_state(body.get("state"))
        found = any((it.get("id") or "") == media_id for it in media["avatar"]["items"])
        if not found:
            return _resp(404, {"error": "MediaNotFound"})
        media["avatar"]["selectedByState"][state] = media_id

    table.update_item(
        Key={"userId": user_id},
        UpdateExpression="SET #media=:media, #updatedAt=:updatedAt",
        ExpressionAttributeNames={"#media": "media", "#updatedAt": "updatedAt"},
        ExpressionAttributeValues={":media": media, ":updatedAt": now},
    )
    return _resp(200, {"ok": True})


def _handle_delete(event, table, user_id: str):
    body = _read_json_body(event) or {}
    kind = (body.get("kind") or "").strip().lower()
    media_id = (body.get("mediaId") or "").strip()
    if kind not in {"logo", "avatar"} or not media_id:
        return _resp(400, {"error": "kind and mediaId are required."})

    _, media = _load_user_with_media(table, user_id)
    now = _iso_now()
    if kind == "logo":
        media["logo"]["items"] = [it for it in media["logo"]["items"] if (it.get("id") or "") != media_id]
        if media["logo"]["selectedId"] == media_id:
            media["logo"]["selectedId"] = None
    else:
        media["avatar"]["items"] = [it for it in media["avatar"]["items"] if (it.get("id") or "") != media_id]
        for state in AVATAR_STATES:
            if media["avatar"]["selectedByState"].get(state) == media_id:
                media["avatar"]["selectedByState"][state] = None

    table.update_item(
        Key={"userId": user_id},
        UpdateExpression="SET #media=:media, #updatedAt=:updatedAt",
        ExpressionAttributeNames={"#media": "media", "#updatedAt": "updatedAt"},
        ExpressionAttributeValues={":media": media, ":updatedAt": now},
    )
    return _resp(200, {"ok": True})


def _handle_get(table, user_id: str):
    _, media = _load_user_with_media(table, user_id)
    logo_items = []
    for item in media["logo"]["items"]:
        logo_items.append({**item, "url": _presign_get_url(item.get("key") or "")})
    avatar_items = []
    for item in media["avatar"]["items"]:
        avatar_items.append({**item, "url": _presign_get_url(item.get("key") or "")})

    selected_logo = next((it for it in logo_items if it.get("id") == media["logo"]["selectedId"]), None)

    logo_defaults = {}
    if DEFAULT_STAND_LOGO_DARK_KEY:
        logo_defaults["dark"] = {
            "id": "default-stand-logo-dark",
            "kind": "logo",
            "name": "stand-letters-white",
            "fileName": "logo_stand_letters_white.png",
            "contentType": "image/png",
            "sizeBytes": 0,
            "key": DEFAULT_STAND_LOGO_DARK_KEY,
            "createdAt": None,
            "url": _presign_get_url(DEFAULT_STAND_LOGO_DARK_KEY),
        }
    if DEFAULT_STAND_LOGO_LIGHT_KEY:
        logo_defaults["light"] = {
            "id": "default-stand-logo-light",
            "kind": "logo",
            "name": "stand-letters-black",
            "fileName": "logo_stand_letters_black.png",
            "contentType": "image/png",
            "sizeBytes": 0,
            "key": DEFAULT_STAND_LOGO_LIGHT_KEY,
            "createdAt": None,
            "url": _presign_get_url(DEFAULT_STAND_LOGO_LIGHT_KEY),
        }

    default_avatar_key_by_state = {
        "idle": DEFAULT_STAND_AVATAR_IDLE_KEY,
        "win": DEFAULT_STAND_AVATAR_WIN_KEY,
        "lose": DEFAULT_STAND_AVATAR_LOSE_KEY,
    }
    selected_avatar_by_state = {}
    for state in ("idle", "win", "lose"):
        selected_id = media["avatar"]["selectedByState"].get(state)
        selected_item = next((it for it in avatar_items if it.get("id") == selected_id), None)
        if not selected_item and default_avatar_key_by_state.get(state):
            default_key = default_avatar_key_by_state[state]
            selected_item = {
                "id": f"default-stand-avatar-{state}",
                "kind": "avatar",
                "name": f"stand-default-avatar-{state}",
                "fileName": f"stand-{state}.mp4",
                "contentType": "video/mp4",
                "sizeBytes": 0,
                "key": default_key,
                "state": state,
                "createdAt": None,
                "url": _presign_get_url(default_key),
            }
        selected_avatar_by_state[state] = selected_item

    return _resp(
        200,
        {
            "ok": True,
            "appearance": media.get("appearance"),
            "logo": {
                "items": logo_items,
                "selectedId": media["logo"]["selectedId"],
                "selected": selected_logo,
                "defaults": logo_defaults,
            },
            "avatar": {
                "items": avatar_items,
                "selectedByState": media["avatar"]["selectedByState"],
                "selected": selected_avatar_by_state,
            },
        },
    )


def lambda_handler(event, context):
    if not USERS_TABLE:
        return _resp(500, {"error": "Missing USERS_TABLE env var"})

    method = _get_http_method(event)
    if method == "OPTIONS":
        return _resp(204, "")

    claims = _get_claims(event)
    user_id = (claims.get("sub") or "").strip()
    if not user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    table = dynamodb.Table(USERS_TABLE)
    action = ((event.get("queryStringParameters") or {}).get("action") or "").strip().lower()

    try:
        if method == "GET":
            return _handle_get(table, user_id)
        if method == "POST" and action == "upload-url":
            return _handle_upload_url(event, user_id)
        if method == "POST":
            return _handle_register(event, table, user_id)
        if method == "PUT":
            return _handle_select(event, table, user_id)
        if method == "DELETE":
            return _handle_delete(event, table, user_id)
        return _resp(405, {"error": f"Method {method} not allowed"})
    except ValueError as e:
        return _resp(400, {"error": str(e)})
    except Exception as e:
        log("user_media_unhandled_error", {"error": str(e), "ts": datetime.now(timezone.utc).isoformat()})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
