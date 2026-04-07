import os
import uuid
import boto3
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _get_claims, _read_json_body

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
INFOCARDS_BUCKET = os.environ.get("INFOCARDS_BUCKET", "")

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")
games_table = dynamodb.Table(GAMES_TABLE)
s3 = boto3.client("s3")

# ========= CONST =========
ALLOWED_CONTENT_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/webp",
    "video/mp4",
    "video/webm",
}

PRESIGNED_URL_TTL = 3600  # 1 hour


def lambda_handler(event, context):
    method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "POST")
    )

    if method == "OPTIONS":
        return _resp(204, "")

    if method != "POST":
        return _resp(405, {"error": f"Method {method} not allowed"})

    try:
        claims = _get_claims(event)
        owner_user_id = (claims.get("sub") or "").strip()
        if not owner_user_id:
            return _resp(401, {"error": "Missing sub claim (unauthorized)"})

        body = _read_json_body(event)
        if body is None:
            return _resp(400, {"error": "Invalid JSON body"})

        game_id = (body.get("gameId") or "").strip()
        files = body.get("files") or []

        if not game_id:
            return _resp(400, {"error": "Missing required field 'gameId'."})

        if not files or not isinstance(files, list):
            return _resp(400, {"error": "Missing required field 'files' (must be a non-empty list)."})

        if len(files) > 20:
            return _resp(400, {"error": "Too many files. Maximum 20 files per request."})

        # Verify game ownership
        try:
            resp = games_table.get_item(Key={"gameId": game_id})
            item = resp.get("Item")
        except ClientError as e:
            log("GetItem failed", {"error": str(e)})
            return _resp(500, {"error": "GetItemFailed", "detail": str(e)})

        if not item:
            return _resp(404, {"error": "GameNotFound", "gameId": game_id})

        if (item.get("ownerUserId") or "") != owner_user_id:
            return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})

        if not INFOCARDS_BUCKET:
            return _resp(500, {"error": "Infocards bucket not configured."})

        # Generate presigned URLs
        upload_urls = []
        for file_info in files:
            file_name = (file_info.get("fileName") or "").strip()
            content_type = (file_info.get("contentType") or "").strip()

            if not file_name:
                return _resp(400, {"error": "Each file entry must have a 'fileName'."})

            if content_type not in ALLOWED_CONTENT_TYPES:
                return _resp(400, {
                    "error": f"Content type '{content_type}' not allowed.",
                    "allowedTypes": sorted(ALLOWED_CONTENT_TYPES),
                })

            key = f"infocards/{game_id}/{uuid.uuid4()}-{file_name}"

            try:
                presigned_url = s3.generate_presigned_url(
                    "put_object",
                    Params={
                        "Bucket": INFOCARDS_BUCKET,
                        "Key": key,
                        "ContentType": content_type,
                    },
                    ExpiresIn=PRESIGNED_URL_TTL,
                )
            except ClientError as e:
                log("Presign failed", {"error": str(e), "key": key})
                return _resp(500, {"error": "PresignFailed", "detail": str(e)})

            upload_urls.append({
                "key": key,
                "url": presigned_url,
                "fileName": file_name,
                "expiresIn": PRESIGNED_URL_TTL,
            })

        return _resp(200, {"ok": True, "gameId": game_id, "uploadUrls": upload_urls})

    except Exception as e:
        log("UnhandledError", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
