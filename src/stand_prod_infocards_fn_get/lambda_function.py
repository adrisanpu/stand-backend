import os
import boto3
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _get_claims, _read_json_body, _iso_now, get_game_type_blob, set_game_type_blob

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
INFOCARDS_BUCKET = os.environ.get("INFOCARDS_BUCKET", "")

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")
games_table = dynamodb.Table(GAMES_TABLE)
s3 = boto3.client("s3")

PRESIGNED_GET_TTL = 3600  # 1 hour


def _get_query_params(event):
    return event.get("queryStringParameters") or {}


def _presign_card_urls(card: dict) -> dict:
    """Inject fresh presigned GET URLs for imageKey/videoKey."""
    if not INFOCARDS_BUCKET:
        return card

    card = dict(card)
    image_key = card.get("imageKey")
    video_key = card.get("videoKey")

    if image_key:
        try:
            card["imageUrl"] = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": INFOCARDS_BUCKET, "Key": image_key},
                ExpiresIn=PRESIGNED_GET_TTL,
            )
        except ClientError as e:
            log("Presign imageKey failed", {"key": image_key, "error": str(e)})
            card["imageUrl"] = None
    else:
        card["imageUrl"] = None

    if video_key:
        try:
            card["videoUrl"] = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": INFOCARDS_BUCKET, "Key": video_key},
                ExpiresIn=PRESIGNED_GET_TTL,
            )
        except ClientError as e:
            log("Presign videoKey failed", {"key": video_key, "error": str(e)})
            card["videoUrl"] = None
    else:
        card["videoUrl"] = None

    return card


def _handle_get(event, owner_user_id):
    params = _get_query_params(event)
    game_id = (params.get("gameId") or "").strip()

    if not game_id:
        return _resp(400, {"error": "Missing required query parameter 'gameId'."})

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

    blob = get_game_type_blob(item, "INFOCARDS")
    raw_cards = blob.get("cards") or []
    cards = [_presign_card_urls(c) for c in raw_cards]

    return _resp(200, {"ok": True, "gameId": game_id, "cards": cards})


def _handle_put(event, owner_user_id):
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"error": "Invalid JSON body"})

    game_id = (body.get("gameId") or "").strip()
    cards = body.get("cards")

    if not game_id:
        return _resp(400, {"error": "Missing required field 'gameId'."})

    if not isinstance(cards, list):
        return _resp(400, {"error": "Field 'cards' must be an array."})

    # Verify ownership
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

    # Strip ephemeral client-side fields; keep only persisted fields
    clean_cards = []
    for card in cards:
        clean_cards.append({
            "id": card.get("id", ""),
            "title": card.get("title", ""),
            "body": card.get("body", ""),
            "imageKey": card.get("imageKey"),
            "videoKey": card.get("videoKey"),
            "order": card.get("order", 0),
        })

    try:
        set_game_type_blob(
            games_table,
            game_id,
            "INFOCARDS",
            {"cards": clean_cards},
            updated_at=_iso_now(),
            condition_expression="attribute_exists(gameId)",
        )
    except ClientError as e:
        log("UpdateItem failed", {"error": str(e)})
        return _resp(500, {"error": "UpdateItemFailed", "detail": str(e)})

    return _resp(200, {"ok": True, "gameId": game_id, "cardsCount": len(clean_cards)})


def lambda_handler(event, context):
    method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "GET")
    )

    if method == "OPTIONS":
        return _resp(204, "")

    try:
        claims = _get_claims(event)
        owner_user_id = (claims.get("sub") or "").strip()
        if not owner_user_id:
            return _resp(401, {"error": "Missing sub claim (unauthorized)"})

        if method == "GET":
            return _handle_get(event, owner_user_id)
        elif method == "PUT":
            return _handle_put(event, owner_user_id)
        else:
            return _resp(405, {"error": f"Method {method} not allowed"})

    except Exception as e:
        log("UnhandledError", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
