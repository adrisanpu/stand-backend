# lambda_function.py  (handler: lambda_function.lambda_handler)
import os
import json
import base64
from decimal import Decimal

import boto3
from stand_common.utils import log, _json_sanitize, _resp

import games.t1mer as t1mer

# ===== ENV =====
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
# ==============

dynamo_r = boto3.resource("dynamodb")
games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)


def _parse_body(event):
    raw = event.get("body")
    if raw and event.get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except Exception as e:
            log("score_body_base64_decode_error", {"error": repr(e)})
            raw = None

    if not raw:
        return {}

    try:
        return json.loads(raw)
    except Exception as e:
        log("score_body_json_parse_error", {"error": repr(e), "raw_sample": str(raw)[:200]})
        return {}


def _method(event):
    return (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "GET")
    ).upper()


def _get_game_meta(game_id: str):
    resp = games_table.get_item(Key={"gameId": game_id})
    return resp.get("Item")


# -------------------- POST: store score --------------------
def _handle_post(event):
    body = _parse_body(event)

    game_id = (body.get("gameId") or "").strip().upper()
    if not game_id:
        return _resp(400, {"ok": False, "error": "MissingGameId"})

    meta = _get_game_meta(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True):
        return _resp(403, {"ok": False, "error": "GameInactive"})

    game_type = (meta.get("gameType") or "").upper()
    if not game_type:
        return _resp(500, {"ok": False, "error": "MissingGameType"})

    log("score_post_request", {"gameId": game_id, "gameType": game_type})

    if game_type == "T1MER":
        result = t1mer.store_score(
            game_id=game_id,
            game_type_upper=game_type,
            gp_table=gp_table,
            payload=body,
            log_fn=log,
        )
        return _resp(200 if result.get("ok") else 400, result)

    return _resp(400, {"ok": False, "error": "UnsupportedGame", "message": f"{game_type} no soporta score."})


# -------------------- GET: ranking --------------------
def _handle_get(event):
    params = event.get("queryStringParameters") or {}

    game_id = (params.get("gameId") or "").strip().upper()
    limit_raw = params.get("limit") or "10"

    try:
        limit = int(limit_raw)
    except Exception:
        limit = 10

    if not game_id:
        return _resp(400, {"ok": False, "error": "MissingGameId"})

    meta = _get_game_meta(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})

    game_type = (meta.get("gameType") or "").upper()
    if not game_type:
        return _resp(500, {"ok": False, "error": "MissingGameType"})

    log("score_get_request", {"gameId": game_id, "gameType": game_type, "limit": limit})

    if game_type == "T1MER":
        result = t1mer.get_ranking(
            game_id=game_id,
            game_type_upper=game_type,
            gp_table=gp_table,
            limit=limit,
            log_fn=log,
        )
        return _resp(200, result)

    return _resp(400, {"ok": False, "error": "UnsupportedGame", "message": f"{game_type} no soporta ranking."})


def lambda_handler(event, context):
    try:
        m = _method(event)

        if m == "OPTIONS":
            return {"statusCode": 204, "body": ""}

        if m == "POST":
            return _handle_post(event)

        if m == "GET":
            return _handle_get(event)

        return _resp(405, {"ok": False, "error": "MethodNotAllowed", "method": m})

    except Exception as e:
        log("score_internal_error", {"error": repr(e)})
        return _resp(500, {"ok": False, "error": "internal_error", "detail": repr(e)})
