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
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "")
RAFFLE_STAND_HANDLE = (os.environ.get("RAFFLE_STAND_HANDLE") or "stand_official").strip().lower().lstrip("@")
# ==============

dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
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


# ===== Raffle follow notification (T1MER) =====

def _raffle_required_follows_with_stand_last(meta: dict) -> list[str]:
    """
    Devuelve la lista de cuentas a seguir para el sorteo (sin @), garantizando
    que la cuenta de Stand vaya al final.
    """
    follows = (meta or {}).get("raffleRequiredFollows") or []
    if not isinstance(follows, list):
        follows = []
    out: list[str] = []
    for h in follows:
        if not h:
            continue
        s = str(h).strip().lower().lstrip("@")
        if s:
            out.append(s)
    if RAFFLE_STAND_HANDLE and RAFFLE_STAND_HANDLE not in out:
        out.append(RAFFLE_STAND_HANDLE)
    return out


def _score_send_bulk(messages):
    if not messages:
        return
    if not IG_SENDER_LAMBDA:
        log("score_send_bulk_skipped_missing_IG_SENDER_LAMBDA", {"count": len(messages)})
        return
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"messages": messages}, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("score_send_bulk_error", {"error": repr(e), "count": len(messages)})


def _send_raffle_follow_after_score(game_id: str, player_id: int, meta: dict) -> None:
    """
    En T1MER, después de guardar el score, avisamos al jugador de las
    condiciones del sorteo (cuentas a seguir).
    """
    handles = _raffle_required_follows_with_stand_last(meta)
    if not handles:
        return

    try:
        resp = gp_table.get_item(Key={"gameId": game_id, "playerId": int(player_id)})
        it = resp.get("Item") or {}
    except Exception as e:
        log("score_raffle_follow_get_player_error", {"gameId": game_id, "playerId": player_id, "error": repr(e)})
        return

    psid = (it.get("instagramPSID") or "").strip()
    if not psid:
        return

    intro = "⚠️ Para participar en el sorteo, sigue estas cuentas:"
    messages = [
        {"psid": psid, "text": intro},
        {"psid": psid, "template": "follow_accounts", "handles": handles},
    ]
    _score_send_bulk(messages)


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

    if game_type == "L3TRAS":
        log("score_l3tras_ack", {"gameId": game_id, "success": body.get("success")})
        return _resp(200, {"ok": True, "gameId": game_id, "gameType": game_type})

    if game_type == "T1MER":
        result = t1mer.store_score(
            game_id=game_id,
            game_type_upper=game_type,
            gp_table=gp_table,
            payload=body,
            log_fn=log,
        )

        # Notificar condiciones del sorteo después de guardar el score
        if result.get("ok") and result.get("playerId") is not None:
            try:
                _send_raffle_follow_after_score(game_id, int(result["playerId"]), meta)
            except Exception as e:
                # No romper el flujo de score por errores de notificación
                log("score_raffle_follow_after_score_error", {"gameId": game_id, "playerId": result.get("playerId"), "error": repr(e)})

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
