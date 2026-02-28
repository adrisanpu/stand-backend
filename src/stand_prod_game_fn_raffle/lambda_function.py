# raffle.py  (handler: raffle.lambda_handler)
import os
import json
import base64
import random
from decimal import Decimal
from datetime import datetime, timezone
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from stand_common.utils import log, _json_sanitize, _resp, _iso_now, _as_int

# ============ ENV VARS ============
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "instagram-sender")
GSI_RAFFLE_ELIGIBLE = os.environ.get("GSI_RAFFLE_ELIGIBLE", "gsi-raffleEligible")
# =================================

dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)


def _parse_event(event):
    """
    Soporta:
      - HTTP API v2 (requestContext.http.method)
      - API Gateway REST (httpMethod)
      - invoke directo
    """
    method = "INVOKE"
    body = {}

    if isinstance(event, dict) and ("httpMethod" in event or "requestContext" in event):
        method = (
            event.get("httpMethod")
            or event.get("requestContext", {}).get("http", {}).get("method", "POST")
        ).upper()

        if method == "OPTIONS":
            return method, None, None, None

        raw = event.get("body") or ""
        if raw and event.get("isBase64Encoded"):
            try:
                raw = base64.b64decode(raw).decode("utf-8")
            except Exception as e:
                log("body_base64_decode_error", {"error": repr(e)})
                raw = ""

        try:
            body = json.loads(raw) if raw else {}
        except Exception as e:
            log("body_json_parse_error", {"error": repr(e), "raw_sample": str(raw)[:200]})
            body = {}

        qs = event.get("queryStringParameters") or {}
        # permitimos también querystring
        if not body.get("gameId") and qs.get("gameId"):
            body["gameId"] = qs.get("gameId")
        if body.get("numberOfWinners") is None and qs.get("numberOfWinners") is not None:
            body["numberOfWinners"] = qs.get("numberOfWinners")
        if body.get("applicableOnlyValidated") is None and qs.get("applicableOnlyValidated") is not None:
            body["applicableOnlyValidated"] = qs.get("applicableOnlyValidated")

    else:
        method = "INVOKE"
        body = event or {}

    game_id = (str(body.get("gameId") or "")).strip().upper()

    n_winners = body.get("numberOfWinners")
    try:
        n_winners = int(n_winners)
    except Exception:
        n_winners = None

    only_validated = body.get("applicableOnlyValidated", False)
    # normaliza strings "true"/"false"
    if isinstance(only_validated, str):
        only_validated = only_validated.strip().lower() in ("1", "true", "yes", "y")

    return method, game_id, n_winners, bool(only_validated)


# ----------------- IG sender -----------------

def _send_bulk_dms(messages):
    """
    messages: list of { psid, text }
    """
    if not messages:
        return
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"messages": messages}, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("igsender_invoke_error", {"error": repr(e), "count": len(messages)})


# ----------------- Dynamo helpers -----------------

def _get_game(game_id: str):
    resp = games_table.get_item(Key={"gameId": game_id})
    return resp.get("Item")


def _save_raffle_winners_to_game(game_id: str, winners: list[dict], only_validated: bool, win_ts: str):
    """
    Persiste en games_table:
      raffleWinners: [{playerId, instagramUsername, instagramPSID, validationCode?, wonAt}]
      raffleLastRunAt: ISO
      raffleOnlyValidated: bool
    """
    # sanitize winners
    out = []
    for w in winners:
        pid = _as_int(w.get("playerId"))
        if pid is None:
            continue

        val_code = w.get("validationCode")
        try:
            if isinstance(val_code, Decimal):
                val_code = int(val_code)
            elif val_code is not None:
                val_code = int(val_code)
        except Exception:
            val_code = None

        row = {
            "playerId": pid,
            "instagramUsername": w.get("instagramUsername"),
            "instagramPSID": w.get("instagramPSID"),
            "wonAt": win_ts,
        }
        if val_code is not None:
            row["validationCode"] = val_code

        out.append(row)

    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression="SET raffleWinners = :w, raffleLastRunAt = :t, raffleOnlyValidated = :ov",
        ExpressionAttributeValues={
            ":w": out,
            ":t": win_ts,
            ":ov": bool(only_validated),
        },
    )


def _query_all_players(game_id: str):
    """
    Query por partición (gameId). Usado para broadcast a todos los jugadores.
    """
    items = []
    last = None
    while True:
        kwargs = {"KeyConditionExpression": Key("gameId").eq(game_id)}
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = gp_table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
    return items


def _query_eligible_players(game_id: str):
    """
    Query GSI gsi-raffleEligible (PK=eligibleForGameId) to get only
    raffle-eligible players. The attribute is removed when a player wins,
    so this index is sparse and contains only current candidates.
    Falls back to _query_all_players + _is_raffle_eligible filter if the
    GSI is unavailable (e.g. during a blue/green deployment).
    """
    try:
        items = []
        last = None
        while True:
            kwargs = {
                "IndexName": GSI_RAFFLE_ELIGIBLE,
                "KeyConditionExpression": Key("eligibleForGameId").eq(game_id),
            }
            if last:
                kwargs["ExclusiveStartKey"] = last
            resp = gp_table.query(**kwargs)
            items.extend(resp.get("Items") or [])
            last = resp.get("LastEvaluatedKey")
            if not last:
                break

        # GSI projection is KEYS_ONLY; fetch full items in batch
        if not items:
            return []
        keys = [{"gameId": it["gameId"], "playerId": it["playerId"]} for it in items]
        result = []
        # BatchGetItem supports up to 100 items per call
        for i in range(0, len(keys), 100):
            batch = keys[i:i + 100]
            resp = dynamo_r.batch_get_item(
                RequestItems={GAMEPLAYER_TABLE: {"Keys": batch}}
            )
            result.extend(resp.get("Responses", {}).get(GAMEPLAYER_TABLE, []))
        return result

    except Exception as e:
        log("raffle_gsi_fallback", {"error": repr(e), "gameId": game_id})
        return [it for it in _query_all_players(game_id) if _is_raffle_eligible(it, "")]


def _is_raffle_eligible(it: dict, game_type_upper: str) -> bool:
    """
    Nuevo: type.<g>.raffleEligible
    Compat: raffleElegible (typo viejo) / raffleEligible en root
    Default: True
    """
    gk = (game_type_upper or "").upper()
    t = it.get("type") or {}
    tb = t.get(gk) or {}

    if "raffleEligible" in tb:
        return bool(tb.get("raffleEligible"))

    # compat root
    if "raffleEligible" in it:
        return bool(it.get("raffleEligible"))
    if "raffleElegible" in it:
        return bool(it.get("raffleElegible"))

    return True


def _is_validated(it: dict) -> bool:
    return bool(it.get("validated", False))


def _has_psid(it: dict) -> bool:
    psid = it.get("instagramPSID")
    return bool(psid and psid != "#")


def _mark_winner(game_id: str, player_id: int, game_type_upper: str, win_ts: str):
    """
    Escribe en type.<g>:
      raffleEligible=false, raffleWin=true, raffleWinAt=...
    Evita solapes en Dynamo UpdateExpression separando paths:
      1) asegura type
      2) asegura type.<g>
      3) set flags
    """
    gk = (game_type_upper or "").upper()
    if not gk:
        raise ValueError("Missing gameType")

    key = {"gameId": game_id, "playerId": int(player_id)}

    # 1) asegurar type (solo [type])
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type = if_not_exists(#type, :tinit)",
        ExpressionAttributeNames={"#type": "type"},
        ExpressionAttributeValues={":tinit": {}},
    )

    # 2) asegurar type.<g> (solo [type, <g>])
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type.#g = if_not_exists(#type.#g, :ginit)",
        ExpressionAttributeNames={"#type": "type", "#g": gk},
        ExpressionAttributeValues={":ginit": {}},
    )

    # 3) set flags + remove sparse GSI key so player leaves the eligible index
    gp_table.update_item(
        Key=key,
        UpdateExpression=(
            "SET #type.#g.raffleEligible = :f, "
            "#type.#g.raffleWin = :t, "
            "#type.#g.raffleWinAt = :ts "
            "REMOVE eligibleForGameId"
        ),
        ExpressionAttributeNames={"#type": "type", "#g": gk},
        ExpressionAttributeValues={":f": False, ":t": True, ":ts": win_ts},
    )


# ----------------- business logic -----------------

def _build_messages(game_type_upper: str, winner_usernames: list[str]):
    # Puedes personalizar por gameType si quieres
    label = game_type_upper.title() if game_type_upper else "juego"

    drums_msg = f"🥁🥁🥁 ¡Atención! Vamos a hacer ahora el sorteo de {label}…"
    countdown = ["3️⃣...", "2️⃣...", "1️⃣..."]
    announce_msg = (
        f"🎉 ¡Ya tenemos personas ganadoras del sorteo de {label}!\n"
        "Ganadores: " + ", ".join(winner_usernames)
    )
    return drums_msg, countdown, announce_msg


def lambda_handler(event, context):
    try:
        method, game_id, n_winners, only_validated = _parse_event(event)

        if method == "OPTIONS":
            return {"statusCode": 204, "body": ""}

        if not game_id:
            return _resp(400, {"ok": False, "error": "MissingGameId"})

        if not n_winners or n_winners <= 0:
            return _resp(400, {"ok": False, "error": "InvalidNumberOfWinners"})

        game = _get_game(game_id)
        if not game:
            return _resp(404, {"ok": False, "error": "GameNotFound"})

        if not game.get("isActive", True):
            return _resp(403, {"ok": False, "error": "GameInactive"})

        game_type = (game.get("gameType") or "").upper()
        if not game_type:
            return _resp(500, {"ok": False, "error": "MissingGameType"})

        # 1) all players → for broadcast DMs
        all_items = _query_all_players(game_id)

        # 2) everyone notifiable (para broadcast)
        notifiable = []
        for it in all_items:
            pid = _as_int(it.get("playerId"))
            if pid is None or pid <= 0:
                continue
            if not _has_psid(it):
                continue
            notifiable.append({
                "playerId": pid,
                "instagramPSID": it.get("instagramPSID"),
                "instagramUsername": it.get("instagramUsername"),
                "item": it,
            })

        # 3) candidates — query the sparse GSI so only eligible players are returned
        eligible_items = _query_eligible_players(game_id)
        candidates = []
        for it in eligible_items:
            pid = _as_int(it.get("playerId"))
            if pid is None or pid <= 0:
                continue
            if not _has_psid(it):
                continue
            if only_validated and not _is_validated(it):
                continue
            candidates.append({
                "playerId": pid,
                "instagramPSID": it.get("instagramPSID"),
                "instagramUsername": it.get("instagramUsername"),
                "validationCode": it.get("validationCode"),
            })

        if not candidates:
            return _resp(200, {"ok": True, "gameId": game_id, "winners": [], "candidates": 0})

        # 4) elegir ganadores
        k = min(n_winners, len(candidates))
        winners = random.sample(candidates, k)

        winner_usernames = [
            (w.get("instagramUsername") or f"Jugador {w['playerId']}")
            for w in winners
        ]

        win_ts = _iso_now()
        # Persistimos winners a nivel game para polling del frontend
        try:
            _save_raffle_winners_to_game(game_id, winners, only_validated, win_ts)
        except Exception as e:
            log("raffle_save_to_game_error", {"error": repr(e), "gameId": game_id})

        # 5) construir mensajes
        drums_msg, countdown, announce_msg = _build_messages(game_type, winner_usernames)

        per_psid_msgs = defaultdict(list)

        # Todos reciben drums + countdown + announcement
        for p in notifiable:
            psid = p["instagramPSID"]
            per_psid_msgs[psid].append(drums_msg)
            for c in countdown:
                per_psid_msgs[psid].append(c)
            per_psid_msgs[psid].append(announce_msg)

        # Ganadores reciben extra + persistimos win
        win_ts = _iso_now()

        for w in winners:
            psid = w.get("instagramPSID")
            pid = int(w["playerId"])

            # code opcional (validationCode)
            val_code = w.get("validationCode")
            try:
                if isinstance(val_code, Decimal):
                    val_code = int(val_code)
                elif val_code is not None:
                    val_code = int(val_code)
            except Exception:
                val_code = None

            if val_code is not None:
                winner_msg = (
                    f"🎁 ¡Enhorabuena! Te ha tocado premio en el sorteo de {game_type}.\n"
                    f"Tu código de validación es: {val_code}.\n"
                    "Pasa por el stand para recoger tu premio. 🎉"
                )
            else:
                winner_msg = (
                    f"🎁 ¡Enhorabuena! Te ha tocado premio en el sorteo de {game_type}.\n"
                    "Pasa por el stand para recoger tu premio. 🎉"
                )

            per_psid_msgs[psid].append(winner_msg)

            try:
                _mark_winner(game_id, pid, game_type, win_ts)
            except Exception as e:
                log("raffle_update_error", {"error": repr(e), "gameId": game_id, "playerId": pid})

        # Flatten para instagram-sender
        messages = []
        for psid, texts in per_psid_msgs.items():
            for text in texts:
                messages.append({"psid": psid, "text": text})

        _send_bulk_dms(messages)

        return _resp(200, {
            "ok": True,
            "gameId": game_id,
            "gameType": game_type,
            "onlyValidated": only_validated,
            "requestedWinners": n_winners,
            "selectedWinners": k,
            "winners": winner_usernames,
            "notifiedPlayers": len(notifiable),
            "candidatePlayers": len(candidates),
        })

    except ClientError as e:
        log("raffle_dynamo_error", {"error": str(e)})
        return _resp(500, {"ok": False, "error": "DynamoError", "detail": str(e)})
    except Exception as e:
        log("raffle_internal_error", {"error": repr(e)})
        return _resp(500, {"ok": False, "error": "internal_error", "message": "Error interno en el sorteo."})
