# raffle.py  (handler: raffle.lambda_handler)
import os
import json
import base64
import random
import urllib.request
import urllib.parse
import urllib.error
from decimal import Decimal
from datetime import datetime, timezone
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from stand_common.utils import log, _json_sanitize, _resp, _iso_now, _as_int, get_game_type_blob

# ============ ENV VARS ============
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "instagram-sender")
IG_GRAPH_VERSION = os.environ.get("IG_GRAPH_VERSION", "v24.0")
INSTAGRAM_SECRET_NAME = os.environ.get("INSTAGRAM_SECRET_NAME", "").strip()
# =================================

# Instagram account that must be followed by default for raffles
DEFAULT_RAFFLE_FOLLOW_ACCOUNT = "stand_official"

# Load Instagram page token for User Profile API (is_user_follow_business)
IG_PAGE_TOKEN = ""
if INSTAGRAM_SECRET_NAME:
    try:
        sm = boto3.client("secretsmanager")
        raw = sm.get_secret_value(SecretId=INSTAGRAM_SECRET_NAME)
        data = json.loads(raw.get("SecretString", "{}"))
        if data:
            IG_PAGE_TOKEN = (data.get("PAGE_TOKEN") or data.get("IG_PAGE_TOKEN") or "").strip()
    except Exception as e:
        log("raffle_instagram_secret_load_failed", {"error": repr(e)})

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

def _graph_get_raffle(path: str, params: dict) -> dict | None:
    """GET request to Instagram Graph API for User Profile (is_user_follow_business)."""
    if not IG_PAGE_TOKEN:
        return None
    base = f"https://graph.facebook.com/{IG_GRAPH_VERSION}/{path}"
    q = dict(params)
    q["access_token"] = IG_PAGE_TOKEN
    url = base + "?" + urllib.parse.urlencode(q)
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=8) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        log("raffle_graph_get_error", {"status": e.code, "path": path, "body": err_body[:300]})
        return None
    except Exception as e:
        log("raffle_graph_get_error", {"error": repr(e), "path": path})
        return None


def _is_user_following_business(psid: str) -> bool:
    """True if the user (psid) follows the business Instagram account (Stand)."""
    if not psid or psid == "#":
        return False
    data = _graph_get_raffle(psid, {"fields": "is_user_follow_business"})
    if not data or not isinstance(data, dict):
        return False
    return data.get("is_user_follow_business") is True


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
    # read existing raffleWinners and normalize to list
    existing_game = games_table.get_item(Key={"gameId": game_id}).get("Item") or {}
    existing_raw = existing_game.get("raffleWinners")
    if isinstance(existing_raw, list):
        existing_winners = list(existing_raw)
    elif existing_raw:
        existing_winners = [existing_raw]
    else:
        existing_winners = []

    # sanitize new winners
    new_winners = []
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

        new_winners.append(row)

    final_winners = existing_winners + new_winners

    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression="SET raffleWinners = :w, raffleLastRunAt = :t, raffleOnlyValidated = :ov",
        ExpressionAttributeValues={
            ":w": final_winners,
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


def _is_validated(it: dict) -> bool:
    return bool(it.get("validated", False))


def _requires_validation(game: dict, game_type: str) -> bool:
    """True if game uses validation code for raffle eligibility; False for no-validation games (e.g. INFOCARDS)."""
    blob = get_game_type_blob(game or {}, game_type)
    default = False if (game_type or "").upper() == "INFOCARDS" else True
    val = blob.get("requiresValidation", default)
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes")
    return bool(val)


def _is_quiz_completed(it: dict, game_type_upper: str) -> bool:
    """Read type.<gameType>.quizCompleted."""
    gk = (game_type_upper or "").upper()
    t = it.get("type") or {}
    tb = t.get(gk) or {}
    return bool(tb.get("quizCompleted", False))


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

    # 3) set flags (raffle eligibility + win metadata + prizeDelivered flag)
    gp_table.update_item(
        Key=key,
        UpdateExpression=(
            "SET #type.#g.raffleEligible = :f, "
            "#type.#g.raffleWin = :t, "
            "#type.#g.raffleWinAt = :ts, "
            "#type.#g.prizeDelivered = :pd "
        ),
        ExpressionAttributeNames={"#type": "type", "#g": gk},
        ExpressionAttributeValues={":f": False, ":t": True, ":ts": win_ts, ":pd": False},
    )


# ----------------- business logic -----------------

def _build_messages(game_type_upper: str, winner_usernames: list[str]):
    # Mensaje único para todos los jugadores con lista de ganadores
    label = game_type_upper.title() if game_type_upper else "juego"
    if winner_usernames:
        winners_list = "\n".join(winner_usernames)
        msg = (
            f"🥁 Sorteo de {label}\n"
            f"🎉 Personas ganadoras: {winners_list}\n"
            "Pasa por el stand para recoger el premio."
        )
    return msg


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

        # 1) all players → base set for selección y broadcast
        all_items = _query_all_players(game_id)

        # everyone notifiable (para broadcast)
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
        quiz_configured = bool(game.get("quizOrder"))
        use_validation_filter = _requires_validation(game, game_type)

        # 2) build pool of possible candidates (players with psid and raffleEligible == true by default)
        base_candidates = []
        for it in all_items:
            pid = _as_int(it.get("playerId"))
            if pid is None or pid <= 0:
                continue
            if not _has_psid(it):
                continue
            # raffleEligible check: read type.<gameType>.raffleEligible; default True if missing
            t = it.get("type") or {}
            tb = t.get(game_type) or {}
            val = tb.get("raffleEligible")
            if val is None:
                ok_raffle_eligible = True
            elif isinstance(val, str):
                ok_raffle_eligible = val.strip().lower() not in ("0", "false", "no", "n")
            else:
                ok_raffle_eligible = bool(val)
            if not ok_raffle_eligible:
                continue
            base_candidates.append({
                "playerId": pid,
                "instagramPSID": it.get("instagramPSID"),
                "instagramUsername": it.get("instagramUsername"),
                "validationCode": it.get("validationCode"),
                "item": it,
            })

        # raffleRequiredFollows: ensure it is always a list and always includes the default account
        raw_raffle_required_follows = game.get("raffleRequiredFollows")
        if isinstance(raw_raffle_required_follows, list):
            raffle_required_follows = list(raw_raffle_required_follows)
        elif raw_raffle_required_follows:
            raffle_required_follows = [str(raw_raffle_required_follows)]
        else:
            raffle_required_follows = []

        if DEFAULT_RAFFLE_FOLLOW_ACCOUNT and DEFAULT_RAFFLE_FOLLOW_ACCOUNT not in raffle_required_follows:
            raffle_required_follows.insert(0, DEFAULT_RAFFLE_FOLLOW_ACCOUNT)

        # 3) randomly select winners with post-selection eligibility checks
        def _is_candidate_eligible(candidate: dict) -> bool:
            it = candidate.get("item") or {}
            ok_validated = True
            if only_validated:
                if use_validation_filter:
                    ok_validated = _is_validated(it)
                else:
                    ok_validated = not quiz_configured or _is_quiz_completed(it, game_type)
            ok_follow = True
            if isinstance(raffle_required_follows, list) and len(raffle_required_follows) > 0:
                ok_follow = _is_user_following_business(candidate.get("instagramPSID") or "")
            eligible = ok_validated and ok_follow
            log("raffle_eligible_check_dev", {"playerId": candidate.get("playerId"), "eligible": eligible, "ok_validated": ok_validated, "ok_follow": ok_follow})  # dev: remove
            return eligible

        winners = []
        tried_indices: set[int] = set()
        max_draws = len(base_candidates)

        while len(winners) < n_winners and len(tried_indices) < max_draws and base_candidates:
            idx = random.randrange(0, len(base_candidates))
            if idx in tried_indices:
                continue
            tried_indices.add(idx)
            cand = base_candidates[idx]
            if _is_candidate_eligible(cand):
                winners.append(cand)

        if not winners:
            return _resp(200, {
                "ok": True,
                "gameId": game_id,
                "winners": [],
                "candidates": len(base_candidates),
                "raffleRequiredFollows": raffle_required_follows if isinstance(raffle_required_follows, list) else [],
            })

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

        # 4) construir mensaje único y enviar
        broadcast_msg = _build_messages(game_type.upper(), winner_usernames)

        per_psid_msgs = defaultdict(list)
        for p in notifiable:
            psid = p["instagramPSID"]
            per_psid_msgs[psid].append(broadcast_msg)

        # 5) persistimos win individual por jugador
        win_ts = _iso_now()
        for w in winners:
            pid = int(w["playerId"])
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

        k = len(winners)
        candidates_count = len(base_candidates)

        return _resp(200, {
            "ok": True,
            "gameId": game_id,
            "gameType": game_type,
            "onlyValidated": only_validated,
            "requestedWinners": n_winners,
            "selectedWinners": k,
            "winners": winner_usernames,
            "notifiedPlayers": len(notifiable),
            "candidatePlayers": candidates_count,
            "raffleRequiredFollows": raffle_required_follows if isinstance(raffle_required_follows, list) else [],
        })

    except ClientError as e:
        log("raffle_dynamo_error", {"error": str(e)})
        return _resp(500, {"ok": False, "error": "DynamoError", "detail": str(e)})
    except Exception as e:
        log("raffle_internal_error", {"error": repr(e)})
        return _resp(500, {"ok": False, "error": "internal_error", "message": "Error interno en el sorteo."})
