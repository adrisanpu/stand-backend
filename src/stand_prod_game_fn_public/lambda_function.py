import os
import json
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import (
    log,
    _resp,
    _read_json_body,
    _json_sanitize,
    _get_http_method,
    _iso_now,
    verify_player_token,
    issue_prize_token,
    verify_prize_token,
    mask_email,
    default_participant_fields,
    is_valid_join_code,
)

GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
ASSIGN_LAMBDA = os.environ.get("ASSIGN_LAMBDA", "")
VALIDATE_LAMBDA = os.environ.get("VALIDATE_LAMBDA", "")
SCORE_LAMBDA = os.environ.get("SCORE_LAMBDA", "")
PLAYER_TOKEN_SECRET = os.environ.get("PLAYER_TOKEN_SECRET", "stand-dev-player-token-v1")

dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)


def _get_path(event):
    return (
        event.get("rawPath")
        or event.get("path")
        or event.get("requestContext", {}).get("http", {}).get("path")
        or ""
    )


def _as_int(x) -> int:
    if isinstance(x, Decimal):
        return int(x)
    try:
        return int(x)
    except Exception:
        return 0


def _player_display_label(player: dict) -> str:
    ig = (player.get("instagramUsername") or "").strip()
    if ig:
        return ig if ig.startswith("@") else f"@{ig}"
    email = (player.get("email") or "").strip()
    if email:
        return mask_email(email)
    return f"Jugador {player.get('playerId', '')}"


def _get_game(game_id: str) -> dict | None:
    resp = games_table.get_item(Key={"gameId": game_id})
    return resp.get("Item")


def _as_float(x) -> float | None:
    if isinstance(x, Decimal):
        return float(x)
    try:
        return float(x)
    except Exception:
        return None


def _t1mer_ranking(game_id: str, limit: int = 10) -> list[dict]:
    """Inline ranking for public display (same rules as games.t1mer.get_ranking)."""
    if limit <= 0:
        limit = 10
    if limit > 100:
        limit = 100
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
    ranking = []
    for it in items:
        pid = _as_int(it.get("playerId"))
        if pid <= 0:
            continue
        tb = (it.get("type") or {}).get("T1MER") or {}
        score_f = _as_float(tb.get("score"))
        timer_f = _as_float(tb.get("timer"))
        if score_f is None or timer_f is None:
            continue
        ranking.append({
            "playerId": pid,
            "username": it.get("instagramUsername") or _player_display_label(it),
            "timer": timer_f,
            "score": score_f,
        })
    ranking.sort(key=lambda x: x["score"])
    ranking = ranking[:limit]
    for idx, entry in enumerate(ranking, start=1):
        entry["rank"] = idx
    return ranking


def _handle_get_game(event):
    params = event.get("queryStringParameters") or {}
    game_id = (params.get("gameId") or "").strip()
    if not game_id or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})

    item = _get_game(game_id)
    if not item:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not item.get("isActive", True):
        return _resp(403, {"ok": False, "error": "GameInactive"})

    game_type = (item.get("gameType") or "").upper()
    pf = item.get("participantFields") or default_participant_fields(game_type)
    recent = item.get("recentPairs") or []
    if not isinstance(recent, list):
        recent = []

    payload = {
        "ok": True,
        "gameId": game_id,
        "gameName": item.get("gameName"),
        "gameType": item.get("gameType"),
        "isActive": bool(item.get("isActive", True)),
        "joinChannel": item.get("joinChannel") or "web",
        "participantFields": pf,
        "playerCount": _as_int(item.get("playersCount")),
        "validatedCount": _as_int(item.get("validatedCount")),
        "recentPairs": recent[-10:],
        "joinUrl": f"/play/{game_id}",
    }
    if game_type == "T1MER":
        payload["ranking"] = _t1mer_ranking(game_id, 10)

    return _resp(200, _json_sanitize(payload))


def _invoke_assign_web(game_id: str, email: str, instagram_username: str | None) -> dict:
    payload = {
        "source": "web",
        "game_id": game_id,
        "email": email,
        "instagram_username": instagram_username,
    }
    resp = lambda_client.invoke(
        FunctionName=ASSIGN_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )
    raw = resp.get("Payload").read().decode("utf-8")
    return json.loads(raw) if raw else {"ok": False, "reason": "empty_assign_response"}


def _reason_to_status(reason: str) -> int:
    return {
        "missing_email": 400,
        "invalid_email": 400,
        "missing_instagram": 400,
        "game_not_found": 404,
        "game_inactive": 403,
        "player_limit_reached": 403,
        "unsupported_game_type": 400,
    }.get(reason, 400)


def _handle_join(event):
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"ok": False, "error": "InvalidJson"})

    game_id = (body.get("gameId") or "").strip()
    email = (body.get("email") or "").strip()
    instagram = (body.get("instagram") or body.get("instagramUsername") or "").strip() or None
    consent = body.get("consent")

    if not game_id or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})
    if not consent:
        return _resp(400, {"ok": False, "error": "ConsentRequired"})
    if not ASSIGN_LAMBDA:
        return _resp(500, {"ok": False, "error": "AssignNotConfigured"})

    result = _invoke_assign_web(game_id, email, instagram)
    if not result.get("ok"):
        return _resp(_reason_to_status(result.get("reason", "")), {"ok": False, **result})

    return _resp(200, _json_sanitize({
        "ok": True,
        "gameId": game_id,
        "gameType": result.get("gameType"),
        "playerToken": result.get("playerToken"),
        "assignment": result.get("assignment"),
        "alreadyJoined": bool(result.get("already_joined")),
    }))


def _get_player(game_id: str, player_id: int) -> dict | None:
    resp = gp_table.get_item(Key={"gameId": game_id, "playerId": int(player_id)})
    return resp.get("Item")


def _invoke_validate(game_id: str, codes: list[str]) -> dict:
    event = {
        "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps({"gameId": game_id, "codes": codes}, ensure_ascii=False),
    }
    resp = lambda_client.invoke(
        FunctionName=VALIDATE_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    )
    raw = resp.get("Payload").read().decode("utf-8")
    outer = json.loads(raw) if raw else {}
    body = outer.get("body")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except Exception:
            return {"valid": False, "error": "BadValidateResponse"}
    return outer


def _append_recent_pair(game_id: str, players: list[dict], validated_at: str):
  try:
      labels = []
      names = []
      for p in players or []:
          pid = p.get("playerId")
          full = _get_player(game_id, int(pid)) if pid is not None else None
          labels.append(_player_display_label(full or p))
          names.append(p.get("characterName") or "")
      entry = {
          "label": " & ".join(labels) if labels else "Pareja validada",
          "characterNames": names,
          "validatedAt": validated_at,
      }
      games_table.update_item(
          Key={"gameId": game_id},
          UpdateExpression="SET recentPairs = list_append(if_not_exists(recentPairs, :empty), :entry), updatedAt = :now",
          ExpressionAttributeValues={":empty": [], ":entry": [entry], ":now": validated_at},
      )
  except ClientError as e:
      log("public_recent_pair_failed", {"error": str(e), "gameId": game_id})


def _handle_validate(event):
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"valid": False, "error": "InvalidJson"})

    game_id = (body.get("gameId") or "").strip()
    player_token = (body.get("playerToken") or "").strip()
    codes_raw = body.get("codes") or []
    codes = [str(c).strip() for c in codes_raw if str(c).strip()]

    if not game_id or len(codes) != 2:
        return _resp(400, {"valid": False, "error": "BadRequest"})
    if not player_token or not VALIDATE_LAMBDA:
        return _resp(400, {"valid": False, "error": "MissingPlayerToken"})

    claims = verify_player_token(player_token, PLAYER_TOKEN_SECRET)
    if not claims or claims.get("gameId") != game_id:
        return _resp(403, {"valid": False, "error": "InvalidPlayerToken"})

    player = _get_player(game_id, claims["playerId"])
    if not player:
        return _resp(403, {"valid": False, "error": "PlayerNotFound"})

    own_code = str(player.get("validationCode", "")).strip()
    if own_code not in codes:
        return _resp(403, {"valid": False, "error": "MustIncludeOwnCode", "message": "Debes incluir tu propio código."})

    result = _invoke_validate(game_id, codes)
    if result.get("valid"):
        _append_recent_pair(game_id, result.get("players") or [], _iso_now())
    return _resp(200, _json_sanitize(result))


def _empareja2_assignment_from_player(player: dict) -> dict | None:
    blob = (player.get("type") or {}).get("EMPAREJA2") or {}
    if not blob:
        return None
    cname = blob.get("characterName")
    code = player.get("validationCode")
    img = None
    bucket = os.environ.get("CHAR_BUCKET", "")
    if cname and bucket:
        try:
            s3 = boto3.client("s3")
            img = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": f"{cname}.png"},
                ExpiresIn=int(os.environ.get("CHAR_URL_TTL", "120")),
            )
        except Exception:
            pass
    return {
        "playerId": player.get("playerId"),
        "characterId": blob.get("characterId"),
        "characterName": cname,
        "partnerCharacterName": blob.get("partnerCharacterName"),
        "characterImageUrl": img,
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
    }


def _empareja2_prizes(player: dict, game_id: str, player_id: int) -> list[dict]:
    """Build prize claim entries for EMPAREJA2 (validation and/or raffle)."""
    blob = (player.get("type") or {}).get("EMPAREJA2") or {}
    prizes = []
    if player.get("validated"):
        prizes.append({
            "kind": "validation",
            "token": issue_prize_token(game_id, player_id, "v", PLAYER_TOKEN_SECRET),
            "delivered": bool(blob.get("validationPrizeDelivered")),
        })
    if blob.get("raffleWin"):
        prizes.append({
            "kind": "raffle",
            "token": issue_prize_token(game_id, player_id, "r", PLAYER_TOKEN_SECRET),
            "delivered": bool(blob.get("prizeDelivered")),
        })
    return prizes


def _set_prize_delivered(game_id: str, player_id: int, game_type: str, attr: str) -> None:
    """Ensure type.<gameType>.<attr> = true (three-step Dynamo path like game PUT)."""
    key = {"gameId": game_id, "playerId": int(player_id)}
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type = if_not_exists(#type, :tinit)",
        ExpressionAttributeNames={"#type": "type"},
        ExpressionAttributeValues={":tinit": {}},
    )
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type.#g = if_not_exists(#type.#g, :ginit)",
        ExpressionAttributeNames={"#type": "type", "#g": game_type},
        ExpressionAttributeValues={":ginit": {}},
    )
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type.#g.#a = :d",
        ExpressionAttributeNames={"#type": "type", "#g": game_type, "#a": attr},
        ExpressionAttributeValues={":d": True},
    )


def _t1mer_assignment_from_player(player: dict) -> dict:
    code = player.get("validationCode")
    return {
        "playerId": player.get("playerId"),
        "displayName": _player_display_label(player),
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
    }


def _invoke_score_store(game_id: str, player_id: int, timer, score) -> dict:
    event = {
        "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps({
            "gameId": game_id,
            "playerId": player_id,
            "timer": timer,
            "score": score,
        }, ensure_ascii=False),
    }
    resp = lambda_client.invoke(
        FunctionName=SCORE_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    )
    raw = resp.get("Payload").read().decode("utf-8")
    outer = json.loads(raw) if raw else {}
    body = outer.get("body")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except Exception:
            return {"ok": False, "error": "BadScoreResponse"}
    return outer if isinstance(outer, dict) else {"ok": False, "error": "BadScoreResponse"}


def _handle_score(event):
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"ok": False, "error": "InvalidJson"})

    game_id = (body.get("gameId") or "").strip()
    player_token = (body.get("playerToken") or "").strip()
    timer = body.get("timer")
    score = body.get("score")

    if not game_id or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})
    if not player_token:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    if timer is None or score is None:
        return _resp(400, {"ok": False, "error": "MissingFields"})

    claims = verify_player_token(player_token, PLAYER_TOKEN_SECRET)
    if not claims or claims.get("gameId") != game_id:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})

    if not SCORE_LAMBDA:
        return _resp(500, {"ok": False, "error": "ScoreNotConfigured"})

    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True):
        return _resp(403, {"ok": False, "error": "GameInactive"})
    game_type = (meta.get("gameType") or "").upper()
    if game_type != "T1MER":
        return _resp(400, {"ok": False, "error": "UnsupportedGameType"})

    player = _get_player(game_id, claims["playerId"])
    if not player:
        return _resp(403, {"ok": False, "error": "PlayerNotFound"})

    result = _invoke_score_store(game_id, claims["playerId"], timer, score)
    status = 200 if result.get("ok") else 400
    return _resp(status, _json_sanitize(result))


def _handle_get_player(event):
    params = event.get("queryStringParameters") or {}
    token = (params.get("playerToken") or "").strip()
    game_id = (params.get("gameId") or "").strip()

    claims = verify_player_token(token, PLAYER_TOKEN_SECRET)
    if not claims:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    if game_id and claims.get("gameId") != game_id:
        return _resp(403, {"ok": False, "error": "TokenGameMismatch"})

    player = _get_player(claims["gameId"], claims["playerId"])
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})

    meta = _get_game(claims["gameId"]) or {}
    game_type = (meta.get("gameType") or "").upper()
    if game_type == "EMPAREJA2":
        assignment = _empareja2_assignment_from_player(player)
        prizes = _empareja2_prizes(player, claims["gameId"], claims["playerId"])
    elif game_type == "T1MER":
        assignment = _t1mer_assignment_from_player(player)
        prizes = []
    else:
        assignment = None
        prizes = []

    return _resp(200, _json_sanitize({
        "ok": True,
        "gameId": claims["gameId"],
        "gameType": game_type,
        "playerToken": token,
        "assignment": assignment,
        "prizes": prizes,
    }))


def _handle_redeem(event):
    body = _read_json_body(event)
    if body is None:
        return _resp(400, {"ok": False, "error": "InvalidJson"})

    prize_token = (body.get("prizeToken") or "").strip()
    claims = verify_prize_token(prize_token, PLAYER_TOKEN_SECRET)
    if not claims:
        return _resp(400, {"ok": False, "error": "InvalidPrizeToken"})

    game_id = claims["gameId"]
    player_id = claims["playerId"]
    kind = claims["kind"]

    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True):
        return _resp(403, {"ok": False, "error": "GameInactive"})

    game_type = (meta.get("gameType") or "").upper()
    if game_type != "EMPAREJA2":
        return _resp(400, {"ok": False, "error": "UnsupportedGameType"})

    player = _get_player(game_id, player_id)
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})

    blob = (player.get("type") or {}).get("EMPAREJA2") or {}

    if kind == "v":
        if not player.get("validated"):
            return _resp(400, {"ok": False, "error": "NotEligible"})
        if blob.get("validationPrizeDelivered"):
            return _resp(409, {"ok": False, "error": "AlreadyDelivered", "kind": "validation"})
        attr = "validationPrizeDelivered"
        kind_out = "validation"
    else:
        if not blob.get("raffleWin"):
            return _resp(400, {"ok": False, "error": "NotEligible"})
        if blob.get("prizeDelivered"):
            return _resp(409, {"ok": False, "error": "AlreadyDelivered", "kind": "raffle"})
        attr = "prizeDelivered"
        kind_out = "raffle"

    try:
        _set_prize_delivered(game_id, player_id, "EMPAREJA2", attr)
    except ClientError as e:
        log("public_redeem_failed", {"error": str(e), "gameId": game_id, "playerId": player_id})
        return _resp(500, {"ok": False, "error": "UpdateFailed"})

    return _resp(200, _json_sanitize({
        "ok": True,
        "gameId": game_id,
        "playerId": player_id,
        "kind": kind_out,
        "delivered": True,
        "label": _player_display_label(player),
    }))


def lambda_handler(event, context):
    try:
        method = _get_http_method(event)
        if method == "OPTIONS":
            return _resp(204, "")

        path = _get_path(event)
        if path.endswith("/game") and method == "GET":
            return _handle_get_game(event)
        if path.endswith("/join") and method == "POST":
            return _handle_join(event)
        if path.endswith("/validate") and method == "POST":
            return _handle_validate(event)
        if path.endswith("/player") and method == "GET":
            return _handle_get_player(event)
        if path.endswith("/redeem") and method == "POST":
            return _handle_redeem(event)
        if path.endswith("/score") and method == "POST":
            return _handle_score(event)

        return _resp(404, {"ok": False, "error": "NotFound"})
    except Exception as e:
        log("public_internal_error", {"error": repr(e)})
        return _resp(500, {"ok": False, "error": "InternalError"})
