import os
import json
from decimal import Decimal

import boto3
from stand_common.stack import (
    game_config as stack_game_config,
    public_assignment as stack_public_assignment,
    response_status as stack_response_status,
    get_ranking as stack_ranking,
)
from botocore.exceptions import BotoCoreError, ClientError
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
    normalize_participant_fields,
    is_valid_join_code,
    issue_player_token,
    verify_access_token,
    get_game_type_blob,
)
from stand_common.game_access import resolve_game_access
from stand_common.player_access import consume_access_nonce, require_access_secret
from stand_common.email_verification import (
    VerificationError, RegistrationError, registration_values, verification_table,
    request_code, confirm_code,
)
from stand_common.prizes import serialize_prize_catalog

GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
ASSIGN_LAMBDA = os.environ.get("ASSIGN_LAMBDA", "")
VALIDATE_LAMBDA = os.environ.get("VALIDATE_LAMBDA", "")
SCORE_LAMBDA = os.environ.get("SCORE_LAMBDA", "")
PLAYER_TOKEN_SECRET = os.environ.get("PLAYER_TOKEN_SECRET", "stand-dev-player-token-v1")
INSTAGRAM_DM_HANDLE = os.environ.get("INSTAGRAM_DM_HANDLE", "playstand_")

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


def _public_prize_catalog(meta: dict):
    if "prizeCatalog" not in meta:
        return None
    return serialize_prize_catalog(
        meta["prizeCatalog"],
        meta.get("ownerUserId") or "",
        meta.get("gameId") or "",
        os.environ.get("USER_MEDIA_BUCKET", ""),
        boto3.client("s3").generate_presigned_url,
    )


def _is_mobile_play(meta: dict) -> bool:
    return resolve_game_access(meta)["playMode"] == "mobile"


def _public_join_url(game_id: str, join_channel: str) -> str:
    """QR/link target: Instagram DM with the game code, or the web form."""
    if join_channel == "instagram":
        return f"https://ig.me/m/{INSTAGRAM_DM_HANDLE}?ref={game_id}"
    return f"/play/{game_id}"


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
            "profilePicUrl": (it.get("instagramProfilePicUrl") or "").strip() or None,
            "timer": timer_f,
            "score": score_f,
        })
    ranking.sort(key=lambda x: x["score"])
    ranking = ranking[:limit]
    for idx, entry in enumerate(ranking, start=1):
        entry["rank"] = idx
    return ranking


def _client_appearance(game: dict) -> dict:
    result = {"clientLogoUrl": None, "appearance": {"colors": None, "themeMode": "dark"}}
    owner = game.get("ownerUserId")
    bucket = os.environ.get("USER_MEDIA_BUCKET")
    if not owner:
        return result
    user = dynamo_r.Table(os.environ["USERS_TABLE"]).get_item(
        Key={"userId": owner}, ProjectionExpression="media",
    ).get("Item") or {}
    media = user.get("media") or {}
    result["appearance"] = media.get("appearance") or result["appearance"]
    logo = media.get("logo") or {}
    selected = logo.get("selectedId")
    if not selected:
        return result
    item = next((item for item in logo.get("items", []) if item.get("id") == selected), None)
    key = item.get("key") if item else None
    if not key or not key.startswith(f"users/{owner}/logos/"):
        log("public_client_logo_invalid", {"gameId": game.get("gameId")})
        return result
    if not bucket:
        raise ValueError("Missing USER_MEDIA_BUCKET for selected client logo")
    result["clientLogoUrl"] = boto3.client("s3").generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600,
    )
    return result


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
    pf = normalize_participant_fields(game_type, item.get("participantFields"))
    recent = item.get("recentPairs") or []
    if not isinstance(recent, list):
        recent = []
    access = resolve_game_access(item)

    payload = {
        "ok": True,
        "gameId": game_id,
        "gameName": item.get("gameName"),
        "gameType": item.get("gameType"),
        "isActive": bool(item.get("isActive", True)),
        "playMode": access["playMode"],
        "joinChannel": access["joinChannel"],
        "participantFields": pf,
        "playerCount": _as_int(item.get("playersCount")),
        "validatedCount": _as_int(item.get("validatedCount")),
        "recentPairs": recent[-10:],
        "joinUrl": _public_join_url(game_id, access["joinChannel"]),
    }
    if "prizeCatalog" in item:
        payload["prizeCatalog"] = _public_prize_catalog(item)
    if game_type == "T1MER":
        payload["ranking"] = _t1mer_ranking(game_id, 10)
    elif game_type == "STACK":
        try:
            payload["stackConfig"] = stack_game_config(item)
        except ValueError:
            return _resp(400, {"ok": False, "error": "InvalidStackConfig"})
        payload["ranking"] = stack_ranking(game_id, gp_table, 10)["results"]
        payload["stackStartSnapshot"] = True
        payload["stackFinishSupported"] = True
    if game_type in {"EMPAREJA2", "T1MER", "STACK"}:
        payload.update(_client_appearance(item))

    return _resp(200, _json_sanitize(payload))


def _invoke_assign_web(game_id: str, email: str | None, instagram_username: str | None, phone: str | None = None,
                       verification: str | None = None) -> dict:
    payload = {
        "source": "web",
        "game_id": game_id,
        "email": email,
        "instagram_username": instagram_username,
        "phone": phone,
    }
    if verification:
        payload["email_verification"] = verification
    resp = lambda_client.invoke(
        FunctionName=ASSIGN_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )
    if resp.get("FunctionError"):
        raise RuntimeError("Assignment Lambda failed")
    raw = resp.get("Payload").read().decode("utf-8")
    result = json.loads(raw) if raw else None
    if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
        raise RuntimeError("Invalid assignment response")
    return result


def _reason_to_status(reason: str) -> int:
    return {
        "missing_email": 400,
        "invalid_email": 400,
        "missing_instagram": 400,
        "invalid_instagram": 400,
        "missing_phone": 400,
        "invalid_phone": 400,
        "game_not_found": 404,
        "game_inactive": 403,
        "player_limit_reached": 403,
        "unsupported_game_type": 400,
        "web_join_disabled": 403,
        "GameNotFound": 404,
        "GameInactive": 403,
        "internal_error": 500,
        "put_failed": 500,
        "race_condition_retry_exhausted": 503,
        "VerificationRequired": 403,
        "VerificationExpired": 410,
        "VerificationAttemptsExceeded": 429,
        "VerificationSuperseded": 409,
        "VerificationUsed": 409,
        "VerificationUnavailable": 503,
    }.get(reason, 400)


def _verification_error(error):
    payload = {"ok": False, "error": error.code}
    if error.retry_after is not None:
        payload["retryAfter"] = error.retry_after
    return _resp(error.status, payload)


def _registration_game(game_id):
    if not isinstance(game_id, str) or not is_valid_join_code(game_id):
        raise VerificationError("InvalidGameId", 400)
    meta = _get_game(game_id)
    if not meta:
        raise VerificationError("GameNotFound", 404)
    if not meta.get("isActive", True):
        raise VerificationError("GameInactive", 403)
    if (meta.get("gameType") or "").upper() not in {"EMPAREJA2", "T1MER", "STACK"}:
        raise VerificationError("unsupported_game_type", 400)
    if resolve_game_access(meta)["joinChannel"] != "web":
        raise VerificationError("web_join_disabled", 403)
    return meta


def _handle_email_request(event):
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})
    try:
        game_id = body.get("gameId")
        meta = _registration_game(game_id)
        if body.get("consent") is not True:
            return _resp(400, {"ok": False, "error": "ConsentRequired"})
        values = registration_values(meta, body)
        if "email" not in values:
            return _resp(400, {"ok": False, "error": "EmailNotRequested"})
        language = body.get("language", "es")
        if language not in ("es", "en"):
            return _resp(400, {"ok": False, "error": "InvalidLanguage"})
        request_context = event.get("requestContext") or {}
        source_ip = (request_context.get("http") or {}).get("sourceIp") or (
            request_context.get("identity") or {}
        ).get("sourceIp")
        result = request_code(
            verification_table(dynamo_r), boto3.client("ses"), PLAYER_TOKEN_SECRET,
            game_id, values, source_ip, language,
        )
        return _resp(200, result)
    except RegistrationError as exc:
        return _resp(400, {"ok": False, "error": str(exc)})
    except VerificationError as exc:
        return _verification_error(exc)


def _handle_join(event):
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})

    if not ASSIGN_LAMBDA:
        return _resp(500, {"ok": False, "error": "AssignNotConfigured"})

    try:
        game_id = body.get("gameId")
        meta = _registration_game(game_id)
        fields = normalize_participant_fields(meta["gameType"], meta.get("participantFields"))
        if fields["email"]["required"]:
            record, receipt = confirm_code(
                verification_table(dynamo_r), PLAYER_TOKEN_SECRET, game_id,
                body.get("challengeId"), body.get("code"),
            )
            values = registration_values(meta, record["registration"])
            if values != record["registration"]:
                raise VerificationError("VerificationSuperseded", 409)
            result = _invoke_assign_web(
                game_id, values.get("email"), values.get("instagram"), values.get("phone"),
                verification=receipt,
            )
        else:
            if body.get("consent") is not True:
                return _resp(400, {"ok": False, "error": "ConsentRequired"})
            values = registration_values(meta, {
                **body, "instagram": body.get("instagram") or body.get("instagramUsername"),
            })
            result = _invoke_assign_web(game_id, values.get("email"), values.get("instagram"), values.get("phone"))
    except RegistrationError as exc:
        return _resp(400, {"ok": False, "error": str(exc)})
    except VerificationError as exc:
        return _verification_error(exc)
    if not result.get("ok"):
        return _resp(_reason_to_status(result.get("reason", "")), {
            **result, "ok": False, "error": result.get("reason", "JoinFailed"),
        })

    return _resp(200, _json_sanitize({
        "ok": True,
        "gameId": game_id,
        "gameType": result.get("gameType"),
        "playerToken": result.get("playerToken"),
        "assignment": result.get("assignment"),
        "alreadyJoined": bool(result.get("already_joined")),
    }))


def _get_player(game_id: str, player_id: int) -> dict | None:
    resp = gp_table.get_item(Key={"gameId": game_id, "playerId": int(player_id)}, ConsistentRead=True)
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

    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"valid": False, "error": "GameNotFound"})
    if not meta.get("isActive", True):
        return _resp(403, {"valid": False, "error": "GameInactive"})
    # Participant-side validation is a mobile-only action; operator validation is unaffected.
    if not _is_mobile_play(meta):
        return _resp(403, {"valid": False, "error": "MobilePlayDisabled"})

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
    images = {}
    bucket = os.environ.get("CHAR_BUCKET", "")
    if bucket:
        s3 = boto3.client("s3")
        for name in (cname, blob.get("partnerCharacterName")):
            if not name or name in images:
                continue
            try:
                images[name] = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket, "Key": f"{name}.png"},
                    ExpiresIn=int(os.environ.get("CHAR_URL_TTL", "120")),
                )
            except (ClientError, BotoCoreError, ValueError) as exc:
                log("character_image_url_error", {"error": str(exc)})
    return {
        "playerId": player.get("playerId"),
        "characterId": blob.get("characterId"),
        "characterName": cname,
        "partnerCharacterName": blob.get("partnerCharacterName"),
        "characterImageUrl": images.get(cname),
        "partnerCharacterImageUrl": images.get(blob.get("partnerCharacterName")),
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
    blob = (player.get("type") or {}).get("T1MER") or {}
    score_raw = blob.get("score")
    timer_raw = blob.get("timer")
    pic = (player.get("instagramProfilePicUrl") or "").strip() or None
    out = {
        "playerId": player.get("playerId"),
        "displayName": _player_display_label(player),
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
        "profilePicUrl": pic,
    }
    if score_raw is not None:
        out["score"] = int(score_raw) if not isinstance(score_raw, Decimal) else int(score_raw)
    if timer_raw is not None:
        out["timer"] = float(timer_raw) if not isinstance(timer_raw, Decimal) else float(timer_raw)
    return out


def _invoke_score_store(game_id: str, player_id: int, timer, score) -> dict:
    return _invoke_score_payload({
        "gameId": game_id,
        "playerId": player_id,
        "timer": timer,
        "score": score,
    })


def _invoke_score_payload(payload: dict) -> dict:
    event = {
        "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps(payload, ensure_ascii=False),
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
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})

    game_id = body.get("gameId")
    player_token = body.get("playerToken")
    if not isinstance(game_id, str):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})
    game_id = game_id.strip()
    if not isinstance(player_token, str):
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    player_token = player_token.strip()
    timer = body.get("timer")
    score = body.get("score")

    if not game_id or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})
    if not player_token:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    if body.get("action") is None and (timer is None or score is None):
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
    if game_type not in ("T1MER", "STACK"):
        return _resp(400, {"ok": False, "error": "UnsupportedGameType"})
    # Participant score/attempts are mobile-only; the operator score endpoint is unaffected.
    if not _is_mobile_play(meta):
        return _resp(403, {"ok": False, "error": "MobilePlayDisabled"})

    player = _get_player(game_id, claims["playerId"])
    if not player:
        return _resp(404 if game_type == "STACK" else 403, {"ok": False, "error": "PlayerNotFound"})

    if game_type == "STACK":
        allowed = {"gameId", "playerToken", "action", "attemptId"}
        if body.get("action") == "finish":
            allowed.add("placements")
        if set(body) - allowed:
            return _resp(400, {"ok": False, "error": "InvalidStackFields"})
        payload = {
            "gameId": game_id,
            "playerId": claims["playerId"],
            "action": body.get("action"),
            "attemptId": body.get("attemptId"),
        }
        if body.get("action") == "finish":
            payload["placements"] = body.get("placements")
        result = _invoke_score_payload(payload)
        return _resp(stack_response_status(result), _json_sanitize(result))
    if timer is None or score is None:
        return _resp(400, {"ok": False, "error": "MissingFields"})
    result = _invoke_score_store(game_id, claims["playerId"], timer, score)
    status = 200 if result.get("ok") else 400
    return _resp(status, _json_sanitize(result))


def _player_response(meta: dict, player: dict, game_id: str, player_id: int, token: str):
    """Shared PlayerResponse: {ok, gameId, gameType, playerToken, assignment, prizes}."""
    game_type = (meta.get("gameType") or "").upper()
    if game_type == "EMPAREJA2":
        assignment = _empareja2_assignment_from_player(player)
        prizes = _empareja2_prizes(player, game_id, player_id)
    elif game_type == "T1MER":
        assignment = _t1mer_assignment_from_player(player)
        prizes = []
    elif game_type == "STACK":
        if not meta.get("isActive", True):
            return _resp(403, {"ok": False, "error": "GameInactive"})
        assignment = stack_public_assignment(player, gp_table)
        prizes = []
    else:
        assignment = None
        prizes = []

    payload = {
        "ok": True,
        "gameId": game_id,
        "gameType": game_type,
        "playerToken": token,
        "assignment": assignment,
        "prizes": prizes,
    }
    if "prizeCatalog" in meta:
        payload["prizeCatalog"] = _public_prize_catalog(meta)
    return _resp(200, _json_sanitize(payload))


def _handle_get_player(event):
    """GET ?gameId&playerToken (legacy) or POST {gameId, playerToken} (keeps the session out of URLs)."""
    if _get_http_method(event) == "POST":
        body = _read_json_body(event)
        if not isinstance(body, dict):
            return _resp(400, {"ok": False, "error": "InvalidJson"})
        params = body
    else:
        params = event.get("queryStringParameters") or {}
    token = params.get("playerToken")
    game_id = params.get("gameId")
    token = token.strip() if isinstance(token, str) else ""
    game_id = game_id.strip() if isinstance(game_id, str) else ""

    claims = verify_player_token(token, PLAYER_TOKEN_SECRET)
    if not claims:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    if game_id and claims.get("gameId") != game_id:
        return _resp(403, {"ok": False, "error": "TokenGameMismatch"})

    player = _get_player(claims["gameId"], claims["playerId"])
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})

    meta = _get_game(claims["gameId"]) or {}
    return _player_response(meta, player, claims["gameId"], claims["playerId"], token)


def _handle_access(event):
    """POST {gameId, accessToken}: one-use personal link -> participant session (PlayerResponse)."""
    try:
        require_access_secret(PLAYER_TOKEN_SECRET)
    except RuntimeError:
        log("public_access_not_configured")
        return _resp(503, {"ok": False, "error": "AccessNotConfigured"})
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})
    game_id = body.get("gameId")
    access_token = body.get("accessToken")
    game_id = game_id.strip() if isinstance(game_id, str) else ""
    if not game_id or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})

    claims, error = verify_access_token(access_token.strip() if isinstance(access_token, str) else None,
                                        PLAYER_TOKEN_SECRET)
    if error == "AccessTokenExpired":
        return _resp(410, {"ok": False, "error": "AccessTokenExpired"})
    if not claims:
        return _resp(403, {"ok": False, "error": "InvalidAccessToken"})
    if claims["gameId"] != game_id:
        return _resp(403, {"ok": False, "error": "TokenGameMismatch"})

    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True):
        return _resp(403, {"ok": False, "error": "GameInactive"})
    if not _is_mobile_play(meta):
        return _resp(403, {"ok": False, "error": "MobilePlayDisabled"})
    player_id = claims["playerId"]
    player = _get_player(game_id, player_id)
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})
    if not player.get("instagramPSID"):
        return _resp(403, {"ok": False, "error": "InstagramPlayerRequired"})
    if meta.get("quizOrder") and not get_game_type_blob(player, meta.get("gameType", "")).get("quizCompleted"):
        return _resp(403, {"ok": False, "error": "QuizIncomplete"})
    if not consume_access_nonce(gp_table, game_id, player_id, claims["nonce"]):
        return _resp(409, {"ok": False, "error": "AccessTokenUsed"})

    log("public_access_exchanged", {"gameId": game_id, "playerId": player_id})
    player_token = issue_player_token(game_id, player_id, PLAYER_TOKEN_SECRET)
    return _player_response(meta, player, game_id, player_id, player_token)


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
        if path.endswith("/email/request") and method == "POST":
            return _handle_email_request(event)
        if path.endswith("/validate") and method == "POST":
            return _handle_validate(event)
        if path.endswith("/player") and method in ("GET", "POST"):
            return _handle_get_player(event)
        if path.endswith("/access") and method == "POST":
            return _handle_access(event)
        if path.endswith("/redeem") and method == "POST":
            return _handle_redeem(event)
        if path.endswith("/score") and method == "POST":
            return _handle_score(event)

        return _resp(404, {"ok": False, "error": "NotFound"})
    except Exception as e:
        log("public_internal_error", {"errorType": type(e).__name__})
        return _resp(500, {"ok": False, "error": "InternalError"})
