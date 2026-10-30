import os
import json
import re
import uuid
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
    web_participant_fields,
    is_valid_join_code,
    issue_player_token,
    get_game_type_blob,
)
from stand_common.game_access import resolve_game_access
from stand_common.email_verification import (
    VerificationError, RegistrationError, registration_values, verification_table,
    request_code, confirm_code,
)
from stand_common.prizes import serialize_prize_catalog
from stand_common.rulet4 import game_config as rulet4_game_config
from stand_common.rulet4_gameplay import public_assignment as rulet4_assignment, spin as rulet4_spin
from stand_common import kivly_registration, kivly_rewards, stack_prizes

GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
ASSIGN_LAMBDA = os.environ.get("ASSIGN_LAMBDA", "")
VALIDATE_LAMBDA = os.environ.get("VALIDATE_LAMBDA", "")
SCORE_LAMBDA = os.environ.get("SCORE_LAMBDA", "")
PLAYER_TOKEN_SECRET = os.environ.get("PLAYER_TOKEN_SECRET", "stand-dev-player-token-v1")
PARTICIPANT_PHOTO_MAX_SIZE = 5 * 1024 * 1024
PARTICIPANT_PHOTO_TYPES = {
    "image/jpeg": ("jpg", b"\xff\xd8\xff"),
    "image/png": ("png", b"\x89PNG\r\n\x1a\n"),
    "image/webp": ("webp", b"RIFF"),
}

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
    username = (player.get("username") or "").strip()
    if username:
        return username
    email = (player.get("email") or "").strip()
    if email:
        return mask_email(email)
    return f"Jugador {player.get('playerId', '')}"


def _player_profile_pic_url(player: dict) -> str | None:
    photo_key = player.get("participantPhotoKey")
    if photo_key:
        bucket = os.environ.get("USER_MEDIA_BUCKET")
        if not bucket:
            raise RuntimeError("Missing USER_MEDIA_BUCKET for participant photo")
        return boto3.client("s3").generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": photo_key}, ExpiresIn=3600,
        )
    return None


def _get_game(game_id: str) -> dict | None:
    resp = games_table.get_item(Key={"gameId": game_id}, ConsistentRead=True)
    return kivly_registration.apply_account_settings(dynamo_r, resp.get("Item"))


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
    """QR/link target for supported games."""
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
            "username": _player_display_label(it),
            "profilePicUrl": _player_profile_pic_url(it),
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
    closed_stack = (item.get("gameType") == "STACK" and "stackPrizeProgram" in item
                    and item.get("stackPrizeState") in ("closing", "closed"))
    if not item.get("isActive", True) and not closed_stack:
        return _resp(403, {"ok": False, "error": "GameInactive"})

    game_type = (item.get("gameType") or "").upper()
    configured_fields = web_participant_fields(game_type, item.get("participantFields"))
    pf = {name: configured_fields[name] for name in ("email", "phone")}
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
    try:
        loyalty = kivly_registration.game_settings(item)
    except (kivly_registration.RegistrationError, kivly_registration.KivlyError) as exc:
        status, error = kivly_registration.error_response(exc)
        return _resp(status, error)
    if loyalty["enabled"]:
        payload["loyalty"] = {key: loyalty[key] for key in (
            "clubName", "privacyUrl", "termsUrl", "consentVersion",
        )}
    if "prizeCatalog" in item:
        payload["prizeCatalog"] = _public_prize_catalog(item)
    if game_type == "T1MER":
        payload["ranking"] = _t1mer_ranking(game_id, 10)
    elif game_type == "STACK":
        try:
            payload["stackConfig"] = stack_game_config(item)
        except ValueError:
            return _resp(400, {"ok": False, "error": "InvalidStackConfig"})
        payload["ranking"] = stack_ranking(
            game_id, gp_table, 10, profile_pic_url_fn=_player_profile_pic_url,
        )["results"]
        payload["stackStartSnapshot"] = True
        payload["stackFinishSupported"] = True
        if "stackPrizeProgram" in item:
            payload["prizeCatalog"] = {}
            payload["stackPrizeState"] = item.get("stackPrizeState", "open")
            payload["stackPrizeRules"] = stack_prizes.promotional_rules(item)
        reward = item.get("kivlyReward")
        if reward and item.get("ownerUserId"):
            reward = kivly_rewards.configured_reward(
                item, kivly_registration.connection(dynamo_r, item["ownerUserId"]),
            )
        if loyalty["enabled"] and reward:
            payload["kivlyReward"] = {"name": reward["name"]}
    if game_type == "RULET4":
        try:
            payload["rulet4Config"] = rulet4_game_config(item)
        except ValueError:
            return _resp(400, {"ok": False, "error": "InvalidRulet4Config"})
    if game_type in {"EMPAREJA2", "T1MER", "STACK", "RULET4"}:
        payload.update(_client_appearance(item))

    return _resp(200, _json_sanitize(payload))


def _invoke_assign_web(game_id: str, email: str | None, phone: str | None = None,
                       verification: str | None = None, username: str | None = None,
                       loyalty_consent_version: str | None = None) -> dict:
    payload = {
        "source": "web",
        "game_id": game_id,
        "email": email,
        "phone": phone,
    }
    if verification:
        payload["email_verification"] = verification
    if username:
        payload["username"] = username
    if loyalty_consent_version:
        payload["loyaltyConsentVersion"] = loyalty_consent_version
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
    if (meta.get("gameType") or "").upper() not in {"EMPAREJA2", "T1MER", "STACK", "RULET4"}:
        raise VerificationError("unsupported_game_type", 400)
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
        record, receipt = confirm_code(
            verification_table(dynamo_r), PLAYER_TOKEN_SECRET, game_id,
            body.get("challengeId"), body.get("code"),
        )
        values = registration_values(meta, record["registration"])
        if values != record["registration"]:
            raise VerificationError("VerificationSuperseded", 409)
        result = _invoke_assign_web(
            game_id, values["email"], values.get("phone"),
            verification=receipt, username=values["username"],
            **({"loyalty_consent_version": values["loyaltyConsentVersion"]}
               if "loyaltyConsentVersion" in values else {}),
        )
    except RegistrationError as exc:
        return _resp(400, {"ok": False, "error": str(exc)})
    except VerificationError as exc:
        return _verification_error(exc)
    if not result.get("ok"):
        status = result.get("status") if (result.get("reason") or "").startswith("Kivly") else None
        return _resp(status if type(status) is int and 400 <= status <= 599 else _reason_to_status(result.get("reason", "")), {
            **result, "ok": False, "error": result.get("reason", "JoinFailed"),
        })

    assignment = result.get("assignment")
    player_token = result.get("playerToken")
    claims = verify_player_token(player_token, PLAYER_TOKEN_SECRET) if isinstance(player_token, str) else None
    if isinstance(assignment, dict) and claims:
        player = _get_player(claims["gameId"], claims["playerId"])
        if player:
            kivly_registration.require_player(dynamo_r, meta, player)
            assignment["profilePicUrl"] = _player_profile_pic_url(player)
            assignment.setdefault("participantName", _player_display_label(player))

    return _resp(200, _json_sanitize({
        "ok": True,
        "gameId": game_id,
        "gameType": result.get("gameType"),
        "playerToken": result.get("playerToken"),
        "assignment": assignment,
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
        "participantName": _player_display_label(player),
        "profilePicUrl": _player_profile_pic_url(player),
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


def _stack_ranking_prize_eligible(meta: dict, rank) -> bool:
    prize = (meta.get("prizeCatalog") or {}).get("ranking") or {}
    top_x, limit = prize.get("topX"), prize.get("limit")
    return bool(
        rank is not None and top_x is not None and rank >= 1 and rank <= top_x
        and (limit is None or rank <= limit)
        and (prize.get("name") or prize.get("imageKey"))
    )


def _stack_prizes(meta: dict, player: dict, rank) -> list[dict]:
    blob = get_game_type_blob(player, "STACK")
    delivered = bool(blob.get("rankingPrizeDelivered"))
    if not delivered and not _stack_ranking_prize_eligible(meta, rank):
        return []
    return [{
        "kind": "ranking",
        "token": issue_prize_token(player["gameId"], player["playerId"], "k", PLAYER_TOKEN_SECRET),
        "delivered": delivered,
    }]


def _t1mer_assignment_from_player(player: dict) -> dict:
    code = player.get("validationCode")
    blob = (player.get("type") or {}).get("T1MER") or {}
    score_raw = blob.get("score")
    timer_raw = blob.get("timer")
    pic = _player_profile_pic_url(player)
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
    closing_finish = (meta.get("gameType") == "STACK" and meta.get("stackPrizeState") == "closing"
                      and body.get("action") == "finish")
    if not meta.get("isActive", True) and not closing_finish:
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
    kivly_registration.require_player(dynamo_r, meta, player)

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


def _handle_rulet4_spin(event):
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})
    if set(body) - {"gameId", "playerToken"}:
        return _resp(400, {"ok": False, "error": "InvalidRulet4Fields"})
    game_id = body.get("gameId")
    if not isinstance(game_id, str) or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})
    token = body.get("playerToken")
    claims = verify_player_token(token, PLAYER_TOKEN_SECRET) if isinstance(token, str) else None
    if not claims or claims.get("gameId") != game_id:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True):
        return _resp(403, {"ok": False, "error": "GameInactive"})
    if (meta.get("gameType") or "").upper() != "RULET4":
        return _resp(400, {"ok": False, "error": "UnsupportedGameType"})
    player = _get_player(game_id, claims["playerId"])
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})
    try:
        kivly_registration.require_player(dynamo_r, meta, player)
        result = rulet4_spin(meta, player, gp_table)
    except ValueError:
        return _resp(400, {"ok": False, "error": "InvalidRulet4Config"})
    return _resp(200, _json_sanitize({
        "ok": True, "gameId": game_id, "playerId": claims["playerId"], "result": result,
    }))


def _handle_kivly_reward(event):
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})
    game_id, token = body.get("gameId"), body.get("playerToken")
    if not isinstance(game_id, str) or not is_valid_join_code(game_id):
        return _resp(400, {"ok": False, "error": "InvalidGameId"})
    if not isinstance(token, str):
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    claims = verify_player_token(token, PLAYER_TOKEN_SECRET)
    if not claims or claims.get("gameId") != game_id:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})
    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True) or not _is_mobile_play(meta):
        return _resp(403, {"ok": False, "error": "GameInactive"})
    if (meta.get("gameType") or "").upper() != "STACK":
        return _resp(400, {"ok": False, "error": "UnsupportedGameType"})
    player = _get_player(game_id, claims["playerId"])
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})
    result = ({"status": "none"} if "prizeProgram" in get_game_type_blob(player, "STACK").get("attempt", {})
              else kivly_rewards.completion_reward(dynamo_r, meta, player))
    return _resp(200, _json_sanitize({
        "ok": True, "gameId": game_id, "playerId": claims["playerId"], "reward": result,
    }))


def _participant_photo_type_is_valid(image_type: str, data: bytes) -> bool:
    if image_type == "image/webp":
        return len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] == b"WEBP"
    signature = PARTICIPANT_PHOTO_TYPES.get(image_type)
    return bool(signature and data.startswith(signature[1]))


def _handle_player_photo(event):
    body = _read_json_body(event)
    if not isinstance(body, dict):
        return _resp(400, {"ok": False, "error": "InvalidJson"})

    token = body.get("playerToken")
    claims = verify_player_token(token.strip(), PLAYER_TOKEN_SECRET) if isinstance(token, str) else None
    if not claims:
        return _resp(403, {"ok": False, "error": "InvalidPlayerToken"})

    game_id = claims["gameId"]
    player_id = claims["playerId"]
    player = _get_player(game_id, player_id)
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})
    if player.get("joinSource") != "web":
        return _resp(403, {"ok": False, "error": "WebPhotoUploadOnly"})
    kivly_registration.require_player(dynamo_r, _get_game(game_id) or {}, player)
    action = body.get("action")
    existing_photo_key = player.get("participantPhotoKey")
    if existing_photo_key:
        if action == "complete" and body.get("photoKey") == existing_photo_key:
            return _resp(200, {
                "ok": True,
                "profilePicUrl": _player_profile_pic_url({"participantPhotoKey": existing_photo_key}),
            })
        return _resp(409, {"ok": False, "error": "PhotoAlreadyUploaded"})

    bucket = os.environ.get("USER_MEDIA_BUCKET")
    if not bucket:
        return _resp(503, {"ok": False, "error": "PhotoStorageUnavailable"})

    s3 = boto3.client("s3")
    if action == "upload-url":
        image_type = body.get("contentType")
        size_bytes = body.get("sizeBytes")
        if image_type not in PARTICIPANT_PHOTO_TYPES:
            return _resp(400, {"ok": False, "error": "UnsupportedPhotoType"})
        if (isinstance(size_bytes, bool) or not isinstance(size_bytes, int)
                or size_bytes <= 0 or size_bytes > PARTICIPANT_PHOTO_MAX_SIZE):
            return _resp(400, {"ok": False, "error": "InvalidPhotoSize"})

        extension = PARTICIPANT_PHOTO_TYPES[image_type][0]
        key = f"players/{game_id}/{int(player_id)}/profile-{uuid.uuid4()}.{extension}"
        upload = s3.generate_presigned_post(
            Bucket=bucket,
            Key=key,
            Fields={"Content-Type": image_type},
            Conditions=[
                {"Content-Type": image_type},
                ["content-length-range", 1, PARTICIPANT_PHOTO_MAX_SIZE],
            ],
            ExpiresIn=300,
        )
        return _resp(200, {
            "ok": True,
            "uploadUrl": upload["url"],
            "uploadFields": upload["fields"],
            "photoKey": key,
        })

    if action != "complete":
        return _resp(400, {"ok": False, "error": "InvalidPhotoAction"})

    photo_key = body.get("photoKey")
    prefix = f"players/{game_id}/{int(player_id)}/profile-"
    match = re.fullmatch(
        re.escape(prefix) + r"([0-9a-f-]{36})\.(jpg|png|webp)",
        photo_key if isinstance(photo_key, str) else "",
    )
    if not match:
        return _resp(400, {"ok": False, "error": "InvalidPhotoKey"})
    try:
        uuid.UUID(match.group(1))
    except ValueError:
        return _resp(400, {"ok": False, "error": "InvalidPhotoKey"})

    try:
        uploaded = s3.head_object(Bucket=bucket, Key=photo_key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return _resp(400, {"ok": False, "error": "PhotoNotUploaded"})
        raise

    image_type = (uploaded.get("ContentType") or "").strip().lower()
    expected_extension = PARTICIPANT_PHOTO_TYPES.get(image_type, (None, None))[0]
    size_bytes = uploaded.get("ContentLength")
    if (expected_extension is None or not photo_key.endswith(f".{expected_extension}")
            or isinstance(size_bytes, bool) or not isinstance(size_bytes, int)
            or size_bytes <= 0 or size_bytes > PARTICIPANT_PHOTO_MAX_SIZE):
        return _resp(400, {"ok": False, "error": "InvalidPhoto"})

    image = s3.get_object(Bucket=bucket, Key=photo_key, Range="bytes=0-11")
    body_stream = image["Body"]
    try:
        image_header = body_stream.read(12)
    finally:
        body_stream.close()
    if not _participant_photo_type_is_valid(image_type, image_header):
        return _resp(400, {"ok": False, "error": "InvalidPhoto"})

    gp_table.update_item(
        Key={"gameId": game_id, "playerId": int(player_id)},
        UpdateExpression="SET participantPhotoKey = :key",
        ConditionExpression="attribute_exists(playerId) AND attribute_not_exists(participantPhotoKey)",
        ExpressionAttributeValues={":key": photo_key},
    )
    return _resp(200, {"ok": True, "profilePicUrl": _player_profile_pic_url({"participantPhotoKey": photo_key})})


def _impostor_assignment_from_player(meta: dict, player: dict, game_id: str) -> dict:
    """Per-player Impostor view: only the requesting player's own role, and the word only for the crew."""
    player_id = _as_int(player.get("playerId"))
    code = player.get("validationCode")
    blob = get_game_type_blob(meta, "IMPOSTOR")
    phase = blob.get("phase") or "lobby"
    turn_order = [_as_int(pid) for pid in (blob.get("turnOrder") or [])]
    in_round = phase != "lobby" and player_id in turn_order
    impostor_id = blob.get("impostorPlayerId")
    is_impostor = in_round and impostor_id is not None and _as_int(impostor_id) == player_id
    role = ("impostor" if is_impostor else "crew") if in_round else None

    current_turn = None
    if phase == "word_round" and turn_order:
        idx = _as_int(blob.get("wordRoundTurnIndex", 0))
        if 0 <= idx < len(turn_order):
            turn_id = turn_order[idx]
            turn_player = player if turn_id == player_id else _get_player(game_id, turn_id)
            current_turn = {
                "isMine": turn_id == player_id,
                "name": _player_display_label(turn_player or {"playerId": turn_id}),
            }

    winner = blob.get("winner") if phase in ("result", "finished") else None
    state = {
        "phase": phase,
        "round": _as_int(blob.get("round", 0)),
        "inRound": in_round,
        "role": role,
        "word": blob.get("selectedWord") if role == "crew" else None,
        "currentTurn": current_turn,
        "winner": winner,
        "won": (winner == "impostor") == is_impostor if winner in ("impostor", "crew") and in_round else None,
    }
    return {
        "playerId": player_id,
        "displayName": _player_display_label(player),
        "validationCode": int(code) if isinstance(code, Decimal) else code,
        "validated": bool(player.get("validated")),
        "impostor": state,
    }


def _player_response(meta: dict, player: dict, game_id: str, player_id: int, token: str):
    """Shared PlayerResponse: {ok, gameId, gameType, playerToken, assignment, prizes}."""
    managed_result = ("prizeProgram" in get_game_type_blob(player, "STACK").get("attempt", {})
                      and get_game_type_blob(player, "STACK").get("attempt", {}).get("status") == "finished")
    if not managed_result:
        kivly_registration.require_player(dynamo_r, meta, player)
    game_type = (meta.get("gameType") or "").upper()
    if game_type == "EMPAREJA2":
        assignment = _empareja2_assignment_from_player(player)
        prizes = _empareja2_prizes(player, game_id, player_id)
    elif game_type == "T1MER":
        assignment = _t1mer_assignment_from_player(player)
        prizes = []
    elif game_type == "IMPOSTOR":
        assignment = _impostor_assignment_from_player(meta, player, game_id)
        prizes = []
    elif game_type == "STACK":
        if not meta.get("isActive", True) and meta.get("stackPrizeState") not in ("closing", "closed"):
            return _resp(403, {"ok": False, "error": "GameInactive"})
        assignment = stack_public_assignment(player, gp_table, _player_profile_pic_url)
        prizes = _stack_prizes(meta, player, assignment.get("rank"))
    elif game_type == "RULET4":
        assignment = rulet4_assignment(player, _player_display_label(player), _player_profile_pic_url(player))
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
    if game_type == "STACK" and "stackPrizeProgram" in meta:
        payload["stackPrizes"] = stack_prizes.public_awards(
            dynamo_r, meta, player,
            lambda aid: issue_prize_token(game_id, player_id, "a", PLAYER_TOKEN_SECRET, award_id=aid),
        )
        if "prizeProgram" in get_game_type_blob(player, "STACK").get("attempt", {}):
            payload["prizes"] = []
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
    if body.get("gameId") is not None and body["gameId"] != game_id:
        return _resp(400, {"ok": False, "error": "TokenGameMismatch"})

    meta = _get_game(game_id)
    if not meta:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    if not meta.get("isActive", True) and not (kind == "a" and meta.get("stackPrizeState") in ("closing", "closed")):
        return _resp(403, {"ok": False, "error": "GameInactive"})

    game_type = (meta.get("gameType") or "").upper()
    if game_type not in {"EMPAREJA2", "STACK"}:
        return _resp(400, {"ok": False, "error": "UnsupportedGameType"})

    player = _get_player(game_id, player_id)
    if not player:
        return _resp(404, {"ok": False, "error": "PlayerNotFound"})

    if kind != "a":
        kivly_registration.require_player(dynamo_r, meta, player)
    blob = get_game_type_blob(player, game_type)

    if game_type == "STACK" and kind == "a":
        try:
            prize = stack_prizes.redeem(dynamo_r, game_id, player_id, claims["awardId"])
        except stack_prizes.PrizeError as exc:
            return _resp(exc.status, {"ok": False, "error": exc.code})
        return _resp(200, _json_sanitize({
            "ok": True, "gameId": game_id, "playerId": player_id, "kind": "award",
            "delivered": True, "label": _player_display_label(player), "prize": prize,
        }))

    if game_type == "STACK":
        if kind != "k":
            return _resp(400, {"ok": False, "error": "NotEligible"})
        if blob.get("rankingPrizeDelivered"):
            return _resp(409, {"ok": False, "error": "AlreadyDelivered", "kind": "ranking"})
        ranking = stack_ranking(game_id, gp_table, None)["results"]
        rank = next((entry["rank"] for entry in ranking if entry["playerId"] == player_id), None)
        if not _stack_ranking_prize_eligible(meta, rank):
            return _resp(400, {"ok": False, "error": "NotEligible"})
        prize = (_public_prize_catalog(meta) or {}).get("ranking")
        try:
            gp_table.update_item(
                Key={"gameId": game_id, "playerId": int(player_id)},
                UpdateExpression="SET #type.#g.#delivered = :yes",
                ConditionExpression=(
                    "attribute_exists(playerId) AND #type.#g.#attempt.#status = :finished "
                    "AND (attribute_not_exists(#type.#g.#delivered) OR #type.#g.#delivered = :no)"
                ),
                ExpressionAttributeNames={
                    "#type": "type", "#g": "STACK", "#delivered": "rankingPrizeDelivered",
                    "#attempt": "attempt", "#status": "status",
                },
                ExpressionAttributeValues={":yes": True, ":no": False, ":finished": "finished"},
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return _resp(409, {"ok": False, "error": "AlreadyDelivered", "kind": "ranking"})
            log("public_redeem_failed", {"error": str(exc), "gameId": game_id, "playerId": player_id})
            return _resp(500, {"ok": False, "error": "UpdateFailed"})
        return _resp(200, _json_sanitize({
            "ok": True, "gameId": game_id, "playerId": player_id, "kind": "ranking",
            "delivered": True, "label": _player_display_label(player),
            "prize": prize,
        }))

    if kind == "v":
        if not player.get("validated"):
            return _resp(400, {"ok": False, "error": "NotEligible"})
        if blob.get("validationPrizeDelivered"):
            return _resp(409, {"ok": False, "error": "AlreadyDelivered", "kind": "validation"})
        attr = "validationPrizeDelivered"
        kind_out = "validation"
    elif kind == "r":
        if not blob.get("raffleWin"):
            return _resp(400, {"ok": False, "error": "NotEligible"})
        if blob.get("prizeDelivered"):
            return _resp(409, {"ok": False, "error": "AlreadyDelivered", "kind": "raffle"})
        attr = "prizeDelivered"
        kind_out = "raffle"
    else:
        return _resp(400, {"ok": False, "error": "NotEligible"})

    prize = (_public_prize_catalog(meta) or {}).get(kind_out)
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
        "prize": prize,
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
        if path.endswith("/player/photo") and method == "POST":
            return _handle_player_photo(event)
        if path.endswith("/redeem") and method == "POST":
            return _handle_redeem(event)
        if path.endswith("/score") and method == "POST":
            return _handle_score(event)
        if path.endswith("/rulet4/spin") and method == "POST":
            return _handle_rulet4_spin(event)
        if path.endswith("/kivly/reward") and method == "POST":
            return _handle_kivly_reward(event)

        return _resp(404, {"ok": False, "error": "NotFound"})
    except stack_prizes.PrizeError as exc:
        return _resp(exc.status, {"ok": False, "error": exc.code})
    except (kivly_registration.RegistrationError, kivly_registration.KivlyError) as exc:
        status, payload = kivly_registration.error_response(exc)
        return _resp(status, payload)
    except Exception as e:
        log("public_internal_error", {"errorType": type(e).__name__})
        return _resp(500, {"ok": False, "error": "InternalError"})
