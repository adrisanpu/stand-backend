import json
import os
import random

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from stand_common.utils import _get_claims, _iso_now, _read_json_body, _resp, get_game_type_blob, log, set_game_type_blob

GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "")

dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")
games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)

DEFAULT_WORDS = ["playa", "montana", "pizza", "cine", "futbol", "musica"]


def _method(event):
    return (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "GET")
    ).upper()


def _get_game(game_id: str):
    return (games_table.get_item(Key={"gameId": game_id}) or {}).get("Item")


def _query_players(game_id: str):
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


def _send_bulk(messages):
    if not messages or not IG_SENDER_LAMBDA:
        return
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"messages": messages}, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("impostor_dm_send_error", {"error": repr(e), "count": len(messages)})


def _get_state_payload(game_id: str):
    game = _get_game(game_id)
    if not game:
        return None
    blob = get_game_type_blob(game, "IMPOSTOR")
    players = _query_players(game_id)
    out_players = []
    for p in players:
        if not p.get("validated", False):
            continue
        out_players.append(
            {
                "playerId": int(p.get("playerId")),
                "instagramUsername": p.get("instagramUsername"),
                "instagramProfilePicUrl": p.get("instagramProfilePicUrl"),
            }
        )
    out_players.sort(key=lambda p: p["playerId"])
    return {
        "ok": True,
        "gameId": game_id,
        "maxPlayers": game.get("maxPlayers"),
        "phase": blob.get("phase", "lobby"),
        "round": blob.get("round", 0),
        "wordRoundTurnIndex": blob.get("wordRoundTurnIndex", 0),
        "players": out_players,
        "impostorPlayerId": blob.get("impostorPlayerId"),
        "selectedWord": blob.get("selectedWord"),
        "votes": blob.get("votes"),
        "winner": blob.get("winner"),
    }


def _handle_start_round(game_id: str):
    game = _get_game(game_id)
    if not game:
        return _resp(404, {"ok": False, "error": "GameNotFound"})

    players = [p for p in _query_players(game_id) if p.get("validated", False)]
    if len(players) < 3:
        return _resp(400, {"ok": False, "error": "NeedAtLeast3ValidatedPlayers"})

    blob = get_game_type_blob(game, "IMPOSTOR")
    words = blob.get("words") if isinstance(blob.get("words"), list) else DEFAULT_WORDS
    selected_word = random.choice(words) if words else random.choice(DEFAULT_WORDS)
    impostor = random.choice(players)
    order = [int(p["playerId"]) for p in players]
    random.shuffle(order)

    new_blob = {
        **blob,
        "phase": "word_round",
        "round": int(blob.get("round", 0)) + 1,
        "wordRoundTurnIndex": 0,
        "selectedWord": selected_word,
        "impostorPlayerId": int(impostor["playerId"]),
        "turnOrder": order,
        "votesByPlayer": {},
        "votes": {},
        "winner": None,
        "startedAt": _iso_now(),
    }
    set_game_type_blob(games_table, game_id, "IMPOSTOR", new_blob, updated_at=_iso_now())

    messages = []
    for p in players:
        psid = (p.get("instagramPSID") or "").strip()
        if not psid:
            continue
        if int(p["playerId"]) == int(impostor["playerId"]):
            txt = "Tu rol es IMPOSTOR. Intenta mezclarte sin revelar que no sabes la palabra."
        else:
            txt = f"Palabra secreta: {selected_word}"
        messages.append({"psid": psid, "text": txt})
    _send_bulk(messages)
    return _resp(200, {"ok": True, "started": True})


def _handle_submit_vote(game_id: str, body: dict):
    voter_player_id = body.get("voterPlayerId")
    target_player_id = body.get("targetPlayerId")
    if voter_player_id is None or target_player_id is None:
        return _resp(400, {"ok": False, "error": "Missing voterPlayerId or targetPlayerId"})
    try:
        voter_player_id = int(voter_player_id)
        target_player_id = int(target_player_id)
    except Exception:
        return _resp(400, {"ok": False, "error": "Invalid player ids"})

    game = _get_game(game_id)
    if not game:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    blob = get_game_type_blob(game, "IMPOSTOR")
    votes_by_player = dict(blob.get("votesByPlayer") or {})
    votes_by_player[str(voter_player_id)] = target_player_id
    blob["votesByPlayer"] = votes_by_player
    blob["phase"] = "voting"
    set_game_type_blob(games_table, game_id, "IMPOSTOR", blob, updated_at=_iso_now())
    return _resp(200, {"ok": True, "voted": True, "votesCount": len(votes_by_player)})


def _handle_advance_turn(game_id: str):
    game = _get_game(game_id)
    if not game:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    blob = get_game_type_blob(game, "IMPOSTOR")
    turn_order = blob.get("turnOrder") or []
    idx = int(blob.get("wordRoundTurnIndex", 0))
    if idx + 1 >= len(turn_order):
        blob["phase"] = "voting"
    else:
        blob["phase"] = "word_round"
        blob["wordRoundTurnIndex"] = idx + 1
    set_game_type_blob(games_table, game_id, "IMPOSTOR", blob, updated_at=_iso_now())
    return _resp(200, {"ok": True})


def _handle_finish_voting(game_id: str):
    game = _get_game(game_id)
    if not game:
        return _resp(404, {"ok": False, "error": "GameNotFound"})
    blob = get_game_type_blob(game, "IMPOSTOR")
    votes_by_player = blob.get("votesByPlayer") or {}
    tally = {}
    for _, target in votes_by_player.items():
        key = int(target)
        tally[key] = int(tally.get(key, 0)) + 1
    voted_impostor = None
    if tally:
        voted_impostor = max(tally, key=lambda k: tally[k])
    real_impostor = blob.get("impostorPlayerId")
    winner = "crew" if voted_impostor is not None and int(voted_impostor) == int(real_impostor) else "impostor"
    blob["votes"] = tally
    blob["winner"] = winner
    blob["phase"] = "result"
    set_game_type_blob(games_table, game_id, "IMPOSTOR", blob, updated_at=_iso_now())
    return _resp(200, {"ok": True, "winner": winner, "votes": tally})


def _ensure_owner(event, game):
    claims = _get_claims(event)
    owner_user_id = (claims.get("sub") or "").strip()
    return owner_user_id and (game.get("ownerUserId") or "") == owner_user_id


def lambda_handler(event, context):
    try:
        method = _method(event)
        if method == "OPTIONS":
            return _resp(204, "")

        if method == "GET":
            qs = event.get("queryStringParameters") or {}
            game_id = (qs.get("gameId") or "").strip()
            if not game_id:
                return _resp(400, {"ok": False, "error": "MissingGameId"})
            game = _get_game(game_id)
            if not game:
                return _resp(404, {"ok": False, "error": "GameNotFound"})
            if not _ensure_owner(event, game):
                return _resp(403, {"ok": False, "error": "Forbidden"})
            payload = _get_state_payload(game_id)
            if not payload:
                return _resp(404, {"ok": False, "error": "GameNotFound"})
            return _resp(200, payload)

        if method == "POST":
            body = _read_json_body(event)
            if body is None:
                return _resp(400, {"ok": False, "error": "Invalid JSON body"})
            game_id = (body.get("gameId") or "").strip()
            action = (body.get("action") or "").strip()
            if not game_id:
                return _resp(400, {"ok": False, "error": "MissingGameId"})
            game = _get_game(game_id)
            if not game:
                return _resp(404, {"ok": False, "error": "GameNotFound"})
            if not _ensure_owner(event, game):
                return _resp(403, {"ok": False, "error": "Forbidden"})
            if (game.get("gameType") or "").upper() != "IMPOSTOR":
                return _resp(400, {"ok": False, "error": "UnsupportedGameType"})

            if action == "start_round":
                return _handle_start_round(game_id)
            if action == "submit_vote":
                return _handle_submit_vote(game_id, body)
            if action == "advance_turn":
                return _handle_advance_turn(game_id)
            if action == "start_voting":
                g = _get_game(game_id)
                blob = get_game_type_blob(g, "IMPOSTOR")
                blob["phase"] = "voting"
                set_game_type_blob(games_table, game_id, "IMPOSTOR", blob, updated_at=_iso_now())
                return _resp(200, {"ok": True})
            if action == "finish_voting":
                return _handle_finish_voting(game_id)
            return _resp(400, {"ok": False, "error": "UnknownAction"})

        return _resp(405, {"ok": False, "error": "MethodNotAllowed", "method": method})
    except ClientError as e:
        log("impostor_dynamo_error", {"error": str(e)})
        return _resp(500, {"ok": False, "error": "DynamoError", "detail": str(e)})
    except Exception as e:
        log("impostor_internal_error", {"error": repr(e)})
        return _resp(500, {"ok": False, "error": "internal_error", "detail": repr(e)})
