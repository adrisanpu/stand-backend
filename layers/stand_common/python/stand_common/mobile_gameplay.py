"""Token-facing, server-authoritative single-phone social games.

The public player POST uses action=play, move, revision and requestId. Moves are
code/reset (L3TRAS), pair (CUPIDO/SEMAFORO), ready/answer (SEMAFORO).
Only the initiating phone advances the pair's quiz; no phone synchronization is
implied. Validation and its awards precede the optional cross quiz, as in the
original game. Public assignments never serialize stored quiz expectations.
"""
import os
import re
import time
from copy import deepcopy

import boto3
from boto3.dynamodb.conditions import Key, Attr

from stand_common import game_forms, kivly_registration, stack_prizes, game_lifecycle
from stand_common.l3tras_word import l3tras_word_from_game_meta
from stand_common.utils import _iso_now, get_game_type_blob, mask_email

TYPES = frozenset({"L3TRAS", "SEMAFORO", "CUPIDO", "INFOCARDS"})


class PlayError(Exception):
    def __init__(self, code, status=409):
        self.code, self.status = code, status


def now_ms():
    return int(time.time() * 1000)


def label(player):
    return player.get("username") or mask_email(player.get("email") or "") or "Jugador"


def _state(player):
    return player.get("mobilePlay") or {"revision": 0, "phase": "idle"}


def _players_by_code(table, game_id, code):
    rows, last = [], None
    while True:
        args = {"KeyConditionExpression": Key("gameId").eq(game_id),
                "FilterExpression": Attr("validationCode").eq(int(code)), "ConsistentRead": True}
        if last:
            args["ExclusiveStartKey"] = last
        page = table.query(**args)
        rows.extend(page.get("Items") or [])
        last = page.get("LastEvaluatedKey")
        if not last:
            return rows


def _eligible(dynamo, meta, player):
    kivly_registration.require_player(dynamo, meta, player)
    game_forms.require(meta, player, "after_registration")
    blob = get_game_type_blob(player, meta["gameType"])
    if "formConfig" not in meta and blob.get("quizRequired") and not blob.get("quizCompleted"):
        raise PlayError("QuizRequired")


def _cards(meta):
    bucket = os.environ.get("INFOCARDS_BUCKET")
    result = []
    for raw in get_game_type_blob(meta, "INFOCARDS").get("cards") or []:
        if not isinstance(raw, dict):
            continue
        card = {k: raw[k] for k in ("id", "title", "body", "order", "imagePositionX", "imagePositionY") if k in raw}
        for kind in ("image", "video"):
            key = raw.get(f"{kind}Key")
            card[f"{kind}Url"] = None
            if key and isinstance(key, str) and key.startswith(f"infocards/{meta['gameId']}/"):
                if not bucket:
                    raise PlayError("CardsUnavailable", 503)
                card[f"{kind}Url"] = boto3.client("s3").generate_presigned_url(
                    "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600)
        result.append(card)
    return sorted(result, key=lambda row: row.get("order", 0))


def assignment(meta, player):
    gt = meta["gameType"]
    if game_forms.pending(meta, player, "after_registration"):
        return {
            "gameType": gt, "playerId": int(player["playerId"]), "displayName": label(player),
            "validationCode": player.get("validationCode"), "validated": bool(player.get("validated")),
            "play": {"revision": 0, "phase": "idle"},
        }
    blob, state = get_game_type_blob(player, gt), _state(player)
    play = {k: state[k] for k in ("revision", "phase", "expiresAt", "word", "slots", "strikes",
                                 "roundIndex", "result", "participants", "lastCorrect") if k in state}
    play["serverNow"] = now_ms()
    if gt == "L3TRAS":
        play.setdefault("word", l3tras_word_from_game_meta(meta))
        play.setdefault("slots", [None] * len(play["word"]))
        if state.get("phase") == "collecting" and now_ms() >= state.get("expiresAt", 0):
            play["phase"] = "expired"
    if gt == "SEMAFORO" and state.get("phase") == "quiz":
        rounds = state.get("rounds") or []
        index = int(state.get("roundIndex", 0))
        if index < len(rounds):
            current = rounds[index]
            play["turn"] = {"guesser": current["guesser"], "target": current["target"],
                            "ready": bool(state.get("expiresAt"))}
            if state.get("expiresAt"):
                play["turn"]["question"] = {k: current[k] for k in ("id", "text", "options")}
    result = {
        "gameType": gt, "playerId": int(player["playerId"]), "displayName": label(player),
        "validationCode": player.get("validationCode"), "validated": bool(player.get("validated")),
        "play": play,
    }
    if gt == "L3TRAS":
        result["letter"] = blob.get("letter")
    if gt == "SEMAFORO":
        answer = (blob.get("quizAnswers") or {}).get("q1")
        status = next((option["title"] for question in game_forms.questions(meta) if question["id"] == "q1"
                       for option in question["options"] if option["payload"] == answer), None)
        if status:
            result["statusLabel"] = status
    if gt == "INFOCARDS":
        result["cards"] = _cards(meta)
    return result


def _rounds(meta, players):
    questions = game_forms.questions(meta)
    if ("formConfig" in meta and game_forms.moment(meta) != "after_registration"
            and not all(get_game_type_blob(p, "SEMAFORO").get("quizAnswers") for p in players)):
        return []
    rounds = []
    for question in [q for q in questions if q["id"] != "q1"][:3]:
        for guesser, target in ((0, 1), (1, 0)):
            expected = get_game_type_blob(players[target], "SEMAFORO").get("quizAnswers", {}).get(question["id"])
            if expected not in [o["payload"] for o in question["options"]]:
                raise PlayError("PartnerQuizRequired")
            rounds.append({**question, "guesser": label(players[guesser]), "target": label(players[target]),
                           "expected": expected})
    return rounds


def _persist(dynamo, meta, player, old, state, validated_players):
    values = {":state": state}
    condition = "attribute_exists(playerId)"
    if player.get("mobilePlay") is not None:
        condition += " AND mobilePlay = :previous"
        values[":previous"] = old
    else:
        condition += " AND attribute_not_exists(mobilePlay)"
    update = "SET mobilePlay = :state"
    own_validated = any(p["playerId"] == player["playerId"] for p in validated_players)
    if own_validated:
        update += ", validated = :yes, validatedAt = :now"
        condition += " AND (attribute_not_exists(validated) OR validated = :no)"
        values.update({":yes": True, ":no": False, ":now": _iso_now()})
    ops = [{"Update": {"TableName": os.environ["GAMEPLAYER_TABLE"],
                      "Key": {"gameId": meta["gameId"], "playerId": player["playerId"]},
                      "UpdateExpression": update, "ConditionExpression": condition,
                      "ExpressionAttributeValues": values}}]
    for partner in validated_players:
        if partner["playerId"] == player["playerId"]:
            continue
        ops.append({"Update": {
            "TableName": os.environ["GAMEPLAYER_TABLE"],
            "Key": {"gameId": meta["gameId"], "playerId": partner["playerId"]},
            "UpdateExpression": "SET validated = :yes, validatedAt = :now",
            "ConditionExpression": "attribute_exists(playerId) AND (attribute_not_exists(validated) OR validated = :no)",
            "ExpressionAttributeValues": {":yes": True, ":no": False, ":now": _iso_now()},
        }})
    game_values = {":yes": True}
    game_expression = ""
    if validated_players:
        game_expression = ", validatedCount = if_not_exists(validatedCount, :zero) + :count"
        game_values.update({":zero": 0, ":count": len(validated_players)})
    ops.insert(0, stack_prizes.game_update(
        meta, game_expression, game_values,
        extra=" AND (attribute_not_exists(isActive) OR isActive = :yes)"
              " AND attribute_not_exists(archivedAt) AND attribute_not_exists(deletedAt)"
              " AND attribute_not_exists(deletionStartedAt)"))
    try:
        stack_prizes.transact(dynamo, ops)
    except stack_prizes.PrizeError:
        raise PlayError("PlayChanged") from None


def reconcile_awards(dynamo, meta, player):
    """Idempotent award outbox also runs on resume after a lost response."""
    state = _state(player)
    for pid in state.get("validatedIds") or []:
        target = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
            Key={"gameId": meta["gameId"], "playerId": pid}, ConsistentRead=True,
        ).get("Item")
        if target and target.get("validated"):
            game_lifecycle.issue_native_award(dynamo, meta, target, "validation")


def act(dynamo, meta, player, body):
    gt = meta["gameType"]
    if gt not in TYPES - {"INFOCARDS"}:
        raise PlayError("UnsupportedGameType", 400)
    if (not meta.get("isActive", True) or meta.get("lifecycleState", "active") != "active"
            or meta.get("archivedAt")):
        raise PlayError("GameInactive", 403)
    _eligible(dynamo, meta, player)
    if set(body) - {"gameId", "playerToken", "action", "move", "revision", "requestId", "code", "answer"}:
        raise PlayError("InvalidPlayFields", 400)
    request_id = body.get("requestId")
    if not isinstance(request_id, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{8,80}", request_id):
        raise PlayError("InvalidRequestId", 400)
    old = _state(player)
    if old.get("lastRequestId") == request_id:
        reconcile_awards(dynamo, meta, player)
        return
    if type(body.get("revision")) is not int or body["revision"] != old["revision"]:
        raise PlayError("PlayChanged")
    state = deepcopy(old)
    move, clock = body.get("move"), now_ms()
    table = dynamo.Table(os.environ["GAMEPLAYER_TABLE"])
    validated = []
    if gt == "L3TRAS":
        if move == "reset":
            if old["phase"] == "finished":
                raise PlayError("AlreadyFinished")
            state = {"phase": "idle"}
        elif move == "code":
            if old["phase"] == "finished" or player.get("validated"):
                raise PlayError("AlreadyFinished")
            word = l3tras_word_from_game_meta(meta)
            if len(word) > 40:
                raise PlayError("InvalidWord", 400)
            if old["phase"] == "idle" or clock >= old.get("expiresAt", clock + 1):
                duration = int(get_game_type_blob(meta, gt).get("validationTimeLimitSeconds") or 60)
                state = {"phase": "collecting", "word": word, "slots": [None] * len(word),
                         "codes": [], "playerIds": [], "expiresAt": clock + max(15, min(duration, 3600)) * 1000}
            code = body.get("code")
            if not isinstance(code, str) or not re.fullmatch(r"[0-9]{4}", code):
                raise PlayError("InvalidCode", 400)
            if code in state["codes"]:
                raise PlayError("CodeAlreadyUsed")
            candidates = _players_by_code(table, meta["gameId"], code)
            if len(candidates) != 1:
                raise PlayError("CodeNotFound", 400)
            target = candidates[0]
            _eligible(dynamo, meta, target)
            if target.get("validated"):
                raise PlayError("AlreadyValidated")
            letter = get_game_type_blob(target, gt).get("letter")
            index = next((i for i, char in enumerate(state["word"])
                          if char == letter and state["slots"][i] is None), None)
            if index is None:
                raise PlayError("LetterNotNeeded", 400)
            state["slots"][index] = letter
            state["codes"].append(code)
            state["playerIds"].append(int(target["playerId"]))
            if all(state["slots"]):
                if int(player["playerId"]) not in state["playerIds"]:
                    raise PlayError("MustIncludeOwnCode", 403)
                validated = [table.get_item(Key={"gameId": meta["gameId"], "playerId": pid},
                                            ConsistentRead=True)["Item"] for pid in state["playerIds"]]
                for target in validated:
                    _eligible(dynamo, meta, target)
                state.update({"phase": "finished", "result": "won"})
        else:
            raise PlayError("InvalidMove", 400)
    elif move == "pair":
        if old["phase"] != "idle" or player.get("validated"):
            raise PlayError("AlreadyValidated")
        code = body.get("code")
        if not isinstance(code, str) or not re.fullmatch(r"[0-9]{4}", code):
            raise PlayError("InvalidCode", 400)
        if int(code) == int(player.get("validationCode") or 0):
            raise PlayError("SameCode", 400)
        partners = _players_by_code(table, meta["gameId"], code)
        if len(partners) != 1:
            raise PlayError("CodeNotFound", 400)
        partner = partners[0]
        _eligible(dynamo, meta, partner)
        if partner.get("validated"):
            raise PlayError("AlreadyValidated")
        validated = [player, partner]
        rounds = _rounds(meta, validated) if gt == "SEMAFORO" else []
        state = {"phase": "quiz" if rounds else "finished", "participants": [label(p) for p in validated],
                 "rounds": rounds, "roundIndex": 0, "strikes": 0}
        if not rounds:
            state["result"] = "won"
    elif gt == "SEMAFORO" and old["phase"] == "quiz" and move == "ready":
        if state.get("expiresAt"):
            raise PlayError("TurnAlreadyStarted")
        state["expiresAt"] = clock + 5000
    elif gt == "SEMAFORO" and old["phase"] == "quiz" and move == "answer":
        if not state.get("expiresAt"):
            raise PlayError("TurnNotStarted")
        current = state["rounds"][int(state["roundIndex"])]
        answer = body.get("answer")
        if answer is not None and answer not in [o["payload"] for o in current["options"]]:
            raise PlayError("InvalidAnswer", 400)
        if answer is None and clock < state["expiresAt"]:
            raise PlayError("TurnNotExpired")
        correct = clock < state["expiresAt"] and answer == current["expected"]
        state["lastCorrect"] = correct
        state["strikes"] += 0 if correct else 1
        state["roundIndex"] += 1
        state.pop("expiresAt", None)
        if state["strikes"] >= 3 or state["roundIndex"] >= len(state["rounds"]):
            state.update({"phase": "finished", "result": "lost" if state["strikes"] >= 3 else "won"})
    else:
        raise PlayError("InvalidMove", 400)
    state.update({"revision": int(old["revision"]) + 1, "lastRequestId": request_id})
    if validated:
        state["validatedIds"] = [int(p["playerId"]) for p in validated]
    _persist(dynamo, meta, player, old, state, validated)
    reconcile_awards(dynamo, meta, {**player, "mobilePlay": state})
