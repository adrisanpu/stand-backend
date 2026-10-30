"""Atomic single-attempt STACK lifecycle."""
import re
import os
import boto3

from botocore.exceptions import ClientError
from stand_common import stack_prizes
from stand_common.stack import CONFIG_VERSION, game_config, replay, player_rank
from stand_common.utils import _iso_now, _parse_iso, get_game_type_blob


_ATTEMPT_ID = re.compile(r"[A-Za-z0-9_-]{1,128}", re.ASCII)


def _success(game_id: str, player_id: int, attempt: dict) -> dict:
    return {
        "ok": True,
        "gameId": game_id,
        "playerId": player_id,
        "attemptId": attempt["attemptId"],
        "status": attempt["status"],
        "configVersion": attempt["configVersion"],
        "startedAt": attempt["startedAt"],
        "configSnapshot": attempt["configSnapshot"],
    }


def _finished_success(game_id, player_id, player, gp_table):
    blob = get_game_type_blob(player, "STACK")
    attempt = blob["attempt"]
    return {
        "ok": True, "gameId": game_id, "playerId": player_id,
        "attemptId": attempt["attemptId"], "status": "finished",
        "configVersion": attempt["configVersion"], "score": blob["score"],
        "scoreUpdatedAt": blob["scoreUpdatedAt"], "finishedAt": attempt["finishedAt"],
        "rank": blob.get("rankAtFinish") if "rankAtFinish" in blob else player_rank(game_id, player_id, gp_table),
    }


def _existing_result(game_id: str, player_id: int, player: dict, attempt_id: str, gp_table=None) -> dict | None:
    blob = get_game_type_blob(player, "STACK")
    attempt = blob.get("attempt")
    if attempt and attempt.get("attemptId") == attempt_id:
        if attempt.get("status") == "started":
            return _success(game_id, player_id, attempt)
        if attempt.get("status") == "finished":
            return _finished_success(game_id, player_id, player, gp_table)
    if attempt or blob.get("score") is not None:
        return {"ok": False, "error": "AttemptAlreadyStarted"}
    return None


def _finish(game_id, player_id, player, gp_table, payload, meta=None):
    blob = get_game_type_blob(player, "STACK")
    attempt = blob.get("attempt")
    if not attempt:
        return {"ok": False, "error": "AttemptNotStarted"}
    if attempt.get("attemptId") != payload["attemptId"]:
        return {"ok": False, "error": "AttemptConflict"}
    try:
        score, minimum, evidence_hash = replay(payload.get("placements"), attempt["configSnapshot"])
    except (ValueError, KeyError):
        return {"ok": False, "error": "InvalidPlacements"}
    if attempt.get("status") == "finished":
        if attempt.get("evidenceHash") == evidence_hash:
            return _finished_success(game_id, player_id, player, gp_table)
        return {"ok": False, "error": "AttemptConflict"}
    if attempt.get("status") != "started" or blob.get("score") is not None:
        return {"ok": False, "error": "AttemptConflict"}
    now = _iso_now()
    started, finished = _parse_iso(attempt.get("startedAt")), _parse_iso(now)
    if started is None or started.tzinfo is None or finished is None or finished.tzinfo is None:
        return {"ok": False, "error": "InvalidAttemptTime"}
    elapsed = (finished - started).total_seconds()
    if elapsed < 0 or elapsed + 1e-6 + minimum * 1e-12 < minimum:
        return {"ok": False, "error": "InvalidAttemptTime"}
    final_attempt = {
        **attempt, "status": "finished", "finishedAt": now, "evidenceHash": evidence_hash,
    }
    if "prizeProgram" in attempt:
        try:
            rank = stack_prizes.finish(boto3.resource("dynamodb"), meta, player, final_attempt, score)
        except stack_prizes.PrizeError as exc:
            return {"ok": False, "error": exc.code}
        return {"ok": True, "gameId": game_id, "playerId": player_id,
                "attemptId": attempt["attemptId"], "status": "finished", "configVersion": CONFIG_VERSION,
                "score": score, "scoreUpdatedAt": now, "finishedAt": now, "rank": rank}
    key = {"gameId": game_id, "playerId": player_id}
    try:
        gp_table.update_item(
            Key=key,
            UpdateExpression=("SET #type.#g.#attempt = :attempt, #type.#g.#score = :score, "
                              "#type.#g.#updated = :now"),
            ConditionExpression=(
                "attribute_exists(gameId) AND attribute_exists(playerId) "
                "AND #type.#g.#attempt.#id = :id AND #type.#g.#attempt.#status = :started "
                "AND attribute_not_exists(#type.#g.#score)"
            ),
            ExpressionAttributeNames={
                "#type": "type", "#g": "STACK", "#attempt": "attempt", "#score": "score",
                "#id": "attemptId", "#status": "status", "#updated": "scoreUpdatedAt",
            },
            ExpressionAttributeValues={
                ":attempt": final_attempt, ":score": score, ":now": now,
                ":id": payload["attemptId"], ":started": "started",
            },
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        current = gp_table.get_item(Key=key, ConsistentRead=True).get("Item")
        if not current:
            return {"ok": False, "error": "PlayerNotFound"}
        winner = get_game_type_blob(current, "STACK").get("attempt") or {}
        if (winner.get("attemptId") == payload["attemptId"] and winner.get("status") == "finished"
                and winner.get("evidenceHash") == evidence_hash):
            return _finished_success(game_id, player_id, current, gp_table)
        return {"ok": False, "error": "AttemptConflict"}
    final_player = {**player, "type": {**(player.get("type") or {}), "STACK": {
        **blob, "attempt": final_attempt, "score": score, "scoreUpdatedAt": now,
    }}}
    return _finished_success(game_id, player_id, final_player, gp_table)


def store_score(game_id: str, gp_table, payload: dict, game_meta: dict) -> dict:
    if os.environ.get("STACK_PRIZES_TABLE"):
        game_meta = boto3.resource("dynamodb").Table(os.environ["GAMES_TABLE"]).get_item(
            Key={"gameId": game_id}, ConsistentRead=True,
        )["Item"]
    action = payload.get("action")
    if action not in ("start", "finish"):
        return {"ok": False, "error": "InvalidStackAction"}
    attempt_id = payload.get("attemptId")
    if not isinstance(attempt_id, str) or not _ATTEMPT_ID.fullmatch(attempt_id):
        return {"ok": False, "error": "InvalidAttemptId"}
    player_id = payload.get("playerId")
    if isinstance(player_id, bool) or not isinstance(player_id, int) or player_id <= 0:
        return {"ok": False, "error": "InvalidPlayerId"}
    allowed = {"gameId", "playerId", "action", "attemptId"}
    if action == "finish":
        allowed.add("placements")
    if set(payload) - allowed:
        return {"ok": False, "error": "InvalidStackFields"}
    key = {"gameId": game_id, "playerId": player_id}
    player = gp_table.get_item(Key=key, ConsistentRead=True).get("Item")
    if not player:
        return {"ok": False, "error": "PlayerNotFound"}
    if action == "finish":
        return _finish(game_id, player_id, player, gp_table, payload, game_meta)
    existing = _existing_result(game_id, player_id, player, attempt_id, gp_table)
    if existing:
        return existing
    try:
        config = game_config(game_meta)
    except ValueError:
        return {"ok": False, "error": "InvalidStackConfig"}
    attempt = {
        "attemptId": attempt_id,
        "status": "started",
        "startedAt": _iso_now(),
        "configVersion": CONFIG_VERSION,
        "configSnapshot": config,
    }
    if "stackPrizeProgram" in game_meta or os.environ.get("STACK_PRIZES_TABLE"):
        try:
            stack_prizes.start(boto3.resource("dynamodb"), game_meta, player, attempt)
        except stack_prizes.PrizeError as exc:
            return {"ok": False, "error": exc.code}
        return _success(game_id, player_id, attempt)
    # Web join initializes both maps. The single conditional write selects the
    # winner even when two starts read the same unused participant concurrently.
    try:
        gp_table.update_item(
            Key=key,
            UpdateExpression="SET #type.#g.#attempt = :attempt",
            ConditionExpression=(
                "attribute_exists(gameId) AND attribute_exists(playerId) "
                "AND attribute_not_exists(#type.#g.#attempt) "
                "AND attribute_not_exists(#type.#g.#score)"
            ),
            ExpressionAttributeNames={
                "#type": "type", "#g": "STACK", "#attempt": "attempt", "#score": "score",
            },
            ExpressionAttributeValues={":attempt": attempt},
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        current = gp_table.get_item(Key=key, ConsistentRead=True).get("Item")
        if not current:
            return {"ok": False, "error": "PlayerNotFound"}
        return _existing_result(game_id, player_id, current, attempt_id, gp_table) or {
            "ok": False, "error": "AttemptAlreadyStarted",
        }
    return _success(game_id, player_id, attempt)
