from boto3.dynamodb.conditions import Key

from stand_common.utils import _as_int, _get_claims, _resp, log, OPTIONAL_PARTICIPANT_FIELDS


def get_form_responses(event, game_id, games_table, gameplayer_table):
    owner_id = (_get_claims(event).get("sub") or "").strip()
    if not owner_id:
        return _resp(401, {"ok": False, "error": "Unauthorized"})
    if not isinstance(game_id, str) or not game_id.strip():
        return _resp(400, {"ok": False, "error": "MissingGameId"})
    game_id = game_id.strip()

    try:
        game = games_table.get_item(Key={"gameId": game_id}).get("Item")
        if not game or game.get("deletedAt"):
            return _resp(404, {"ok": False, "error": "GameNotFound"})
        if game.get("ownerUserId") != owner_id:
            return _resp(403, {"ok": False, "error": "Forbidden"})

        questions = []
        question_order = game.get("quizOrder") or []
        question_map = game.get("quizQuestions") or {}
        if isinstance(question_order, list) and isinstance(question_map, dict):
            for question_id in question_order:
                if not isinstance(question_id, str):
                    continue
                question = question_map.get(question_id)
                if not isinstance(question, dict):
                    continue
                options = question.get("options") or []
                questions.append({
                    "id": question_id,
                    "text": question.get("text") if isinstance(question.get("text"), str) else "",
                    "options": [
                        {"title": option["title"], "payload": option["payload"]}
                        for option in options
                        if isinstance(option, dict)
                        and isinstance(option.get("title"), str)
                        and isinstance(option.get("payload"), str)
                    ] if isinstance(options, list) else [],
                })

        game_type = (game.get("gameType") or "").upper()
        players = []
        last_key = None
        while True:
            query = {"KeyConditionExpression": Key("gameId").eq(game_id)}
            if last_key:
                query["ExclusiveStartKey"] = last_key
            result = gameplayer_table.query(**query)
            for item in result.get("Items") or []:
                player_id = _as_int(item.get("playerId"))
                if player_id is None:
                    player_id = str(item.get("playerId") or "")
                type_map = item.get("type") or {}
                blob = type_map.get(game_type) or {} if isinstance(type_map, dict) else {}
                answers = blob.get("quizAnswers") or {} if isinstance(blob, dict) else {}
                if not isinstance(answers, dict):
                    answers = {}
                answers = {
                    key: value for key, value in answers.items()
                    if isinstance(key, str) and isinstance(value, str)
                }
                completed_at = blob.get("quizCompletedAt") if isinstance(blob, dict) else None
                players.append({
                    "playerId": player_id,
                    "name": item.get("username") if isinstance(item.get("username"), str) else None,
                    "email": item.get("email") if isinstance(item.get("email"), str) else None,
                    "phone": item.get("phone") if isinstance(item.get("phone"), str) else None,
                    **{name: item.get(name) if isinstance(item.get(name), str) else None
                       for name in OPTIONAL_PARTICIPANT_FIELDS},
                    "completed": bool(blob.get("quizCompleted", False)) if isinstance(blob, dict) else False,
                    "completedAt": completed_at if isinstance(completed_at, str) else None,
                    "answers": answers,
                })
            last_key = result.get("LastEvaluatedKey")
            if not last_key:
                break

        return _resp(200, {
            "ok": True,
            "gameId": game_id,
            "gameName": game.get("gameName") or game_id,
            "questions": questions,
            "players": players,
        })
    except Exception as exc:
        log("form_responses_read_failed", {"gameId": game_id, "error": repr(exc)})
        return _resp(500, {"ok": False, "error": "FormResponsesUnavailable"})
