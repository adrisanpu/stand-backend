import json
from unittest.mock import MagicMock

from tests.test_stack_integration import ROOT, load, operator_event

quiz = load(
    "quiz_form_responses",
    ROOT / "src" / "stand_prod_game_fn_quiz" / "lambda_function.py",
)

GAME = {
    "gameId": "ABCDEF",
    "gameName": "Quiz event",
    "gameType": "T1MER",
    "ownerUserId": "owner",
    "quizOrder": ["q1"],
    "quizQuestions": {"q1": {"text": "Question?", "options": [
        {"title": "First", "payload": "ABCDEF_q1_a"},
    ]}},
}


def response_body(response):
    return json.loads(response["body"])


def api_event(method="GET", **query):
    event = operator_event({}, **query)
    event["httpMethod"] = method
    return event


def test_analytics_returns_all_paginated_participants_and_legacy_answer_fields(monkeypatch):
    games = MagicMock()
    games.get_item.return_value = {"Item": GAME}
    players = MagicMock()
    players.query.side_effect = [
        {
            "Items": [{
                "gameId": "ABCDEF", "playerId": 1, "username": "Ana",
                "email": "ana@example.test",
                "type": {"T1MER": {"quizCompleted": True, "quizCompletedAt": "2026-01-02T03:04:05Z",
                                   "quizAnswers": {"q1": "ABCDEF_q1_a"}}},
            }],
            "LastEvaluatedKey": {"gameId": "ABCDEF", "playerId": 1},
        },
        {"Items": [{"gameId": "ABCDEF", "playerId": 2, "type": {"T1MER": {}}}]},
    ]
    monkeypatch.setattr(quiz, "games_table", games)
    monkeypatch.setattr(quiz, "gp_table", players)

    result = quiz.lambda_handler(api_event(gameId="ABCDEF", analytics="1"), None)

    assert result["statusCode"] == 200
    data = response_body(result)
    assert data == {
        "ok": True, "gameId": "ABCDEF", "gameName": "Quiz event",
        "questions": [{"id": "q1", "text": "Question?",
                       "options": [{"title": "First", "payload": "ABCDEF_q1_a"}]}],
        "players": [
            {"playerId": 1, "name": "Ana", "email": "ana@example.test", "completed": True,
             "phone": None, "address": None, "company": None, "jobTitle": None, "birthDate": None,
             "completedAt": "2026-01-02T03:04:05Z", "answers": {"q1": "ABCDEF_q1_a"}},
            {"playerId": 2, "name": None, "email": None, "completed": False,
             "phone": None, "address": None, "company": None, "jobTitle": None, "birthDate": None,
             "completedAt": None, "answers": {}},
        ],
    }
    assert players.query.call_count == 2
    assert all(call.kwargs.get("KeyConditionExpression") is not None
               for call in players.query.call_args_list)


def test_analytics_checks_owner_before_querying_participants(monkeypatch):
    games = MagicMock()
    games.get_item.return_value = {"Item": {**GAME, "ownerUserId": "other"}}
    players = MagicMock()
    monkeypatch.setattr(quiz, "games_table", games)
    monkeypatch.setattr(quiz, "gp_table", players)

    result = quiz.lambda_handler(api_event(gameId="ABCDEF", analytics="1"), None)

    assert result["statusCode"] == 403
    players.query.assert_not_called()


def test_analytics_includes_registered_players_when_quiz_is_unconfigured(monkeypatch):
    games = MagicMock()
    games.get_item.return_value = {"Item": {**GAME, "quizOrder": [], "quizQuestions": {}}}
    players = MagicMock()
    players.query.return_value = {"Items": [{"playerId": 7, "username": "Sam"}]}
    monkeypatch.setattr(quiz, "games_table", games)
    monkeypatch.setattr(quiz, "gp_table", players)

    result = quiz.lambda_handler(api_event(gameId="ABCDEF", analytics="1"), None)

    assert len(response_body(result)["players"]) == 1
    assert response_body(result)["questions"] == []


def test_legacy_get_remains_on_existing_export_contract(monkeypatch):
    games = MagicMock()
    games.get_item.return_value = {"Item": {**GAME, "quizOrder": [], "quizQuestions": {}}}
    players = MagicMock()
    players.query.return_value = {"Items": [{"playerId": 1}]}
    monkeypatch.setattr(quiz, "games_table", games)
    monkeypatch.setattr(quiz, "gp_table", players)

    result = quiz.lambda_handler(api_event(gameId="ABCDEF"), None)

    data = response_body(result)
    assert data["quizEnabled"] is False
    assert data["players"] == [{
        "playerId": 1, "quizRequired": False, "quizCompleted": False,
        "quizCurrentQuestion": None, "quizAnswers": {},
    }]
    assert "gameName" not in data


def test_legacy_post_requires_owner_before_loading_game(monkeypatch):
    games = MagicMock()
    monkeypatch.setattr(quiz, "games_table", games)
    event = api_event("POST")
    event["body"] = json.dumps({"gameId": "ABCDEF", "questions": [{"id": "q1", "text": "Q", "options": []}]})
    event["requestContext"]["authorizer"]["jwt"]["claims"] = {}

    result = quiz.lambda_handler(event, None)

    assert result["statusCode"] == 401
    games.get_item.assert_not_called()


def test_legacy_post_blocks_form_editor_and_completed_quiz(monkeypatch):
    games = MagicMock()
    players = MagicMock()
    monkeypatch.setattr(quiz, "games_table", games)
    monkeypatch.setattr(quiz, "gp_table", players)
    body = {"gameId": "ABCDEF", "questions": [{"id": "q1", "text": "Q", "options": []}]}
    event = api_event("POST")
    event["body"] = json.dumps(body)

    games.get_item.return_value = {"Item": {**GAME, "formConfig": {}}}
    result = quiz.lambda_handler(event, None)
    assert result["statusCode"] == 409
    assert response_body(result)["error"] == "FormEditorRequired"
    players.query.assert_not_called()

    games.get_item.return_value = {"Item": {**GAME, "quizLockedAt": "2026-01-02T03:04:05Z"}}
    result = quiz.lambda_handler(event, None)
    assert result["statusCode"] == 409
    assert response_body(result)["error"] == "QuizLocked"
    players.query.assert_not_called()

    games.get_item.return_value = {"Item": GAME}
    players.query.return_value = {"Items": [{"type": {"T1MER": {"quizCompleted": True}}}]}
    result = quiz.lambda_handler(event, None)
    assert result["statusCode"] == 409
    assert response_body(result)["error"] == "QuizLocked"
    games.update_item.assert_not_called()


def test_legacy_post_noop_does_not_reset_participants_and_guards_editor_races(monkeypatch):
    games = MagicMock()
    games.get_item.return_value = {"Item": GAME}
    players = MagicMock()
    players.query.return_value = {"Items": [{"type": {"T1MER": {"quizCompleted": False}}}]}
    monkeypatch.setattr(quiz, "games_table", games)
    monkeypatch.setattr(quiz, "gp_table", players)
    event = api_event("POST")
    event["body"] = json.dumps({
        "gameId": "ABCDEF",
        "questions": [{"id": "q1", "text": "Question?", "options": [
            {"title": "First", "payload": "ABCDEF_q1_a"},
        ]}],
    })

    result = quiz.lambda_handler(event, None)
    assert result["statusCode"] == 200
    assert response_body(result)["unchanged"] is True
    players.update_item.assert_not_called()
    games.update_item.assert_not_called()

    changed = json.loads(event["body"])
    changed["questions"][0]["text"] = "Changed?"
    event["body"] = json.dumps(changed)
    games.update_item.side_effect = type("ConditionalFailure", (Exception,), {
        "response": {"Error": {"Code": "ConditionalCheckFailedException"}},
    })()
    result = quiz.lambda_handler(event, None)
    assert result["statusCode"] == 409
    assert games.update_item.call_args.kwargs["ConditionExpression"].endswith(
        "AND attribute_not_exists(quizLockedAt) AND attribute_not_exists(formConfig)"
    )
