import copy
import json
from unittest.mock import MagicMock

import pytest
from stand_common import game_forms, mobile_gameplay as mobile, stack_prizes
from tests.test_public_web import public_mod, SECRET
from stand_common.utils import issue_player_token


@pytest.fixture
def game(monkeypatch):
    monkeypatch.setenv("GAMES_TABLE", "games")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    monkeypatch.setattr(mobile, "now_ms", lambda: 100_000)
    monkeypatch.setattr(mobile.kivly_registration, "require_player", MagicMock())
    return {"gameId": "ABC123", "gameType": "L3TRAS", "isActive": True,
            "type": {"L3TRAS": {"objectiveWord": "AA", "validationTimeLimitSeconds": 15}}}


@pytest.fixture
def world(monkeypatch, game):
    rows = {i: {"gameId": game["gameId"], "playerId": i, "username": f"Player {i}",
                "validationCode": 1000 + i, "type": {
                    "L3TRAS": {"letter": "A"}, "SEMAFORO": {"quizCompleted": True, "quizAnswers": {"q2": "a"}},
                }} for i in range(1, 4)}
    db = MagicMock()
    db.Table.return_value.get_item.side_effect = lambda Key, **_: {"Item": copy.deepcopy(rows[Key["playerId"]])}
    monkeypatch.setattr(mobile, "_players_by_code", lambda table, gid, code: [
        copy.deepcopy(p) for p in rows.values() if str(p["validationCode"]) == str(code)])
    transactions = []
    def persist(dynamo, meta, player, old, state, validated):
        transactions.append(copy.deepcopy(state))
        rows[player["playerId"]]["mobilePlay"] = copy.deepcopy(state)
        for participant in validated:
            rows[participant["playerId"]]["validated"] = True
    monkeypatch.setattr(mobile, "_persist", persist)
    awards = MagicMock()
    monkeypatch.setattr(mobile.game_lifecycle, "issue_native_award", awards)
    return rows, db, transactions, awards


def move(game, world, kind, **kwargs):
    rows, db, _, _ = world
    player = copy.deepcopy(rows[1])
    revision = mobile._state(player)["revision"]
    body = {"action": "play", "move": kind, "revision": revision, "requestId": f"request_{revision}", **kwargs}
    mobile.act(db, game, player, body)
    return body


def test_word_uses_unique_codes_and_server_letters_and_attributes_all_players(game, world):
    rows, _, transactions, awards = world
    move(game, world, "code", code="1001")
    assert rows[1]["mobilePlay"]["slots"] == ["A", None]
    with pytest.raises(mobile.PlayError) as error:
        move(game, world, "code", code="1001")
    assert error.value.code == "CodeAlreadyUsed"
    move(game, world, "code", code="1002")
    assert rows[1]["mobilePlay"]["result"] == "won"
    assert rows[1]["validated"] and rows[2]["validated"]
    assert rows[1]["mobilePlay"]["validatedIds"] == [1, 2]
    assert len(transactions) == 2
    assert awards.call_count == 2


def test_word_cannot_finish_without_token_owner(game, world):
    move(game, world, "code", code="1002")
    with pytest.raises(mobile.PlayError) as error:
        move(game, world, "code", code="1003")
    assert error.value.code == "MustIncludeOwnCode"
    assert not world[0][1].get("validated")


def test_expired_word_restarts_server_timer_and_discards_old_letters(game, world, monkeypatch):
    move(game, world, "code", code="1001")
    assert world[0][1]["mobilePlay"]["expiresAt"] == 115000
    monkeypatch.setattr(mobile, "now_ms", lambda: 116000)
    assert mobile.assignment(game, world[0][1])["play"]["phase"] == "expired"
    move(game, world, "code", code="1002")
    assert world[0][1]["mobilePlay"]["slots"] == ["A", None]
    assert world[0][1]["mobilePlay"]["playerIds"] == [2]
    assert world[0][1]["mobilePlay"]["expiresAt"] == 131000


def test_retry_returns_same_result_without_second_write(game, world):
    rows, db, transactions, _ = world
    body = move(game, world, "code", code="1001")
    mobile.act(db, game, rows[1], body)
    assert len(transactions) == 1
    body["requestId"] = "other-request"
    with pytest.raises(mobile.PlayError) as error:
        mobile.act(db, game, rows[1], body)
    assert error.value.code == "PlayChanged"


@pytest.mark.parametrize("extra", [{"score": 999}, {"success": True}, {"playerId": 2}, {"letter": "A"}])
def test_rejects_client_result_or_identity(game, world, extra):
    with pytest.raises(mobile.PlayError) as error:
        move(game, world, "code", code="1001", **extra)
    assert error.value.code == "InvalidPlayFields"
    assert not world[2]


@pytest.mark.parametrize("change", [{"isActive": False}, {"lifecycleState": "closing"}, {"archivedAt": "now"}])
def test_closed_games_cannot_accept_actions(game, world, change):
    game.update(change)
    with pytest.raises(mobile.PlayError) as error:
        move(game, world, "code", code="1001")
    assert error.value.code == "GameInactive"


def quiz_game(game):
    game.update(gameType="SEMAFORO", quizOrder=["q1", "q2"],
                quizQuestions={"q1": {"text": "Status", "options": [{"title": "Green", "payload": "green"}]},
                               "q2": {"text": "Coffee?", "options": [
                                   {"title": "Yes", "payload": "a"}, {"title": "No", "payload": "b"}]}})


def test_cross_quiz_hides_answers_alternates_and_checks_on_server(game, world):
    quiz_game(game)
    move(game, world, "pair", code="1002")
    public = mobile.assignment(game, world[0][1])
    assert public["play"]["turn"] == {"guesser": "Player 1", "target": "Player 2", "ready": False}
    assert "expected" not in json.dumps(public)
    assert "quizAnswers" not in json.dumps(public)
    assert "question" not in public["play"]["turn"]
    move(game, world, "ready")
    assert mobile.assignment(game, world[0][1])["play"]["turn"]["question"]["text"] == "Coffee?"
    move(game, world, "answer", answer="a")
    state = mobile.assignment(game, world[0][1])["play"]
    assert state["lastCorrect"] and state["strikes"] == 0
    assert state["turn"]["guesser"] == "Player 2"
    move(game, world, "ready")
    move(game, world, "answer", answer="b")
    assert world[0][1]["mobilePlay"]["phase"] == "finished"
    assert world[0][1]["mobilePlay"]["strikes"] == 1


def test_expired_cross_quiz_answer_is_wrong_and_resume_keeps_deadline(game, world, monkeypatch):
    quiz_game(game)
    move(game, world, "pair", code="1002")
    move(game, world, "ready")
    assert mobile.assignment(game, world[0][1])["play"]["expiresAt"] == 105000
    monkeypatch.setattr(mobile, "now_ms", lambda: 105000)
    move(game, world, "answer", answer="a")
    assert world[0][1]["mobilePlay"]["strikes"] == 1
    assert not world[0][1]["mobilePlay"]["lastCorrect"]


def test_cross_quiz_three_strikes_ends_without_extra_turn(game, world, monkeypatch):
    quiz_game(game)
    game["quizOrder"] += ["q3", "q4"]
    for qid in ("q3", "q4"):
        game["quizQuestions"][qid] = copy.deepcopy(game["quizQuestions"]["q2"])
        for row in world[0].values():
            row["type"]["SEMAFORO"]["quizAnswers"][qid] = "a"
    move(game, world, "pair", code="1002")
    for _ in range(3):
        move(game, world, "ready")
        move(game, world, "answer", answer="b")
    assert world[0][1]["mobilePlay"]["result"] == "lost"
    assert world[0][1]["mobilePlay"]["phase"] == "finished"
    with pytest.raises(mobile.PlayError):
        move(game, world, "ready")


def test_partner_pending_quiz_is_rejected_before_pair_write(game, world):
    quiz_game(game)
    game["formConfig"] = {**game_forms.DEFAULT, "quizMoment": "after_registration"}
    world[0][2]["type"]["SEMAFORO"]["quizCompleted"] = False
    with pytest.raises(game_forms.FormError):
        move(game, world, "pair", code="1002")
    assert not world[2]


def test_after_game_questionnaire_does_not_deadlock_pair_validation(game, world):
    quiz_game(game)
    game["formConfig"] = {**game_forms.DEFAULT, "quizMoment": "after_game"}
    for participant in world[0].values():
        participant["type"]["SEMAFORO"]["quizAnswers"] = {}
        participant["type"]["SEMAFORO"]["quizCompleted"] = False
    move(game, world, "pair", code="1002")
    assert world[0][1]["mobilePlay"]["phase"] == "finished"


def test_registration_questionnaire_hides_infocards_until_completed(game, world):
    game.update(gameType="INFOCARDS", formConfig={**game_forms.DEFAULT, "quizMoment": "after_registration"},
                type={"INFOCARDS": {"cards": [{"id": "secret", "title": "Private content"}]}})
    assert "cards" not in mobile.assignment(game, world[0][1])


def test_cupido_pair_is_distinct_and_has_no_unimplemented_matching_claim(game, world):
    game["gameType"] = "CUPIDO"
    with pytest.raises(mobile.PlayError) as error:
        move(game, world, "pair", code="1001")
    assert error.value.code == "SameCode"
    move(game, world, "pair", code="1002")
    assert world[0][1]["mobilePlay"]["result"] == "won"


def test_infocards_only_presigns_game_keys_and_strips_storage_fields(game, world, monkeypatch):
    game.update(gameType="INFOCARDS", type={"INFOCARDS": {"cards": [
        {"id": "second", "title": "Second", "order": 2, "imageKey": "infocards/FOREIGN/private"},
        {"id": "first", "title": "First", "order": 1, "imageKey": "infocards/ABC123/image"},
    ]}})
    monkeypatch.setenv("INFOCARDS_BUCKET", "cards")
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "signed"
    monkeypatch.setattr(mobile.boto3, "client", lambda *_: s3)
    cards = mobile.assignment(game, world[0][1])["cards"]
    assert [card["id"] for card in cards] == ["first", "second"]
    assert cards[0]["imageUrl"] == "signed"
    assert cards[1]["imageUrl"] is None
    assert "imageKey" not in json.dumps(cards)
    s3.generate_presigned_url.assert_called_once()


def test_transaction_guards_active_game_state_owner_and_all_player_validation(game, monkeypatch):
    db = MagicMock()
    monkeypatch.setattr(stack_prizes, "transact", MagicMock())
    own = {"playerId": 1, "mobilePlay": {"revision": 1, "phase": "idle"}}
    mobile._persist(db, game, own, own["mobilePlay"], {"revision": 2}, [own, {"playerId": 2}])
    ops = stack_prizes.transact.call_args.args[1]
    assert len(ops) == 3
    assert "isActive" in ops[0]["Update"]["ConditionExpression"]
    assert "deletedAt" in ops[0]["Update"]["ConditionExpression"]
    assert ops[0]["Update"]["ExpressionAttributeValues"][":count"] == 2
    assert "mobilePlay = :previous" in ops[1]["Update"]["ConditionExpression"]
    assert "validated = :no" in ops[1]["Update"]["ConditionExpression"]
    assert "validated = :no" in ops[2]["Update"]["ConditionExpression"]


def test_transaction_conflict_is_retryable(game, monkeypatch):
    monkeypatch.setattr(stack_prizes, "transact", MagicMock(side_effect=stack_prizes.PrizeError("StackPrizeConflict")))
    with pytest.raises(mobile.PlayError) as error:
        mobile._persist(MagicMock(), game, {"playerId": 1}, {"revision": 0}, {"revision": 1}, [])
    assert error.value.code == "PlayChanged"


@pytest.mark.parametrize("token_game", ["FOREIGN", None])
def test_public_play_requires_own_game_token(game, monkeypatch, token_game):
    token = issue_player_token(token_game, 1, SECRET) if token_game else "invalid"
    action = MagicMock()
    monkeypatch.setattr(public_mod.mobile_gameplay, "act", action)
    response = public_mod._handle_get_player({"httpMethod": "POST", "body": json.dumps({
        "gameId": "ABC123", "playerToken": token, "action": "play",
    })})
    assert response["statusCode"] == 403
    action.assert_not_called()


def test_public_play_executes_and_rereads_safe_assignment(game, world, monkeypatch):
    token = issue_player_token("ABC123", 1, SECRET)
    monkeypatch.setattr(public_mod, "_get_game", lambda *_: game)
    monkeypatch.setattr(public_mod, "_get_player", lambda *_: world[0][1])
    monkeypatch.setattr(public_mod, "dynamo_r", world[1])
    response = public_mod._handle_get_player({"httpMethod": "POST", "body": json.dumps({
        "gameId": "ABC123", "playerToken": token, "action": "play",
        "move": "code", "code": "1001", "requestId": "request-1", "revision": 0,
    })})
    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["assignment"]["gameType"] == "L3TRAS"
    assert payload["assignment"]["play"]["slots"] == ["A", None]
    assert "playerIds" not in payload["assignment"]["play"]
