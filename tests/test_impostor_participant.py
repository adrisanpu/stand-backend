"""Impostor delivers each participant's role and word through the Web player session."""
import json
from unittest.mock import patch

import pytest

from stand_common.utils import issue_player_token
from tests.test_public_web import public_mod, SECRET


def _meta(**blob):
    return {"gameId": "G1", "gameType": "IMPOSTOR", "isActive": True, "type": {"IMPOSTOR": blob}}


PLAYERS = {
    1: {"gameId": "G1", "playerId": 1, "username": "Ana", "validated": True, "validationCode": 1111},
    2: {"gameId": "G1", "playerId": 2, "username": "Bea", "validated": True, "validationCode": 2222},
    3: {"gameId": "G1", "playerId": 3, "username": "Carla", "validated": True, "validationCode": 3333},
    4: {"gameId": "G1", "playerId": 4, "username": "Late", "validated": True, "validationCode": 4444},
}

ROUND = {
    "phase": "word_round", "round": 1, "selectedWord": "playa", "impostorPlayerId": 2,
    "turnOrder": [3, 1, 2], "wordRoundTurnIndex": 1,
}


def _get_player_state(meta, player_id):
    token = issue_player_token("G1", player_id, SECRET)
    with patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "_get_player", side_effect=lambda _g, pid: PLAYERS.get(int(pid))):
        response = public_mod._handle_get_player({
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"gameId": "G1", "playerToken": token}),
        })
    assert response["statusCode"] == 200
    return json.loads(response["body"])["assignment"]


def test_lobby_shows_code_without_role():
    assignment = _get_player_state(_meta(phase="lobby"), 1)
    assert assignment["validationCode"] == 1111
    assert assignment["displayName"] == "Ana"
    assert assignment["impostor"]["role"] is None
    assert assignment["impostor"]["word"] is None
    assert assignment["impostor"]["inRound"] is False


def test_crew_receives_word_and_current_turn():
    state = _get_player_state(_meta(**ROUND), 1)["impostor"]
    assert state["role"] == "crew"
    assert state["word"] == "playa"
    assert state["currentTurn"] == {"isMine": True, "name": "Ana"}


def test_impostor_never_receives_word():
    state = _get_player_state(_meta(**ROUND), 2)["impostor"]
    assert state["role"] == "impostor"
    assert state["word"] is None
    assert state["currentTurn"] == {"isMine": False, "name": "Ana"}


def test_player_outside_turn_order_waits_for_next_round():
    state = _get_player_state(_meta(**ROUND), 4)["impostor"]
    assert state["inRound"] is False
    assert state["role"] is None
    assert state["word"] is None


@pytest.mark.parametrize("player_id,winner,won", [(1, "crew", True), (2, "crew", False), (2, "impostor", True)])
def test_result_reports_whether_player_won(player_id, winner, won):
    state = _get_player_state(_meta(**{**ROUND, "phase": "result", "winner": winner}), player_id)["impostor"]
    assert state["winner"] == winner
    assert state["won"] is won
    assert state["currentTurn"] is None
