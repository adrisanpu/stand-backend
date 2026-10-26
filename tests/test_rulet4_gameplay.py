import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from threading import Lock
from unittest.mock import MagicMock, patch

import pytest

from stand_common import rulet4_gameplay as roulette
from stand_common.utils import issue_player_token
from tests.test_public_web import assign_mod, public_mod, SECRET

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src" / "stand_prod_game_score"))
spec = importlib.util.spec_from_file_location(
    "rulet4_score", ROOT / "src" / "stand_prod_game_score" / "lambda_function.py",
)
score = importlib.util.module_from_spec(spec)
spec.loader.exec_module(score)

CONFIG = {"panels": [
    {"id": "a", "label": "A", "color": "#112233", "count": 2},
    {"id": "b", "label": "B", "color": "#445566", "count": 1},
]}
META = {
    "gameId": "ABC123", "gameType": "RULET4", "isActive": True,
    "ownerUserId": "owner", "type": {"RULET4": CONFIG},
    "participantFields": {"email": {"required": True}},
}
PLAYER = {
    "gameId": "ABC123", "playerId": 1, "username": "Player",
    "type": {"RULET4": {"raffleEligible": False}},
}


class ConditionalFailure(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class ResultTable:
    def __init__(self):
        self.player = deepcopy(PLAYER)
        self.lock = Lock()
        self.writes = 0

    def update_item(self, **kwargs):
        assert "attribute_not_exists(#type.#game.#result)" in kwargs["ConditionExpression"]
        with self.lock:
            blob = self.player["type"]["RULET4"]
            if "result" in blob:
                raise ConditionalFailure()
            blob["result"] = deepcopy(kwargs["ExpressionAttributeValues"][":result"])
            self.writes += 1

    def get_item(self, **kwargs):
        assert kwargs["ConsistentRead"] is True
        with self.lock:
            return {"Item": deepcopy(self.player)}


@pytest.mark.parametrize("index,label", [(0, "A"), (1, "A"), (2, "B")])
def test_server_selects_panel_by_weight_and_persists_snapshot(index, label, monkeypatch):
    table = ResultTable()
    def choose(limit):
        assert limit == 3
        return index
    monkeypatch.setattr(roulette.secrets, "randbelow", choose)
    result = roulette.spin(META, PLAYER, table)
    assert result["index"] == index
    assert result["panel"]["label"] == label
    assert result["configSnapshot"] == CONFIG
    assert result["spunAt"]
    assert table.player["type"]["RULET4"]["result"] == result
    assert table.player["type"]["RULET4"]["raffleEligible"] is False


def test_concurrent_spins_and_lost_response_retries_return_one_result(monkeypatch):
    table = ResultTable()
    monkeypatch.setattr(roulette, "ClientError", ConditionalFailure)
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: roulette.spin(META, PLAYER, table), range(32)))
    assert table.writes == 1
    assert all(result == results[0] for result in results)
    # A stored result survives configuration changes, including a now-invalid configuration.
    invalid_meta = {**META, "type": {"RULET4": {"panels": []}}}
    assert roulette.spin(invalid_meta, table.player, table) == results[0]


def test_storage_errors_are_not_reported_as_a_successful_spin(monkeypatch):
    table = MagicMock()
    table.update_item.side_effect = ConditionalFailure()
    table.get_item.return_value = {}
    monkeypatch.setattr(roulette, "ClientError", ConditionalFailure)
    with pytest.raises(ConditionalFailure):
        roulette.spin(META, PLAYER, table)


def test_nonconditional_storage_error_is_propagated(monkeypatch):
    class StorageFailure(ConditionalFailure):
        response = {"Error": {"Code": "ProvisionedThroughputExceededException"}}
    table = MagicMock()
    table.update_item.side_effect = StorageFailure()
    monkeypatch.setattr(roulette, "ClientError", ConditionalFailure)
    with pytest.raises(StorageFailure):
        roulette.spin(META, PLAYER, table)
    table.get_item.assert_not_called()


def event(body):
    return {
        "rawPath": "/v1/public/rulet4/spin",
        "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps(body),
    }


def spin_body(**extra):
    return {
        "gameId": "ABC123", "playerToken": issue_player_token("ABC123", 1, SECRET), **extra,
    }


def test_public_spin_and_recovery_expose_the_same_result(monkeypatch):
    table = ResultTable()
    monkeypatch.setattr(public_mod, "_get_game", lambda _: META)
    monkeypatch.setattr(public_mod, "_get_player", lambda *_: deepcopy(table.player))
    monkeypatch.setattr(public_mod, "gp_table", table)
    response = public_mod.lambda_handler(event(spin_body()), None)
    assert response["statusCode"] == 200
    result = json.loads(response["body"])["result"]
    response = public_mod._player_response(META, table.player, "ABC123", 1, "token")
    assignment = json.loads(response["body"])["assignment"]
    assert assignment["gameType"] == "RULET4"
    assert assignment["result"] == result
    assert "email" not in assignment
    assert "phone" not in assignment


@pytest.mark.parametrize("body,meta,player,status,error", [
    ([], META, PLAYER, 400, "InvalidJson"),
    (spin_body(index=0), META, PLAYER, 400, "InvalidRulet4Fields"),
    (spin_body(prize="A"), META, PLAYER, 400, "InvalidRulet4Fields"),
    (spin_body(gameId="bad"), META, PLAYER, 400, "InvalidGameId"),
    (spin_body(playerToken="invalid"), META, PLAYER, 403, "InvalidPlayerToken"),
    (spin_body(playerToken=issue_player_token("XYZ123", 1, SECRET)), META, PLAYER, 403, "InvalidPlayerToken"),
    (spin_body(), None, PLAYER, 404, "GameNotFound"),
    (spin_body(), {**META, "isActive": False}, PLAYER, 403, "GameInactive"),
    (spin_body(), {**META, "gameType": "T1MER"}, PLAYER, 400, "UnsupportedGameType"),
    (spin_body(), META, None, 404, "PlayerNotFound"),
    (spin_body(), {**META, "type": {"RULET4": {"panels": []}}}, PLAYER, 400, "InvalidRulet4Config"),
])
def test_public_spin_rejects_invalid_requests_without_writes(body, meta, player, status, error, monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(public_mod, "_get_game", lambda _: meta)
    monkeypatch.setattr(public_mod, "_get_player", lambda *_: player)
    monkeypatch.setattr(public_mod, "gp_table", table)
    response = public_mod.lambda_handler(event(body), None)
    assert response["statusCode"] == status
    assert json.loads(response["body"])["error"] == error
    table.update_item.assert_not_called()


def test_public_metadata_exposes_rulet4_config_not_private_fields(monkeypatch):
    monkeypatch.setattr(public_mod, "_get_game", lambda _: {**META, "ownerUserId": None})
    response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    payload = json.loads(response["body"])
    assert payload["rulet4Config"] == CONFIG
    assert "ownerUserId" not in payload


@pytest.mark.parametrize("owner,status", [("owner", 200), ("someone-else", 403), (None, 403)])
def test_operator_uses_same_result_and_requires_ownership(owner, status, monkeypatch):
    table = ResultTable()
    monkeypatch.setattr(score, "_get_game_meta", lambda _: META)
    monkeypatch.setattr(score, "gp_table", table)
    operator_event = {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": owner}}}},
        "body": json.dumps({"gameId": "ABC123", "playerId": "1"}),
    }
    response = score._handle_post(operator_event)
    assert response["statusCode"] == status
    assert table.writes == (1 if status == 200 else 0)
    if status == 200:
        monkeypatch.setattr(public_mod, "_get_game", lambda _: META)
        monkeypatch.setattr(public_mod, "_get_player", lambda *_: deepcopy(table.player))
        monkeypatch.setattr(public_mod, "gp_table", table)
        replay = public_mod.lambda_handler(event(spin_body()), None)
        assert json.loads(replay["body"])["result"] == json.loads(response["body"])["result"]
        assert table.writes == 1


@pytest.mark.parametrize("extra,error", [
    ({"playerId": True}, "InvalidPlayerId"),
    ({"playerId": 1.5}, "InvalidPlayerId"),
    ({"playerId": -1}, "InvalidPlayerId"),
    ({"playerId": None}, "InvalidPlayerId"),
    ({"playerId": "not-a-number"}, "InvalidPlayerId"),
    ({"playerId": "1" * 5000}, "InvalidPlayerId"),
    ({"index": 0}, "InvalidRulet4Fields"),
    ({"prize": "A"}, "InvalidRulet4Fields"),
])
def test_operator_rejects_client_selected_results_and_invalid_ids(extra, error, monkeypatch):
    monkeypatch.setattr(score, "_get_game_meta", lambda _: META)
    table = MagicMock()
    monkeypatch.setattr(score, "gp_table", table)
    operator_event = {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "body": json.dumps({"gameId": "ABC123", "playerId": 1, **extra}),
    }
    response = score._handle_post(operator_event)
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == error
    table.get_item.assert_not_called()
    table.update_item.assert_not_called()


def test_web_assign_creates_rulet4_participant_with_verified_email(verified_web_assignment):
    meta = META
    with patch.object(assign_mod, "_get_game_meta", return_value=meta), \
         patch.object(assign_mod, "_find_existing_player_by_email", return_value=None), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=True), \
         patch.object(assign_mod, "_get_last_player_id", return_value=0), \
         patch.object(assign_mod, "_put_player") as put:
        response = assign_mod._handle_web_assign({
            "source": "web", "game_id": "ABC123", "username": "Player", "email": "player@club.test",
        })
    assert response["ok"] is True
    assert response["assignment"]["gameType"] == "RULET4"
    assert response["assignment"]["result"] is None
    assert put.call_args.args[0]["type"]["RULET4"]["raffleEligible"] is False


def test_rulet4_registration_requires_email_even_with_legacy_disabled_setting():
    meta = {**META, "participantFields": {"email": {"required": False}}}
    with patch.object(assign_mod, "_get_game_meta", return_value=meta), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=False), \
         patch.object(assign_mod, "_put_player") as put:
        response = assign_mod._handle_web_assign({"game_id": "ABC123", "username": "Player"})
    assert response["reason"] == "missing_email"
    put.assert_not_called()


@pytest.mark.parametrize("meta,reason", [
    ({**META, "isActive": False}, "game_inactive"),
    (META, "player_limit_reached"),
])
def test_rulet4_registration_respects_game_and_capacity(meta, reason, verified_web_assignment):
    with patch.object(assign_mod, "_get_game_meta", return_value=meta), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_find_existing_player_by_email", return_value=None), \
         patch.object(assign_mod, "_put_verified_player", return_value="full"), \
         patch.object(assign_mod, "_put_player") as put:
        response = assign_mod._handle_web_assign({"game_id": "ABC123", "username": "Player", "email": "player@club.test"})
    assert response["reason"] == reason
    put.assert_not_called()
