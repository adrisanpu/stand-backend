"""STACK CRUD, web join and public-to-dispatcher contracts, entirely offline."""
import importlib.util
import io
import json
import sys
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from stand_common.stack import DEFAULT_CONFIG, game_config, validate_config
from stand_common.utils import _json_sanitize, default_participant_fields, issue_player_token
from tests.test_public_web import assign_mod, public_mod, SECRET, _score_event
from tests.test_stack_score import AttemptTable, ConditionalFailure, HISTORY, ranked_player

ROOT = Path(__file__).parent.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


game = load("stack_crud", ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py")
sys.path.insert(0, str(ROOT / "src" / "stand_prod_game_score"))
dispatcher = load("stack_dispatcher", ROOT / "src" / "stand_prod_game_score" / "lambda_function.py")
META = {
    "gameId": "ABCDEF", "gameType": "STACK", "gameName": "Stack event",
    "ownerUserId": "owner", "isActive": True, "type": {"STACK": deepcopy(DEFAULT_CONFIG)},
}


def operator_event(body, **query):
    return {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "body": json.dumps(body),
        "queryStringParameters": query,
    }


@pytest.mark.parametrize("config", [
    {"initialSpeed": float("nan")}, {"initialSpeed": float("inf")},
    {"initialSpeed": True}, {"initialSpeed": "2"}, {"initialSpeed": None},
    {"initialSpeed": 0}, {"initialSpeed": 6}, {"speedIncrement": -0.01},
    {"maxSpeed": 11}, {"perfectTolerance": 0.26}, {"initialWidth": 0},
    {"initialDepth": 0}, {"layerHeight": 0}, {"movementRange": 3},
    {"version": 1}, {"version": "2"}, {"unknown": 1}, [],
    {"configVersion": "1"}, {"initialWidth": 3}, {"initialDepth": 2},
    {"layerHeight": 0.24}, {"movementRange": 4}, {"skin": "donut"}, {"skin": None}, {"skin": 1},
])
def test_config_rejects_invalid_fields(config):
    with pytest.raises(ValueError):
        validate_config(config)


def test_config_skin_is_editable_and_legacy_configs_default_to_blocks():
    assert validate_config({"skin": "baklava"})["skin"] == "baklava"
    legacy = {key: value for key, value in DEFAULT_CONFIG.items() if key != "skin"}
    assert game_config({"type": {"STACK": legacy}})["skin"] == "blocks"
    assert validate_config({"initialSpeed": 3}, base=legacy)["skin"] == "blocks"


def test_config_defaults_and_partial_merge_are_dynamodb_safe():
    assert validate_config() == DEFAULT_CONFIG
    config = validate_config({"initialSpeed": 3}, base=validate_config({"maxSpeed": 6}))
    assert config["initialSpeed"] == Decimal("3")
    assert config["maxSpeed"] == Decimal("6")
    assert config["perfectTolerance"] == Decimal("0.06")


def test_crud_create_stack_web_and_get_settings():
    table = MagicMock()
    with patch.object(game, "games_table", table), \
         patch.object(game, "_get_user", return_value={}), \
         patch.object(game, "_is_user_pro", return_value=True), \
         patch.object(game, "_new_join_code", return_value="ABCDEF"), \
         patch.object(game, "_load_default_quiz_from_catalog", return_value=(None, None)):
        response = game._handle_post(operator_event({"gameType": "STACK"}))
        assert response["statusCode"] == 201
        item = table.put_item.call_args.kwargs["Item"]
        assert item["type"]["STACK"] == DEFAULT_CONFIG
        assert item["joinChannel"] == "web"
        assert item["playMode"] == "mobile"
        assert item["participantFields"] == default_participant_fields("STACK")
        assert item["requiresValidation"] is False
        table.get_item.return_value = {"Item": item}
        for query in ({"gameId": "ABCDEF"}, {"gameName": "Stack event"}):
            table.query.return_value = {"Items": [item]}
            result = game._handle_get(operator_event({}, **query))
            assert result["statusCode"] == 200
            assert json.loads(result["body"])["game"]["type"]["STACK"] == _json_sanitize(DEFAULT_CONFIG)
            assert "settings" not in json.loads(result["body"])


def test_create_custom_config_and_reject_invalid_without_writes():
    table = MagicMock()
    with patch.object(game, "games_table", table), \
         patch.object(game, "_get_user", return_value={}), \
         patch.object(game, "_is_user_pro", return_value=True), \
         patch.object(game, "_new_join_code", return_value="ABCDEF"), \
         patch.object(game, "_load_default_quiz_from_catalog", return_value=(None, None)):
        result = game._handle_post(operator_event({
            "gameType": "STACK", "settings": {"stack": {"initialSpeed": 3}},
        }))
        assert result["statusCode"] == 201
        assert table.put_item.call_args.kwargs["Item"]["type"]["STACK"]["initialSpeed"] == 3
        table.reset_mock()
        for invalid in ({"settings": {"stack": {"initialSpeed": 8}}}, {"playMode": "master"},
                        {"joinChannel": "email"}):
            result = game._handle_post(operator_event({"gameType": "STACK", **invalid}))
            assert result["statusCode"] == 400
        table.put_item.assert_not_called()


def test_put_merges_valid_settings_using_existing_blob_helper():
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    with patch.object(game, "games_table", table):
        result = game._handle_put(operator_event({
            "gameId": "ABCDEF", "settings": {"stack": {"initialSpeed": 3}},
        }))
    assert result["statusCode"] == 200
    assert table.update_item.call_count == 2
    write = table.update_item.call_args.kwargs
    assert write["ExpressionAttributeValues"][":payload"] == {
        **DEFAULT_CONFIG, "initialSpeed": Decimal("3"),
    }
    assert table.update_item.call_args_list[0].kwargs["ConditionExpression"] == (
        "attribute_exists(gameId) AND ownerUserId = :owner"
    )


@pytest.mark.parametrize("meta,updates,status", [
    (META, {"settings": {"stack": {"initialSpeed": float("nan")}}, "isActive": False}, 400),
    (META, {"settings": {"stack": []}}, 400),
    (META, {"playMode": "master"}, 400),
    (META, {"joinChannel": "instagram", "playMode": "master"}, 400),
    ({**META, "ownerUserId": "other"}, {"settings": {"stack": {"initialSpeed": 3}}}, 403),
    ({**META, "gameType": "T1MER"}, {"settings": {"stack": {"initialSpeed": 3}}}, 400),
])
def test_put_rejects_before_any_write(meta, updates, status):
    table = MagicMock()
    table.get_item.return_value = {"Item": meta}
    with patch.object(game, "games_table", table):
        assert game._handle_put(operator_event({"gameId": "ABCDEF", **updates}))["statusCode"] == status
    table.update_item.assert_not_called()


def test_web_join_creates_identity_but_no_attempt_and_dedup_recovers(verified_web_assignment):
    captured = {}

    def put(player):
        captured.update(deepcopy(player))

    with patch.object(assign_mod, "_get_game_meta", return_value=META), \
         patch.object(assign_mod, "_find_existing_player_by_email", return_value=None), \
         patch.object(assign_mod, "_put_player", side_effect=put), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=True), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_get_last_player_id", return_value=0), \
         patch.object(assign_mod, "PLAYER_TOKEN_SECRET", SECRET):
        result = assign_mod._handle_web_assign({
            "game_id": "ABCDEF", "email": "ana@example.com", "source": "web",
            "username": "Ana", "phone": "+34 612345678",
        })
    assert result["ok"] is True
    assert result["assignment"] == {
        "gameType": "STACK", "playerId": 1, "displayName": "Ana", "attempt": None,
    }
    assert captured["type"]["STACK"]
    assert "attempt" not in captured["type"]["STACK"]
    assert "score" not in captured["type"]["STACK"]
    assert captured["type"]["STACK"]["raffleEligible"] is False
    captured["type"]["STACK"]["attempt"] = {
        "attemptId": "old-start", "status": "started", "startedAt": "2026-10-16T00:00:00Z",
        "configVersion": "1", "configSnapshot": deepcopy(DEFAULT_CONFIG),
    }
    with patch.object(assign_mod, "_get_game_meta", return_value=META), \
         patch.object(assign_mod, "_find_existing_player_by_email", return_value=captured), \
         patch.object(assign_mod, "_put_player") as put_mock:
        recovered = assign_mod._handle_web_assign({
            "game_id": "ABCDEF", "email": "ana@example.com", "username": "Different registration name",
            "phone": "+34 612345678",
        })
    assert recovered["already_joined"] is True
    assert recovered["assignment"]["attempt"]["attemptId"] == "old-start"
    assert recovered["assignment"]["displayName"] == "Ana"
    put_mock.assert_not_called()


def test_public_join_forwards_stack_assignment():
    result = {"ok": True, "gameType": "STACK", "playerToken": "signed",
              "assignment": {"playerId": 7, "attempt": None}}
    with patch.object(public_mod, "ASSIGN_LAMBDA", "assign"), \
         patch.object(public_mod, "_get_game", return_value=META), \
         patch.object(public_mod, "verification_table"), \
         patch.object(public_mod, "confirm_code", return_value=({
             "registration": {"email": "ana@example.com", "username": "Ana", "phone": "+34 612345678"},
         }, "receipt")), \
         patch.object(public_mod, "_invoke_assign_web", return_value=result):
        response = public_mod._handle_join({"body": json.dumps({
            "gameId": "ABCDEF", "email": "ana@example.com", "consent": True,
        })})
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["assignment"] == result["assignment"]


@patch.object(public_mod, "_client_appearance", return_value={})
def test_public_game_preview_returns_stack_config_without_writing(_branding):
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    gp = MagicMock()
    gp.query.return_value = {"Items": []}
    with patch.object(public_mod, "games_table", table), patch.object(public_mod, "gp_table", gp):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABCDEF"}})
    result = json.loads(response["body"])
    assert result["stackConfig"] == _json_sanitize(DEFAULT_CONFIG)
    assert result["ranking"] == []
    assert result["stackStartSnapshot"] is True
    assert result["stackFinishSupported"] is True
    assert result["joinUrl"] == "/play/ABCDEF"
    table.update_item.assert_not_called()


@pytest.mark.parametrize("field", [field for field in DEFAULT_CONFIG if field != "skin"])
def test_missing_persisted_config_is_rejected_without_silent_defaults(field):
    incomplete = deepcopy(DEFAULT_CONFIG)
    incomplete.pop(field)
    meta = {**META, "type": {"STACK": incomplete}}
    with pytest.raises(ValueError):
        game_config(meta)
    with patch.object(public_mod, "_get_game", return_value=meta):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABCDEF"}})
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "InvalidStackConfig"


def test_public_score_dispatcher_and_recover_end_to_end_offline(monkeypatch, verified_web_assignment):
    table = AttemptTable()
    monkeypatch.setattr(dispatcher.stack, "ClientError", ConditionalFailure)
    token = issue_player_token("ABCDEF", 7, SECRET)
    mock_games = MagicMock()
    mock_games.get_item.return_value = {"Item": META}

    def invoke(**kwargs):
        event = json.loads(kwargs["Payload"].decode())
        response = dispatcher.lambda_handler(event, None)
        return {"Payload": io.BytesIO(json.dumps(response).encode())}

    client = MagicMock()
    client.invoke.side_effect = invoke
    with patch.object(public_mod, "SCORE_LAMBDA", "score"), \
         patch.object(public_mod, "games_table", mock_games), \
         patch.object(public_mod, "gp_table", table), \
         patch.object(public_mod, "lambda_client", client), \
         patch.object(dispatcher, "games_table", mock_games), \
         patch.object(dispatcher, "gp_table", table):
        body = {"gameId": "ABCDEF", "playerToken": token, "action": "start", "attemptId": "request-1"}
        response = public_mod.lambda_handler(_score_event(body), None)
        assert response["statusCode"] == 200
        assert json.loads(response["body"]) == {
            "ok": True, "gameId": "ABCDEF", "playerId": 7, "attemptId": "request-1",
            "status": "started", "configVersion": "1",
            "startedAt": table.player["type"]["STACK"]["attempt"]["startedAt"],
            "configSnapshot": _json_sanitize(DEFAULT_CONFIG),
        }
        assert public_mod._handle_score(_score_event(body)) == response
        assert public_mod._handle_score(_score_event({**body, "attemptId": "request-2"}))["statusCode"] == 409
        assert public_mod._handle_score(_score_event({**body, "action": "finish", "placements": []}))["statusCode"] == 400
        before = deepcopy(table.player)
        recovered = public_mod._handle_get_player({
            "queryStringParameters": {"gameId": "ABCDEF", "playerToken": token},
        })
        assert json.loads(recovered["body"])["assignment"]["attempt"]["status"] == "started"
        assert table.player == before
        assert table.writes == 1
        assert "score" not in table.player["type"]["STACK"]
        table.player["type"]["STACK"]["attempt"]["startedAt"] = "2026-01-01T00:00:00Z"
        finish_body = {**body, "action": "finish", "placements": HISTORY}
        completed = public_mod._handle_score(_score_event(finish_body))
        assert completed["statusCode"] == 200
        result = json.loads(completed["body"])
        assert result["score"] == 1 and result["rank"] == 1 and result["status"] == "finished"
        assert public_mod._handle_score(_score_event(finish_body)) == completed
        assert public_mod._handle_score(_score_event(body)) == completed
        forwarded = json.loads(json.loads(client.invoke.call_args.kwargs["Payload"])["body"])
        assert forwarded["playerId"] == 7 and "playerToken" not in forwarded
        recovered = json.loads(public_mod._handle_get_player({
            "queryStringParameters": {"gameId": "ABCDEF", "playerToken": token},
        })["body"])["assignment"]
        assert recovered["score"] == 1 and recovered["rank"] == 1
        assert recovered["attempt"]["status"] == "finished"
        assert recovered["attempt"]["finishedAt"] == result["finishedAt"]
        with patch.object(assign_mod, "_get_game_meta", return_value=META), \
             patch.object(assign_mod, "_find_existing_player_by_email", return_value=table.player), \
             patch.object(assign_mod, "gp_table", table), \
             patch.object(assign_mod, "_put_player") as put:
            rejoin = assign_mod._handle_web_assign({
                "game_id": "ABCDEF", "email": "ana@example.com", "username": "Ana", "phone": "+34 612345678",
            })
        assert rejoin["assignment"] == recovered
        put.assert_not_called()
        assert table.writes == 2


@pytest.mark.parametrize("change,meta,player,error,status", [
    ({"playerToken": "bad"}, META, {"playerId": 7}, "InvalidPlayerToken", 403),
    ({"playerToken": issue_player_token("OTHERX", 7, SECRET)}, META, {"playerId": 7}, "InvalidPlayerToken", 403),
    ({}, {**META, "isActive": False}, {"playerId": 7}, "GameInactive", 403),
    ({}, None, {"playerId": 7}, "GameNotFound", 404),
    ({}, META, None, "PlayerNotFound", 404),
    ({"score": 100}, META, {"playerId": 7}, "InvalidStackFields", 400),
    ({"playerId": 8}, META, {"playerId": 7}, "InvalidStackFields", 400),
])
def test_public_start_rejects_invalid_context_without_invoking(change, meta, player, error, status):
    token = issue_player_token("ABCDEF", 7, SECRET)
    with patch.object(public_mod, "SCORE_LAMBDA", "score"), \
         patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "_get_player", return_value=player), \
         patch.object(public_mod, "_invoke_score_payload") as invoke:
        result = public_mod._handle_score(_score_event({
            "gameId": "ABCDEF", "playerToken": token, "action": "start", "attemptId": "request-1", **change,
        }))
    assert result["statusCode"] == status
    assert json.loads(result["body"])["error"] == error
    invoke.assert_not_called()


def test_t1mer_score_invoke_preserves_payload_without_action():
    client = MagicMock()
    client.invoke.return_value = {
        "Payload": io.BytesIO(json.dumps({
            "statusCode": 200, "body": json.dumps({"ok": True, "rank": 1, "score": 50}),
        }).encode()),
    }
    with patch.object(public_mod, "lambda_client", client), \
         patch.object(public_mod, "SCORE_LAMBDA", "score"):
        result = public_mod._invoke_score_store("ABCDEF", 7, "10.050", 50)
    assert result == {"ok": True, "rank": 1, "score": 50}
    event = json.loads(client.invoke.call_args.kwargs["Payload"])
    assert json.loads(event["body"]) == {
        "gameId": "ABCDEF", "playerId": 7, "timer": "10.050", "score": 50,
    }


def test_t1mer_dispatcher_keeps_score_and_ranking_handlers():
    table = MagicMock()
    table.get_item.return_value = {"Item": {**META, "gameType": "T1MER"}}
    gp = MagicMock()
    payload = {"gameId": "ABCDEF", "playerId": 7, "timer": "10.050", "score": 50}
    with patch.object(dispatcher, "games_table", table), \
         patch.object(dispatcher, "gp_table", gp), \
         patch.object(dispatcher.t1mer, "store_score", return_value={"ok": True, "rank": 1}) as store, \
         patch.object(dispatcher.t1mer, "get_ranking", return_value={"ok": True, "results": []}) as ranking, \
         patch.object(dispatcher.stack, "store_score") as stack_store:
        result = dispatcher._handle_post({"body": json.dumps(payload)})
        assert result["statusCode"] == 200
        assert store.call_args.kwargs["payload"] == payload
        result = dispatcher._handle_get({"queryStringParameters": {"gameId": "ABCDEF"}})
        assert result["statusCode"] == 200
        assert ranking.call_args.kwargs["game_type_upper"] == "T1MER"
        stack_store.assert_not_called()


def test_stack_empty_ranking_queries_without_inventing_scores():
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    gp = MagicMock()
    gp.query.return_value = {"Items": []}
    with patch.object(dispatcher, "games_table", table), \
         patch.object(dispatcher, "gp_table", gp), \
         patch.object(dispatcher.t1mer, "get_ranking") as timer_ranking:
        response = dispatcher.lambda_handler({
            "requestContext": {"http": {"method": "GET"}},
            "queryStringParameters": {"gameId": "ABCDEF"},
        }, None)
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"ok": True, "results": []}
    gp.query.assert_called_once()
    gp.update_item.assert_not_called()
    timer_ranking.assert_not_called()


@patch.object(public_mod, "_client_appearance", return_value={})
def test_public_and_operator_ranking_share_order_and_schema(_branding):
    games = MagicMock()
    games.get_item.return_value = {"Item": META}
    gp = MagicMock()
    gp.query.return_value = {"Items": [
        ranked_player(2, 4), ranked_player(1, 9, username="winner"),
    ]}
    with patch.object(public_mod, "games_table", games), patch.object(public_mod, "gp_table", gp), \
         patch.object(dispatcher, "games_table", games), patch.object(dispatcher, "gp_table", gp):
        public = json.loads(public_mod._handle_get_game({
            "queryStringParameters": {"gameId": "ABCDEF"},
        })["body"])
        operator = json.loads(dispatcher._handle_get({
            "queryStringParameters": {"gameId": "ABCDEF"},
        })["body"])
    assert public["ranking"] == operator["results"]
    assert [entry["playerId"] for entry in operator["results"]] == [1, 2]


@pytest.mark.parametrize("extra", [{"score": 100}, {"playerId": 9}, {"timer": 1}, {"configSnapshot": {}}])
def test_public_finish_strict_envelope_does_not_invoke(extra):
    token = issue_player_token("ABCDEF", 7, SECRET)
    with patch.object(public_mod, "SCORE_LAMBDA", "score"), \
         patch.object(public_mod, "_get_game", return_value=META), \
         patch.object(public_mod, "_get_player", return_value={"playerId": 7}), \
         patch.object(public_mod, "_invoke_score_payload") as invoke:
        result = public_mod._handle_score(_score_event({
            "gameId": "ABCDEF", "playerToken": token, "action": "finish",
            "attemptId": "request-1", "placements": HISTORY, **extra,
        }))
    assert result["statusCode"] == 400
    invoke.assert_not_called()
