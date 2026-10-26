"""RULET4 configuration and owner CRUD contracts, without AWS calls."""
import importlib.util
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from stand_common.rulet4 import DEFAULT_CONFIG, game_config, validate_config


spec = importlib.util.spec_from_file_location(
    "rulet4_game_crud", Path(__file__).parent.parent / "src" / "stand_prod_game_fn_game" / "lambda_function.py",
)
game = importlib.util.module_from_spec(spec)
spec.loader.exec_module(game)
META = {"gameId": "ABCDEF", "gameType": "RULET4", "ownerUserId": "owner", "isActive": True}


def event(body=None, **query):
    return {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "body": json.dumps(body or {}), "queryStringParameters": query,
    }


def config(**panel):
    return {"panels": [{"id": "prize", "label": " Prize ", "count": 2, **panel}]}


def test_absent_defaults_are_independent_and_edits_normalize():
    first = game_config(META)
    first["panels"].clear()
    assert game_config({**META, "type": {}}) == DEFAULT_CONFIG
    assert validate_config(config(count=Decimal("3"))) == {
        "panels": [{"id": "prize", "label": "Prize", "color": "#DC2626", "count": 3}],
    }
    assert validate_config({}, base=config()) == validate_config(config())
    with pytest.raises(ValueError):
        validate_config({})


@pytest.mark.parametrize("invalid", [
    None, {}, [], {"panels": []}, {"panels": "bad"}, {"panels": [None]},
    config(count=True), config(count=0), config(count=-1), config(count=1.5),
    config(count="2"), config(count=361), config(count=Decimal("NaN")),
    config(count=Decimal("Infinity")), config(label=" "), config(label="a" * 81),
    config(id=""), config(id="bad id"), config(id="a" * 65),
    config(color="red"), config(color=None), config(unknown=1),
    {"panels": config()["panels"] * 2},
    {"panels": [{"id": str(i), "label": "a", "count": 1} for i in range(33)]},
    {"panels": [{"id": "a", "label": "a", "count": 360}, {"id": "b", "label": "b", "count": 1}]},
])
def test_malformed_saved_config_never_defaults(invalid):
    with pytest.raises(ValueError):
        game_config({**META, "type": {"RULET4": invalid}})


@pytest.mark.parametrize("invalid_type", [None, [], "bad"])
def test_malformed_type_is_explicitly_rejected(invalid_type):
    with pytest.raises(ValueError):
        game_config({**META, "type": invalid_type})


def test_owner_create_persists_defaults_and_custom_settings():
    table = MagicMock()
    with patch.object(game, "games_table", table), \
         patch.object(game, "_get_user", return_value={}), \
         patch.object(game, "_is_user_pro", return_value=True), \
         patch.object(game, "_new_join_code", return_value="ABCDEF"), \
         patch.object(game, "_load_default_quiz_from_catalog", return_value=(None, None)):
        for settings in ({}, {"rulet4": config()}):
            response = game._handle_post(event({"gameType": "RULET4", "settings": settings}))
            assert response["statusCode"] == 201
            assert table.put_item.call_args.kwargs["Item"]["type"]["RULET4"] == validate_config(settings.get("rulet4"))
        table.reset_mock()
        for invalid in ({}, None, [], config(count=0)):
            assert game._handle_post(event({"gameType": "RULET4", "settings": {"rulet4": invalid}}))["statusCode"] == 400
        table.put_item.assert_not_called()


@pytest.mark.parametrize("stored", [False, True])
def test_read_returns_config_and_explicit_persistence_marker(stored):
    item = {**META, **({"type": {"RULET4": config()}} if stored else {})}
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    table.query.return_value = {"Items": [item]}
    with patch.object(game, "games_table", table):
        for query in ({"gameId": "ABCDEF"}, {"gameName": "Roulette"}):
            response = game._handle_get(event(**query))
            assert response["statusCode"] == 200
            result = json.loads(response["body"])["game"]
            assert result["type"]["RULET4"] == game_config(item)
            assert result["rulet4ConfigStored"] is stored


@pytest.mark.parametrize("item", [META, {**META, "type": {"RULET4": deepcopy(DEFAULT_CONFIG), "other": {}}}])
def test_update_is_one_conditional_atomic_config_write(item):
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table):
        response = game._handle_put(event({"gameId": "ABCDEF", "settings": {"rulet4": config()}}))
    assert response["statusCode"] == 200
    table.update_item.assert_called_once()
    write = table.update_item.call_args.kwargs
    assert "ownerUserId = :owner" in write["ConditionExpression"]
    assert "gameType = :gameType" in write["ConditionExpression"]
    assert write["ExpressionAttributeValues"][":config"] == (
        validate_config(config()) if "type" in item else {"RULET4": validate_config(config())}
    )


@pytest.mark.parametrize("item,settings,status", [
    (META, {"rulet4": []}, 400),
    (META, {"rulet4": None}, 400),
    (META, {"rulet4": config(count=0)}, 400),
    ({**META, "ownerUserId": "other"}, {"rulet4": config()}, 403),
    ({**META, "gameType": "STACK"}, {"rulet4": config()}, 400),
    ({**META, "type": {"RULET4": {}}}, {"rulet4": config()}, 400),
])
def test_invalid_updates_never_write(item, settings, status):
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table):
        assert game._handle_put(event({"gameId": "ABCDEF", "settings": settings, "isActive": False}))["statusCode"] == status
    table.update_item.assert_not_called()


def test_corrupt_saved_config_read_returns_error_not_default():
    table = MagicMock()
    table.get_item.return_value = {"Item": {**META, "type": {"RULET4": {}}}}
    with patch.object(game, "games_table", table):
        response = game.lambda_handler({**event(gameId="ABCDEF"), "httpMethod": "GET"}, None)
    assert response["statusCode"] == 500
    assert "RULET4 configuration" in json.loads(response["body"])["detail"]
