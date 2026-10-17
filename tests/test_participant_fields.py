"""Configurable /play registration fields, persistence and API contracts."""
import importlib.util
import io
import itertools
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from stand_common.utils import default_participant_fields, normalize_participant_fields
from tests.test_public_web import assign_mod, public_mod

ROOT = Path(__file__).parent.parent
spec = importlib.util.spec_from_file_location(
    "participant_game", ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py",
)
game = importlib.util.module_from_spec(spec)
spec.loader.exec_module(game)

WEB_TYPES = ("EMPAREJA2", "T1MER", "STACK")
FIELDS = ("email", "instagram", "phone")
CONFIGS = [
    dict(zip(FIELDS, ({"required": enabled} for enabled in flags)))
    for flags in itertools.product((False, True), repeat=3)
]
VALUES = {"email": " Ana@Example.COM ", "instagram": " @ana ", "phone": " +34 612 345 678 "}
STORED = {"email": "ana@example.com", "instagramUsername": "@ana", "phone": "+34 612 345 678"}


def operator_event(body):
    return {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "body": json.dumps(body),
    }


@pytest.fixture
def web(monkeypatch, verified_web_assignment):
    meta = {"gameId": "ABCDEF", "gameType": "T1MER", "isActive": True, "joinChannel": "web"}
    put, find, reserve = MagicMock(), MagicMock(return_value=None), MagicMock(return_value=True)
    monkeypatch.setattr(assign_mod, "_get_game_meta", lambda _: meta)
    monkeypatch.setattr(assign_mod, "_put_player", put)
    monkeypatch.setattr(assign_mod, "_find_existing_player_by_email", find)
    monkeypatch.setattr(assign_mod, "_reserve_player_slot", reserve)
    monkeypatch.setattr(assign_mod, "_ensure_game_counters", MagicMock())
    monkeypatch.setattr(assign_mod, "_get_last_player_id", lambda _: 0)
    monkeypatch.setattr(assign_mod, "ASSIGNERS", {})
    return meta, put, find, reserve


def assign_body(values):
    return {
        "source": "web", "game_id": "ABCDEF",
        **{("instagram_username" if name == "instagram" else name): value for name, value in values.items()},
    }


@pytest.mark.parametrize("gt", WEB_TYPES)
def test_defaults_and_legacy_partial_config(gt):
    defaults = {"email": {"required": True}, "instagram": {"required": False}, "phone": {"required": False}}
    assert default_participant_fields(gt) == defaults
    assert normalize_participant_fields(gt, {}) == defaults
    assert normalize_participant_fields(gt, {"instagram": {"required": True}}) == {
        **defaults, "instagram": {"required": True},
    }
    assert default_participant_fields("RULET4") == {
        "email": {"required": False}, "instagram": {"required": True}, "phone": {"required": False},
    }


@pytest.mark.parametrize("gt", WEB_TYPES)
@pytest.mark.parametrize("config", CONFIGS)
def test_enabled_only_join_and_persistence(web, gt, config):
    meta, put, find, _ = web
    meta.update(gameType=gt, participantFields=config)
    submitted = {name: VALUES[name] for name in FIELDS if config[name]["required"]}
    result = assign_mod.lambda_handler(assign_body(submitted), None)
    assert result["ok"] is True
    assert result["playerToken"]
    item = put.call_args.args[0]
    for name, key in (("email", "email"), ("instagram", "instagramUsername"), ("phone", "phone")):
        if config[name]["required"]:
            assert item[key] == STORED[key]
        else:
            assert key not in item
    if config["email"]["required"]:
        find.assert_called_once_with("ABCDEF", "ana@example.com")
    else:
        find.assert_not_called()
    if gt == "T1MER" and not submitted:
        assert result["assignment"]["displayName"] == "Jugador 1"
    assert item["type"][gt]["raffleEligible"] is False


@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize("bad,reason", [
    (None, "missing"), ("", "missing"), ("   ", "missing"),
    (123, "invalid"), ({}, "invalid"), ([], "invalid"), (False, "invalid"),
])
def test_required_fields_validate_before_reserving_slot(web, field, bad, reason):
    meta, put, _, reserve = web
    meta["participantFields"] = {name: {"required": name == field} for name in FIELDS}
    result = assign_mod.lambda_handler(assign_body({field: bad}), None)
    assert result == {"ok": False, "reason": f"{reason}_{field}"}
    put.assert_not_called()
    reserve.assert_not_called()


@pytest.mark.parametrize("values,reason", [
    ({"email": "invalid"}, "invalid_email"),
    ({"instagram": "@@"}, "missing_instagram"),
])
def test_required_email_format_and_empty_handle(web, values, reason):
    meta, put, _, reserve = web
    meta["participantFields"] = {name: {"required": name in values} for name in FIELDS}
    assert assign_mod.lambda_handler(assign_body(values), None)["reason"] == reason
    put.assert_not_called()
    reserve.assert_not_called()


@pytest.mark.parametrize("gt", WEB_TYPES)
def test_disabled_values_are_ignored_even_if_malformed(web, gt):
    meta, put, find, _ = web
    meta.update(gameType=gt, participantFields={name: {"required": False} for name in FIELDS})
    result = assign_mod.lambda_handler(assign_body({"email": {}, "instagram": [], "phone": 123}), None)
    assert result["ok"] is True
    assert not ({"email", "instagramUsername", "phone"} & put.call_args.args[0].keys())
    find.assert_not_called()


def test_legacy_required_email_and_dedup_are_preserved(web):
    _, put, find, reserve = web
    assert assign_mod.lambda_handler(assign_body({}), None)["reason"] == "missing_email"
    assert assign_mod.lambda_handler(assign_body({"email": "bad"}), None)["reason"] == "invalid_email"
    find.return_value = {"playerId": 7, "email": "ana@example.com", "type": {"T1MER": {}}}
    result = assign_mod.lambda_handler(assign_body({"email": VALUES["email"]}), None)
    assert result["already_joined"] is True
    assert result["assignment"]["playerId"] == 7
    put.assert_not_called()
    reserve.assert_not_called()


@pytest.mark.parametrize("config", CONFIGS)
def test_public_metadata_and_join_match_assignment_config(web, config, monkeypatch):
    meta, put, _, _ = web
    meta["participantFields"] = config
    monkeypatch.setattr(public_mod, "_get_game", lambda _: meta)
    monkeypatch.setattr(public_mod, "_client_appearance", lambda _: {})
    monkeypatch.setattr(public_mod, "_t1mer_ranking", lambda *_: [])
    monkeypatch.setattr(public_mod, "ASSIGN_LAMBDA", "assign")
    response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABCDEF"}})
    assert json.loads(response["body"])["participantFields"] == config

    def invoke(**kwargs):
        payload = json.loads(kwargs["Payload"])
        result = assign_mod.lambda_handler(payload, None)
        return {"Payload": io.BytesIO(json.dumps(result).encode())}

    body = {"gameId": "ABCDEF", "consent": True}
    body.update({name: VALUES[name] for name in FIELDS if config[name]["required"]})
    pending = {name: STORED[{"instagram": "instagramUsername"}.get(name, name)]
               for name in FIELDS if config[name]["required"]}
    if "instagram" in pending:
        pending["instagram"] = pending["instagram"].lstrip("@")
    with patch.object(public_mod, "verification_table"), \
         patch.object(public_mod, "confirm_code", return_value=({"registration": pending}, "server-receipt")), \
         patch.object(public_mod.lambda_client, "invoke", side_effect=invoke):
        response = public_mod._handle_join({"body": json.dumps(body)})
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["playerToken"]
    assert put.call_count == 1


def test_public_metadata_fills_missing_phone_and_legacy_defaults(monkeypatch):
    monkeypatch.setattr(public_mod, "_get_game", lambda _: {
        "gameType": "EMPAREJA2", "participantFields": {"email": {"required": False}},
    })
    monkeypatch.setattr(public_mod, "_client_appearance", lambda _: {})
    response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABCDEF"}})
    assert json.loads(response["body"])["participantFields"] == {
        name: {"required": False} for name in FIELDS
    }


def test_public_required_phone_error_is_400(web, monkeypatch):
    meta, put, _, reserve = web
    meta["participantFields"] = {"email": {"required": False}, "phone": {"required": True}}
    monkeypatch.setattr(public_mod, "ASSIGN_LAMBDA", "assign")
    monkeypatch.setattr(public_mod, "_get_game", lambda _: meta)
    with patch.object(public_mod, "_invoke_assign_web", side_effect=lambda gid, email, ig, phone:
                      assign_mod.lambda_handler(assign_body({"email": email, "instagram": ig, "phone": phone}), None)):
        response = public_mod._handle_join({"body": json.dumps({"gameId": "ABCDEF", "consent": True})})
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "missing_phone"
    put.assert_not_called()
    reserve.assert_not_called()


@pytest.mark.parametrize("config", CONFIGS)
def test_operator_create_update_and_get_preserve_phone_config(config):
    table = MagicMock()
    with patch.object(game, "games_table", table), \
         patch.object(game, "_get_user", return_value={}), \
         patch.object(game, "_is_user_pro", return_value=True), \
         patch.object(game, "_new_join_code", return_value="ABCDEF"), \
         patch.object(game, "_load_default_quiz_from_catalog", return_value=(None, None)):
        created = game._handle_post(operator_event({"gameType": "T1MER", "participantFields": config}))
        assert created["statusCode"] == 201
        stored = table.put_item.call_args.kwargs["Item"]
        assert stored["participantFields"] == config
        table.get_item.return_value = {"Item": stored}
        updated = game._handle_put(operator_event({"gameId": "ABCDEF", "participantFields": config}))
    assert updated["statusCode"] == 200
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":pf"] == config
    assert game._public_game_shape(stored)["participantFields"] == config


@pytest.mark.parametrize("config", [
    "bad", [], False, {"phone": True}, {"phone": None}, {"phone": {"required": "false"}},
    {"phone": {"required": 1}}, {"phone": {"enabled": True}}, {"unknown": {"required": True}},
])
def test_invalid_config_rejected_before_any_write(config):
    table = MagicMock()
    table.get_item.return_value = {"Item": {"gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "owner"}}
    with patch.object(game, "games_table", table):
        created = game._handle_post(operator_event({"gameType": "T1MER", "participantFields": config}))
        updated = game._handle_put(operator_event({
            "gameId": "ABCDEF", "participantFields": config, "isActive": False,
        }))
    assert created["statusCode"] == updated["statusCode"] == 400
    table.put_item.assert_not_called()
    table.update_item.assert_not_called()


def test_web_config_does_not_enable_web_registration_for_instagram_campaign(web):
    meta, put, _, reserve = web
    meta.update(joinChannel="instagram", participantFields={name: {"required": False} for name in FIELDS})
    assert assign_mod.lambda_handler(assign_body({}), None)["reason"] == "web_join_disabled"
    put.assert_not_called()
    reserve.assert_not_called()
