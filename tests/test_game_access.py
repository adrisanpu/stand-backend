"""Approved mobile/web access policy and legacy gameplay compatibility."""
import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from stand_common.game_access import (
    GAME_ACCESS_CAPABILITIES,
    GameAccessError,
    access_for_create,
    access_for_update,
    resolve_game_access,
    validate_game_access,
)
from stand_common.stack import DEFAULT_CONFIG
from stand_common.utils import (
    issue_player_token,
)
from tests.test_public_web import assign_mod, public_mod, SECRET

ROOT = Path(__file__).parent.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


game = load("access_crud", ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py")
quiz = load("access_quiz", ROOT / "src" / "stand_prod_game_fn_quiz" / "lambda_function.py")

ALL_TYPES = ["EMPAREJA2", "T1MER", "STACK", "L3TRAS", "RULET4", "CUPIDO", "SEMAFORO", "INFOCARDS", "IMPOSTOR"]
APPROVED = {gt: {("mobile", "web")} for gt in ALL_TYPES}


# ---------------- matrix and legacy resolution ----------------

def test_matrix_matches_approved_plan_and_frontend_contract():
    assert set(GAME_ACCESS_CAPABILITIES) == set(ALL_TYPES)
    for gt in ALL_TYPES:
        caps = GAME_ACCESS_CAPABILITIES[gt]
        allowed = {
            (pm, jc) for pm in ("master", "mobile") for jc in ("instagram", "web")
            if pm in caps["playModes"] and jc in caps["joinChannels"]
        }
        assert allowed == APPROVED[gt], gt


@pytest.mark.parametrize("gt", ALL_TYPES)
@pytest.mark.parametrize("pm", ["master", "mobile"])
@pytest.mark.parametrize("jc", ["instagram", "web"])
def test_validate_matrix(gt, pm, jc):
    if (pm, jc) in APPROVED[gt]:
        assert validate_game_access(gt, pm, jc) == {"playMode": pm, "joinChannel": jc}
    else:
        with pytest.raises(GameAccessError):
            validate_game_access(gt, pm, jc)


@pytest.mark.parametrize("pm,jc", [("desktop", "web"), ("mobile", "email"), (None, "web"), ("mobile", 1)])
def test_validate_rejects_unknown_values(pm, jc):
    with pytest.raises(GameAccessError):
        validate_game_access("EMPAREJA2", pm, jc)


@pytest.mark.parametrize("item", [
    {"gameType": "EMPAREJA2"},
    {"gameType": "EMPAREJA2", "joinChannel": "web"},
    {"gameType": "EMPAREJA2", "joinChannel": "instagram"},
    {"gameType": "T1MER", "joinChannel": "instagram"},
    {"gameType": "T1MER"},
    {"gameType": "T1MER", "joinChannel": "web", "playMode": "master"},
    {"gameType": "EMPAREJA2", "joinChannel": "instagram", "playMode": "mobile"},
    {"gameType": "STACK"},
    {"gameType": "STACK", "playMode": "tablet", "joinChannel": "email"},
    {"gameType": "L3TRAS"},
])
def test_legacy_resolution(item):
    resolved = resolve_game_access(item)
    assert (resolved["playMode"], resolved["joinChannel"]) == ("mobile", "web")


def test_create_defaults_and_partial_update_merge():
    assert access_for_create("EMPAREJA2") == {"playMode": "mobile", "joinChannel": "web"}
    with pytest.raises(GameAccessError):
        access_for_create("T1MER", join_channel="instagram")
    legacy = {"gameType": "EMPAREJA2", "joinChannel": "instagram"}
    assert access_for_update(legacy, join_channel="web") == {"playMode": "mobile", "joinChannel": "web"}
    with pytest.raises(GameAccessError):
        access_for_update(legacy, play_mode="master")
    with pytest.raises(GameAccessError):
        access_for_update({"gameType": "STACK"}, play_mode="master")


# ---------------- operator CRUD ----------------

def operator_event(body, **query):
    return {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "body": json.dumps(body),
        "queryStringParameters": query,
    }


def _create(body):
    table = MagicMock()
    with patch.object(game, "games_table", table), \
         patch.object(game, "_get_user", return_value={}), \
         patch.object(game, "_is_user_pro", return_value=True), \
         patch.object(game, "_new_join_code", return_value="ABCDEF"), \
         patch.object(game, "_load_default_quiz_from_catalog", return_value=(None, None)):
        return game._handle_post(operator_event(body)), table


@pytest.mark.parametrize("body,expected", [
    ({"gameType": "EMPAREJA2", "playMode": "mobile", "joinChannel": "web"}, ("mobile", "web")),
    ({"gameType": "EMPAREJA2"}, ("mobile", "web")),
    ({"gameType": "STACK"}, ("mobile", "web")),
    ({"gameType": "L3TRAS"}, ("mobile", "web")),
])
def test_create_persists_both_fields(body, expected):
    response, table = _create(body)
    assert response["statusCode"] == 201
    item = table.put_item.call_args.kwargs["Item"]
    assert (item["playMode"], item["joinChannel"]) == expected
    created = json.loads(response["body"])["game"]
    assert (created["playMode"], created["joinChannel"]) == expected
    assert created["showRaffleOnHome"] is True


@pytest.mark.parametrize("show", [True, False])
def test_home_raffle_visibility_create_and_read(show):
    response, table = _create({"gameType": "T1MER", "showRaffleOnHome": show})
    assert response["statusCode"] == 201
    item = table.put_item.call_args.kwargs["Item"]
    assert game._public_game_shape(item)["showRaffleOnHome"] is show
    assert game._public_game_shape({"gameType": "T1MER"})["showRaffleOnHome"] is True


@pytest.mark.parametrize("show", [True, False])
def test_home_raffle_visibility_can_be_updated_alone(show):
    table = MagicMock()
    table.get_item.return_value = {"Item": {"gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "owner"}}
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", "showRaffleOnHome": show}))
    assert response["statusCode"] == 200
    write = table.update_item.call_args.kwargs
    assert write["ExpressionAttributeValues"][":show"] is show
    assert "ownerUserId = :owner" in write["ConditionExpression"]


@pytest.mark.parametrize("show", [None, "false", 0, 1, []])
def test_home_raffle_visibility_rejects_non_booleans(show):
    response, table = _create({"gameType": "T1MER", "showRaffleOnHome": show})
    assert response["statusCode"] == 400
    table.put_item.assert_not_called()
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", "showRaffleOnHome": show}))
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_home_raffle_visibility_rejects_other_owner():
    table = MagicMock()
    table.get_item.return_value = {"Item": {"gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "someone-else"}}
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", "showRaffleOnHome": False}))
    assert response["statusCode"] == 403
    table.update_item.assert_not_called()


@pytest.mark.parametrize("body", [
    {"gameType": "STACK", "playMode": "master"},
    {"gameType": "EMPAREJA2", "joinChannel": "instagram"},
    {"gameType": "L3TRAS", "joinChannel": "instagram"},
    {"gameType": "CUPIDO", "playMode": "mobile", "joinChannel": "instagram"},
    {"gameType": "EMPAREJA2", "playMode": "tablet"},
    {"gameType": "EMPAREJA2", "joinChannel": None},
])
def test_create_rejects_invalid_without_write(body):
    response, table = _create(body)
    assert response["statusCode"] == 400
    table.put_item.assert_not_called()


def test_get_returns_resolved_fields_for_legacy_games():
    table = MagicMock()
    legacy = {
        "gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "owner",
        "playMode": "master", "joinChannel": "unsupported",
    }
    other = {"gameId": "QWERTY", "gameType": "RULET4", "ownerUserId": "owner"}
    table.get_item.return_value = {"Item": legacy}
    table.query.return_value = {"Items": [legacy, other]}
    with patch.object(game, "games_table", table):
        one = json.loads(game._handle_get(operator_event({}, gameId="ABCDEF"))["body"])["game"]
        listed = json.loads(game._handle_get(operator_event({}))["body"])["games"]
    assert (one["playMode"], one["joinChannel"]) == ("mobile", "web")
    assert [(g["playMode"], g["joinChannel"]) for g in listed] == [("mobile", "web")] * 2


def test_put_persists_both_fields_in_one_conditional_write():
    table = MagicMock()
    stored = {"gameId": "ABCDEF", "gameType": "EMPAREJA2", "ownerUserId": "owner", "joinChannel": "web"}
    table.get_item.return_value = {"Item": stored}
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", "playMode": "mobile", "joinChannel": "web"}))
    assert response["statusCode"] == 200
    assert table.update_item.call_count == 1
    write = table.update_item.call_args.kwargs
    assert write["UpdateExpression"] == "SET playMode = :pm, joinChannel = :jc, updatedAt = :u"
    values = write["ExpressionAttributeValues"]
    assert (values[":pm"], values[":jc"]) == ("mobile", "web")
    assert "ownerUserId = :owner" in write["ConditionExpression"]


def test_put_channel_only_on_legacy_game_persists_both():
    table = MagicMock()
    table.get_item.return_value = {"Item": {"gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "owner"}}
    with patch.object(game, "games_table", table):
        assert game._handle_put(operator_event({"gameId": "ABCDEF", "joinChannel": "web"}))["statusCode"] == 200
    values = table.update_item.call_args.kwargs["ExpressionAttributeValues"]
    assert (values[":pm"], values[":jc"]) == ("mobile", "web")


@pytest.mark.parametrize("meta,updates", [
    ({"gameType": "L3TRAS"}, {"joinChannel": "instagram", "isActive": False}),
    ({"gameType": "STACK", "type": {"STACK": deepcopy(DEFAULT_CONFIG)}},
     {"playMode": "master", "settings": {"stack": {"initialSpeed": 3}}}),
    ({"gameType": "EMPAREJA2"}, {"playMode": "phone", "participantFields": {"email": {"required": True}}}),
    ({"gameType": "EMPAREJA2"}, {"playMode": "master", "participantFields": "bad"}),
])
def test_put_invalid_access_rejected_before_any_write(meta, updates):
    table = MagicMock()
    table.get_item.return_value = {"Item": {"gameId": "ABCDEF", "ownerUserId": "owner", **meta}}
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", **updates}))
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


@pytest.mark.parametrize("access", [
    {"playMode": "master"},
    {"joinChannel": "instagram"},
    {"playMode": "mobile", "joinChannel": "instagram"},
    {"playMode": None},
    {"joinChannel": None},
])
def test_put_rejects_explicit_unsupported_access_values(access):
    table = MagicMock()
    table.get_item.return_value = {"Item": {
        "gameId": "ABCDEF", "gameType": "EMPAREJA2", "ownerUserId": "owner",
    }}
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", **access}))
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


ACCESS_SECRET = "test-only-personal-access-key-32-characters"


def mobile_meta(gt="T1MER", **extra):
    return {"gameId": "ABCDEF", "gameType": gt, "isActive": True,
            "playMode": "mobile", "joinChannel": "instagram", **extra}


def ig_player(gt="T1MER", **extra):
    blob = ({"characterId": 1, "characterName": "Alice", "partnerCharacterName": "Bob"}
            if gt == "EMPAREJA2" else {})
    return {"gameId": "ABCDEF", "playerId": 7, "instagramPSID": "psid7",
            "instagramUsername": "@player", "validationCode": 1234, "validated": False,
            "type": {gt: blob}, **extra}


def post_event(path, body):
    return {"rawPath": path, "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps(body)}
























def test_post_player_supports_session_recovery_without_query_credentials():
    table = MagicMock()
    table.get_item.return_value = {"Item": ig_player()}
    token = issue_player_token("ABCDEF", 7, ACCESS_SECRET)
    with patch.object(public_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(public_mod, "gp_table", table), \
         patch.object(public_mod, "_get_game", return_value=mobile_meta()):
        result = public_mod.lambda_handler(post_event("/v1/public/player", {
            "gameId": "ABCDEF", "playerToken": token}), None)
    assert result["statusCode"] == 200
    assert json.loads(result["body"])["assignment"]["playerId"] == 7


def test_personal_access_endpoint_is_removed():
    result = public_mod.lambda_handler(post_event("/v1/public/access", {
        "gameId": "ABCDEF", "accessToken": "retired-token",
    }), None)
    assert result["statusCode"] == 404


@pytest.mark.parametrize("payload", [
    {"game_id": "ABCDEF", "psid": "psid7", "username_at": "@player"},
    {"source": "instagram", "game_id": "ABCDEF", "psid": "psid7"},
])
def test_assignment_rejects_non_web_enrollment(payload):
    with patch.object(assign_mod, "_put_player") as create:
        result = assign_mod.lambda_handler(payload, None)
    assert result == {"ok": False, "reason": "unsupported_source"}
    create.assert_not_called()


@pytest.mark.parametrize("kind,event", [
    ("quiz_start", {"kind": "quiz_start", "gameId": "ABCDEF", "psid": ["psid7"]}),
    ("quiz_answer", {"kind": "quiz_answer", "psid": "psid7", "quizPayload": "q1_yes"}),
])
def test_instagram_quiz_events_are_disconnected(kind, event):
    with patch.object(quiz.gp_table, "update_item") as update:
        result = quiz.lambda_handler(event, None)
    assert result == {"ok": True, "ignored": True}
    update.assert_not_called()


def test_stored_master_mode_still_allows_mobile_gameplay_actions():
    token = issue_player_token("ABCDEF", 7, ACCESS_SECRET)
    with patch.object(public_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(public_mod, "VALIDATE_LAMBDA", "validate"), \
         patch.object(public_mod, "SCORE_LAMBDA", "score"), \
         patch.object(public_mod, "_get_player", return_value=ig_player()), \
         patch.object(public_mod, "_get_game", return_value=mobile_meta(playMode="master")), \
         patch.object(public_mod, "_invoke_validate", return_value={"ok": True}) as validate, \
         patch.object(public_mod, "_invoke_score_store", return_value={"ok": True}) as score:
        for path, extra in [("validate", {"codes": ["1234", "4321"]}), ("score", {"timer": "10.0", "score": 0})]:
            result = public_mod.lambda_handler(post_event(f"/v1/public/{path}", {
                "gameId": "ABCDEF", "playerToken": token, **extra}), None)
            assert result["statusCode"] == 200
    validate.assert_called_once()
    score.assert_called_once()




@pytest.mark.parametrize("gt", ["EMPAREJA2", "T1MER", "STACK"])
@pytest.mark.parametrize("jc", ["instagram", "web"])
def test_public_metadata_normalizes_legacy_access_and_uses_web_join_target(gt, jc):
    meta = mobile_meta(gt, joinChannel=jc, type={"STACK": deepcopy(DEFAULT_CONFIG)})
    with patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "_t1mer_ranking", return_value=[]), \
         patch.object(public_mod, "stack_ranking", return_value={"results": []}):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABCDEF"}})
    assert response["statusCode"] == 200
    data = json.loads(response["body"])
    assert data["playMode"] == "mobile" and data["joinChannel"] == "web"
    assert data["joinUrl"] == "/play/ABCDEF"


@pytest.mark.parametrize("gt", ["EMPAREJA2", "T1MER"])
@pytest.mark.parametrize("pm", ["master", "mobile"])
def test_web_registration_normalizes_old_game_modes(gt, pm, verified_web_assignment):
    player = ig_player(gt)
    with patch.object(assign_mod, "_get_game_meta", return_value=mobile_meta(gt, playMode=pm, joinChannel="web")), \
         patch.object(assign_mod, "_find_existing_player_by_email", return_value=None), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=True), \
         patch.object(assign_mod, "_get_last_player_id", return_value=6), \
         patch.object(assign_mod, "_put_player") as create, \
         patch.object(assign_mod, "ASSIGNERS", {
             gt: lambda ctx: {"type": player["type"]}}):
        result = assign_mod._handle_web_assign({
            "source": "web", "game_id": "ABCDEF", "email": "player@example.test", "username": "Player",
        })
    assert result["ok"]
    assert result["assignment"]["validationCode"]
    assert result["assignment"]["playerId"] == 7
    assert result["playerToken"]
    create.assert_called_once()
    assert create.call_args.args[0]["username"] == "Player"
    assert "instagramPSID" not in create.call_args.args[0]
