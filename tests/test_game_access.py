"""playMode/joinChannel matrix, legacy resolution, mobile-only enforcement and
one-use personal access links (Instagram -> mobile). Entirely offline."""
import base64
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
from stand_common.player_access import consume_access_nonce, issue_access_link
from stand_common.stack import DEFAULT_CONFIG
from stand_common.utils import (
    hash_access_nonce,
    issue_access_token,
    issue_player_token,
    issue_prize_token,
    verify_access_token,
    verify_player_token,
    verify_prize_token,
)
from tests.test_public_web import assign_mod, public_mod, SECRET

ROOT = Path(__file__).parent.parent
ORIGIN = "https://dev.playstand.app"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


game = load("access_crud", ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py")
quiz = load("access_quiz", ROOT / "src" / "stand_prod_game_fn_quiz" / "lambda_function.py")
sender = load("access_sender", ROOT / "src" / "stand_prod_messaging_fn_instagram_sender" / "lambda_function.py")

ALL_TYPES = ["EMPAREJA2", "T1MER", "STACK", "L3TRAS", "RULET4", "CUPIDO", "SEMAFORO", "INFOCARDS", "IMPOSTOR"]
APPROVED = {
    "EMPAREJA2": {("master", "instagram"), ("master", "web"), ("mobile", "instagram"), ("mobile", "web")},
    "T1MER": {("master", "instagram"), ("master", "web"), ("mobile", "instagram"), ("mobile", "web")},
    "STACK": {("mobile", "instagram"), ("mobile", "web")},
}


class ConditionalFailure(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class FakeGP:
    """Gameplayer table modelling the nonce SET/REMOVE conditions."""

    def __init__(self, *players):
        self.items = {(p["gameId"], int(p["playerId"])): deepcopy(p) for p in players}
        self.other_writes = 0

    def get_item(self, *, Key, ConsistentRead=False):
        item = self.items.get((Key["gameId"], int(Key["playerId"])))
        return {"Item": deepcopy(item)} if item else {}

    def update_item(self, **kwargs):
        key = (kwargs["Key"]["gameId"], int(kwargs["Key"]["playerId"]))
        item = self.items.get(key)
        values = kwargs["ExpressionAttributeValues"]
        expr = kwargs["UpdateExpression"]
        if expr == "SET accessNonceHash = :h, accessIssuedAt = :t":
            if not item:
                raise ConditionalFailure()
            item["accessNonceHash"], item["accessIssuedAt"] = values[":h"], values[":t"]
        elif expr == "REMOVE accessNonceHash, accessIssuedAt":
            assert kwargs["ConditionExpression"] == "attribute_exists(playerId) AND accessNonceHash = :h"
            if not item or item.get("accessNonceHash") != values[":h"]:
                raise ConditionalFailure()
            item.pop("accessNonceHash", None)
            item.pop("accessIssuedAt", None)
        else:
            self.other_writes += 1

    def put(self, item):
        self.items[(item["gameId"], int(item["playerId"]))] = deepcopy(item)


def token_from_url(url):
    assert url.startswith(f"{ORIGIN}/play/")
    path, fragment = url.split("#", 1)
    assert "?" not in path
    assert fragment.startswith("access=")
    return path.rsplit("/", 1)[1], fragment[len("access="):]


@pytest.fixture(autouse=True)
def _origin(monkeypatch):
    monkeypatch.setenv("PUBLIC_APP_ORIGIN", ORIGIN)
    monkeypatch.setenv("ACCESS_LINK_TTL_SECONDS", "900")


# ---------------- matrix and legacy resolution ----------------

def test_matrix_matches_approved_plan_and_frontend_contract():
    assert set(GAME_ACCESS_CAPABILITIES) == set(ALL_TYPES)
    for gt in ALL_TYPES:
        caps = GAME_ACCESS_CAPABILITIES[gt]
        allowed = {
            (pm, jc) for pm in ("master", "mobile") for jc in ("instagram", "web")
            if pm in caps["playModes"] and jc in caps["joinChannels"]
        }
        assert allowed == APPROVED.get(gt, {("master", "instagram")}), gt


@pytest.mark.parametrize("gt", ALL_TYPES)
@pytest.mark.parametrize("pm", ["master", "mobile"])
@pytest.mark.parametrize("jc", ["instagram", "web"])
def test_validate_matrix(gt, pm, jc):
    if (pm, jc) in APPROVED.get(gt, {("master", "instagram")}):
        assert validate_game_access(gt, pm, jc) == {"playMode": pm, "joinChannel": jc}
    else:
        with pytest.raises(GameAccessError):
            validate_game_access(gt, pm, jc)


@pytest.mark.parametrize("pm,jc", [("desktop", "web"), ("mobile", "email"), (None, "web"), ("mobile", 1)])
def test_validate_rejects_unknown_values(pm, jc):
    with pytest.raises(GameAccessError):
        validate_game_access("EMPAREJA2", pm, jc)


@pytest.mark.parametrize("item,expected", [
    ({"gameType": "EMPAREJA2"}, ("mobile", "web")),
    ({"gameType": "EMPAREJA2", "joinChannel": "web"}, ("mobile", "web")),
    ({"gameType": "EMPAREJA2", "joinChannel": "instagram"}, ("master", "instagram")),
    ({"gameType": "T1MER", "joinChannel": "instagram"}, ("master", "instagram")),
    ({"gameType": "T1MER"}, ("mobile", "web")),
    ({"gameType": "T1MER", "joinChannel": "web", "playMode": "master"}, ("master", "web")),
    ({"gameType": "EMPAREJA2", "joinChannel": "instagram", "playMode": "mobile"}, ("mobile", "instagram")),
    ({"gameType": "STACK"}, ("mobile", "web")),
    ({"gameType": "STACK", "joinChannel": "instagram"}, ("mobile", "instagram")),
    ({"gameType": "L3TRAS"}, ("master", "instagram")),
])
def test_legacy_resolution(item, expected):
    resolved = resolve_game_access(item)
    assert (resolved["playMode"], resolved["joinChannel"]) == expected


@pytest.mark.parametrize("item", [
    {"gameType": "STACK", "playMode": "master"},
    {"gameType": "L3TRAS", "joinChannel": "web"},
    {"gameType": "INFOCARDS", "playMode": "mobile"},
    {"gameType": "T1MER", "joinChannel": ""},
])
def test_invalid_explicit_stored_access_is_not_silently_reinterpreted(item):
    with pytest.raises(GameAccessError):
        resolve_game_access(item)


def test_create_defaults_and_partial_update_merge():
    assert access_for_create("EMPAREJA2") == {"playMode": "mobile", "joinChannel": "web"}
    assert access_for_create("T1MER", join_channel="instagram") == {"playMode": "master", "joinChannel": "instagram"}
    assert access_for_create("STACK", join_channel="instagram") == {"playMode": "mobile", "joinChannel": "instagram"}
    assert access_for_create("RULET4") == {"playMode": "master", "joinChannel": "instagram"}
    legacy = {"gameType": "EMPAREJA2", "joinChannel": "instagram"}
    # Legacy (no playMode) keeps the old coupling for channel-only saves.
    assert access_for_update(legacy, join_channel="web") == {"playMode": "mobile", "joinChannel": "web"}
    explicit = {**legacy, "playMode": "master"}
    assert access_for_update(explicit, join_channel="web") == {"playMode": "master", "joinChannel": "web"}
    assert access_for_update(explicit, play_mode="mobile") == {"playMode": "mobile", "joinChannel": "instagram"}
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
    ({"gameType": "EMPAREJA2", "playMode": "master", "joinChannel": "web"}, ("master", "web")),
    ({"gameType": "T1MER", "playMode": "mobile", "joinChannel": "instagram"}, ("mobile", "instagram")),
    ({"gameType": "EMPAREJA2"}, ("mobile", "web")),
    ({"gameType": "STACK", "joinChannel": "instagram"}, ("mobile", "instagram")),
    ({"gameType": "L3TRAS"}, ("master", "instagram")),
])
def test_create_persists_both_fields(body, expected):
    response, table = _create(body)
    assert response["statusCode"] == 201
    item = table.put_item.call_args.kwargs["Item"]
    assert (item["playMode"], item["joinChannel"]) == expected
    created = json.loads(response["body"])["game"]
    assert (created["playMode"], created["joinChannel"]) == expected


@pytest.mark.parametrize("body", [
    {"gameType": "STACK", "playMode": "master"},
    {"gameType": "L3TRAS", "joinChannel": "web"},
    {"gameType": "CUPIDO", "playMode": "mobile"},
    {"gameType": "EMPAREJA2", "playMode": "tablet"},
])
def test_create_rejects_invalid_without_write(body):
    response, table = _create(body)
    assert response["statusCode"] == 400
    table.put_item.assert_not_called()


def test_get_returns_resolved_fields_for_legacy_games():
    table = MagicMock()
    legacy = {"gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "owner", "joinChannel": "instagram"}
    other = {"gameId": "QWERTY", "gameType": "RULET4", "ownerUserId": "owner"}
    table.get_item.return_value = {"Item": legacy}
    table.query.return_value = {"Items": [legacy, other]}
    with patch.object(game, "games_table", table):
        one = json.loads(game._handle_get(operator_event({}, gameId="ABCDEF"))["body"])["game"]
        listed = json.loads(game._handle_get(operator_event({}))["body"])["games"]
    assert (one["playMode"], one["joinChannel"]) == ("master", "instagram")
    assert [(g["playMode"], g["joinChannel"]) for g in listed] == [("master", "instagram")] * 2


def test_put_persists_both_fields_in_one_conditional_write():
    table = MagicMock()
    stored = {"gameId": "ABCDEF", "gameType": "EMPAREJA2", "ownerUserId": "owner", "joinChannel": "web"}
    table.get_item.return_value = {"Item": stored}
    with patch.object(game, "games_table", table):
        response = game._handle_put(operator_event({"gameId": "ABCDEF", "playMode": "master", "joinChannel": "web"}))
    assert response["statusCode"] == 200
    assert table.update_item.call_count == 1
    write = table.update_item.call_args.kwargs
    assert write["UpdateExpression"] == "SET playMode = :pm, joinChannel = :jc, updatedAt = :u"
    values = write["ExpressionAttributeValues"]
    assert (values[":pm"], values[":jc"]) == ("master", "web")
    assert "ownerUserId = :owner" in write["ConditionExpression"]


def test_put_channel_only_on_legacy_game_persists_both():
    table = MagicMock()
    table.get_item.return_value = {"Item": {"gameId": "ABCDEF", "gameType": "T1MER", "ownerUserId": "owner"}}
    with patch.object(game, "games_table", table):
        assert game._handle_put(operator_event({"gameId": "ABCDEF", "joinChannel": "instagram"}))["statusCode"] == 200
    values = table.update_item.call_args.kwargs["ExpressionAttributeValues"]
    assert (values[":pm"], values[":jc"]) == ("master", "instagram")


@pytest.mark.parametrize("meta,updates", [
    ({"gameType": "L3TRAS"}, {"joinChannel": "web", "isActive": False}),
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


def exchange(table, meta, token, game_id="ABCDEF"):
    with patch.object(public_mod, "gp_table", table), \
         patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET):
        result = public_mod.lambda_handler(
            post_event("/v1/public/access", {"gameId": game_id, "accessToken": token}), None)
    return result["statusCode"], json.loads(result["body"])


def test_access_token_has_separate_purpose_and_expiry():
    token = issue_access_token("ABCDEF", 7, "nonce", ACCESS_SECRET)
    claims, error = verify_access_token(token, ACCESS_SECRET)
    assert error is None
    assert claims == {"gameId": "ABCDEF", "playerId": 7, "nonce": "nonce"}
    assert verify_player_token(token, ACCESS_SECRET) is None
    assert verify_prize_token(token, ACCESS_SECRET) is None
    assert verify_access_token(issue_player_token("ABCDEF", 7, ACCESS_SECRET), ACCESS_SECRET)[0] is None
    assert verify_access_token(issue_prize_token("ABCDEF", 7, "v", ACCESS_SECRET), ACCESS_SECRET)[0] is None
    with patch("stand_common.utils.time.time", return_value=1000):
        expiring = issue_access_token("ABCDEF", 7, "nonce", ACCESS_SECRET, ttl_seconds=60)
    with patch("stand_common.utils.time.time", return_value=1060):
        assert verify_access_token(expiring, ACCESS_SECRET) == (None, "AccessTokenExpired")


@pytest.mark.parametrize("token", [None, "", 1, "x", "x.y", "x." + "é" * 64, "x." + "0" * 65])
def test_malformed_access_tokens_rejected_without_exception(token):
    assert verify_access_token(token, ACCESS_SECRET)[0] is None


def test_personal_link_stores_only_nonce_hash_and_replaces_prior_link():
    table = FakeGP(ig_player())
    first = issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET)
    second = issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET)
    gid, token = token_from_url(second)
    assert gid == "ABCDEF"
    old, _ = verify_access_token(token_from_url(first)[1], ACCESS_SECRET)
    new, _ = verify_access_token(token, ACCESS_SECRET)
    stored = table.items[("ABCDEF", 7)]
    assert stored["accessNonceHash"] == hash_access_nonce(new["nonce"])
    assert new["nonce"] not in json.dumps(stored)
    assert token not in json.dumps(stored)
    assert not consume_access_nonce(table, gid, 7, old["nonce"])
    assert consume_access_nonce(table, gid, 7, new["nonce"])
    assert not consume_access_nonce(table, gid, 7, new["nonce"])


@pytest.mark.parametrize("secret", ["", SECRET, "short"])
def test_personal_link_requires_private_configured_secret(secret):
    table = FakeGP(ig_player())
    with pytest.raises(RuntimeError, match="PLAYER_TOKEN_SECRET"):
        issue_access_link(table, "ABCDEF", 7, secret)
    assert "accessNonceHash" not in table.items[("ABCDEF", 7)]
    with patch.object(public_mod, "PLAYER_TOKEN_SECRET", secret):
        result = public_mod._handle_access(post_event("/v1/public/access", {}))
    assert result["statusCode"] == 503


@pytest.mark.parametrize("origin", ["", "http://example.test", "https://example.test/path",
                                   "https://user:pass@example.test", "https://example.test#access=bad"])
def test_personal_link_rejects_invalid_origin(origin):
    with pytest.raises(RuntimeError):
        issue_access_link(FakeGP(ig_player()), "ABCDEF", 7, ACCESS_SECRET, origin=origin)


@pytest.mark.parametrize("gt", ["EMPAREJA2", "T1MER", "STACK"])
def test_exchange_restores_same_player_once_and_does_not_start_attempt(gt):
    table = FakeGP(ig_player(gt))
    url = issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET)
    token = token_from_url(url)[1]
    status, payload = exchange(table, mobile_meta(gt), token)
    assert status == 200, payload
    assert payload["gameId"] == "ABCDEF"
    assert payload["gameType"] == gt
    assert payload["assignment"]["playerId"] == 7
    assert verify_player_token(payload["playerToken"], ACCESS_SECRET) == {"gameId": "ABCDEF", "playerId": 7}
    assert "accessNonceHash" not in json.dumps(payload)
    if gt == "STACK":
        assert payload["assignment"]["attempt"] is None
    assert table.other_writes == 0
    assert exchange(table, mobile_meta(gt), token)[0] == 409


def test_empareja2_exchange_includes_both_character_images(monkeypatch):
    monkeypatch.setenv("CHAR_BUCKET", "characters")
    table = FakeGP(ig_player("EMPAREJA2"))
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    s3 = MagicMock()
    s3.generate_presigned_url.side_effect = lambda operation, **kwargs: f"signed:{kwargs['Params']['Key']}"
    with patch.object(public_mod.boto3, "client", return_value=s3):
        status, payload = exchange(table, mobile_meta("EMPAREJA2"), token)
    assert status == 200, payload
    assert payload["assignment"]["characterImageUrl"] == "signed:Alice.png"
    assert payload["assignment"]["partnerCharacterImageUrl"] == "signed:Bob.png"


@pytest.mark.parametrize("change,expected", [
    ({"isActive": False}, 403), ({"playMode": "master"}, 403),
    ({"quizOrder": ["q1"]}, 403),
])
def test_exchange_rejects_inactive_incompatible_or_incomplete_quiz_without_consuming(change, expected):
    table = FakeGP(ig_player())
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    assert exchange(table, mobile_meta(**change), token)[0] == expected
    assert "accessNonceHash" in table.items[("ABCDEF", 7)]


def test_exchange_rejects_cross_game_tampering_and_expired_links():
    table = FakeGP(ig_player())
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    assert exchange(table, mobile_meta(), token, game_id="ABC123")[0] == 403
    assert exchange(table, mobile_meta(), token[:-1] + ("0" if token[-1] != "0" else "1"))[0] == 403
    expired = issue_access_token("ABCDEF", 7, "nonce", ACCESS_SECRET, ttl_seconds=-1)
    assert exchange(table, mobile_meta(), expired)[0] == 410
    assert "accessNonceHash" in table.items[("ABCDEF", 7)]


def test_exchange_requires_existing_instagram_player():
    table = FakeGP()
    token = issue_access_token("ABCDEF", 7, "nonce", ACCESS_SECRET)
    assert exchange(table, mobile_meta(), token)[0] == 404
    player = ig_player()
    player.pop("instagramPSID")
    table = FakeGP(player)
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    assert exchange(table, mobile_meta(), token)[0] == 403


def test_post_player_supports_session_recovery_without_query_credentials():
    table = FakeGP(ig_player())
    token = issue_player_token("ABCDEF", 7, ACCESS_SECRET)
    with patch.object(public_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(public_mod, "gp_table", table), \
         patch.object(public_mod, "_get_game", return_value=mobile_meta()):
        result = public_mod.lambda_handler(post_event("/v1/public/player", {
            "gameId": "ABCDEF", "playerToken": token}), None)
    assert result["statusCode"] == 200
    assert json.loads(result["body"])["assignment"]["playerId"] == 7


@pytest.mark.parametrize("gt", ["EMPAREJA2", "T1MER", "STACK"])
def test_repeat_instagram_join_reissues_link_without_new_player_or_attempt(gt):
    table = FakeGP(ig_player(gt))
    sent = []
    with patch.object(assign_mod, "gp_table", table), \
         patch.object(assign_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(assign_mod, "_get_game_meta", return_value=mobile_meta(gt)), \
         patch.object(assign_mod, "_find_existing_player_by_psid", return_value=ig_player(gt)), \
         patch.object(assign_mod, "_put_player") as create, \
         patch.object(assign_mod, "_reserve_player_slot") as reserve, \
         patch.object(assign_mod, "_send_bulk_messages", side_effect=sent.extend):
        result = assign_mod._handle_instagram_assign({
            "game_id": "ABCDEF", "psid": "psid7", "username_at": "@player"})
    assert result["ok"] and result["already_joined"]
    create.assert_not_called()
    reserve.assert_not_called()
    assert table.other_writes == 0
    links = [m for m in sent if m.get("template") == "access_link"]
    assert len(links) == 1
    assert verify_access_token(token_from_url(links[0]["url"])[1], ACCESS_SECRET)[0]["playerId"] == 7
    assert "pantalla" not in " ".join(m.get("text", "") for m in sent).lower()


def test_repeat_instagram_join_cannot_skip_pending_quiz():
    with patch.object(assign_mod, "_get_game_meta", return_value=mobile_meta(quizOrder=["q1"])), \
         patch.object(assign_mod, "_find_existing_player_by_psid", return_value=ig_player()), \
         patch.object(assign_mod, "_enqueue_quiz_start") as start, \
         patch.object(assign_mod, "_send_access_link_then_raffle") as link, \
         patch.object(assign_mod, "_send_single_dm"):
        result = assign_mod._handle_instagram_assign({
            "game_id": "ABCDEF", "psid": "psid7", "username_at": "@player"})
    assert result["ok"]
    start.assert_called_once()
    link.assert_not_called()


def test_new_instagram_mobile_join_preserves_image_link_raffle_order():
    table = FakeGP()
    sent = []
    meta = mobile_meta("EMPAREJA2", raffleRequiredFollows=["account"])
    player = ig_player("EMPAREJA2")
    with patch.object(assign_mod, "gp_table", table), \
         patch.object(assign_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(assign_mod, "_get_game_meta", return_value=meta), \
         patch.object(assign_mod, "_find_existing_player_by_psid", return_value=None), \
         patch.object(assign_mod, "_put_player", side_effect=table.put), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=True), \
         patch.object(assign_mod, "_get_last_player_id", return_value=6), \
         patch.object(assign_mod, "ASSIGNERS", {"EMPAREJA2": lambda ctx: (
             {"type": player["type"]}, "Bienvenido", [{"psid": "psid7", "image_url": "https://image.test/a.png"}])}), \
         patch.object(assign_mod.time, "sleep"), \
         patch.object(assign_mod, "_send_bulk_messages", side_effect=sent.extend):
        result = assign_mod._handle_instagram_assign({
            "game_id": "ABCDEF", "psid": "psid7", "username_at": "@player"})
    assert result["ok"] and result["created_new"]
    assert "image_url" in sent[0]
    assert sent[2]["template"] == "access_link"
    assert sent[-1]["template"] == "follow_accounts"
    assert table.items[("ABCDEF", 7)]["type"] == player["type"]


def test_quiz_completion_sends_mobile_link_not_master_code():
    meta = mobile_meta(quizOrder=["q1"])
    player = ig_player()
    with patch.object(quiz, "_get_quiz_meta", return_value=(["q1"], {}, meta)), \
         patch.object(quiz, "_get_player_by_psid_and_game", return_value=player), \
         patch.object(quiz, "_parse_quiz_payload", return_value=("ABCDEF", "q1", "yes")), \
         patch.object(quiz, "_save_quiz_answer"), \
         patch.object(quiz, "_set_quiz_state") as state, \
         patch.object(quiz, "_send_access_link_then_raffle", return_value=True) as link, \
         patch.object(quiz, "_send_validation_code_then_raffle") as code:
        result = quiz.lambda_handler({"kind": "quiz_answer", "psid": "psid7", "quizPayload": "answer"}, None)
    assert result["ok"] and result["accessSent"]
    state.assert_called_once_with("ABCDEF", 7, "T1MER", None, completed=True)
    link.assert_called_once()
    code.assert_not_called()


def test_duplicate_completed_quiz_answer_does_not_invalidate_delivered_link():
    player = ig_player()
    player["type"]["T1MER"]["quizCompleted"] = True
    with patch.object(quiz, "_get_quiz_meta", return_value=(["q1"], {}, mobile_meta(quizOrder=["q1"]))), \
         patch.object(quiz, "_get_player_by_psid_and_game", return_value=player), \
         patch.object(quiz, "_parse_quiz_payload", return_value=("ABCDEF", "q1", "yes")), \
         patch.object(quiz, "_save_quiz_answer") as save, \
         patch.object(quiz, "_send_access_link_then_raffle") as link:
        result = quiz.lambda_handler({"kind": "quiz_answer", "psid": "psid7", "quizPayload": "answer"}, None)
    assert result["ok"] and result["completed"]
    save.assert_not_called()
    link.assert_not_called()


def test_public_master_mode_blocks_mobile_actions_only():
    token = issue_player_token("ABCDEF", 7, ACCESS_SECRET)
    with patch.object(public_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(public_mod, "VALIDATE_LAMBDA", "validate"), \
         patch.object(public_mod, "SCORE_LAMBDA", "score"), \
         patch.object(public_mod, "_get_game", return_value=mobile_meta(playMode="master")), \
         patch.object(public_mod, "_invoke_validate") as validate, \
         patch.object(public_mod, "_invoke_score_payload") as score:
        for path, extra in [("validate", {"codes": ["1234", "4321"]}), ("score", {"timer": "10.0", "score": 0})]:
            result = public_mod.lambda_handler(post_event(f"/v1/public/{path}", {
                "gameId": "ABCDEF", "playerToken": token, **extra}), None)
            assert result["statusCode"] == 403
            assert json.loads(result["body"])["error"] == "MobilePlayDisabled"
    validate.assert_not_called()
    score.assert_not_called()


def test_sender_uses_button_without_plaintext_personal_url():
    url = f"{ORIGIN}/play/ABCDEF#access=private"
    payload = sender._build_access_link_template(url, 15)
    card = payload["attachment"]["payload"]["elements"][0]
    assert card["buttons"][0]["url"] == url
    assert "private" not in card["title"] + card["subtitle"]
    with patch.object(sender, "_send_template_message", return_value={"ok": True}) as send:
        result = sender.lambda_handler({"messages": [{
            "psid": "psid7", "template": "access_link", "url": url, "ttlMinutes": 15}]}, None)
    assert "private" not in json.dumps(result)
    send.assert_called_once_with("psid7", payload)


@pytest.mark.parametrize("gt", ["EMPAREJA2", "T1MER", "STACK"])
@pytest.mark.parametrize("jc", ["instagram", "web"])
def test_public_metadata_has_independent_mode_and_correct_qr_target(gt, jc):
    meta = mobile_meta(gt, joinChannel=jc, type={"STACK": deepcopy(DEFAULT_CONFIG)})
    with patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "_t1mer_ranking", return_value=[]), \
         patch.object(public_mod, "stack_ranking", return_value={"results": []}):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABCDEF"}})
    assert response["statusCode"] == 200
    data = json.loads(response["body"])
    assert data["playMode"] == "mobile" and data["joinChannel"] == jc
    assert data["joinUrl"] == (
        f"https://ig.me/m/{public_mod.INSTAGRAM_DM_HANDLE}?ref=ABCDEF" if jc == "instagram" else "/play/ABCDEF")


@pytest.mark.parametrize("gt", ["EMPAREJA2", "T1MER"])
@pytest.mark.parametrize("pm", ["master", "mobile"])
def test_web_registration_assigns_code_for_either_gameplay_mode(gt, pm, verified_web_assignment):
    player = ig_player(gt)
    with patch.object(assign_mod, "_get_game_meta", return_value=mobile_meta(gt, playMode=pm, joinChannel="web")), \
         patch.object(assign_mod, "_find_existing_player_by_email", return_value=None), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=True), \
         patch.object(assign_mod, "_get_last_player_id", return_value=6), \
         patch.object(assign_mod, "_put_player") as create, \
         patch.object(assign_mod, "ASSIGNERS", {
             gt: lambda ctx: ({"type": player["type"]}, "", [])}):
        result = assign_mod._handle_web_assign({"source": "web", "game_id": "ABCDEF", "email": "player@example.test"})
    assert result["ok"]
    assert result["assignment"]["validationCode"]
    assert result["assignment"]["playerId"] == 7
    assert result["playerToken"]
    create.assert_called_once()
    assert "instagramPSID" not in create.call_args.args[0]


def test_completed_quiz_link_can_be_exchanged_without_repeating_quiz():
    player = ig_player()
    player["type"]["T1MER"]["quizCompleted"] = True
    table = FakeGP(player)
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    assert exchange(table, mobile_meta(quizOrder=["q1"]), token)[0] == 200


def test_changing_qr_channel_does_not_revoke_existing_personal_mobile_access():
    table = FakeGP(ig_player())
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    assert exchange(table, mobile_meta(joinChannel="web"), token)[0] == 200


def test_pending_stack_attempt_survives_link_exchange_unchanged():
    player = ig_player("STACK")
    attempt = {"attemptId": "attempt1", "status": "started", "startedAt": "2026-10-16T10:00:00Z", "configVersion": "1"}
    player["type"]["STACK"]["attempt"] = attempt
    table = FakeGP(player)
    token = token_from_url(issue_access_link(table, "ABCDEF", 7, ACCESS_SECRET))[1]
    status, result = exchange(table, mobile_meta("STACK"), token)
    assert status == 200
    assert result["assignment"]["attempt"] == attempt
    assert table.items[("ABCDEF", 7)]["type"]["STACK"]["attempt"] == attempt
    assert table.other_writes == 0


def test_access_delivery_failure_is_explicit_and_does_not_send_master_instructions():
    messages = []
    with patch.object(assign_mod, "PLAYER_TOKEN_SECRET", ""), \
         patch.object(assign_mod, "_send_bulk_messages", side_effect=messages.extend):
        assert not assign_mod._send_access_link_then_raffle("psid7", "ABCDEF", 7, mobile_meta())
    assert len(messages) == 1
    assert "No he podido generar" in messages[0]["text"]
    assert not any(m.get("template") == "access_link" for m in messages)


def test_instagram_stack_registration_matches_web_initial_attempt_and_raffle_state():
    table = FakeGP()
    with patch.object(assign_mod, "gp_table", table), \
         patch.object(assign_mod, "PLAYER_TOKEN_SECRET", ACCESS_SECRET), \
         patch.object(assign_mod, "_get_game_meta", return_value=mobile_meta("STACK")), \
         patch.object(assign_mod, "_find_existing_player_by_psid", return_value=None), \
         patch.object(assign_mod, "_put_player", side_effect=table.put), \
         patch.object(assign_mod, "_ensure_game_counters"), \
         patch.object(assign_mod, "_reserve_player_slot", return_value=True), \
         patch.object(assign_mod, "_get_last_player_id", return_value=6), \
         patch.object(assign_mod, "_send_bulk_messages"):
        result = assign_mod._handle_instagram_assign({
            "game_id": "ABCDEF", "psid": "psid7", "username_at": "@player"})
    assert result["ok"]
    stack = table.items[("ABCDEF", 7)]["type"]["STACK"]
    assert stack["raffleEligible"] is False
    assert not stack.get("attempt")