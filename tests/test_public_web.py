"""Tests for participant tokens and public web join helpers."""
import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))

from stand_common.utils import (  # noqa: E402
    issue_player_token,
    verify_player_token,
    issue_prize_token,
    verify_prize_token,
    is_valid_email,
    is_valid_join_code,
    normalize_email,
    mask_email,
    default_participant_fields,
)

ASSIGN_DIR = ROOT / "src" / "stand_prod_game_fn_assign"
sys.path.insert(0, str(ASSIGN_DIR))
ASSIGN_PATH = ASSIGN_DIR / "lambda_function.py"
_spec = importlib.util.spec_from_file_location("assign_lambda", ASSIGN_PATH)
assign_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(assign_mod)

PUBLIC_DIR = ROOT / "src" / "stand_prod_game_fn_public"
sys.path.insert(0, str(PUBLIC_DIR))
PUBLIC_PATH = PUBLIC_DIR / "lambda_function.py"
_pspec = importlib.util.spec_from_file_location("public_lambda", PUBLIC_PATH)
public_mod = importlib.util.module_from_spec(_pspec)
assert _pspec.loader is not None
# boto3 tables init at import — patch resource before load
with patch("boto3.resource"), patch("boto3.client"):
    _pspec.loader.exec_module(public_mod)

SECRET = "test-secret"
public_mod.PLAYER_TOKEN_SECRET = SECRET


@pytest.fixture
def photo_game(monkeypatch):
    monkeypatch.setattr(public_mod, "_get_game", lambda _: {"gameType": "STACK"})


@pytest.mark.parametrize("code", ["K9R4QM", "012345"])
def test_valid_join_codes(code):
    assert is_valid_join_code(code) is True


@pytest.mark.parametrize("code", ["12345", "1234567", "I23456", "12345 ", ""])
def test_invalid_join_codes(code):
    assert is_valid_join_code(code) is False


@pytest.mark.parametrize("selected,key,expected", [
    ("client", "users/owner/logos/client.png", "signed:client"),
    (None, "users/owner/logos/client.png", None),
    ("missing", "users/owner/logos/client.png", None),
    ("client", "users/someone-else/logos/client.png", None),
])
def test_public_game_exposes_only_selected_owner_logo(selected, key, expected, monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    monkeypatch.setenv("USERS_TABLE", "users")
    table = MagicMock()
    table.get_item.return_value = {"Item": {"media": {"logo": {
        "selectedId": selected, "items": [{"id": "client", "key": key}],
    }}}}
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "signed:client"
    meta = {"gameId": "ABC123", "gameType": "EMPAREJA2", "ownerUserId": "owner"}
    with patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod.dynamo_r, "Table", return_value=table), \
         patch.object(public_mod.boto3, "client", return_value=s3):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["clientLogoUrl"] == expected
    assert "media" not in payload
    assert "ownerUserId" not in payload
    if selected:
        table.get_item.assert_called_once_with(Key={"userId": "owner"}, ProjectionExpression="media")
    if expected:
        s3.generate_presigned_url.assert_called_once_with(
            "get_object", Params={"Bucket": "media", "Key": key}, ExpiresIn=3600,
        )
    else:
        s3.generate_presigned_url.assert_not_called()


def test_public_stack_game_exposes_only_the_selected_reward_name(monkeypatch):
    meta = {
        "gameId": "ABC123", "gameType": "STACK", "ownerUserId": "owner",
        "kivlyReward": {"id": "private-reward-id", "name": "Coffee", "locationId": "location"},
    }
    monkeypatch.setattr(public_mod, "_get_game", lambda *_: meta)
    monkeypatch.setattr(public_mod.kivly_registration, "game_settings", lambda *_: {
        "enabled": True, "clubName": "Club", "privacyUrl": "https://club.test/privacy",
        "termsUrl": "https://club.test/terms", "consentVersion": "version",
    })
    monkeypatch.setattr(public_mod.kivly_registration, "connection", lambda *_: {"locationId": "location"})
    monkeypatch.setattr(public_mod, "stack_game_config", lambda *_: {})
    monkeypatch.setattr(public_mod, "stack_ranking", lambda *_args, **_kwargs: {"results": []})
    monkeypatch.setattr(public_mod, "_client_appearance", lambda *_: {
        "clientLogoUrl": None, "appearance": {"colors": None, "themeMode": "dark"},
    })

    response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})

    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    assert payload["kivlyReward"] == {"name": "Coffee"}
    assert "private-reward-id" not in response["body"]


def test_public_logo_without_owner_does_not_read_users(monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    with patch.object(public_mod.dynamo_r, "Table") as table:
        assert public_mod._client_appearance({}) == {
            "clientLogoUrl": None, "appearance": {"colors": None, "themeMode": "dark"},
        }
    table.assert_not_called()


def test_public_game_hides_legacy_instagram_registration_field():
    meta = {
        "gameId": "ABC123",
        "gameType": "OTHER",
        "participantFields": {
            "email": {"required": False},
            "instagram": {"required": True},
            "phone": {"required": True},
        },
    }
    with patch.object(public_mod, "_get_game", return_value=meta):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["participantFields"] == {
        "email": {"required": True},
        "phone": {"required": False, "enabled": True},
    }


def test_public_player_display_ignores_legacy_instagram_values():
    player = {
        "playerId": 7,
        "username": "Web player",
        "instagramUsername": "@legacy",
        "instagramProfilePicUrl": "https://example.invalid/legacy.png",
    }
    assert public_mod._player_display_label(player) == "Web player"
    assert public_mod._player_profile_pic_url(player) is None


def test_public_appearance_uses_owner_colors_and_logo(monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    monkeypatch.setenv("USERS_TABLE", "users")
    appearance = {"colors": {"primary": "#ff0000", "accent": "#00ff00"}, "themeMode": "light"}
    table = MagicMock()
    table.get_item.return_value = {"Item": {"media": {
        "appearance": appearance,
        "logo": {"selectedId": "chosen", "items": [{"id": "chosen", "key": "users/owner/logos/custom.png"}]},
    }}}
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "signed:custom"
    with patch.object(public_mod.dynamo_r, "Table", return_value=table), \
         patch.object(public_mod.boto3, "client", return_value=s3):
        result = public_mod._client_appearance({"ownerUserId": "owner"})
    assert result == {"clientLogoUrl": "signed:custom", "appearance": appearance}


def test_public_game_adds_signed_prize_catalog_at_top_level(monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    key = "games/owner/ABC123/ranking/01234567-89ab-cdef-0123-456789abcdef.png"
    meta = {
        "gameId": "ABC123",
        "gameType": "T1MER",
        "ownerUserId": "owner",
        "prizeCatalog": {"ranking": {"name": "Top", "limit": 3, "imageKey": key}},
    }
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "signed:prize"
    with patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "_client_appearance", return_value={
             "clientLogoUrl": None, "appearance": {"colors": None, "themeMode": "dark"},
         }), \
         patch.object(public_mod, "_t1mer_ranking", return_value=[]), \
         patch.object(public_mod.boto3, "client", return_value=s3):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["prizeCatalog"]["ranking"]["imageUrl"] == "signed:prize"
    assert "game" not in payload
    s3.generate_presigned_url.assert_called_once_with(
        "get_object", Params={"Bucket": "media", "Key": key}, ExpiresIn=3600,
    )


def test_player_response_adds_catalog_and_keeps_won_raffle_prize(monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    game_id = "ABC123"
    validation_key = (
        "games/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    )
    raffle_key = "games/owner/ABC123/raffle/11234567-89ab-cdef-0123-456789abcdef.png"
    meta = {
        "gameId": game_id,
        "gameType": "EMPAREJA2",
        "ownerUserId": "owner",
        "prizeCatalog": {
            "validation": {"name": "Validado", "limit": 3, "imageKey": validation_key},
            "raffle": {"name": "Sorteo", "limit": None, "imageKey": raffle_key},
        },
    }
    player = {
        "playerId": 7,
        "validated": True,
        "type": {"EMPAREJA2": {"raffleWin": True}},
    }
    token = issue_player_token(game_id, 7, SECRET)
    s3 = MagicMock()
    s3.generate_presigned_url.side_effect = ["signed:validation", "signed:raffle"]
    with patch.object(public_mod, "_get_player", return_value=player), \
         patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod.boto3, "client", return_value=s3):
        response = public_mod._handle_get_player({
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"gameId": game_id, "playerToken": token}),
        })
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["prizeCatalog"]["validation"]["imageUrl"] == "signed:validation"
    assert payload["prizeCatalog"]["raffle"]["imageUrl"] == "signed:raffle"
    assert {prize["kind"] for prize in payload["prizes"]} == {"validation", "raffle"}


@pytest.mark.parametrize(("game_type", "slot"), [
    ("EMPAREJA2", "validation"),
    ("T1MER", "ranking"),
    ("STACK", "ranking"),
])
def test_mobile_player_game_types_return_configured_catalog(game_type, slot, monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    meta = {
        "gameId": "ABC123",
        "gameType": game_type,
        "ownerUserId": "owner",
        "prizeCatalog": {slot: {"name": "Gift", "limit": None, "imageKey": None}},
    }
    with patch.object(public_mod, "stack_public_assignment", return_value={}), \
         patch.object(public_mod, "_t1mer_ranking", return_value=[]):
        response = public_mod._player_response(meta, {"playerId": 1}, "ABC123", 1, "token")
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["prizeCatalog"][slot]["imageUrl"] is None


def test_legacy_player_response_omits_prize_catalog():
    player = {"playerId": 7, "validated": True, "type": {"EMPAREJA2": {}}}
    token = issue_player_token("ABC123", 7, SECRET)
    with patch.object(public_mod, "_get_player", return_value=player), \
         patch.object(public_mod, "_get_game", return_value={"gameType": "EMPAREJA2"}):
        response = public_mod._handle_get_player({
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"gameId": "ABC123", "playerToken": token}),
        })
    assert response["statusCode"] == 200
    assert "prizeCatalog" not in json.loads(response["body"])


@pytest.mark.parametrize("game_type", ["EMPAREJA2", "T1MER", "STACK"])
def test_all_mobile_games_publish_owner_appearance(game_type):
    branding = {"clientLogoUrl": None, "appearance": {
        "colors": {"primary": "#ff0000", "accent": "#00ff00"}, "themeMode": "dark",
    }}
    slot = "validation" if game_type == "EMPAREJA2" else "ranking"
    with patch.object(public_mod, "_get_game", return_value={
             "gameId": "ABC123",
             "gameType": game_type,
             "prizeCatalog": {slot: {"name": "Gift", "limit": None, "imageKey": None}},
         }), \
         patch.object(public_mod, "_client_appearance", return_value=branding), \
         patch.object(public_mod, "_t1mer_ranking", return_value=[]), \
         patch.object(public_mod, "stack_game_config", return_value={}), \
         patch.object(public_mod, "stack_ranking", return_value={"results": []}):
        response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["appearance"] == branding["appearance"]
    assert json.loads(response["body"])["prizeCatalog"][slot]["imageUrl"] is None


def test_web_participant_can_request_a_scoped_photo_upload(monkeypatch, photo_game):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    token = issue_player_token("ABC123", 7, SECRET)
    s3 = MagicMock()
    s3.generate_presigned_post.return_value = {
        "url": "signed:post",
        "fields": {"key": "players/ABC123/7/profile-photo.png", "policy": "signed"},
    }
    with patch.object(public_mod, "_get_player", return_value={"joinSource": "web"}), \
         patch.object(public_mod.boto3, "client", return_value=s3):
        response = public_mod._handle_player_photo({"body": json.dumps({
            "action": "upload-url",
            "playerToken": token,
            "contentType": "image/png",
            "sizeBytes": 1024,
        })})

    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["uploadUrl"] == "signed:post"
    assert payload["uploadFields"]["policy"] == "signed"
    assert payload["photoKey"].startswith("players/ABC123/7/profile-")
    s3.generate_presigned_post.assert_called_once_with(
        Bucket="media",
        Key=payload["photoKey"],
        Fields={"Content-Type": "image/png"},
        Conditions=[
            {"Content-Type": "image/png"},
            ["content-length-range", 1, 5 * 1024 * 1024],
        ],
        ExpiresIn=300,
    )


@pytest.mark.parametrize(("image_type", "size", "error"), [
    ("image/gif", 100, "UnsupportedPhotoType"),
    ("image/png", 0, "InvalidPhotoSize"),
    ("image/png", 5 * 1024 * 1024 + 1, "InvalidPhotoSize"),
])
def test_photo_upload_rejects_unsupported_or_oversized_images(monkeypatch, image_type, size, error, photo_game):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    token = issue_player_token("ABC123", 7, SECRET)
    with patch.object(public_mod, "_get_player", return_value={"joinSource": "web"}):
        response = public_mod._handle_player_photo({"body": json.dumps({
            "action": "upload-url", "playerToken": token,
            "contentType": image_type, "sizeBytes": size,
        })})
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == error


def test_only_web_participants_can_upload_profile_photos():
    token = issue_player_token("ABC123", 7, SECRET)
    with patch.object(public_mod, "_get_player", return_value={"instagramPSID": "psid"}):
        response = public_mod._handle_player_photo({"body": json.dumps({
            "action": "upload-url", "playerToken": token,
            "contentType": "image/png", "sizeBytes": 10,
        })})
    assert response["statusCode"] == 403
    assert json.loads(response["body"])["error"] == "WebPhotoUploadOnly"


def test_photo_completion_validates_image_and_persists_s3_key(monkeypatch, photo_game):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    token = issue_player_token("ABC123", 7, SECRET)
    photo_key = "players/ABC123/7/profile-01234567-89ab-cdef-0123-456789abcdef.png"
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentType": "image/png", "ContentLength": 12}
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"\x89PNG\r\n\x1a\nxxxx"))}
    s3.generate_presigned_url.return_value = "signed:get"
    update_item = MagicMock()
    with patch.object(public_mod, "_get_player", return_value={"joinSource": "web"}), \
         patch.object(public_mod.boto3, "client", return_value=s3), \
         patch.object(public_mod.gp_table, "update_item", update_item):
        response = public_mod._handle_player_photo({"body": json.dumps({
            "action": "complete", "playerToken": token, "photoKey": photo_key,
        })})

    assert response["statusCode"] == 200
    assert json.loads(response["body"])["profilePicUrl"] == "signed:get"
    update_item.assert_called_once_with(
        Key={"gameId": "ABC123", "playerId": 7},
        UpdateExpression="SET participantPhotoKey = :key",
        ConditionExpression="attribute_exists(playerId) AND attribute_not_exists(participantPhotoKey)",
        ExpressionAttributeValues={":key": photo_key},
    )
    s3.get_object.assert_called_once_with(Bucket="media", Key=photo_key, Range="bytes=0-11")


def test_photo_completion_rejects_non_image_bytes(monkeypatch, photo_game):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    token = issue_player_token("ABC123", 7, SECRET)
    photo_key = "players/ABC123/7/profile-01234567-89ab-cdef-0123-456789abcdef.png"
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentType": "image/png", "ContentLength": 12}
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"not-an-image"))}
    update_item = MagicMock()
    with patch.object(public_mod, "_get_player", return_value={"joinSource": "web"}), \
         patch.object(public_mod.boto3, "client", return_value=s3), \
         patch.object(public_mod.gp_table, "update_item", update_item):
        response = public_mod._handle_player_photo({"body": json.dumps({
            "action": "complete", "playerToken": token, "photoKey": photo_key,
        })})
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "InvalidPhoto"
    update_item.assert_not_called()


def test_player_response_returns_signed_photo_for_play(monkeypatch):
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    player = {
        "playerId": 7,
        "joinSource": "web",
        "participantPhotoKey": "players/ABC123/7/profile-photo.png",
        "type": {"T1MER": {}},
    }
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "signed:profile-photo"
    with patch.object(public_mod.boto3, "client", return_value=s3):
        response = public_mod._player_response(
            {"gameType": "T1MER"}, player, "ABC123", 7, "player-token",
        )
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["assignment"]["profilePicUrl"] == "signed:profile-photo"
    s3.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={"Bucket": "media", "Key": player["participantPhotoKey"]},
        ExpiresIn=3600,
    )


@pytest.mark.parametrize("partner", ["Bob", None])
def test_web_assignment_includes_partner_image(partner):
    player = {
        "playerId": 7, "validationCode": 1234,
        "type": {"EMPAREJA2": {"characterName": "Alice", "partnerCharacterName": partner}},
    }
    with patch("assigners.empareja2._character_image_url", side_effect=lambda name: f"signed:{name}") as image:
        result = assign_mod._empareja2_web_assignment(player)
    assert result["characterImageUrl"] == "signed:Alice"
    assert result["partnerCharacterImageUrl"] == ("signed:Bob" if partner else None)
    assert image.call_count == (2 if partner else 1)


@pytest.mark.parametrize("bucket", ["characters", ""])
def test_recovered_empareja2_assignment_includes_partner_image(bucket, monkeypatch):
    monkeypatch.setenv("CHAR_BUCKET", bucket)
    player = {
        "playerId": 7, "validationCode": 1234,
        "type": {"EMPAREJA2": {"characterName": "Alice", "partnerCharacterName": "Bob"}},
    }
    s3 = MagicMock()
    s3.generate_presigned_url.side_effect = lambda operation, **kwargs: f"signed:{kwargs['Params']['Key']}"
    token = issue_player_token("ABC123", 7, SECRET)
    with patch.object(public_mod, "_get_player", return_value=player), \
         patch.object(public_mod, "_get_game", return_value={"gameType": "EMPAREJA2"}), \
         patch.object(public_mod.boto3, "client", return_value=s3):
        response = public_mod._handle_get_player({
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"gameId": "ABC123", "playerToken": token}),
        })
    assert response["statusCode"] == 200
    assignment = json.loads(response["body"])["assignment"]
    assert assignment["characterImageUrl"] == ("signed:Alice.png" if bucket else None)
    assert assignment["partnerCharacterImageUrl"] == ("signed:Bob.png" if bucket else None)
    assert s3.generate_presigned_url.call_count == (2 if bucket else 0)


def test_partner_image_signing_failure_keeps_own_image_and_logs(monkeypatch):
    monkeypatch.setenv("CHAR_BUCKET", "characters")
    player = {"type": {"EMPAREJA2": {"characterName": "Alice", "partnerCharacterName": "Bob"}}}
    s3 = MagicMock()
    s3.generate_presigned_url.side_effect = ["signed:Alice.png", ValueError("Cannot sign")]
    with patch.object(public_mod.boto3, "client", return_value=s3), \
         patch.object(public_mod, "ClientError", RuntimeError), \
         patch.object(public_mod, "BotoCoreError", RuntimeError), \
         patch.object(public_mod, "log") as log:
        result = public_mod._empareja2_assignment_from_player(player)
    assert result["characterImageUrl"] == "signed:Alice.png"
    assert result["partnerCharacterImageUrl"] is None
    log.assert_called_once_with("character_image_url_error", {"error": "Cannot sign"})


def test_player_token_roundtrip():
    token = issue_player_token("ABC123", 7, SECRET, ttl_seconds=3600)
    claims = verify_player_token(token, SECRET)
    assert claims == {"gameId": "ABC123", "playerId": 7}


def test_player_token_rejects_bad_sig():
    token = issue_player_token("ABC123", 1, SECRET)
    bad = token[:-1] + ("a" if token[-1] != "a" else "b")
    assert verify_player_token(bad, SECRET) is None


def test_prize_token_roundtrip():
    token = issue_prize_token("ABC123", 7, "v", SECRET, ttl_seconds=3600)
    claims = verify_prize_token(token, SECRET)
    assert claims == {"gameId": "ABC123", "playerId": 7, "kind": "v"}

    raffle = issue_prize_token("ABC123", 7, "r", SECRET, ttl_seconds=3600)
    assert verify_prize_token(raffle, SECRET)["kind"] == "r"
    assert raffle != token


def test_prize_token_rejects_bad_sig():
    token = issue_prize_token("ABC123", 1, "v", SECRET)
    bad = token[:-1] + ("a" if token[-1] != "a" else "b")
    assert verify_prize_token(bad, SECRET) is None


def test_prize_token_rejects_invalid_kind():
    with pytest.raises(ValueError):
        issue_prize_token("ABC123", 1, "x", SECRET)


def test_email_helpers():
    assert is_valid_email("ana@example.com")
    assert not is_valid_email("not-an-email")
    assert normalize_email(" Ana@Example.COM ") == "ana@example.com"
    assert mask_email("ana@example.com").startswith("a")


def test_default_participant_fields_empareja2():
    pf = default_participant_fields("EMPAREJA2")
    assert pf["email"]["required"] is True
    assert pf["instagram"]["required"] is False


def test_default_participant_fields_t1mer():
    pf = default_participant_fields("T1MER")
    assert pf["email"]["required"] is True
    assert pf["instagram"]["required"] is False


@patch.object(assign_mod, "_get_game_meta", return_value={"gameType": "EMPAREJA2"})
def test_web_assign_requires_email(_mock_meta):
    out = assign_mod._handle_web_assign({"source": "web", "game_id": "ABC123", "email": ""})
    assert out["ok"] is False
    assert out["reason"] == "missing_email"


@patch.object(assign_mod, "_get_game_meta", return_value=None)
def test_web_assign_game_not_found(mock_meta):
    out = assign_mod._handle_web_assign({"source": "web", "game_id": "ABC123", "email": "a@b.com"})
    assert out["reason"] == "game_not_found"


@pytest.mark.usefixtures("verified_web_assignment")
@patch.object(assign_mod, "_find_existing_player_by_email", return_value=None)
@patch.object(assign_mod, "_empareja2_web_assignment", return_value={"characterName": "Alice", "validationCode": 1234, "validated": False})
@patch.object(assign_mod, "_put_player")
@patch.object(assign_mod, "_reserve_player_slot", return_value=True)
@patch.object(assign_mod, "_ensure_game_counters")
@patch.object(assign_mod, "_get_last_player_id", return_value=0)
@patch.object(assign_mod, "ASSIGNERS")
@patch.object(assign_mod, "_get_game_meta")
def test_web_assign_creates_player(mock_meta, mock_assigners, *_mocks):
    mock_meta.return_value = {
        "gameId": "ABC123",
        "gameType": "EMPAREJA2",
        "isActive": True,
        "maxPlayers": 25,
        "participantFields": default_participant_fields("EMPAREJA2"),
    }
    mock_assigner = MagicMock(return_value={
        "type": {"EMPAREJA2": {"characterName": "Alice", "partnerCharacterName": "Bob", "pairGroupId": "1", "characterId": 1}},
    })
    mock_assigners.get.return_value = mock_assigner

    out = assign_mod._handle_web_assign({
        "source": "web",
        "game_id": "ABC123",
        "email": "player@example.com",
        "username": "Player",
    })
    assert out["ok"] is True
    assert out["created_new"] is True
    assert out["playerToken"]
    assert out["assignment"]["characterName"] == "Alice"


@pytest.mark.usefixtures("verified_web_assignment")
@patch.object(assign_mod, "_find_existing_player_by_email", return_value=None)
@patch.object(assign_mod, "_put_player")
@patch.object(assign_mod, "_reserve_player_slot", return_value=True)
@patch.object(assign_mod, "_ensure_game_counters")
@patch.object(assign_mod, "_get_last_player_id", return_value=0)
@patch.object(assign_mod, "ASSIGNERS")
@patch.object(assign_mod, "_get_game_meta")
def test_web_assign_creates_t1mer_player(mock_meta, mock_assigners, *_mocks):
    mock_meta.return_value = {
        "gameId": "TMR001",
        "gameType": "T1MER",
        "isActive": True,
        "maxPlayers": 50,
        "participantFields": default_participant_fields("T1MER"),
    }
    mock_assigners.get.return_value = MagicMock(return_value={"type": {"T1MER": {}}})

    out = assign_mod._handle_web_assign({
        "source": "web",
        "game_id": "TMR001",
        "email": "timer@example.com",
        "username": "Timer player",
    })
    assert out["ok"] is True
    assert out["gameType"] == "T1MER"
    assert out["playerToken"]
    assert out["assignment"]["displayName"] == "Timer player"
    assert "playerId" in out["assignment"]


@patch.object(assign_mod, "_empareja2_web_assignment", return_value={"characterName": "X", "validationCode": 1234, "validated": False})
@patch.object(assign_mod, "_find_existing_player_by_email")
@patch.object(assign_mod, "_get_game_meta")
def test_web_assign_dedup_email(mock_meta, mock_find, _mock_assignment, verified_web_assignment):
    mock_meta.return_value = {
        "gameId": "ABC123",
        "gameType": "EMPAREJA2",
        "isActive": True,
        "participantFields": default_participant_fields("EMPAREJA2"),
    }
    mock_find.return_value = {
        "playerId": 3,
        "validationCode": 1234,
        "validated": False,
        "type": {"EMPAREJA2": {"characterName": "X", "partnerCharacterName": "Y", "characterId": 1}},
    }

    out = assign_mod._handle_web_assign({
            "source": "web",
            "game_id": "ABC123",
            "email": "player@example.com",
            "username": "Player",
        })
    assert out["ok"] is True
    assert out["already_joined"] is True


def _redeem_event(prize_token: str) -> dict:
    return {
        "requestContext": {"http": {"method": "POST"}},
        "rawPath": "/v1/public/redeem",
        "body": json.dumps({"prizeToken": prize_token}),
    }


def _score_event(body: dict) -> dict:
    return {
        "requestContext": {"http": {"method": "POST"}},
        "rawPath": "/v1/public/score",
        "body": json.dumps(body),
    }


def test_public_score_rejects_bad_token():
    resp = public_mod._handle_score(_score_event({
        "gameId": "ABCDEF",
        "playerToken": "not-a-token",
        "timer": "10.1",
        "score": 100,
    }))
    assert resp["statusCode"] == 403
    assert json.loads(resp["body"])["error"] == "InvalidPlayerToken"


def test_public_score_happy():
    mock_games = MagicMock()
    mock_gp = MagicMock()
    mock_games.get_item.return_value = {
        "Item": {"gameId": "ABCDEF", "gameType": "T1MER", "isActive": True}
    }
    mock_gp.get_item.return_value = {
        "Item": {"gameId": "ABCDEF", "playerId": 2, "email": "a@b.com"}
    }
    token = issue_player_token("ABCDEF", 2, SECRET)
    public_mod.SCORE_LAMBDA = "score-fn"
    with patch.object(public_mod, "games_table", mock_games), \
         patch.object(public_mod, "gp_table", mock_gp), \
         patch.object(public_mod, "_invoke_score_store", return_value={"ok": True, "rank": 1, "score": 100}) as mock_inv:
        resp = public_mod._handle_score(_score_event({
            "gameId": "ABCDEF",
            "playerToken": token,
            "timer": "10.100",
            "score": 100,
        }))
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["ok"] is True
    assert body["rank"] == 1
    mock_inv.assert_called_once_with("ABCDEF", 2, "10.100", 100)


def test_redeem_validation_happy():
    mock_games = MagicMock()
    mock_gp = MagicMock()
    mock_games.get_item.return_value = {
        "Item": {"gameId": "ABC123", "gameType": "EMPAREJA2", "isActive": True}
    }
    mock_gp.get_item.return_value = {
        "Item": {
            "gameId": "ABC123",
            "playerId": 5,
            "validated": True,
            "email": "a@b.com",
            "type": {"EMPAREJA2": {}},
        }
    }
    token = issue_prize_token("ABC123", 5, "v", SECRET)
    with patch.object(public_mod, "games_table", mock_games), patch.object(public_mod, "gp_table", mock_gp):
        resp = public_mod._handle_redeem(_redeem_event(token))
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["ok"] is True
    assert body["kind"] == "validation"
    assert body["prize"] is None
    assert mock_gp.update_item.call_count == 3


def test_redeem_already_delivered():
    mock_games = MagicMock()
    mock_gp = MagicMock()
    mock_games.get_item.return_value = {
        "Item": {"gameId": "ABC123", "gameType": "EMPAREJA2", "isActive": True}
    }
    mock_gp.get_item.return_value = {
        "Item": {
            "gameId": "ABC123",
            "playerId": 5,
            "validated": True,
            "type": {"EMPAREJA2": {"validationPrizeDelivered": True}},
        }
    }
    token = issue_prize_token("ABC123", 5, "v", SECRET)
    with patch.object(public_mod, "games_table", mock_games), patch.object(public_mod, "gp_table", mock_gp):
        resp = public_mod._handle_redeem(_redeem_event(token))
    assert resp["statusCode"] == 409
    assert json.loads(resp["body"])["error"] == "AlreadyDelivered"


def test_redeem_invalid_token():
    resp = public_mod._handle_redeem(_redeem_event("not-a-token"))
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "InvalidPrizeToken"
