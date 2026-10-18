import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock
import pytest

ROOT = Path(__file__).parent.parent
LAMBDA_PATH = ROOT / "src" / "stand_prod_user_fn_media" / "lambda_function.py"

spec = importlib.util.spec_from_file_location("user_media_lambda", LAMBDA_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mod)


def _event(method="GET", body=None, qs=None):
    return {
        "httpMethod": method,
        "queryStringParameters": qs or {},
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "user-1"}}}},
        "body": json.dumps(body) if body is not None else None,
    }


def _setup_tables():
    table = MagicMock()
    table.get_item.return_value = {"Item": {"userId": "user-1", "media": mod._empty_media()}}
    mod.dynamodb = MagicMock()
    mod.dynamodb.Table.return_value = table
    mod.USERS_TABLE = "users"
    mod.USER_MEDIA_BUCKET = "bucket"
    return table


def test_upload_url_rejects_avatar_videos():
    _setup_tables()
    mod.s3 = MagicMock()

    response = mod.lambda_handler(
        _event(
            method="POST",
            qs={"action": "upload-url"},
            body={
                "files": [
                    {
                        "kind": "avatar",
                        "state": "win",
                        "fileName": "avatar-win.mp4",
                        "contentType": "video/mp4",
                        "sizeBytes": 1024,
                        "name": "avatar_main",
                    }
                ]
            },
        ),
        None,
    )
    payload = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert payload["error"] == "kind must be 'logo'."
    mod.s3.generate_presigned_url.assert_not_called()


@pytest.mark.parametrize(("method", "body"), [
    ("POST", {"items": [{"kind": "avatar", "id": "a1", "key": "users/u/avatars/a.mp4"}]}),
    ("PUT", {"kind": "avatar", "mediaId": "a1", "state": "idle"}),
    ("DELETE", {"kind": "avatar", "mediaId": "a1"}),
])
def test_avatar_media_mutations_are_rejected(method, body):
    table = _setup_tables()
    response = mod.lambda_handler(_event(method=method, body=body), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_select_logo_requires_existing_media_id():
    table = _setup_tables()
    response = mod.lambda_handler(
        _event(method="PUT", body={"kind": "logo", "mediaId": "missing"}),
        None,
    )
    assert response["statusCode"] == 404
    table.update_item.assert_not_called()


@pytest.mark.parametrize("colors,mode", [
    ({"primary": "#ff1234", "accent": "#1234FF"}, "light"),
    (None, "dark"),
])
def test_appearance_is_persisted_and_returned_with_media(colors, mode):
    table = _setup_tables()
    media = mod._empty_media()
    media["logo"]["items"] = [{"id": "client", "key": "users/user-1/logos/logo.png"}]
    media["logo"]["selectedId"] = "client"
    media["avatar"] = {"items": [{"id": "old", "key": "users/user-1/avatars/old.mp4"}]}
    table.get_item.return_value = {"Item": {"userId": "user-1", "media": media}}
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://get"
    appearance = {"colors": colors, "themeMode": mode}
    response = mod.lambda_handler(_event("PUT", {"kind": "appearance", "appearance": appearance}), None)
    assert response["statusCode"] == 200
    saved = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":media"]
    assert saved["appearance"] == appearance
    assert saved["logo"] == media["logo"]
    assert saved["avatar"] == media["avatar"]
    table.get_item.return_value = {"Item": {"userId": "user-1", "media": saved}}
    response = mod.lambda_handler(_event("GET"), None)
    assert json.loads(response["body"])["appearance"] == appearance


@pytest.mark.parametrize("appearance", [
    None, {}, {"themeMode": "invalid"},
    {"themeMode": "dark", "colors": {"primary": "#123456"}},
    {"themeMode": "dark", "colors": {"primary": "red", "accent": "#123456"}},
    {"themeMode": "dark", "colors": {"primary": "#123456", "accent": 123}},
])
def test_appearance_rejects_invalid_values_without_writes(appearance):
    table = _setup_tables()
    response = mod.lambda_handler(_event("PUT", {"kind": "appearance", "appearance": appearance}), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_get_returns_logo_without_avatar_media():
    table = _setup_tables()
    media = mod._empty_media()
    media["logo"]["items"] = [{"id": "l1", "key": "users/u/logos/a.png", "name": "a"}]
    media["logo"]["selectedId"] = "l1"
    media["avatar"] = {
        "items": [{"id": "a1", "key": "users/u/avatars/v.mp4", "name": "v", "state": "idle"}],
        "selectedByState": {"idle": "a1"},
    }
    table.get_item.return_value = {"Item": {"userId": "user-1", "media": media}}
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://get"

    response = mod.lambda_handler(_event(method="GET"), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["logo"]["selected"]["id"] == "l1"
    assert "avatar" not in payload


def test_get_returns_logo_defaults_when_no_user_logo_selected():
    _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://get"

    response = mod.lambda_handler(_event(method="GET"), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["logo"]["selected"] is None
    assert payload["logo"]["defaults"]["dark"]["key"] == mod.DEFAULT_STAND_LOGO_DARK_KEY
    assert payload["logo"]["defaults"]["light"]["key"] == mod.DEFAULT_STAND_LOGO_LIGHT_KEY
    assert payload["logo"]["defaults"]["dark"]["url"] == "https://get"
