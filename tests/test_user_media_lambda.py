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

def test_profile_photo_upload_is_separate_from_logos_and_legacy_avatars():
    table = _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://upload"
    response = mod.lambda_handler(_event("POST", {
        "files": [{"kind": "profile-photo", "fileName": "photo.png", "contentType": "image/png", "sizeBytes": 100}],
    }, {"action": "upload-url"}), None)
    assert response["statusCode"] == 200
    entry = json.loads(response["body"])["uploadUrls"][0]
    assert entry["key"].startswith("users/user-1/profile-photos/")
    params = mod.s3.generate_presigned_url.call_args.kwargs["Params"]
    assert params["ContentLength"] == 100
    assert params["ContentType"] == "image/png"
    table.update_item.assert_not_called()


@pytest.mark.parametrize("content_type,size", [
    ("image/svg+xml", 100), ("video/mp4", 100), ("image/png", 0), ("image/png", -1),
    ("image/png", 5 * 1024 * 1024 + 1),
])
def test_rejects_invalid_profile_photo_uploads(content_type, size):
    _setup_tables()
    mod.s3 = MagicMock()
    response = mod.lambda_handler(_event("POST", {
        "files": [{"kind": "profile-photo", "fileName": "photo", "contentType": content_type, "sizeBytes": size}],
    }, {"action": "upload-url"}), None)
    assert response["statusCode"] == 400
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


@pytest.mark.parametrize(("content_type", "size", "status"), [
    ("image/png", 5 * 1024 * 1024, 200),
    ("video/mp4", 100 * 1024 * 1024, 200),
    ("video/webm", 100 * 1024 * 1024, 200),
    ("video/mp4", 100 * 1024 * 1024 + 1, 400),
    ("image/jpeg", 5 * 1024 * 1024 + 1, 400),
    ("video/quicktime", 100, 400),
    ("image/png", 0, 400),
    ("image/png", 1.5, 400),
    ("video/mp4", True, 400),
])
def test_home_background_upload_urls_enforce_type_and_exact_size_limits(content_type, size, status):
    _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://upload"
    response = mod.lambda_handler(_event("POST", {
        "files": [{
            "kind": "home-background", "fileName": "scene", "contentType": content_type,
            "sizeBytes": size, "name": "event",
        }],
    }, {"action": "upload-url"}), None)
    assert response["statusCode"] == status
    if status == 200:
        entry = json.loads(response["body"])["uploadUrls"][0]
        assert entry["kind"] == "home-background"
        assert entry["meta"]["kind"] == "home-background"
        assert entry["key"].startswith("users/user-1/home-backgrounds/")
        params = mod.s3.generate_presigned_url.call_args.kwargs["Params"]
        assert params["ContentLength"] == size
        assert params["ContentType"] == content_type
    else:
        mod.s3.generate_presigned_url.assert_not_called()


def test_register_home_background_checks_owner_object_type_and_size_without_touching_logo():
    table = _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.head_object.return_value = {"ContentType": "video/webm", "ContentLength": 1200}
    item = {
        "id": "background-1", "kind": "home-background",
        "name": "stage", "fileName": "stage.webm", "contentType": "video/webm",
        "sizeBytes": 1200, "key": "users/user-1/home-backgrounds/stage.webm",
    }
    response = mod.lambda_handler(_event("POST", {"items": [item]}), None)
    assert response["statusCode"] == 200
    saved = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":media"]
    assert saved["logo"] == {"items": [], "selectedId": None}
    assert saved["home-background"]["items"][0]["sizeBytes"] == 1200
    assert mod.s3.head_object.call_args.kwargs["Key"] == item["key"]


@pytest.mark.parametrize(("key", "actual_type", "size"), [
    ("users/other/home-backgrounds/a.png", "image/png", 100),
    ("users/user-1/home-backgrounds/a.png", "image/jpeg", 100),
    ("users/user-1/home-backgrounds/a.png", "image/png", 5 * 1024 * 1024 + 1),
])
def test_register_rejects_unowned_or_invalid_home_background_objects(key, actual_type, size):
    table = _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.head_object.return_value = {"ContentType": actual_type, "ContentLength": size}
    response = mod.lambda_handler(_event("POST", {"items": [{
        "id": "bad", "kind": "home-background", "contentType": "image/png", "key": key,
    }]}), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


@pytest.mark.parametrize(("actual_size", "status"), [
    (100 * 1024 * 1024, 200),
    (100 * 1024 * 1024 + 1, 400),
])
def test_register_enforces_exact_video_object_size_limit(actual_size, status):
    table = _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.head_object.return_value = {"ContentType": "video/mp4", "ContentLength": actual_size}
    response = mod.lambda_handler(_event("POST", {"items": [{
        "id": "clip", "kind": "home-background", "contentType": "video/mp4",
        "key": "users/user-1/home-backgrounds/clip.mp4",
    }]}), None)
    assert response["statusCode"] == status
    if status == 400:
        table.update_item.assert_not_called()


def test_get_returns_separate_home_background_library_and_delete_does_not_change_logo():
    table = _setup_tables()
    media = mod._empty_media()
    media["logo"]["items"] = [{"id": "logo-1", "key": "users/user-1/logos/logo.png"}]
    media["logo"]["selectedId"] = "logo-1"
    media["home-background"]["items"] = [{
        "id": "background-1", "key": "users/user-1/home-backgrounds/background.png",
        "name": "background",
    }, {
        "id": "foreign", "key": "users/other/home-backgrounds/foreign.png",
    }]
    table.get_item.return_value = {"Item": {"userId": "user-1", "media": media}}
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://get"

    response = mod.lambda_handler(_event("GET"), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["home-background"]["items"][0]["url"] == "https://get"
    assert len(payload["home-background"]["items"]) == 1
    assert payload["logo"]["selected"]["id"] == "logo-1"

    response = mod.lambda_handler(_event("DELETE", {
        "kind": "home-background", "mediaId": "background-1",
    }), None)
    assert response["statusCode"] == 200
    saved = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":media"]
    assert saved["home-background"]["items"] == []
    assert saved["logo"]["selectedId"] == "logo-1"
