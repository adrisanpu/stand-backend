import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

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


def test_upload_url_for_avatar_contains_state_in_key():
    _setup_tables()
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_post.return_value = {
        "url": "https://upload",
        "fields": {"key": "test-key", "policy": "policy"},
    }

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
    assert response["statusCode"] == 200
    assert "__win__" in payload["uploadUrls"][0]["key"]


def test_select_logo_requires_existing_media_id():
    table = _setup_tables()
    response = mod.lambda_handler(
        _event(method="PUT", body={"kind": "logo", "mediaId": "missing"}),
        None,
    )
    assert response["statusCode"] == 404
    table.update_item.assert_not_called()


def test_get_returns_logo_and_avatar_selected_shape():
    table = _setup_tables()
    media = mod._empty_media()
    media["logo"]["items"] = [{"id": "l1", "key": "users/u/logos/a.png", "name": "a"}]
    media["logo"]["selectedId"] = "l1"
    media["avatar"]["items"] = [{"id": "a1", "key": "users/u/avatars/v.mp4", "name": "v", "state": "idle"}]
    media["avatar"]["selectedByState"]["idle"] = "a1"
    table.get_item.return_value = {"Item": {"userId": "user-1", "media": media}}
    mod.s3 = MagicMock()
    mod.s3.generate_presigned_url.return_value = "https://get"

    response = mod.lambda_handler(_event(method="GET"), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["logo"]["selected"]["id"] == "l1"
    assert payload["avatar"]["selected"]["idle"]["id"] == "a1"


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
