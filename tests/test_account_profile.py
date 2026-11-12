import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from tests.email_fake import FakeDynamoError

ROOT = Path(__file__).parent.parent
spec = importlib.util.spec_from_file_location(
    "account_profile_lambda", ROOT / "src" / "stand_prod_user_fn_user" / "lambda_function.py",
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
KEY = "users/user-1/profile-photos/12345678-1234-1234-1234-123456789abc.png"


def event(method="PUT", body=None, sub="user-1"):
    return {
        "httpMethod": method,
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": sub, "email": "ana@example.com"}}}},
        "body": json.dumps(body) if body is not None else None,
    }


@pytest.fixture
def table(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(mod, "USERS_TABLE", "users")
    monkeypatch.setattr(mod, "USER_MEDIA_BUCKET", "bucket")
    monkeypatch.setattr(mod, "ClientError", FakeDynamoError)
    monkeypatch.setattr(mod, "dynamodb", MagicMock())
    mod.dynamodb.Table.return_value = table
    monkeypatch.setattr(mod, "s3", MagicMock())
    mod.s3.head_object.return_value = {"ContentType": "image/png", "ContentLength": 100}
    mod.s3.generate_presigned_url.return_value = "https://signed-photo"
    table.update_item.return_value = {
        "Attributes": {"userId": "user-1", "name": "Ana", "profilePhotoKey": KEY, "plan": "ENTERPRISE"},
    }
    return table


def test_saves_name_and_verified_photo_without_replacing_other_account_data(table):
    response = mod.lambda_handler(event(body={"name": " Ana ", "profilePhotoKey": KEY}), None)
    assert response["statusCode"] == 200
    user = json.loads(response["body"])["user"]
    assert user["name"] == "Ana"
    assert user["plan"] == "ENTERPRISE"
    assert user["profilePicUrl"] == "https://signed-photo"
    mod.s3.head_object.assert_called_once_with(Bucket="bucket", Key=KEY)
    write = table.update_item.call_args.kwargs
    assert write["Key"] == {"userId": "user-1"}
    assert write["ExpressionAttributeValues"][":name"] == "Ana"
    assert "#plan=if_not_exists(#plan,:free)" in write["UpdateExpression"]
    assert "media" not in write["ExpressionAttributeNames"].values()
    assert "kivlyConnection" not in write["ExpressionAttributeNames"].values()
    table.put_item.assert_not_called()


def test_photo_removal_is_persisted_without_touching_logos(table):
    table.update_item.return_value = {"Attributes": {
        "userId": "user-1", "name": "Ana", "profilePhotoKey": None, "media": {"logo": {"selectedId": "logo"}},
    }}
    response = mod.lambda_handler(event(body={"name": "Ana", "profilePhotoKey": None}), None)
    assert response["statusCode"] == 200
    user = json.loads(response["body"])["user"]
    assert user["profilePicUrl"] is None
    assert user["media"]["logo"]["selectedId"] == "logo"
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":photo"] is None
    mod.s3.head_object.assert_not_called()


@pytest.mark.parametrize("body", [
    {"name": "", "profilePhotoKey": None},
    {"name": " ", "profilePhotoKey": None},
    {"name": "x" * 101, "profilePhotoKey": None},
    {"name": 123, "profilePhotoKey": None},
    {"name": "Ana", "profilePhotoKey": KEY.replace("user-1", "user-2")},
    {"name": "Ana", "profilePhotoKey": "users/user-1/logos/logo.png"},
    {"name": "Ana", "profilePhotoKey": "users/user-1/profile-photos/../logo.png"},
    {"name": "Ana", "profilePhotoKey": False},
    {"name": "Ana", "profilePhotoKey": None, "plan": "ENTERPRISE"},
    {"name": "Ana", "profilePhotoKey": None, "email": "other@example.com"},
    {"name": "Ana"},
    [],
])
def test_rejects_invalid_or_unowned_profile_fields(table, body):
    response = mod.lambda_handler(event(body=body), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


@pytest.mark.parametrize("metadata", [
    {"ContentType": "video/mp4", "ContentLength": 100},
    {"ContentType": "image/svg+xml", "ContentLength": 100},
    {"ContentType": "image/png", "ContentLength": 0},
    {"ContentType": "image/png", "ContentLength": 5 * 1024 * 1024 + 1},
])
def test_checks_uploaded_object_before_associating_it(table, metadata):
    mod.s3.head_object.return_value = metadata
    response = mod.lambda_handler(event(body={"name": "Ana", "profilePhotoKey": KEY}), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_missing_photo_is_a_visible_validation_error(table):
    mod.s3.head_object.side_effect = FakeDynamoError("NoSuchKey")
    response = mod.lambda_handler(event(body={"name": "Ana", "profilePhotoKey": KEY}), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_requires_authentication(table):
    response = mod.lambda_handler(event(body={"name": "Ana", "profilePhotoKey": None}, sub=None), None)
    assert response["statusCode"] == 401
    table.update_item.assert_not_called()


def test_get_resolves_persisted_photo_and_name(table):
    table.get_item.return_value = {"Item": {
        "userId": "user-1", "name": "Ana", "profilePhotoKey": KEY, "plan": "FREE",
    }}
    response = mod.lambda_handler(event("GET"), None)
    user = json.loads(response["body"])["user"]
    assert user["name"] == "Ana"
    assert user["profilePhotoKey"] == KEY
    assert user["profilePicUrl"] == "https://signed-photo"


def test_get_returns_claim_email_before_user_bootstrap(table):
    table.get_item.return_value = {}
    response = mod.lambda_handler(event("GET"), None)
    assert json.loads(response["body"])["user"]["email"] == "ana@example.com"


def test_database_failure_does_not_report_success(table):
    table.update_item.side_effect = RuntimeError("Unavailable")
    response = mod.lambda_handler(event(body={"name": "Ana", "profilePhotoKey": None}), None)
    assert response["statusCode"] == 500
