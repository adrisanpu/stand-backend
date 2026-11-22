import json
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from stand_common import credential_store, kivly_registration, kivly_settings
from tests.email_fake import FakeDynamoError


def test_read_json_selects_store_from_reference(monkeypatch):
    ssm, secrets = MagicMock(), MagicMock()
    ssm.get_parameter.return_value = {"Parameter": {"Value": '{"key":"parameter"}'}}
    secrets.get_secret_value.return_value = {"SecretString": '{"key":"secret"}'}
    monkeypatch.setattr(credential_store.boto3, "client", lambda service: {
        "ssm": ssm, "secretsmanager": secrets,
    }[service])

    assert credential_store.read_json("/stand/dev/stripe") == {"key": "parameter"}
    assert credential_store.read_json("stand/dev/stripe") == {"key": "secret"}
    ssm.get_parameter.assert_called_once_with(Name="/stand/dev/stripe", WithDecryption=True)
    secrets.get_secret_value.assert_called_once_with(SecretId="stand/dev/stripe")


def test_kivly_client_reads_and_caches_parameter_by_connection_version(monkeypatch):
    parameter = MagicMock()
    parameter.get_parameter.return_value = {"Parameter": {
        "Value": '{"client_id":"id","client_secret":"secret"}',
    }}
    monkeypatch.setattr(credential_store.boto3, "client", lambda service: parameter)
    constructor = MagicMock()
    monkeypatch.setattr(kivly_registration, "KivlyClient", constructor)
    kivly_registration._cached_client.cache_clear()
    try:
        conn = {"secretArn": "/stand/dev/kivly/owner/version", "version": "v1"}
        first = kivly_registration._client(conn)
        assert kivly_registration._client(conn) is first
        parameter.get_parameter.assert_called_once_with(Name=conn["secretArn"], WithDecryption=True)
        constructor.assert_called_once_with("id", "secret")
    finally:
        kivly_registration._cached_client.cache_clear()


def test_kivly_parameter_connection_and_disconnect(monkeypatch):
    ssm, dynamo = MagicMock(), MagicMock()
    monkeypatch.setenv("USERS_TABLE", "users")
    monkeypatch.setenv("STAGE", "dev")
    monkeypatch.setenv("KIVLY_CREDENTIALS_STORE", "parameter")
    monkeypatch.setattr(kivly_settings.boto3, "client", lambda service: ssm)
    client = MagicMock()
    client.get_context.return_value = type("Context", (), {
        "location_id": "location", "location_name": "Club", "owner_id": "owner",
    })()
    monkeypatch.setattr(kivly_settings, "KivlyClient", lambda *_: client)

    response = kivly_settings._change_connection(
        "owner", "PUT", {"clientId": "id", "clientSecret": "secret"}, dynamo, None,
    )
    assert response["statusCode"] == 200
    written = ssm.put_parameter.call_args.kwargs
    assert written["Name"].startswith("/stand/dev/kivly/")
    assert written["Type"] == "SecureString"
    assert written["Tier"] == "Standard"
    assert written["Overwrite"] is False
    assert json.loads(written["Value"]) == {"client_id": "id", "client_secret": "secret"}
    conn = dynamo.Table.return_value.update_item.call_args.kwargs["ExpressionAttributeValues"][":new"]
    assert conn["secretArn"] == written["Name"]

    response = kivly_settings._change_connection("owner", "DELETE", {}, dynamo, conn)
    assert response["statusCode"] == 200
    ssm.delete_parameter.assert_called_once_with(Name=written["Name"])


def test_kivly_parameter_conflict_cleans_unreferenced_parameter(monkeypatch):
    ssm, dynamo = MagicMock(), MagicMock()
    dynamo.Table.return_value.update_item.side_effect = FakeDynamoError("ConditionalCheckFailedException")
    monkeypatch.setenv("USERS_TABLE", "users")
    monkeypatch.setenv("STAGE", "dev")
    monkeypatch.setenv("KIVLY_CREDENTIALS_STORE", "parameter")
    monkeypatch.setattr(kivly_settings.boto3, "client", lambda service: ssm)
    client = MagicMock()
    client.get_context.return_value = type("Context", (), {
        "location_id": "location", "location_name": "Club", "owner_id": "owner",
    })()
    monkeypatch.setattr(kivly_settings, "KivlyClient", lambda *_: client)

    with pytest.raises(kivly_registration.RegistrationError, match="KivlyConnectionChanged"):
        kivly_settings._change_connection(
            "owner", "PUT", {"clientId": "id", "clientSecret": "secret"}, dynamo, None,
        )
    ssm.delete_parameter.assert_called_once_with(Name=ssm.put_parameter.call_args.kwargs["Name"])


def test_kivly_migration_keeps_previous_secrets_manager_secret(monkeypatch):
    ssm, dynamo = MagicMock(), MagicMock()
    monkeypatch.setenv("USERS_TABLE", "users")
    monkeypatch.setenv("STAGE", "dev")
    monkeypatch.setenv("KIVLY_CREDENTIALS_STORE", "parameter")
    monkeypatch.setattr(kivly_settings.boto3, "client", lambda service: ssm)
    client = MagicMock()
    client.get_context.return_value = type("Context", (), {
        "location_id": "location", "location_name": "Club", "owner_id": "owner",
    })()
    monkeypatch.setattr(kivly_settings, "KivlyClient", lambda *_: client)
    conn = {"secretArn": "legacy-secret-arn", "version": "old", "locationId": "location"}

    response = kivly_settings._change_connection(
        "owner", "PUT", {"clientId": "id", "clientSecret": "secret"}, dynamo, conn,
    )
    assert json.loads(response["body"]) == {"ok": True, "warning": "KivlySecretCleanupRequired"}
    ssm.delete_secret.assert_not_called()
    ssm.delete_parameter.assert_not_called()


@pytest.mark.parametrize("filename", [
    "stand_prod_billing_fn_checkout", "stand_prod_webhook_fn_stripe",
])
def test_stripe_loads_parameter_when_selected(monkeypatch, filename):
    monkeypatch.setenv("STRIPE_CREDENTIALS_STORE", "parameter")
    monkeypatch.setenv("STRIPE_SECRET_NAME", "stand/dev/stripe")
    monkeypatch.setenv("STRIPE_PARAMETER_NAME", "/stand/dev/stripe")
    reader = MagicMock(return_value={
        "STRIPE_SECRET_KEY": "key", "STRIPE_PRICE_ID": "price", "STRIPE_WEBHOOK_SECRET": "hook",
    })
    monkeypatch.setattr(credential_store, "read_json", reader)
    monkeypatch.setitem(sys.modules, "stripe", MagicMock())
    path = Path(__file__).resolve().parents[1] / "src" / filename / "lambda_function.py"
    spec = importlib.util.spec_from_file_location("credential_test_" + filename, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    reader.assert_called_once_with("/stand/dev/stripe")
    assert module.STRIPE_SECRET_KEY == "key"


def test_stripe_migration_is_non_destructive_and_explicit():
    path = Path(__file__).resolve().parents[1] / "scripts" / "migrate_stripe_parameter.py"
    spec = importlib.util.spec_from_file_location("stripe_migration_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    secrets, ssm, session = MagicMock(), MagicMock(), MagicMock()
    session.client.side_effect = lambda name: {"secretsmanager": secrets, "ssm": ssm}[name]
    secrets.get_secret_value.return_value = {"SecretString": '{"STRIPE_SECRET_KEY":"private"}'}

    assert module.migrate(session, "dev", False) == "/stand/dev/stripe"
    ssm.put_parameter.assert_not_called()
    assert module.migrate(session, "dev", True) == "/stand/dev/stripe"
    ssm.put_parameter.assert_called_once_with(
        Name="/stand/dev/stripe", Value='{"STRIPE_SECRET_KEY":"private"}',
        Type="SecureString", Tier="Standard", Overwrite=False,
    )
    secrets.delete_secret.assert_not_called()
