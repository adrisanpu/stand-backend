"""Account-scoped Kivly connection endpoint tests."""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from stand_common import kivly_settings
from tests.email_fake import FakeDynamoError

read_connection = kivly_settings.registration.connection


def test_reward_creation_is_exposed_as_an_authenticated_sam_route():
    template = (Path(__file__).resolve().parents[1] / "template.yaml").read_text()
    route = template.split("        GameKivlyRewardsPost:\n", 1)[1].split("        StackPrizesGet:", 1)[0]
    assert "Type: HttpApi" in route
    assert "ApiId: !Ref GameApi" in route
    assert "Path: /v1/game/kivly/rewards" in route
    assert "Method: POST" in route
    assert "Authorizer: NONE" not in route
    assert "DefaultAuthorizer: CognitoAuthorizer" in template


def account_event(body=None):
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}}}
    if body is not None:
        event["body"] = json.dumps(body)
    return event


def game_event(game_id, body=None):
    event = account_event(body)
    if body is None:
        event["queryStringParameters"] = {"gameId": game_id}
    return event


def test_account_get_returns_connection_without_game_lookup(monkeypatch):
    dynamo = MagicMock()
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: None)

    response = kivly_settings.handle_account(account_event(), "GET", dynamo)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {
        "ok": True, "connection": {
            "connected": False, "locationId": None, "locations": [], "clubSettings": None,
        },
    }
    dynamo.Table.assert_not_called()


def test_account_put_saves_credentials_without_game_id(monkeypatch):
    dynamo, users, secrets = MagicMock(), MagicMock(), MagicMock()
    secrets.create_secret.return_value = {"ARN": "secret-arn"}
    monkeypatch.setenv("USERS_TABLE", "users")
    monkeypatch.setenv("STAGE", "test")
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: None)
    monkeypatch.setattr(kivly_settings.boto3, "client", lambda *_: secrets)
    dynamo.Table.return_value = users

    class FakeKivlyClient:
        def __init__(self, client_id, client_secret):
            assert (client_id, client_secret) == ("client-id", "client-secret")

        def get_context(self):
            return type("Context", (), {"location_id": "location", "location_name": "Location club", "owner_id": "kivly-owner"})()

        def _customers(self, location_id, owner_id, page):
            assert (location_id, owner_id, page) == ("location", "kivly-owner", 1)

    monkeypatch.setattr(kivly_settings, "KivlyClient", FakeKivlyClient)
    response = kivly_settings.handle_account(
        account_event({"clientId": "client-id", "clientSecret": "client-secret"}),
        "PUT",
        dynamo,
    )

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"ok": True}
    assert json.loads(secrets.create_secret.call_args.kwargs["SecretString"]) == {
        "client_id": "client-id", "client_secret": "client-secret",
    }
    users.update_item.assert_called_once()
    saved = users.update_item.call_args.kwargs["ExpressionAttributeValues"][":new"]
    assert saved["locationId"] == "location"
    assert saved["ownerId"] == "kivly-owner"
    assert saved["clubSettings"] == {
        "clubName": "Location club",
        "privacyUrl": "https://playstand.app/pages/privacy-es.html",
        "termsUrl": "https://playstand.app/pages/legal.html",
    }


def test_account_get_exposes_only_authorized_saved_location(monkeypatch):
    conn = {"locationId": "location", "ownerId": "kivly-owner", "secretArn": "private", "version": "v1"}
    client = MagicMock()
    client.get_context.return_value = type("Context", (), {
        "location_id": "location", "location_name": "Test club", "owner_id": "kivly-owner",
    })()
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)
    response = kivly_settings.handle_account(account_event(), "GET", MagicMock())
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"ok": True, "connection": {
        "connected": True, "locationId": "location",
        "locations": [{"id": "location", "name": "Test club"}],
        "clubSettings": None,
    }}


def test_account_get_rejects_location_outside_saved_scope(monkeypatch):
    conn = {"locationId": "saved-location", "ownerId": "kivly-owner"}
    client = MagicMock()
    client.get_context.return_value = type("Context", (), {
        "location_id": "other-location", "owner_id": "kivly-owner",
    })()
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)
    response = kivly_settings.handle_account(account_event(), "GET", MagicMock())
    assert response["statusCode"] == 409
    client._customers.assert_not_called()


def test_game_settings_use_location_saved_on_account():
    settings = kivly_settings.registration.prepare_settings(
        {"gameType": "STACK"},
        {"enabled": True, "clubName": "Club",
         "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"},
        {"locationId": "saved-location"},
    )
    assert settings["locationId"] == "saved-location"
    assert settings["consentVersion"]


@pytest.mark.parametrize("host", ["example.com", "club.example.com"])
def test_demo_legal_links_cannot_enable_real_registration(host):
    with pytest.raises(kivly_settings.registration.RegistrationError):
        kivly_settings.registration.prepare_settings(
            {"gameType": "STACK"},
            {"enabled": True, "clubName": "Demo",
             "privacyUrl": f"https://{host}/privacy", "termsUrl": "https://club.test/terms"},
            {"locationId": "saved-location"},
        )


def test_demo_details_can_be_saved_with_registration_disabled():
    settings = kivly_settings.registration.prepare_settings(
        {"gameType": "STACK"},
        {"enabled": False, "clubName": "Demo",
         "privacyUrl": "https://example.com/privacy", "termsUrl": "https://example.com/terms"},
        None,
    )
    assert settings["enabled"] is False


def test_game_settings_ignore_old_per_game_club_configuration():
    meta = {"ownerUserId": "owner", "gameType": "STACK"}
    meta["kivly"] = {
        "enabled": False, "clubName": "Saved club",
        "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms",
    }
    settings = kivly_settings.registration.account_game_settings(meta, {"locationId": "location"})
    assert settings == {"enabled": False, "clubName": "", "privacyUrl": "", "termsUrl": ""}


@pytest.mark.parametrize("game_type", ["STACK", "T1MER", "L3TRAS", "EMPAREJA2", "IMPOSTOR", "RULET4", "INFOCARDS", "SEMAFORO"])
def test_account_connection_applies_only_to_stack_without_game_opt_in(monkeypatch, game_type):
    conn = {
        "locationId": "location",
        "clubSettings": {"clubName": "Club", "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"},
    }
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    meta = {"ownerUserId": "owner", "gameType": game_type, "kivly": {"enabled": False}}
    effective = kivly_settings.registration.apply_account_settings(MagicMock(), meta)
    assert effective["kivly"]["enabled"] is (game_type == "STACK")
    if game_type == "STACK":
        assert effective["kivly"]["locationId"] == "location"
        assert effective["participantFields"]["email"] == {"required": True}
    else:
        assert "locationId" not in effective["kivly"]
    assert meta["kivly"]["enabled"] is False
    assert kivly_settings.registration.game_settings(effective)["enabled"] is (game_type == "STACK")


def test_global_disconnect_disables_existing_game_requirement(monkeypatch):
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: None)
    effective = kivly_settings.registration.apply_account_settings(MagicMock(), {
        "ownerUserId": "owner", "gameType": "STACK", "kivly": {"enabled": True},
    })
    assert effective["kivly"]["enabled"] is False


def test_account_saves_shared_club_without_replacing_credentials(monkeypatch):
    monkeypatch.setenv("USERS_TABLE", "users")
    conn = {"locationId": "location", "ownerId": "kivly-owner", "secretArn": "secret", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    dynamo = MagicMock()
    club = {"clubName": "Shared club", "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"}
    response = kivly_settings.handle_account(account_event({"clubSettings": club}), "PUT", dynamo)
    assert response["statusCode"] == 200
    update = dynamo.Table.return_value.update_item.call_args.kwargs
    assert update["ExpressionAttributeValues"][":settings"] == club
    assert update["ExpressionAttributeValues"][":old"] == "v1"
    assert update["ConditionExpression"] == "#c.#v = :old"
    assert "secretArn" not in update["UpdateExpression"]


def test_shared_storefront_save_and_get_preserve_legal_consent_and_membership(monkeypatch):
    registration = kivly_settings.registration
    monkeypatch.setenv("USERS_TABLE", "users")
    club = {"clubName": "Club", "privacyUrl": "https://club.test/privacy",
            "termsUrl": "https://club.test/terms"}
    conn = {"locationId": "location", "ownerId": "kivly-owner", "secretArn": "secret",
            "version": "v1", "clubSettings": club}
    monkeypatch.setattr(registration, "connection", lambda *_: conn)
    dynamo = MagicMock()
    before = registration.account_game_settings({"gameType": "STACK"}, conn)
    response = kivly_settings.handle_account(account_event({
        "clubSettings": {**club, "storefrontUrl": "https://shared.kivly.store/"},
    }), "PUT", dynamo)
    assert response["statusCode"] == 200
    values = dynamo.Table.return_value.update_item.call_args.kwargs["ExpressionAttributeValues"]
    conn.update(clubSettings=values[":settings"], version=values[":new"])
    after = registration.account_game_settings({"gameType": "STACK"}, conn)
    assert after["consentVersion"] == before["consentVersion"]
    game = {"ownerUserId": "owner", "gameType": "STACK", "kivly": after,
            "participantFields": {"email": {"required": True}}}
    registration.require_player(dynamo, game, {"kivlyMembership": {
        "customerId": "customer", "locationId": "location", "consentVersion": before["consentVersion"],
    }})
    client = MagicMock()
    client.get_context.return_value = type("Context", (), {
        "location_id": "location", "owner_id": "kivly-owner", "location_name": "Club",
    })()
    monkeypatch.setattr(registration, "_client", lambda *_: client)
    response = kivly_settings.handle_account(account_event(), "GET", dynamo)
    assert json.loads(response["body"])["connection"]["clubSettings"]["storefrontUrl"] == "https://shared.kivly.store"
    conn["clubSettings"]["termsUrl"] = "https://club.test/new-terms"
    assert registration.account_game_settings({"gameType": "STACK"}, conn)["consentVersion"] != before["consentVersion"]


@pytest.mark.parametrize("value", [None, False, 5, "http://shared.kivly.store", "https://kivly.store",
                                  "https://.kivly.store", "https://a.kivly.store.evil.test",
                                  "https://a.kivly.store/add-to-wallet", "https://a.kivly.store?x=y",
                                  "https://user@a.kivly.store", "https://a.kivly.store:444"])
def test_account_rejects_unsafe_shared_storefront_without_writing(monkeypatch, value):
    conn = {"locationId": "location", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    dynamo = MagicMock()
    response = kivly_settings.handle_account(account_event({"clubSettings": {
        "clubName": "Club", "privacyUrl": "https://club.test/privacy",
        "termsUrl": "https://club.test/terms", "storefrontUrl": value,
    }}), "PUT", dynamo)
    assert response["statusCode"] == 400
    dynamo.Table.assert_not_called()


@pytest.mark.parametrize("supplied,expected", [(None, "https://shared.kivly.store"), ("", "")])
def test_old_club_save_preserves_shared_storefront_and_explicit_empty_clears(monkeypatch, supplied, expected):
    monkeypatch.setenv("USERS_TABLE", "users")
    club = {"clubName": "Club", "privacyUrl": "https://club.test/privacy",
            "termsUrl": "https://club.test/terms", "storefrontUrl": "https://shared.kivly.store"}
    conn = {"locationId": "location", "version": "v1", "clubSettings": club}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    raw = {k: v for k, v in club.items() if k != "storefrontUrl"}
    if supplied is not None:
        raw["storefrontUrl"] = supplied
    dynamo = MagicMock()
    response = kivly_settings.handle_account(account_event({"clubSettings": raw}), "PUT", dynamo)
    assert response["statusCode"] == 200
    assert dynamo.Table.return_value.update_item.call_args.kwargs[
        "ExpressionAttributeValues"][":settings"]["storefrontUrl"] == expected


def test_account_club_save_requires_connection(monkeypatch):
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: None)
    dynamo = MagicMock()
    response = kivly_settings.handle_account(account_event({"clubSettings": {}}), "PUT", dynamo)
    assert response["statusCode"] == 409
    dynamo.Table.assert_not_called()


def test_game_activation_uses_shared_club_not_stale_form_values():
    club = {"clubName": "Shared club", "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"}
    result = kivly_settings.registration.prepare_settings(
        {"gameType": "STACK"},
        {"enabled": True, "clubName": "Stale", "privacyUrl": "https://example.com/privacy"},
        {"locationId": "location", "clubSettings": club},
    )
    assert {key: result[key] for key in club} == club
    assert result["locationId"] == "location"


@pytest.mark.parametrize("conn", [None, {}, {"locationId": None}, {"locationId": ""}, {"locationId": " "}])
def test_activation_requires_a_connected_location(conn):
    with pytest.raises(kivly_settings.registration.RegistrationError) as error:
        kivly_settings.registration.prepare_settings(
            {"gameType": "STACK"},
            {"enabled": True, "clubName": "Club",
             "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"},
            conn,
        )
    assert error.value.code == "KivlyConnectionRequired"


def membership_game():
    settings = kivly_settings.registration.prepare_settings(
        {"gameType": "STACK"},
        {"enabled": True, "clubName": "Club",
         "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"},
        {"locationId": "location"},
    )
    return {"ownerUserId": "owner", "gameType": "STACK", "kivly": settings,
            "participantFields": {"email": {"required": True}}}


@pytest.mark.parametrize("conn,code", [
    (None, "KivlyConnectionRequired"),
    ({"locationId": "different-location"}, "KivlyActivationUnverified"),
])
def test_membership_still_requires_the_games_connected_location(monkeypatch, conn, code):
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    with pytest.raises(kivly_settings.registration.RegistrationError) as error:
        kivly_settings.registration._required_connection(MagicMock(), membership_game())
    assert error.value.code == code


def test_membership_still_requires_current_consent(monkeypatch):
    game = membership_game()
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: {"locationId": "location"})
    with pytest.raises(kivly_settings.registration.RegistrationError) as error:
        kivly_settings.registration.ensure_membership(MagicMock(), game, {"loyaltyConsentVersion": "old"})
    assert error.value.code == "KivlyConsentRequired"
    with pytest.raises(kivly_settings.registration.RegistrationError) as error:
        kivly_settings.registration.require_player(MagicMock(), game, {})
    assert error.value.code == "KivlyMembershipRequired"


def test_confirmed_membership_is_reused_without_external_creation(monkeypatch):
    game = membership_game()
    conn = {"locationId": "location", "ownerId": "kivly-owner", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_identity", lambda *_: "customer-key")
    table, client = MagicMock(), MagicMock()
    table.get_item.return_value = {"Item": {"state": "confirmed", "customerId": "customer-1"}}
    monkeypatch.setattr(kivly_settings.registration, "_table", lambda *_: table)
    client.find_customer.return_value = {"id": "customer-1", "is_active": True}
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)
    result = kivly_settings.registration.ensure_membership(
        MagicMock(), game,
        {"email": "test@club.test", "loyaltyConsentVersion": game["kivly"]["consentVersion"]},
    )
    assert result["customerId"] == "customer-1"
    assert result["locationId"] == "location"
    client.find_customer.assert_called_once_with("location", "kivly-owner", "test@club.test")
    client.create_customer.assert_not_called()
    table.put_item.assert_not_called()


@pytest.mark.parametrize("phone", [None, "+34 612345678"])
def test_new_membership_is_created_once_for_the_connected_business(monkeypatch, phone):
    game = membership_game()
    conn = {"locationId": "location", "ownerId": "kivly-owner", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_identity", lambda *_: "customer-key")
    table, client = MagicMock(), MagicMock()
    table.get_item.return_value = {}
    client.find_customer.return_value = None
    client.create_customer.return_value = {"id": "customer-1", "is_active": True}
    monkeypatch.setattr(kivly_settings.registration, "_table", lambda *_: table)
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)

    result = kivly_settings.registration.ensure_membership(
        MagicMock(), game,
        {"email": "test@club.test", "username": "Test",
         **({"phone": phone} if phone else {}),
         "loyaltyConsentVersion": game["kivly"]["consentVersion"]},
    )

    intent = table.put_item.call_args.kwargs
    assert intent["ConditionExpression"] == "attribute_not_exists(id)"
    assert intent["Item"]["state"] == "creating"
    client.create_customer.assert_called_once_with(
        "location", "kivly-owner", "Test", "test@club.test", intent["Item"]["externalId"],
        **({"phone": phone} if phone else {}),
    )
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":s"] == "confirmed"
    assert result["customerId"] == "customer-1"
    kivly_settings.registration.require_player(MagicMock(), game, {"kivlyMembership": result})


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
def test_account_requires_authenticated_owner_before_any_lookup(method):
    dynamo = MagicMock()
    response = kivly_settings.handle_account({}, method, dynamo)
    assert response["statusCode"] == 401
    dynamo.Table.assert_not_called()


@pytest.mark.parametrize("body", [None, {}, {"clientId": "id"}, {"clientId": "id", "clientSecret": 123}])
def test_account_rejects_invalid_credential_form_without_external_calls(monkeypatch, body):
    client = MagicMock()
    monkeypatch.setattr(kivly_settings, "KivlyClient", client)
    response = kivly_settings.handle_account(account_event(body), "PUT", MagicMock())
    assert response["statusCode"] == 400
    client.assert_not_called()


def test_disconnect_removes_only_connection_and_schedules_secret_cleanup(monkeypatch):
    monkeypatch.setenv("USERS_TABLE", "users")
    conn = {"secretArn": "private", "locationId": "location", "ownerId": "owner", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    secrets, dynamo = MagicMock(), MagicMock()
    monkeypatch.setattr(kivly_settings.boto3, "client", lambda *_: secrets)
    response = kivly_settings.handle_account(account_event(), "DELETE", dynamo)
    assert json.loads(response["body"]) == {"ok": True}
    update = dynamo.Table.return_value.update_item.call_args.kwargs
    assert update["UpdateExpression"] == "REMOVE #c"
    assert update["ConditionExpression"] == "#c.#v = :old"
    secrets.delete_secret.assert_called_once_with(SecretId="private", RecoveryWindowInDays=7)
    dynamo.Table.return_value.delete_item.assert_not_called()


def test_failed_secret_cleanup_is_reported_without_reverting_disconnect(monkeypatch):
    monkeypatch.setenv("USERS_TABLE", "users")
    conn = {"secretArn": "private", "locationId": "location", "ownerId": "owner", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    secrets, dynamo = MagicMock(), MagicMock()
    secrets.delete_secret.side_effect = FakeDynamoError("AccessDeniedException")
    monkeypatch.setattr(kivly_settings.boto3, "client", lambda *_: secrets)
    response = kivly_settings.handle_account(account_event(), "DELETE", dynamo)
    assert json.loads(response["body"]) == {"ok": True, "warning": "KivlySecretCleanupRequired"}
    dynamo.Table.return_value.update_item.assert_called_once()


def test_concurrent_account_change_is_a_conflict(monkeypatch):
    monkeypatch.setenv("USERS_TABLE", "users")
    conn = {"secretArn": "private", "locationId": "location", "ownerId": "owner", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    dynamo = MagicMock()
    dynamo.Table.return_value.update_item.side_effect = FakeDynamoError("ConditionalCheckFailedException")
    response = kivly_settings.handle_account(account_event(), "DELETE", dynamo)
    assert response["statusCode"] == 409
    assert json.loads(response["body"])["error"] == "KivlyConnectionChanged"


@pytest.mark.parametrize("key,value", [
    ("clubName", ""), ("privacyUrl", ""), ("termsUrl", "javascript:invalid"),
    ("privacyUrl", "https://user:password@club.test/privacy"),
])
def test_invalid_shared_club_settings_cannot_break_public_registration(monkeypatch, key, value):
    conn = {"locationId": "location", "version": "v1"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    dynamo = MagicMock()
    club = {"clubName": "Club", "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"}
    response = kivly_settings.handle_account(
        account_event({"clubSettings": {**club, key: value}}), "PUT", dynamo,
    )
    assert response["statusCode"] == 400
    dynamo.Table.assert_not_called()


def test_connection_reads_only_private_owner_setting_consistently(monkeypatch):
    monkeypatch.setenv("USERS_TABLE", "users")
    conn = {"secretArn": "private", "locationId": "location", "ownerId": "owner", "version": "v1"}
    dynamo = MagicMock()
    dynamo.Table.return_value.get_item.return_value = {"Item": {"kivlyConnection": conn}}
    assert read_connection(dynamo, "owner") == conn
    dynamo.Table.return_value.get_item.assert_called_once_with(
        Key={"userId": "owner"}, ConsistentRead=True, ProjectionExpression="kivlyConnection",
    )


def test_connection_read_failure_cannot_silently_disable_club(monkeypatch):
    monkeypatch.setenv("USERS_TABLE", "users")
    dynamo = MagicMock()
    dynamo.Table.return_value.get_item.side_effect = FakeDynamoError("ProvisionedThroughputExceededException")
    with pytest.raises(kivly_settings.registration.RegistrationError, match="KivlyUnavailable"):
        read_connection(dynamo, "owner")


def test_generic_user_response_hides_private_connection():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "src" / "stand_prod_user_fn_user" / "lambda_function.py"
    spec = importlib.util.spec_from_file_location("kivly_user_redaction_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module._normalize_user({"userId": "owner", "kivlyConnection": {"secretArn": "private"}})
    assert result["userId"] == "owner"
    assert "kivlyConnection" not in result


def test_game_reward_catalog_is_limited_to_stack_owner_and_connected_account(monkeypatch):
    dynamo, games, client = MagicMock(), MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": membership_game()}
    client.list_rewards.return_value = [{"id": "reward-1", "name": "Coffee"}]
    conn = {"locationId": "location", "ownerId": "kivly-owner"}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)

    response = kivly_settings.handle_game(game_event("GAME1"), "GET", dynamo, games, catalog=True)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"ok": True, "rewards": client.list_rewards.return_value}
    client.list_rewards.assert_called_once_with("location", "kivly-owner")


def test_game_reward_catalog_creates_reward_for_connected_stack_owner(monkeypatch):
    dynamo, games, client = MagicMock(), MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": membership_game()}
    client.create_reward.return_value = {"id": "reward-1", "name": "Coffee"}
    conn = {"locationId": "location", "ownerId": "kivly-owner"}
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "true")
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)
    reward = {
        "title": "Coffee", "description": "A coffee", "rewardType": "free_item",
        "value": 1, "pointsRequired": 25,
    }

    response = kivly_settings.handle_game(
        game_event("GAME1", {"gameId": "GAME1", **reward}),
        "POST", dynamo, games, catalog=True,
    )

    assert response["statusCode"] == 201
    assert json.loads(response["body"]) == {
        "ok": True, "reward": {"id": "reward-1", "name": "Coffee"},
    }
    client.create_reward.assert_called_once_with("location", "kivly-owner", reward)


def test_game_reward_creation_requires_connection_and_enabled_external_writes(monkeypatch):
    dynamo, games, client = MagicMock(), MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": membership_game()}
    reward = {
        "gameId": "GAME1", "title": "Coffee", "description": "A coffee",
        "rewardType": "free_item", "value": 1, "pointsRequired": 25,
    }
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: None)
    response = kivly_settings.handle_game(
        game_event("GAME1", reward), "POST", dynamo, games, catalog=True,
    )
    assert response["statusCode"] == 409
    client.create_reward.assert_not_called()

    monkeypatch.setattr(
        kivly_settings.registration, "connection",
        lambda *_: {"locationId": "location", "ownerId": "kivly-owner"},
    )
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "false")
    response = kivly_settings.handle_game(
        game_event("GAME1", reward), "POST", dynamo, games, catalog=True,
    )
    assert response["statusCode"] == 503
    assert json.loads(response["body"])["error"] == "KivlyWritesDisabled"
    client.create_reward.assert_not_called()


def test_game_reward_creation_rejects_unexpected_fields(monkeypatch):
    dynamo, games, client = MagicMock(), MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": membership_game()}
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "true")
    monkeypatch.setattr(
        kivly_settings.registration, "connection",
        lambda *_: {"locationId": "location", "ownerId": "kivly-owner"},
    )
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)

    response = kivly_settings.handle_game(
        game_event("GAME1", {
            "gameId": "GAME1", "title": "Coffee", "description": "A coffee",
            "rewardType": "free_item", "value": 1, "pointsRequired": 25,
            "locationId": "untrusted-location",
        }),
        "POST", dynamo, games, catalog=True,
    )

    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "KivlyRewardInvalid"
    client.create_reward.assert_not_called()


def test_game_reward_creation_exposes_uncertain_result_without_retry(monkeypatch):
    dynamo, games, client = MagicMock(), MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": membership_game()}
    client.create_reward.side_effect = kivly_settings.registration.KivlyError(
        "KivlyRewardCreationUncertain", 503,
    )
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "true")
    monkeypatch.setattr(
        kivly_settings.registration, "connection",
        lambda *_: {"locationId": "location", "ownerId": "kivly-owner"},
    )
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)

    response = kivly_settings.handle_game(
        game_event("GAME1", {
            "gameId": "GAME1", "title": "Coffee", "description": "A coffee",
            "rewardType": "free_item", "value": 1, "pointsRequired": 25,
        }),
        "POST", dynamo, games, catalog=True,
    )

    assert response["statusCode"] == 503
    assert json.loads(response["body"]) == {
        "ok": False, "error": "KivlyRewardCreationUncertain",
    }
    client.create_reward.assert_called_once()


def test_game_reward_selection_validates_catalog_and_saves_only_stack_reward(monkeypatch):
    dynamo, games, client = MagicMock(), MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": membership_game()}
    client.list_rewards.return_value = [{"id": "reward-1", "name": "Coffee"}]
    conn = {"locationId": "location", "ownerId": "kivly-owner", "clubSettings": {
        "clubName": "Club", "privacyUrl": "https://club.test/privacy",
        "termsUrl": "https://club.test/terms",
    }}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(kivly_settings.registration, "_client", lambda *_: client)

    response = kivly_settings.handle_game(
        game_event("GAME1", {"gameId": "GAME1", "rewardId": "reward-1"}),
        "PUT", dynamo, games,
    )

    assert response["statusCode"] == 200
    values = games.update_item.call_args.kwargs["ExpressionAttributeValues"]
    assert values[":reward"] == {"id": "reward-1", "name": "Coffee", "locationId": "location"}
    assert values[":owner"] == "owner"
    client.list_rewards.assert_called_once_with("location", "kivly-owner")


def test_game_reward_catalog_rejects_other_games_and_disconnected_account(monkeypatch):
    dynamo, games = MagicMock(), MagicMock()
    games.get_item.return_value = {"Item": {"ownerUserId": "owner", "gameType": "RULET4"}}
    response = kivly_settings.handle_game(game_event("GAME1"), "GET", dynamo, games, catalog=True)
    assert response["statusCode"] == 400
    dynamo.Table.assert_not_called()

    games.get_item.return_value = {"Item": membership_game()}
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: None)
    response = kivly_settings.handle_game(game_event("GAME1"), "GET", dynamo, games, catalog=True)
    assert response["statusCode"] == 409
