"""Account-scoped Kivly connection endpoint tests."""
import json
from unittest.mock import MagicMock

import pytest

from stand_common import kivly_settings


def account_event(body=None):
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}}}
    if body is not None:
        event["body"] = json.dumps(body)
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


def test_game_settings_ignore_old_per_game_club_configuration(monkeypatch):
    games = MagicMock()
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: {"locationId": "location"})
    monkeypatch.setattr(kivly_settings, "_verify_connection", lambda *_: type("Context", (), {
        "location_name": "Connected club",
    })())
    event = account_event()
    event["queryStringParameters"] = {"gameId": "GAME1"}
    meta = {"ownerUserId": "owner", "gameType": "STACK"}
    games.get_item.return_value = {"Item": meta}
    response = kivly_settings.handle(event, "GET", MagicMock(), games)
    assert json.loads(response["body"])["settings"]["clubName"] == "Connected club"
    meta["kivly"] = {
        "enabled": False, "clubName": "Saved club",
        "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms",
    }
    response = kivly_settings.handle(event, "GET", MagicMock(), games)
    assert json.loads(response["body"])["settings"]["clubName"] == "Connected club"
    assert json.loads(response["body"])["settings"]["enabled"] is False


@pytest.mark.parametrize("game_type", ["STACK", "T1MER", "L3TRAS", "EMPAREJA2", "IMPOSTOR", "RULET4", "INFOCARDS", "SEMAFORO"])
def test_global_connection_only_requires_club_registration_for_stack(monkeypatch, game_type):
    conn = {
        "locationId": "location",
        "clubSettings": {"clubName": "Club", "privacyUrl": "https://club.test/privacy", "termsUrl": "https://club.test/terms"},
    }
    get_connection = MagicMock(return_value=conn)
    monkeypatch.setattr(kivly_settings.registration, "connection", get_connection)
    meta = {"ownerUserId": "owner", "gameType": game_type, "kivly": {"enabled": False}}
    effective = kivly_settings.registration.apply_account_settings(MagicMock(), meta)
    assert meta["kivly"]["enabled"] is False
    if game_type == "STACK":
        assert effective["kivly"]["enabled"] is True
        assert effective["kivly"]["locationId"] == "location"
        assert effective["participantFields"]["email"] == {"required": True}
        assert get_connection.call_count == 1
        assert kivly_settings.registration.game_settings(effective)["enabled"] is True
    else:
        assert effective["kivly"]["enabled"] is False
        assert effective.get("participantFields", {}) == meta.get("participantFields", {})
        assert get_connection.call_count == 0
        assert kivly_settings.registration.game_settings(effective)["enabled"] is False


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
    return {"ownerUserId": "owner", "kivly": settings, "participantFields": {"email": {"required": True}}}


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
    monkeypatch.setattr(kivly_settings.registration, "connection", lambda *_: {"locationId": "location"})
    monkeypatch.setattr(kivly_settings.registration, "_identity", lambda *_: "customer-key")
    table, client = MagicMock(), MagicMock()
    table.get_item.return_value = {"Item": {"state": "confirmed", "customerId": "customer-1"}}
    monkeypatch.setattr(kivly_settings.registration, "_table", lambda *_: table)
    monkeypatch.setattr(kivly_settings.registration, "_client", client)
    result = kivly_settings.registration.ensure_membership(
        MagicMock(), game,
        {"email": "test@club.test", "loyaltyConsentVersion": game["kivly"]["consentVersion"]},
    )
    assert result["customerId"] == "customer-1"
    assert result["locationId"] == "location"
    client.assert_not_called()
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
