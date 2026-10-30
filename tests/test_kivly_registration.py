"""Offline integration coverage: email challenge -> club membership -> player."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, RLock
from unittest.mock import MagicMock

import pytest

from stand_common import kivly_registration as registration
from stand_common.kivly import KivlyError
from stand_common.stack import DEFAULT_CONFIG
from tests.email_fake import FakeDynamoError
from tests.test_email_verification import (
    GAME_ID, EMAIL, rig, request_code, join, sent_code, response_body, api_call,
)


class RegistrationTable:
    def __init__(self):
        self.items = {}
        self.lock = RLock()
        self.intents = []

    def get_item(self, Key, **kwargs):
        with self.lock:
            record = self.items.get(Key["id"])
            return {"Item": deepcopy(record)} if record else {}

    def put_item(self, Item, ConditionExpression, ExpressionAttributeValues=None, **kwargs):
        assert "expiresAt" not in Item
        with self.lock:
            current = self.items.get(Item["id"])
            if ConditionExpression == "attribute_not_exists(id)":
                valid = current is None
            else:
                assert ConditionExpression == "#s = :confirmed AND customerId = :previous"
                valid = (current is not None and current["state"] == "confirmed"
                         and current["customerId"] == ExpressionAttributeValues[":previous"])
            if not valid:
                raise FakeDynamoError("ConditionalCheckFailedException")
            self.items[Item["id"]] = deepcopy(Item)
            self.intents.append(deepcopy(Item))

    def update_item(self, Key, ExpressionAttributeValues, **kwargs):
        values = ExpressionAttributeValues
        with self.lock:
            record = self.items[Key["id"]]
            valid = (record["state"] == values[":expected"]
                     and (record.get("customerId") == values[":previous"] if ":previous" in values
                          else record.get("externalId") == values[":external"]))
            if not valid:
                raise FakeDynamoError("ConditionalCheckFailedException")
            record.update(state=values[":s"], customerId=values[":c"], confirmedAt=values[":at"])


@pytest.fixture
def club(rig, monkeypatch):
    conn = {
        "locationId": "location", "ownerId": "business-owner", "version": "v1",
        "secretArn": "private-arn",
        "clubSettings": {
            "clubName": "Club", "privacyUrl": "https://club.test/privacy",
            "termsUrl": "https://club.test/terms",
        },
    }
    monkeypatch.setattr(registration, "connection", lambda *_: conn)
    game = rig["dynamo"].rows("games")[0]
    game.update(ownerUserId="owner", gameType="STACK", participantFields={"phone": {"enabled": True}},
                type={"STACK": deepcopy(DEFAULT_CONFIG)})
    rig["dynamo"].seed("games", game)
    table, client = RegistrationTable(), MagicMock()
    client.find_customer.return_value = None
    def create_customer(*args, **kwargs):
        customer = {"id": "customer-1", "is_active": True, "external_id": args[4], "phone": kwargs.get("phone")}
        client.find_customer.return_value = deepcopy(customer)
        return customer

    client.create_customer.side_effect = create_customer
    monkeypatch.setattr(registration, "_table", lambda *_: table)
    monkeypatch.setattr(registration, "_client", lambda *_: client)
    settings = registration.account_game_settings(game, conn)
    return {**rig, "conn": conn, "table": table, "client": client, "settings": settings}


def club_request(club, **overrides):
    return request_code(**{"loyaltyConsentVersion": club["settings"]["consentVersion"],
                           "phone": "+34 612345678", **overrides})


def club_join(club, requested):
    return join(club, response_body(requested)["challengeId"], sent_code(club))


def test_public_metadata_has_only_registration_club_fields(club, monkeypatch):
    from tests.test_public_web import public_mod

    monkeypatch.setattr(public_mod, "_client_appearance", lambda *_: {})
    response = public_mod.lambda_handler({
        "rawPath": "/v1/public/game",
        "requestContext": {"http": {"method": "GET"}},
        "queryStringParameters": {"gameId": GAME_ID},
    }, None)
    assert response["statusCode"] == 200
    body = response_body(response)
    assert body["loyalty"] == {
        key: club["settings"][key] for key in ("clubName", "privacyUrl", "termsUrl", "consentVersion")
    }
    assert body["participantFields"]["email"] == {"required": True}
    assert body["participantFields"]["phone"] == {"required": True, "enabled": True}
    assert "secretArn" not in str(body)


def test_disconnected_stack_omits_loyalty_and_registers_without_external_calls(club, monkeypatch):
    from tests.test_public_web import public_mod

    monkeypatch.setattr(registration, "connection", lambda *_: None)
    monkeypatch.setattr(public_mod, "_client_appearance", lambda *_: {})
    metadata = public_mod.lambda_handler({
        "rawPath": "/v1/public/game", "requestContext": {"http": {"method": "GET"}},
        "queryStringParameters": {"gameId": GAME_ID},
    }, None)
    assert metadata["statusCode"] == 200
    assert "loyalty" not in response_body(metadata)
    response = club_join(club, request_code(phone="+34 612345678"))
    assert response["statusCode"] == 200
    assert "kivlyMembership" not in club["dynamo"].rows("players")[0]
    club["client"].create_customer.assert_not_called()
    club["client"].find_customer.assert_not_called()


def test_failed_stack_connection_lookup_is_not_silently_treated_as_disconnected(club, monkeypatch):
    def unavailable(*_):
        raise registration.RegistrationError("KivlyUnavailable")

    monkeypatch.setattr(registration, "connection", unavailable)
    response = request_code(phone="+34 612345678")
    assert response["statusCode"] == 503
    assert response_body(response)["error"] == "KivlyUnavailable"
    assert club["ses"].sent == []
    club["client"].create_customer.assert_not_called()


@pytest.mark.parametrize("version", [None, "", "stale"])
def test_club_consent_is_required_before_email_is_sent(club, version):
    response = club_request(club, **{"loyaltyConsentVersion": version})
    assert response["statusCode"] == 400
    assert response_body(response)["error"] == "KivlyConsentRequired"
    assert club["ses"].sent == []
    club["client"].assert_not_called()
    club["client"].create_customer.assert_not_called()


def test_only_verified_challenge_can_create_customer_and_player(club):
    requested = club_request(club, username="Full Name", phone="+34 612345678")
    assert requested["statusCode"] == 200
    club["client"].create_customer.assert_not_called()
    wrong_code = "000000" if sent_code(club) != "000000" else "999999"
    denied = join(club, response_body(requested)["challengeId"], wrong_code)
    assert denied["statusCode"] == 400
    club["client"].create_customer.assert_not_called()
    assert club["dynamo"].rows("players") == []

    response = club_join(club, requested)
    assert response["statusCode"] == 200
    player = club["dynamo"].rows("players")[0]
    assert player["username"] == "Full Name"
    assert player["email"] == EMAIL
    assert player["phone"] == "+34 612345678"
    assert player["emailVerificationMethod"] == "email-code"
    assert player["kivlyMembership"]["customerId"] == "customer-1"
    intent = club["table"].intents[0]
    assert intent["state"] == "creating"
    club["client"].create_customer.assert_called_once_with(
        "location", "business-owner", "Full Name", EMAIL, intent["externalId"], phone="+34 612345678",
    )
    assert next(iter(club["table"].items.values()))["state"] == "confirmed"

    again = club_join(club, requested)
    assert again["statusCode"] == 200
    assert response_body(again)["playerToken"] == response_body(response)["playerToken"]
    assert len(club["dynamo"].rows("players")) == 1
    assert club["dynamo"].rows("games")[0]["playersCount"] == 1
    club["client"].create_customer.assert_called_once()


def test_existing_active_customer_is_reused_without_modification(club):
    club["client"].find_customer.return_value = {"id": "existing", "is_active": True, "phone": "+34 999999999"}
    response = club_join(club, club_request(club))
    assert response["statusCode"] == 200
    assert club["dynamo"].rows("players")[0]["kivlyMembership"]["customerId"] == "existing"
    assert club["dynamo"].rows("players")[0]["kivlyMembership"]["phone"] == "+34 999999999"
    assert club["dynamo"].rows("players")[0]["phone"] == "+34 612345678"
    club["client"].create_customer.assert_not_called()


def confirmed_identity(club, customer_id="deleted-customer"):
    game = registration.apply_account_settings(club["dynamo"], club["dynamo"].rows("games")[0])
    key = registration._identity(game, club["conn"], EMAIL)
    record = {"id": key, "state": "confirmed", "customerId": customer_id}
    club["table"].items[key] = deepcopy(record)
    values = {"email": EMAIL, "username": "Full Name",
              "loyaltyConsentVersion": club["settings"]["consentVersion"]}
    return game, values, record


def test_rejoining_with_confirmed_identity_reuses_live_customer_without_writes(club):
    game, values, record = confirmed_identity(club, "existing")
    club["client"].find_customer.return_value = {"id": "existing", "is_active": True}
    membership = registration.ensure_membership(club["dynamo"], game, values)
    assert membership["customerId"] == "existing"
    club["client"].find_customer.assert_called_once()
    club["client"].create_customer.assert_not_called()
    assert club["table"].items[record["id"]]["state"] == "confirmed"


def test_rejoining_after_external_customer_deletion_creates_once(club):
    game, values, record = confirmed_identity(club)
    membership = registration.ensure_membership(club["dynamo"], game, values)
    assert membership["customerId"] == "customer-1"
    assert club["table"].items[record["id"]]["customerId"] == "customer-1"
    assert club["table"].intents[0]["state"] == "creating"
    assert registration.ensure_membership(club["dynamo"], game, values)["customerId"] == "customer-1"
    club["client"].create_customer.assert_called_once()


def test_confirmed_identity_can_bind_to_recreated_live_customer_without_post(club):
    game, values, record = confirmed_identity(club)
    club["client"].find_customer.return_value = {"id": "replacement", "is_active": True}
    membership = registration.ensure_membership(club["dynamo"], game, values)
    assert membership["customerId"] == "replacement"
    assert club["table"].items[record["id"]]["customerId"] == "replacement"
    club["client"].create_customer.assert_not_called()


def test_deleted_player_can_rejoin_same_email_with_new_verification(club, monkeypatch):
    from stand_common import email_verification as verification
    from tests.test_email_verification import SECRET

    first = club_join(club, club_request(club))
    assert first["statusCode"] == 200
    previous = club["dynamo"].rows("players")[0]
    identity = verification.identity_id(SECRET, GAME_ID, EMAIL)
    # These are the player and email-mapping removals made by the operator deletion.
    del club["dynamo"].Table("players").items[(GAME_ID, int(previous["playerId"]))]
    del club["dynamo"].Table("verification").items[(identity,)]
    game = club["dynamo"].rows("games")[0]
    game["playersCount"] = 0
    game["lastPlayerId"] = previous["playerId"]
    club["dynamo"].seed("games", game)
    now = verification.time.time()
    monkeypatch.setattr(verification.time, "time", lambda: now + 61)

    second = club_join(club, club_request(club))

    assert second["statusCode"] == 200
    current = club["dynamo"].rows("players")[0]
    assert current["playerId"] > previous["playerId"]
    assert current["kivlyMembership"]["customerId"] == previous["kivlyMembership"]["customerId"]
    assert response_body(second)["playerToken"] != response_body(first)["playerToken"]
    club["client"].create_customer.assert_called_once()


def test_confirmed_inactive_customer_does_not_trigger_recreation(club):
    game, values, record = confirmed_identity(club)
    club["client"].find_customer.return_value = {"id": "inactive", "is_active": False}
    with pytest.raises(registration.RegistrationError, match="KivlyCustomerInvalid"):
        registration.ensure_membership(club["dynamo"], game, values)
    assert club["table"].items[record["id"]] == record
    club["client"].create_customer.assert_not_called()


def test_failed_lookup_preserves_confirmed_identity_and_never_creates(club):
    game, values, record = confirmed_identity(club)
    club["client"].find_customer.side_effect = KivlyError("KivlySearchIncomplete")
    with pytest.raises(KivlyError, match="KivlySearchIncomplete"):
        registration.ensure_membership(club["dynamo"], game, values)
    assert club["table"].items[record["id"]] == record
    club["client"].create_customer.assert_not_called()


def test_deleted_customer_recreation_respects_read_only_mode(club, monkeypatch):
    game, values, record = confirmed_identity(club)
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "false")
    with pytest.raises(registration.RegistrationError, match="KivlyWritesDisabled"):
        registration.ensure_membership(club["dynamo"], game, values)
    assert club["table"].items[record["id"]] == record
    club["client"].create_customer.assert_not_called()


def test_uncertain_recreation_preserves_intent_and_never_reposts(club):
    game, values, record = confirmed_identity(club)
    club["client"].create_customer.side_effect = KivlyError("KivlyUnavailable")
    for _ in range(2):
        with pytest.raises(registration.RegistrationError, match="KivlyReconciliationRequired"):
            registration.ensure_membership(club["dynamo"], game, values)
    assert club["table"].items[record["id"]]["state"] == "creating"
    club["client"].create_customer.assert_called_once()


def test_concurrent_recreation_of_deleted_customer_creates_only_once(club):
    game, values, _ = confirmed_identity(club)
    barrier = Barrier(2)

    def missing_customer(*_):
        barrier.wait(timeout=5)
        return None

    club["client"].find_customer.side_effect = missing_customer

    def attempt():
        try:
            return registration.ensure_membership(club["dynamo"], game, values)
        except registration.RegistrationError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt(), range(2)))
    assert "KivlyRegistrationBusy" in results
    assert any(isinstance(result, dict) for result in results)
    club["client"].create_customer.assert_called_once()


def test_inactive_customer_fails_closed_without_player_or_external_write(club):
    club["client"].find_customer.return_value = {"id": "existing", "is_active": False}
    response = club_join(club, club_request(club))
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "KivlyCustomerInvalid"
    assert club["dynamo"].rows("players") == []
    club["client"].create_customer.assert_not_called()


def test_lost_create_response_is_durable_and_never_automatically_reposted(club):
    club["client"].create_customer.side_effect = KivlyError("KivlyUnavailable")
    requested = club_request(club)
    first = club_join(club, requested)
    assert first["statusCode"] == 409
    assert response_body(first)["error"] == "KivlyReconciliationRequired"
    assert next(iter(club["table"].items.values()))["state"] == "creating"
    assert club["dynamo"].rows("players") == []
    again = club_join(club, requested)
    assert again["statusCode"] == 409
    club["client"].create_customer.assert_called_once()

    intent = club["table"].intents[0]
    club["client"].find_customer.return_value = {
        "id": "recovered", "is_active": True, "external_id": intent["externalId"],
    }
    recovered = club_join(club, requested)
    assert recovered["statusCode"] == 200
    assert club["dynamo"].rows("players")[0]["kivlyMembership"]["customerId"] == "recovered"
    club["client"].create_customer.assert_called_once()


@pytest.mark.parametrize("operation,stage", [("find_customer", "customer_lookup"),
                                             ("create_customer", "customer_create")])
def test_registration_diagnostic_records_safe_stage_and_http_status(club, monkeypatch, operation, stage):
    events = []
    monkeypatch.setattr(registration, "log", lambda name, fields: events.append((name, fields)))
    getattr(club["client"], operation).side_effect = KivlyError("KivlyRequestFailed", 401)

    response = club_join(club, club_request(club))

    assert response["statusCode"] in (409, 503)
    diagnostic = next(fields for name, fields in events if name == "kivly_registration_request_failed")
    assert diagnostic == {"stage": stage, "code": "KivlyRequestFailed", "httpStatus": 401}
    assert EMAIL not in str(diagnostic)
    if operation == "create_customer":
        assert next(iter(club["table"].items.values()))["state"] == "creating"
        club["client"].create_customer.assert_called_once()


def test_registration_can_diagnose_customer_lookup_without_external_creation(club, monkeypatch):
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "false")
    club["client"].find_customer.return_value = None

    response = club_join(club, club_request(club))

    assert response["statusCode"] == 503
    assert response_body(response)["error"] == "KivlyWritesDisabled"
    club["client"].find_customer.assert_called_once()
    club["client"].create_customer.assert_not_called()
    assert club["table"].items == {}


def test_uncertain_attempt_cannot_attach_unrelated_same_email_customer(club):
    club["client"].create_customer.side_effect = KivlyError("KivlyUnavailable")
    requested = club_request(club)
    club_join(club, requested)
    club["client"].find_customer.return_value = {
        "id": "unrelated", "is_active": True, "external_id": "another-attempt",
    }
    response = club_join(club, requested)
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "KivlyReconciliationRequired"
    assert club["dynamo"].rows("players") == []
    club["client"].create_customer.assert_called_once()


def test_settings_change_invalidates_challenge_before_external_write(club):
    requested = club_request(club)
    club["conn"]["clubSettings"]["clubName"] = "Changed Club"
    response = club_join(club, requested)
    assert response["statusCode"] == 400
    assert response_body(response)["error"] == "KivlyConsentRequired"
    club["client"].create_customer.assert_not_called()


def test_storefront_only_change_does_not_invalidate_pending_email_consent(club):
    requested = club_request(club)
    club["conn"]["clubSettings"]["storefrontUrl"] = "https://shared.kivly.store"
    club["conn"]["version"] = "new-settings-revision"
    response = club_join(club, requested)
    assert response["statusCode"] == 200
    assert registration.account_game_settings({"gameType": "STACK"}, club["conn"])[
        "consentVersion"] == club["settings"]["consentVersion"]
    club["client"].create_customer.assert_called_once()


def test_client_cannot_inject_consent_only_in_final_join(club):
    requested = club_request(club)
    club["conn"]["clubSettings"]["clubName"] = "Changed Club"
    new_version = registration.account_game_settings({"gameType": "STACK"}, club["conn"])["consentVersion"]
    response = api_call("join", {
        "gameId": GAME_ID, "challengeId": response_body(requested)["challengeId"],
        "code": sent_code(club), "loyaltyConsentVersion": new_version,
    })
    assert response["statusCode"] == 400
    club["client"].create_customer.assert_not_called()


def test_game_at_capacity_never_creates_external_customer(club):
    game = club["dynamo"].rows("games")[0]
    game["playersCount"] = game["maxPlayers"]
    club["dynamo"].seed("games", game)
    response = club_join(club, club_request(club))
    assert response["statusCode"] == 403
    assert response_body(response)["error"] == "player_limit_reached"
    club["client"].create_customer.assert_not_called()


def test_concurrent_claims_allow_only_one_customer_creation(club):
    barrier = Barrier(2)

    def no_customer(*_):
        barrier.wait(timeout=5)
        return None

    club["client"].find_customer.side_effect = no_customer
    game = registration.apply_account_settings(club["dynamo"], club["dynamo"].rows("games")[0])
    values = {"email": EMAIL, "username": "Full Name",
              "loyaltyConsentVersion": club["settings"]["consentVersion"]}

    def attempt():
        try:
            return registration.ensure_membership(club["dynamo"], game, values)
        except registration.RegistrationError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt(), range(2)))
    assert "KivlyRegistrationBusy" in results
    assert any(isinstance(result, dict) and result["customerId"] == "customer-1" for result in results)
    club["client"].create_customer.assert_called_once()


def test_disconnect_during_external_create_never_grants_player_session(club, monkeypatch):
    game = registration.apply_account_settings(club["dynamo"], club["dynamo"].rows("games")[0])
    connection_calls = iter([club["conn"], None])
    monkeypatch.setattr(registration, "connection", lambda *_: next(connection_calls))
    with pytest.raises(registration.RegistrationError, match="KivlyConnectionRequired"):
        registration.ensure_membership(club["dynamo"], game, {
            "email": EMAIL, "username": "Full Name",
            "loyaltyConsentVersion": club["settings"]["consentVersion"],
        })
    assert next(iter(club["table"].items.values()))["state"] == "confirmed"


def test_identity_is_private_normalized_and_account_location_scoped(monkeypatch):
    monkeypatch.setenv("PLAYER_TOKEN_SECRET", "private-test-key-at-least-32-characters")
    identity = registration._identity({"ownerUserId": "owner"}, {"locationId": "location"}, EMAIL)
    assert EMAIL not in identity
    assert identity == registration._identity(
        {"ownerUserId": "owner"}, {"locationId": "location"}, EMAIL.upper(),
    )
    assert identity != registration._identity(
        {"ownerUserId": "another-owner"}, {"locationId": "location"}, EMAIL,
    )
    assert identity != registration._identity(
        {"ownerUserId": "owner"}, {"locationId": "another-location"}, EMAIL,
    )


@pytest.mark.parametrize("game_type", ["RULET4", "T1MER", "EMPAREJA2"])
def test_non_stack_join_does_not_request_consent_or_contact_kivly(club, monkeypatch, game_type):
    from tests.test_public_web import public_mod
    from stand_common.rulet4 import validate_config

    game = club["dynamo"].rows("games")[0]
    game["gameType"] = game_type
    game["kivly"] = {"enabled": True}
    if game_type == "RULET4":
        game["type"] = {"RULET4": validate_config()}
    club["dynamo"].seed("games", game)
    connection_lookup = MagicMock(side_effect=AssertionError("Non-STACK must bypass account integration"))
    monkeypatch.setattr(registration, "connection", connection_lookup)
    monkeypatch.setattr(public_mod, "_client_appearance", lambda *_: {})
    metadata = public_mod.lambda_handler({
        "rawPath": "/v1/public/game", "requestContext": {"http": {"method": "GET"}},
        "queryStringParameters": {"gameId": GAME_ID},
    }, None)
    assert metadata["statusCode"] == 200
    assert "loyalty" not in response_body(metadata)
    assert response_body(metadata)["participantFields"]["email"] == {"required": True}

    requested = request_code(username="Full Name", phone="+34 612345678")
    response = club_join(club, requested)
    assert response["statusCode"] == 200
    player = club["dynamo"].rows("players")[0]
    assert "kivlyMembership" not in player
    assert player["emailVerificationMethod"] == "email-code"
    assert player["username"] == "Full Name"
    assert player["phone"] == "+34 612345678"
    registration.require_player(club["dynamo"], game, player)
    connection_lookup.assert_not_called()
    club["client"].find_customer.assert_not_called()
    club["client"].create_customer.assert_not_called()
    assert club["table"].items == {}
