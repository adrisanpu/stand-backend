"""Offline end-to-end coverage of queued registration, exclusion and approval."""
from copy import deepcopy
from threading import RLock
from types import SimpleNamespace
import importlib.util
import json
from pathlib import Path
import re
from unittest.mock import MagicMock

import pytest

from stand_common import kivly_registration as registration
from stand_common import kivly_registration_api as api
from stand_common import kivly_resolver as resolver
from stand_common.kivly import KivlyClient, KivlyError
from stand_common.stack import DEFAULT_CONFIG
from tests.email_fake import FakeDynamoError
from tests.test_email_verification import (
    GAME_ID, EMAIL, rig, request_code, join, sent_code, response_body, FakeKeyExpression,
)


class RegistrationTable:
    """Evaluate the actual conditional writes rather than accepting MagicMocks."""
    def __init__(self):
        self.items, self.writes, self.lock = {}, [], RLock()

    def get_item(self, Key, **kwargs):
        with self.lock:
            row = self.items.get(Key["id"])
            return {"Item": deepcopy(row)} if row else {}

    def _condition(self, expression, row, values, names):
        expression = re.sub(
            r"attribute_not_exists\((\w+)\)",
            lambda match: str(match[1] not in row), expression,
        )
        tokens = re.findall(r"<=|>=|<>|=|<|>|\(|\)|[#:]\w+|\w+", expression)
        converted = []
        for token in tokens:
            if token in ("AND", "OR"):
                converted.append(token.lower())
            elif token in ("True", "False", "(", ")", "<", ">", "<=", ">="):
                converted.append(token)
            elif token == "=":
                converted.append("==")
            elif token.startswith(":"):
                converted.append(repr(values[token]))
            else:
                converted.append(repr(row.get(names.get(token, token))))
        return eval(" ".join(converted), {"__builtins__": {}})

    def put_item(self, Item, ConditionExpression, ExpressionAttributeValues=None,
                 ExpressionAttributeNames=None, **kwargs):
        with self.lock:
            current = self.items.get(Item["id"], {})
            if not self._condition(ConditionExpression, current, ExpressionAttributeValues or {},
                                   ExpressionAttributeNames or {}):
                raise FakeDynamoError("ConditionalCheckFailedException")
            self.items[Item["id"]] = deepcopy(Item)
            self.writes.append(deepcopy(Item))

    def update_item(self, Key, UpdateExpression, ConditionExpression,
                    ExpressionAttributeValues, **kwargs):
        with self.lock:
            row = deepcopy(self.items.get(Key["id"], Key))
            if not self._condition(ConditionExpression, row, ExpressionAttributeValues, {}):
                raise FakeDynamoError("ConditionalCheckFailedException")
            setting, _, removal = UpdateExpression.removeprefix("SET ").partition(" REMOVE ")
            for assignment in setting.split(","):
                field, value = assignment.strip().split(" = ")
                row[field] = ExpressionAttributeValues[value]
            for field in removal.split():
                row.pop(field, None)
            self.items[Key["id"]] = row

    def scan(self, FilterExpression, ExpressionAttributeNames, ExpressionAttributeValues, **kwargs):
        rows = [deepcopy(row) for row in self.items.values() if self._condition(
            FilterExpression, row, ExpressionAttributeValues, ExpressionAttributeNames,
        )]
        return {"Items": rows}


class OfflineClient(KivlyClient):
    def __init__(self):
        super().__init__("offline-id", "offline-secret")
        self.rows, self.calls, self.posts = [], [], 0
        self.hide_filtered, self.lose_post, self.reuse_wrong = False, False, False
        self.reuse_wrong_phone = False
        self.reuse_existing = False

    def get_context(self):
        return SimpleNamespace(location_id="location", owner_id="business-owner",
                               requests_per_minute=60, requests_per_hour=2000)

    def _customers(self, location_id, owner_id, *, page=None, customer=None, search=None, limit=100):
        assert (location_id, owner_id) == ("location", "business-owner")
        self.calls.append({"page": page, "search": search, "customer": customer, "limit": limit})
        if customer is not None:
            self.posts += 1
            row = {**customer, "id": f"created-{self.posts}", "is_active": True}
            if self.reuse_existing:
                row = {
                    "id": "existing", "name": "Kivly Name", "email": customer["email"].upper(),
                    "phone": "0034 612345678", "external_id": "original-external",
                    "is_active": True,
                }
                self.rows.append(row)
                if self.lose_post:
                    raise KivlyError("KivlyUnavailable")
                return deepcopy(row)
            if self.reuse_wrong:
                row = {**row, "email": "someone-else@club.test", "external_id": "someone-elses-attempt"}
                return row
            if self.reuse_wrong_phone:
                row = {**row, "phone": "+34 699999999", "external_id": "someone-elses-attempt"}
                return row
            self.rows.append(row)
            if self.lose_post:
                raise KivlyError("KivlyUnavailable")
            return deepcopy(row)
        rows = self.rows
        if search:
            rows = ([] if self.hide_filtered else [
                row for row in rows
                if resolver.email_key(search) in resolver.email_key(row.get("email"))
                or (resolver.phone_key(search) and
                    resolver.phone_key(search) == resolver.phone_key(row.get("phone")))
            ])
        start = (page - 1) * limit
        return {"data": deepcopy(rows[start:start + limit]), "total": len(self.rows),
                "page": page, "limit": limit, "has_next": start + limit < len(rows),
                "has_previous": page > 1}

    def _scoped_request(self, location_id, owner_id, path, **kwargs):
        assert (location_id, owner_id) == ("location", "business-owner")
        identifier = path.rsplit("/", 1)[-1]
        self.calls.append({"get": identifier})
        row = next((row for row in self.rows if row["id"] == identifier), None)
        if row is None:
            raise KivlyError("KivlyRequestFailed", 404)
        return deepcopy(row)


@pytest.fixture
def club(rig, monkeypatch):
    monkeypatch.setenv("GAMES_TABLE", "games")
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "registrations")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    from stand_common import kivly_participation
    monkeypatch.setattr(kivly_participation, "Key", FakeKeyExpression)
    monkeypatch.setattr(kivly_participation, "ClientError", FakeDynamoError)
    conn = {
        "locationId": "location", "ownerId": "business-owner", "version": "v1",
        "secretArn": "private-arn",
        "clubSettings": {"clubName": "Club", "privacyUrl": "https://club.test/privacy",
                         "termsUrl": "https://club.test/terms"},
    }
    monkeypatch.setattr(registration, "connection", lambda *_: conn)
    game = rig["dynamo"].rows("games")[0]
    game.update(ownerUserId="owner", gameType="STACK", participantFields={"phone": {"enabled": True}},
                type={"STACK": deepcopy(DEFAULT_CONFIG)})
    rig["dynamo"].seed("games", game)
    table, client, queue, clock = RegistrationTable(), OfflineClient(), [], [1700000000]
    monkeypatch.setattr(registration, "_table", lambda *_: table)
    monkeypatch.setattr(registration, "_client", lambda *_: client)
    monkeypatch.setattr(resolver.time, "time", lambda: clock[0])
    monkeypatch.setattr(resolver, "enqueue", lambda identifier, delay=0, **extra:
                        queue.append((identifier, delay, extra)))
    settings = registration.account_game_settings(game, conn)
    meta = registration.apply_account_settings(rig["dynamo"], game)
    values = {"username": "Full Name", "email": EMAIL, "phone": "+34 612345678",
              "loyaltyConsentVersion": settings["consentVersion"]}
    return {**rig, "conn": conn, "table": table, "client": client, "settings": settings,
            "meta": meta, "values": values, "queue": queue, "clock": clock}


def customer(**extra):
    return {"id": "existing", "email": EMAIL, "phone": "+34 612345678",
            "name": "Kivly Name", "is_active": True, **extra}


def request(club, **extra):
    values = {**club["values"], **extra}
    try:
        return registration.ensure_membership(club["dynamo"], club["meta"], values)
    except registration.RegistrationError as error:
        assert error.code == "KivlyRegistrationPending"
        assert error.retry_after == 5
    return resolver.request_id(club["meta"], club["conn"], club["settings"], values)


def drain(club, limit=1000):
    count = 0
    while club["queue"]:
        identifier, delay, extra = club["queue"].pop(0)
        club["clock"][0] += max(delay, 4)
        resolver.process(club["dynamo"], identifier)
        count += 1
        assert count <= limit


def record(club, identifier):
    return club["table"].items[identifier]


def test_verified_challenge_waits_then_retries_same_code_without_duplicate_player(club):
    requested = request_code(loyaltyConsentVersion=club["settings"]["consentVersion"],
                             phone=club["values"]["phone"])
    challenge = response_body(requested)["challengeId"]
    wrong = "000000" if sent_code(club) != "000000" else "999999"
    assert join(club, challenge, wrong)["statusCode"] == 400
    assert not club["queue"] and not club["client"].posts
    waiting = join(club, challenge, sent_code(club))
    assert waiting["statusCode"] == 503
    assert response_body(waiting)["error"] == "KivlyRegistrationPending"
    assert response_body(waiting)["retryAfter"] == 5
    assert club["dynamo"].rows("players") == []
    drain(club)
    joined = join(club, challenge, sent_code(club))
    assert joined["statusCode"] == 200
    assert club["dynamo"].rows("players")[0]["kivlyMembership"]["status"] == "confirmed"
    again = join(club, challenge, sent_code(club))
    assert again["statusCode"] == 200
    assert response_body(joined)["playerToken"] == response_body(again)["playerToken"]
    assert len(club["dynamo"].rows("players")) == 1
    assert club["client"].posts == 1


def test_disconnected_stack_never_enqueues_or_calls_external_client(club, monkeypatch):
    monkeypatch.setattr(registration, "connection", lambda *_: None)
    meta = registration.apply_account_settings(club["dynamo"], club["meta"])
    assert registration.ensure_membership(club["dynamo"], meta, {}) is None
    assert not club["queue"] and not club["client"].calls


def test_normal_new_registration_does_not_enumerate_customer_directory(club):
    identifier = request(club)
    assert not club["client"].calls
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert not any(call.get("page") and call.get("search") is None for call in club["client"].calls)
    assert club["client"].posts == 1
    assert len(record(club, identifier)["matches"]) == 0


def test_stale_confirmed_customer_revalidates_by_id_without_directory_scan(club):
    identifier = request(club)
    drain(club)
    club["client"].calls.clear()
    club["clock"][0] += resolver.FRESH_SECONDS + 1
    assert request(club) == identifier
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert {"get": "created-1"} in club["client"].calls
    assert not any(call.get("page") and call.get("search") is None for call in club["client"].calls)
    assert club["client"].posts == 1


def test_positive_email_and_phone_match_fast_path_never_scans_directory(club):
    club["client"].rows = [customer(), customer(id="other", email="other@test", phone=None)]
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert not any(call.get("page") and call.get("search") is None for call in club["client"].calls)
    assert club["client"].posts == 0


def test_email_match_reuses_kivly_phone_without_overwrite(club):
    club["client"].rows = [customer(phone="+34 999999999")]
    identifier = request(club)
    drain(club)
    membership = request(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert membership["phone"] == "+34 999999999"
    assert club["client"].posts == 0


def test_email_and_phone_pointing_to_distinct_profiles_is_conflict(club):
    club["client"].rows = [customer(phone="+34 999999999"),
                           customer(id="phone-owner", email="someone@club.test")]
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["error"] == "KivlyIdentityConflict"
    assert club["client"].posts == 0
    with pytest.raises(registration.RegistrationError, match="KivlyIdentityConflict"):
        registration.ensure_membership(club["dynamo"], club["meta"], club["values"])


@pytest.mark.parametrize("email", ["someone@club.test", "", None])
def test_phone_only_match_requires_authenticated_organizer_then_revalidation(club, email):
    club["client"].rows = [customer(email=email)]
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["state"] == "review_required"
    with pytest.raises(registration.RegistrationError, match="KivlyIdentityReviewRequired"):
        registration.ensure_membership(club["dynamo"], club["meta"], club["values"])
    event = {"queryStringParameters": {"gameId": GAME_ID},
             "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}}}
    listing = response_body(api.handle(event, "GET", club["dynamo"]))
    assert listing["requests"][0]["customer"]["name"] == "Kivly Name"
    assert listing["requests"][0]["requestId"] == identifier
    approval = {**event, "body": json.dumps({"requestId": identifier})}
    assert api.handle(approval, "POST", club["dynamo"])["statusCode"] == 200
    assert record(club, identifier)["approvedBy"] == "owner"
    assert record(club, identifier)["state"] == "pending"
    drain(club)
    assert request(club)["customerId"] == "existing"
    assert club["client"].rows[0]["email"] == email
    assert club["client"].posts == 0


def test_review_does_not_accept_client_supplied_candidate_or_wrong_owner(club):
    event = {"queryStringParameters": {"gameId": GAME_ID},
             "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "other"}}}}}
    assert api.handle(event, "GET", club["dynamo"])["statusCode"] == 403
    event["requestContext"]["authorizer"]["jwt"]["claims"]["sub"] = "owner"
    event["body"] = json.dumps({"requestId": "registration:" + "a" * 64, "customerId": "attacker"})
    assert api.handle(event, "POST", club["dynamo"])["statusCode"] == 400
    assert api.handle({}, "GET", club["dynamo"])["statusCode"] == 401


def test_approval_rechecks_connection_version_and_candidate(club):
    club["client"].rows = [customer(email="other@club.test")]
    identifier = request(club)
    drain(club)
    event = {"queryStringParameters": {"gameId": GAME_ID},
             "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
             "body": json.dumps({"requestId": identifier})}
    club["conn"]["version"] = "v2"
    assert api.handle(event, "POST", club["dynamo"])["statusCode"] == 409
    club["conn"]["version"] = "v1"
    assert api.handle(event, "POST", club["dynamo"])["statusCode"] == 200
    club["client"].rows[0]["phone"] = "+34 888888888"
    drain(club)
    assert record(club, identifier)["state"] == "failed"
    assert club["client"].posts == 0


def test_legacy_confirmed_email_mapping_uses_id_and_checks_other_phone_profile(club):
    key = registration._identity(club["meta"], club["conn"], EMAIL)
    club["table"].items[key] = {"id": key, "state": "confirmed", "customerId": "existing"}
    club["client"].rows = [customer(phone="+34 999999999"),
                           customer(id="conflict", email="other@club.test")]
    identifier = request(club)
    drain(club)
    assert {"get": "existing"} in club["client"].calls
    assert record(club, identifier)["error"] == "KivlyIdentityConflict"
    assert club["client"].posts == 0


def test_legacy_email_mapping_never_silently_binds_another_customer(club):
    key = registration._identity(club["meta"], club["conn"], EMAIL)
    club["table"].items[key] = {"id": key, "state": "confirmed", "customerId": "old"}
    club["client"].rows = [customer(id="old", email="changed@club.test"), customer()]
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["state"] == "failed"
    assert club["client"].posts == 0


def test_lost_post_response_recovers_exact_external_id_without_repeating_post(club):
    club["client"].lose_post = True
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert club["client"].posts == 1
    assert request(club)["customerId"] == "created-1"


def test_process_restart_after_committed_create_intent_never_repeats_post(club):
    identifier = request(club)
    record(club, identifier).update(phase="create", createStarted=True, externalId="stand-exact")
    club["client"].rows = [customer(external_id="stand-exact")]
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert club["client"].posts == 0


def test_create_response_can_reuse_existing_profile_with_original_external_id(club):
    club["client"].hide_filtered = True
    club["client"].reuse_existing = True
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"
    assert record(club, identifier)["reusedExisting"] is True
    assert record(club, identifier)["externalId"] != "original-external"
    membership = request(club)
    assert membership["customerId"] == "existing"
    assert membership["phone"] == "0034 612345678"
    assert record(club, identifier)["customer"]["name"] == "Kivly Name"
    assert club["client"].posts == 1
    assert {"get": "existing"} in club["client"].calls
    assert not any(call.get("page") and call.get("search") is None for call in club["client"].calls)


def test_uncertain_reused_profile_response_requires_exact_external_id_reconciliation(club):
    club["client"].hide_filtered = True
    club["client"].reuse_existing = True
    club["client"].lose_post = True
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["error"] == "KivlyReconciliationRequired"
    assert record(club, identifier)["state"] == "failed"
    assert club["client"].posts == 1
    assert "customerId" not in record(club, identifier)


def test_duplicate_customer_profiles_fail_closed_without_creation(club):
    club["client"].rows = [customer(), customer(id="duplicate")]
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["error"] == "KivlyIdentityConflict"
    assert club["client"].posts == 0


@pytest.mark.parametrize("mismatch", ["email", "phone"])
def test_post_contact_mismatch_blocks_membership_and_freezes_new_creates(club, mismatch):
    club["client"].reuse_wrong = mismatch == "email"
    club["client"].reuse_wrong_phone = mismatch == "phone"
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["error"] == "KivlyIdentityConflict"
    assert club["client"].posts == 1
    assert not any(call.get("page") and call.get("search") is None for call in club["client"].calls)
    locks = [item for key, item in club["table"].items.items() if key.startswith("kivly-create:")]
    assert locks[0]["uncertain"] is True
    club["clock"][0] += 10000
    other = request(club, email="different@club.test")
    drain(club)
    assert record(club, other)["error"] == "KivlyReconciliationRequired"
    assert club["client"].posts == 1


@pytest.mark.parametrize("customer_id", [None, "", "x" * 513, "bad\nid", registration.PENDING_CUSTOMER_ID])
def test_customer_response_requires_valid_customer_id(club, customer_id):
    with pytest.raises(registration.RegistrationError, match="KivlyCustomerInvalid"):
        resolver._validate_identity(
            {
                "email": EMAIL, "phone": "+34 612345678",
                "createStarted": True, "externalId": "attempt",
            },
            {
                "id": customer_id, "email": EMAIL, "phone": "+34 612345678",
                "external_id": "attempt", "is_active": True,
            },
        )


def test_two_different_emails_same_phone_never_create_two_customers(club):
    first = request(club)
    second = request(club, email="second@club.test")
    drain(club)
    assert record(club, first)["state"] == "confirmed"
    assert record(club, second)["state"] == "review_required"
    assert club["client"].posts == 1


def test_lease_prevents_simultaneous_work_on_same_request(club):
    identifier = request(club)
    acquired = resolver._acquire(club["table"], identifier, club["clock"][0])
    assert acquired
    assert resolver._acquire(club["table"], identifier, club["clock"][0]) is None
    resolver.process(club["dynamo"], identifier)
    assert not club["client"].calls
    club["clock"][0] += resolver.LEASE_SECONDS + 1
    assert resolver._acquire(club["table"], identifier, club["clock"][0])


def test_durable_global_clock_defers_work_across_distinct_requests(club):
    first = request(club)
    second = request(club, email="second@club.test", phone="+34 699999999")
    club["queue"].clear()
    resolver.process(club["dynamo"], first)
    resolver.process(club["dynamo"], second)
    assert record(club, second)["phase"] == "context"
    assert any(identifier == second and delay >= 3 for identifier, delay, _ in club["queue"])


def test_external_write_switch_blocks_creation_not_existing_lookup(club, monkeypatch):
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "false")
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["error"] == "KivlyWritesDisabled"
    assert club["client"].posts == 0


@pytest.mark.parametrize("field,value", [("isActive", False), ("maxPlayers", 0)])
def test_worker_never_creates_for_inactive_or_full_games(club, field, value):
    game = club["dynamo"].rows("games")[0]
    if field == "maxPlayers":
        game.update(maxPlayers=1, playersCount=1)
    else:
        game[field] = value
    club["dynamo"].seed("games", game)
    identifier = request(club)
    drain(club)
    assert record(club, identifier)["error"] in ("GameInactive", "GameFull")
    assert club["client"].posts == 0


def test_stalled_outbox_is_requeued_by_schedule(club, monkeypatch):
    identifier = request(club)
    club["queue"].clear()
    club["clock"][0] += 61
    worker = load_worker()
    monkeypatch.setattr(worker.boto3, "resource", lambda *_: club["dynamo"])
    assert worker.lambda_handler({"source": "aws.events"}, None) == {"ok": True}
    assert club["queue"][0][0] == identifier
    drain(club)
    assert record(club, identifier)["state"] == "confirmed"


def load_worker():
    path = Path(__file__).parents[1] / "src" / "stand_kivly_registration" / "lambda_function.py"
    spec = importlib.util.spec_from_file_location("registration_worker_tests", path)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    return worker


def test_actual_queue_handler_checkpoints_success_and_returns_only_failed_message(club, monkeypatch):
    worker = load_worker()
    monkeypatch.setattr(worker.boto3, "resource", lambda *_: club["dynamo"])
    identifier = request(club)
    result = worker.lambda_handler({"Records": [
        {"messageId": "valid", "body": json.dumps({"requestId": identifier})},
        {"messageId": "invalid", "body": json.dumps({"requestId": "not-a-registration"})},
    ]}, None)
    assert result == {"batchItemFailures": [{"itemIdentifier": "invalid"}]}
    assert record(club, identifier)["phase"] == "lookup_email"


def test_worker_reserves_time_and_does_not_start_near_lambda_deadline(club, monkeypatch):
    worker = load_worker()
    monkeypatch.setattr(worker.boto3, "resource", lambda *_: club["dynamo"])
    identifier = request(club)
    context = SimpleNamespace(get_remaining_time_in_millis=lambda: 29000)
    result = worker.lambda_handler({"Records": [
        {"messageId": "retry-later", "body": json.dumps({"requestId": identifier})},
    ]}, context)
    assert result == {"batchItemFailures": [{"itemIdentifier": "retry-later"}]}
    assert record(club, identifier)["phase"] == "context"


def test_scope_change_blocks_pending_request_before_external_calls(club):
    identifier = request(club)
    club["conn"]["version"] = "reconnected"
    drain(club)
    assert record(club, identifier)["error"] == "KivlyConnectionChanged"
    assert not club["client"].calls


def test_consent_change_blocks_pending_request_before_external_calls(club):
    identifier = request(club)
    club["conn"]["clubSettings"]["termsUrl"] = "https://club.test/new-terms"
    drain(club)
    assert record(club, identifier)["error"] == "KivlyConnectionChanged"
    assert club["client"].posts == 0
    assert not club["client"].calls


def test_request_key_normalizes_contacts_and_is_independent_of_challenge(club):
    first = resolver.request_id(club["meta"], club["conn"], club["settings"], club["values"])
    values = {**club["values"], "email": " " + EMAIL.upper() + " ", "phone": "0034 612-345-678",
              "challengeId": "unrelated"}
    assert resolver.request_id(club["meta"], club["conn"], club["settings"], values) == first
    assert EMAIL not in first
    values["phone"] = "+34 699999999"
    assert resolver.request_id(club["meta"], club["conn"], club["settings"], values) != first


@pytest.mark.parametrize("membership", [
    {"customerId": registration.PENDING_CUSTOMER_ID, "phone": "+34 612345678"},
    {"customerId": "real", "phone": "+34 612345678", "status": "pending"},
    {"customerId": "real", "phone": "invalid"},
    {"customerId": ""},
])
def test_player_gate_rejects_placeholder_pending_and_invalid_phone(club, membership):
    player = {"kivlyMembership": {"locationId": "location",
                                "consentVersion": club["settings"]["consentVersion"], **membership}}
    with pytest.raises(registration.RegistrationError, match="KivlyMembershipRequired"):
        registration.require_player(club["dynamo"], club["meta"], player)


def test_historical_real_membership_without_status_remains_valid(club, monkeypatch):
    from stand_common import kivly_participation
    unique = MagicMock()
    monkeypatch.setattr(kivly_participation, "require_unique_player", unique)
    player = {"kivlyMembership": {"locationId": "location", "customerId": "real",
                                "phone": "+34 612345678",
                                "consentVersion": club["settings"]["consentVersion"]}}
    registration.require_player(club["dynamo"], club["meta"], player)
    unique.assert_called_once_with(club["dynamo"], player)
