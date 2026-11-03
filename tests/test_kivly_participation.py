from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from stand_common import kivly_participation as participation
from stand_common.kivly_registration import RegistrationError
from tests.email_fake import FakeDynamo, FakeDynamoError
from tests.test_email_verification import (
    FakeKeyExpression, rig, assign_mod, request_code, response_body, join, sent_code,
)


MEMBERSHIP = {"locationId": "location", "customerId": "customer", "status": "confirmed"}


@pytest.fixture
def store(monkeypatch):
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "memberships")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    monkeypatch.setattr(participation, "Key", FakeKeyExpression)
    monkeypatch.setattr(participation, "ClientError", FakeDynamoError)
    return FakeDynamo()


def player(pid=1, **fields):
    return {"gameId": "ABC123", "playerId": pid, "kivlyMembership": deepcopy(MEMBERSHIP),
            "type": {"STACK": {}}, **fields}


def test_legacy_identity_claim_is_adopted_without_changing_player(store):
    original = player(email="first@example.com")
    store.seed("players", original)
    assert participation.existing_player(store, "ABC123", MEMBERSHIP) == original
    assert store.rows("players") == [original]
    assert store.rows("memberships")[0]["playerId"] == 1


def test_historical_duplicate_is_not_arbitrarily_selected(store):
    for pid in (1, 2):
        store.seed("players", player(pid))
    with pytest.raises(RegistrationError, match="KivlyDuplicateParticipation"):
        participation.existing_player(store, "ABC123", MEMBERSHIP)
    assert store.rows("memberships") == []


def test_participation_is_scoped_to_game_and_location(store):
    store.seed("players", player(gameId="OTHER"))
    assert participation.existing_player(store, "ABC123", MEMBERSHIP) is None
    assert participation.claim_id("ABC123", MEMBERSHIP) != participation.claim_id("OTHER", MEMBERSHIP)
    assert participation.claim_id("ABC123", MEMBERSHIP) != participation.claim_id(
        "ABC123", {**MEMBERSHIP, "locationId": "different"},
    )


@pytest.mark.parametrize("membership", [
    {}, {**MEMBERSHIP, "customerId": "pending-reconciliation"},
    {**MEMBERSHIP, "status": "pending"}, {**MEMBERSHIP, "customerId": ""},
])
def test_invalid_identity_never_gets_a_claim(store, membership):
    with pytest.raises(RegistrationError, match="KivlyMembershipRequired"):
        participation.existing_player(store, "ABC123", membership)


def test_unstarted_or_same_started_attempt_can_resume_but_finished_cannot():
    participation.require_joinable(player())
    participation.require_joinable(player(type={"STACK": {"attempt": {"status": "started"}}}))
    for blob in ({"score": 0}, {"attempt": {"status": "finished"}}, {"attempt": {"status": "excluded"}}):
        with pytest.raises(RegistrationError, match="KivlyAlreadyPlayed"):
            participation.require_joinable(player(type={"STACK": blob}))


def test_wrong_player_cannot_use_another_players_claim(store):
    store.seed("players", player())
    participation.require_unique_player(store, player())
    with pytest.raises(RegistrationError, match="KivlyDuplicateParticipation"):
        participation.require_unique_player(store, player(2))


def test_two_emails_race_for_one_customer_in_assignment_transaction(rig, monkeypatch):
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "memberships")
    table = rig["dynamo"].Table("verification")
    for number in (1, 2):
        rig["dynamo"].seed("verification", {
            "id": f"challenge:{number}", "verifiedAt": 1, "expiresAt": 9999999999, "attempts": 0,
        })
        rig["dynamo"].seed("verification", {"id": f"recipient:{number}", "challengeId": str(number)})

    def create(number):
        record = {"id": f"challenge:{number}", "recipientId": f"recipient:{number}"}
        candidate = player(number, email=f"user{number}@example.com")
        return assign_mod._put_verified_player(table, record, candidate, 10, "now")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, (1, 2)))
    assert sorted(results) == ["created", "identity_exists"]
    assert len(rig["dynamo"].rows("players")) == 1
    assert len(rig["dynamo"].rows("memberships")) == 1
    assert rig["dynamo"].rows("games")[0]["playersCount"] == 1


@pytest.fixture
def shared_identity(rig, monkeypatch):
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "memberships")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    monkeypatch.setattr(participation, "Key", FakeKeyExpression)
    monkeypatch.setattr(participation, "ClientError", FakeDynamoError)
    monkeypatch.setattr(assign_mod.kivly_registration, "ensure_membership",
                        lambda *_: deepcopy(MEMBERSHIP))
    return rig


def register(rig, email):
    request = request_code(email=email)
    assert request["statusCode"] == 200
    return join(rig, response_body(request)["challengeId"], sent_code(rig))


def test_different_verified_email_reuses_same_customer_player_even_when_full(shared_identity):
    first = register(shared_identity, "first@example.com")
    assert first["statusCode"] == 200
    game = shared_identity["dynamo"].rows("games")[0]
    game["maxPlayers"] = 1
    shared_identity["dynamo"].seed("games", game)
    second = register(shared_identity, "second@example.com")
    assert second["statusCode"] == 200
    assert response_body(first)["playerToken"] == response_body(second)["playerToken"]
    assert len(shared_identity["dynamo"].rows("players")) == 1
    assert shared_identity["dynamo"].rows("players")[0]["email"] == "first@example.com"
    assert shared_identity["dynamo"].rows("games")[0]["playersCount"] == 1


def test_finished_customer_cannot_reenter_using_another_email(shared_identity):
    assert register(shared_identity, "first@example.com")["statusCode"] == 200
    original = shared_identity["dynamo"].rows("players")[0]
    original["type"]["STACK"] = {"attempt": {"status": "finished"}, "score": 0}
    shared_identity["dynamo"].seed("players", original)
    denied = register(shared_identity, "second@example.com")
    assert denied["statusCode"] == 409
    assert response_body(denied)["error"] == "KivlyAlreadyPlayed"
    assert len(shared_identity["dynamo"].rows("players")) == 1


def test_started_customer_reentry_keeps_original_attempt(shared_identity):
    assert register(shared_identity, "first@example.com")["statusCode"] == 200
    original = shared_identity["dynamo"].rows("players")[0]
    original["type"]["STACK"] = {"attempt": {"status": "started", "attemptId": "only-attempt"}}
    shared_identity["dynamo"].seed("players", original)
    assert register(shared_identity, "second@example.com")["statusCode"] == 200
    assert shared_identity["dynamo"].rows("players")[0]["type"]["STACK"]["attempt"]["attemptId"] == "only-attempt"


@pytest.mark.parametrize("action", ["start", "finish"])
def test_direct_score_never_bypasses_membership_guard(monkeypatch, action):
    from tests.test_stack_score import stack, AttemptTable, META

    table = AttemptTable()
    monkeypatch.setattr(stack.kivly_registration, "apply_account_settings", lambda _, meta: meta)

    def denied(*_):
        raise RegistrationError("KivlyMembershipRequired", 403)

    monkeypatch.setattr(stack.kivly_registration, "require_player", denied)
    result = stack.store_score("ABCDEF", table, {
        "playerId": 7, "action": action, "attemptId": "attempt",
    }, META)
    assert result == {"ok": False, "error": "KivlyMembershipRequired"}
    assert table.writes == 0


def test_legacy_adoption_and_new_player_compete_for_same_claim_atomically(rig, monkeypatch):
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "memberships")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    monkeypatch.setattr(participation, "ClientError", FakeDynamoError)
    original = player(kivlyMembership={
        "locationId": "location", "customerId": "pending-reconciliation",
    })
    rig["dynamo"].seed("players", original)
    verification = rig["dynamo"].Table("verification")
    rig["dynamo"].seed("verification", {
        "id": "challenge:2", "verifiedAt": 1, "expiresAt": 9999999999, "attempts": 0,
    })
    rig["dynamo"].seed("verification", {"id": "recipient:2", "challengeId": "2"})

    def adopt():
        try:
            participation.bind_existing_player(rig["dynamo"], original, MEMBERSHIP)
            return "adopted"
        except RegistrationError:
            return "blocked"

    def create():
        return assign_mod._put_verified_player(
            verification, {"id": "challenge:2", "recipientId": "recipient:2"},
            player(2, email="second@example.com"), 10, "now",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        adoption, creation = pool.submit(adopt), pool.submit(create)
        outcomes = adoption.result(), creation.result()
    assert outcomes in (("adopted", "identity_exists"), ("blocked", "created"))
    confirmed = [p for p in rig["dynamo"].rows("players")
                 if p["kivlyMembership"].get("customerId") == MEMBERSHIP["customerId"]]
    assert len(confirmed) == 1
    assert rig["dynamo"].rows("memberships")[0]["playerId"] == confirmed[0]["playerId"]
