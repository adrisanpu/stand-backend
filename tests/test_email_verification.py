import io
import json
import re
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from stand_common import email_verification as verification
from stand_common.email_verification import identity_id
from stand_common.utils import default_participant_fields
from tests.email_fake import FakeDynamo, FakeDynamoError
from tests.test_public_web import assign_mod, public_mod


SECRET = "email-verification-test-secret-long-enough"
GAME_ID = "ABC123"
EMAIL = "player@example.com"


class FakeKeyExpression:
    def __init__(self, field, value=None):
        self.field = field
        self.value = value

    def eq(self, value):
        return FakeKeyExpression(self.field, value)

    def __and__(self, other):
        return [self, other]


class FakeSes:
    def __init__(self, exception=None):
        self.sent = []
        self.exception = exception

    def send_email(self, **kwargs):
        self.sent.append(kwargs)
        if self.exception:
            raise self.exception


class FakeLambda:
    def __init__(self):
        self.calls = []

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        event = json.loads(kwargs["Payload"].decode("utf-8"))
        result = assign_mod.lambda_handler(event, None)
        return {"Payload": io.BytesIO(json.dumps(result).encode("utf-8"))}


@pytest.fixture
def rig(monkeypatch):
    dynamo = FakeDynamo()
    ses = FakeSes()
    invoke = FakeLambda()
    monkeypatch.setenv("EMAIL_VERIFICATION_TABLE", "verification")
    monkeypatch.setenv("EMAIL_SENDER_ADDRESS", "no-reply@playstand.app")
    monkeypatch.setenv("PLAYER_TOKEN_SECRET", SECRET)
    monkeypatch.setenv("EMAIL_MAX_PER_HOUR", "5")
    monkeypatch.setenv("EMAIL_IP_MAX_PER_HOUR", "3000")
    monkeypatch.setenv("EMAIL_GLOBAL_MAX_PER_HOUR", "20000")
    monkeypatch.setattr(verification, "log", lambda *_: None)
    monkeypatch.setattr(verification, "ClientError", FakeDynamoError)
    monkeypatch.setattr(verification, "BotoCoreError", FakeDynamoError)
    monkeypatch.setattr(assign_mod, "PLAYER_TOKEN_SECRET", SECRET)
    monkeypatch.setattr(public_mod, "PLAYER_TOKEN_SECRET", SECRET)
    monkeypatch.setattr(assign_mod, "dynamo_r", dynamo)
    monkeypatch.setattr(public_mod, "dynamo_r", dynamo)
    monkeypatch.setattr(assign_mod, "games_table", dynamo.Table("games"))
    monkeypatch.setattr(assign_mod, "gp_table", dynamo.Table("players"))
    monkeypatch.setattr(public_mod, "games_table", dynamo.Table("games"))
    monkeypatch.setattr(public_mod, "gp_table", dynamo.Table("players"))
    monkeypatch.setattr(assign_mod, "Key", FakeKeyExpression)
    monkeypatch.setattr(assign_mod, "ClientError", FakeDynamoError)
    monkeypatch.setattr(assign_mod, "ASSIGNERS", {
        "T1MER": lambda _event: ({"type": {"T1MER": {}}}, "", []),
    })
    monkeypatch.setattr(public_mod, "ASSIGN_LAMBDA", "assign-web")
    monkeypatch.setattr(public_mod, "lambda_client", invoke)
    monkeypatch.setattr(public_mod.boto3, "client", lambda service: ses if service == "ses" else MagicMock())
    dynamo.seed("games", {
        "gameId": GAME_ID,
        "gameType": "T1MER",
        "isActive": True,
        "maxPlayers": 10,
        "participantFields": default_participant_fields("T1MER"),
        "playersCount": 0,
    })
    return {"dynamo": dynamo, "ses": ses, "invoke": invoke}


def api_event(path, body, ip="203.0.113.8"):
    return {
        "rawPath": f"/v1/public/{path}",
        "requestContext": {"http": {"method": "POST", "sourceIp": ip}},
        "body": json.dumps(body),
    }


def api_call(path, body, ip="203.0.113.8"):
    return public_mod.lambda_handler(api_event(path, body, ip), None)


def response_body(response):
    return json.loads(response["body"])


def request_code(email=EMAIL, game_id=GAME_ID, ip="203.0.113.8", **overrides):
    body = {"gameId": game_id, "email": email, "consent": True, **overrides}
    return api_call("email/request", body, ip)


def sent_code(rig, index=-1):
    text = rig["ses"].sent[index]["Message"]["Body"]["Text"]["Data"]
    return re.search(r"\b(\d{6})\b", text).group(1)


def join(rig, challenge_id, code, game_id=GAME_ID):
    return api_call("join", {"gameId": game_id, "challengeId": challenge_id, "code": code})


def test_request_issues_six_digit_code_including_leading_zero_and_needs_no_identity(rig, monkeypatch, capsys):
    monkeypatch.setattr(verification.secrets, "randbelow", lambda _limit: 42)
    response = request_code()
    body = response_body(response)

    assert response["statusCode"] == 200
    assert sent_code(rig) == "000042"
    assert len(body["challengeId"]) >= 32
    assert body["emailMasked"] != EMAIL
    assert not {"playerId", "sessionId"} & set(body)
    assert "playerId" not in rig["ses"].sent[0]
    stored = next(
        row for row in rig["dynamo"].rows("verification")
        if row["id"].startswith("challenge:")
    )
    assert not {"playerId", "sessionId"} & set(stored)
    output = capsys.readouterr().out
    assert EMAIL not in output
    assert sent_code(rig) not in output
    assert sent_code(rig) not in json.dumps(body)


@pytest.mark.parametrize(
    ("body_change", "game_change", "expected_status", "expected_error"),
    [
        ({"consent": False}, {}, 400, "ConsentRequired"),
        ({"email": "not-an-email"}, {}, 400, "invalid_email"),
        ({"language": "fr"}, {}, 400, "InvalidLanguage"),
        ({}, {"isActive": False}, 403, "GameInactive"),
        ({}, {"gameType": "IMPOSTOR"}, 400, "unsupported_game_type"),
        ({}, {"joinChannel": "instagram"}, 403, "web_join_disabled"),
    ],
)
def test_request_rejects_invalid_registration_before_sending(
    rig, body_change, game_change, expected_status, expected_error
):
    if game_change:
        game = rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]
        game.update(game_change)
        rig["dynamo"].seed("games", game)
    response = api_call("email/request", {"gameId": GAME_ID, "email": EMAIL, "consent": True, **body_change})
    assert response["statusCode"] == expected_status
    assert response_body(response)["error"] == expected_error
    assert rig["ses"].sent == []
    assert rig["dynamo"].rows("verification") == []


@pytest.mark.parametrize("missing", ["sourceIp", "sender"])
def test_request_requires_source_ip_and_configured_sender(rig, monkeypatch, missing):
    if missing == "sender":
        monkeypatch.delenv("EMAIL_SENDER_ADDRESS")
        event = api_event("email/request", {"gameId": GAME_ID, "email": EMAIL, "consent": True})
    else:
        event = api_event("email/request", {"gameId": GAME_ID, "email": EMAIL, "consent": True})
        event["requestContext"]["http"].pop("sourceIp")
    response = public_mod.lambda_handler(event, None)
    assert response["statusCode"] == 503
    assert response_body(response)["error"] == "VerificationUnavailable"
    assert rig["dynamo"].rows("verification") == []
    assert rig["ses"].sent == []


def test_wrong_codes_count_atomically_and_expiry_is_checked_without_ttl_deletion(rig, monkeypatch):
    response = response_body(request_code())
    challenge_id = response["challengeId"]
    for attempt in range(4):
        failed = join(rig, challenge_id, "999999")
        assert failed["statusCode"] == 400
        assert response_body(failed)["error"] == "InvalidVerificationCode"
    exhausted = join(rig, challenge_id, "999999")
    assert exhausted["statusCode"] == 429
    assert response_body(exhausted)["error"] == "VerificationAttemptsExceeded"
    challenge = rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{challenge_id}"}
    )["Item"]
    assert challenge["attempts"] == 5
    assert join(rig, challenge_id, sent_code(rig))["statusCode"] == 429

    current = [1_700_000_000]
    monkeypatch.setattr(verification.time, "time", lambda: current[0])
    expired = response_body(request_code(email="expired@example.com"))
    current[0] += verification.CODE_TTL + 1
    result = join(rig, expired["challengeId"], sent_code(rig))
    assert result["statusCode"] == 410
    assert response_body(result)["error"] == "VerificationExpired"
    # The challenge remains stored: correctness does not depend on asynchronous TTL cleanup.
    assert rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{expired['challengeId']}"}
    )["Item"]["expiresAt"] < current[0]


def test_resend_cooldown_supersedes_previous_code_and_email_hourly_quota(rig, monkeypatch):
    clock = [1_700_000_000]
    monkeypatch.setattr(verification.time, "time", lambda: clock[0])
    first = response_body(request_code())
    early = request_code()
    assert early["statusCode"] == 429
    assert response_body(early)["error"] == "EmailRateLimited"

    clock[0] += verification.RESEND_SECONDS
    second = response_body(request_code())
    old = join(rig, first["challengeId"], sent_code(rig, 0))
    assert old["statusCode"] == 409
    assert response_body(old)["error"] == "VerificationSuperseded"
    assert join(rig, second["challengeId"], sent_code(rig, 1))["statusCode"] == 200

    for index in range(3):
        clock[0] += verification.RESEND_SECONDS
        assert request_code()["statusCode"] == 200
    clock[0] += verification.RESEND_SECONDS
    limited = request_code()
    assert limited["statusCode"] == 429
    assert response_body(limited)["error"] == "EmailRateLimited"


@pytest.mark.parametrize(
    ("email_limit", "ip_limit", "global_limit", "second_ip", "expected_success"),
    [
        (5, 1, 10, "203.0.113.8", 1),
        (5, 10, 1, "203.0.113.9", 1),
    ],
)
def test_hourly_ip_and_global_limits_cross_email_addresses(
    rig, monkeypatch, email_limit, ip_limit, global_limit, second_ip, expected_success
):
    monkeypatch.setenv("EMAIL_MAX_PER_HOUR", str(email_limit))
    monkeypatch.setenv("EMAIL_IP_MAX_PER_HOUR", str(ip_limit))
    monkeypatch.setenv("EMAIL_GLOBAL_MAX_PER_HOUR", str(global_limit))
    first = request_code(email="first@example.com")
    second = request_code(email="second@example.com", ip=second_ip)
    assert first["statusCode"] == 200
    assert second["statusCode"] == 429
    assert sum(call["Destination"]["ToAddresses"][0] in ("first@example.com", "second@example.com")
               for call in rig["ses"].sent) == expected_success


def test_concurrent_requests_cannot_overrun_ip_quota(rig, monkeypatch):
    monkeypatch.setenv("EMAIL_IP_MAX_PER_HOUR", "3")
    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(
            lambda index: request_code(email=f"person{index}@example.com"), range(12)
        ))
    assert sum(response["statusCode"] == 200 for response in responses) == 3
    assert sum(response["statusCode"] == 429 for response in responses) == 9
    assert len(rig["ses"].sent) == 3
    rate = [row for row in rig["dynamo"].rows("verification") if row["id"].startswith("rate:ip:")]
    assert len(rate) == 1 and rate[0]["count"] == 3


def test_concurrent_wrong_codes_stop_at_five_attempts(rig):
    challenge_id = response_body(request_code())["challengeId"]
    with ThreadPoolExecutor(max_workers=20) as pool:
        responses = list(pool.map(lambda _: join(rig, challenge_id, "111111"), range(20)))
    stored = rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{challenge_id}"}
    )["Item"]
    assert stored["attempts"] == 5
    assert sum(response["statusCode"] == 400 for response in responses) == 4
    assert sum(response["statusCode"] == 429 for response in responses) == 16


@pytest.mark.parametrize("tamper", ["receipt", "email", "game"])
def test_assign_rejects_forged_or_misbound_receipts_before_email_recovery(rig, tamper):
    requested = response_body(request_code())
    challenge_id = requested["challengeId"]
    code = sent_code(rig)
    assert join(rig, challenge_id, code)["statusCode"] == 200
    challenge = rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{challenge_id}"}
    )["Item"]
    receipt = challenge_id + ".forged" if tamper == "receipt" else (
        challenge_id + "." + verification._digest(
            SECRET, "receipt", f"{challenge_id}:{challenge['binding']}"
        )
    )
    game_id = "DEF456" if tamper == "game" else GAME_ID
    email = "other@example.com" if tamper == "email" else EMAIL
    rig["dynamo"].seed("players", {
        "gameId": game_id, "playerId": 1, "email": email, "type": {"T1MER": {}},
    })
    rig["dynamo"].seed("games", {
        "gameId": game_id, "gameType": "T1MER", "isActive": True,
        "maxPlayers": 10, "participantFields": default_participant_fields("T1MER"),
    })
    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(
            assign_mod, "_find_verified_player",
            lambda *_args: pytest.fail("invalid receipt reached existing-player recovery"),
        )
        result = assign_mod.lambda_handler({
            "source": "web", "game_id": game_id, "email": email,
            "email_verification": receipt,
        }, None)
    finally:
        monkeypatch.undo()
    assert result["ok"] is False
    assert result["reason"] in {"VerificationRequired", "VerificationSuperseded"}


def test_direct_web_assign_without_receipt_cannot_recover_existing_player(rig, monkeypatch):
    rig["dynamo"].seed("players", {
        "gameId": GAME_ID, "playerId": 7, "email": EMAIL, "type": {"T1MER": {}},
    })
    monkeypatch.setattr(
        assign_mod, "_find_verified_player",
        lambda *_args: pytest.fail("unverified request recovered an existing player"),
    )
    result = assign_mod.lambda_handler({
        "source": "web", "game_id": GAME_ID, "email": EMAIL,
    }, None)
    assert result == {"ok": False, "reason": "VerificationRequired"}
    assert rig["dynamo"].rows("players")[0]["playerId"] == 7


def test_public_join_does_not_accept_email_as_a_verification_bypass(rig):
    rig["dynamo"].seed("players", {
        "gameId": GAME_ID, "playerId": 7, "email": EMAIL, "type": {"T1MER": {}},
    })
    response = api_call("join", {"gameId": GAME_ID, "email": EMAIL})
    assert response["statusCode"] == 403
    assert response_body(response)["error"] == "VerificationRequired"
    assert rig["invoke"].calls == []
    assert rig["dynamo"].rows("players")[0]["playerId"] == 7


def test_email_optional_join_remains_available_without_verification(rig):
    game = rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]
    game["participantFields"] = {"email": {"required": False}, "instagram": {"required": False}}
    rig["dynamo"].seed("games", game)
    response = api_call("join", {"gameId": GAME_ID, "consent": True})
    assert response["statusCode"] == 200
    assert response_body(response)["ok"] is True
    player = rig["dynamo"].rows("players")[0]
    assert "email" not in player
    assert "emailVerifiedAt" not in player
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1
    assert rig["invoke"].calls[0]["Payload"]


def test_verified_join_atomically_creates_player_count_and_identity(rig):
    challenge = response_body(request_code())
    response = join(rig, challenge["challengeId"], sent_code(rig))

    assert response["statusCode"] == 200
    result = response_body(response)
    assert result["ok"] is True
    assert result["playerToken"]
    assert "email_verification" not in result
    invoke_payload = json.loads(rig["invoke"].calls[0]["Payload"].decode("utf-8"))
    assert invoke_payload["email_verification"].startswith(challenge["challengeId"] + ".")
    assert invoke_payload["email"] == EMAIL
    player = rig["dynamo"].rows("players")[0]
    assert player["email"] == EMAIL
    assert player["emailVerificationMethod"] == "email-code"
    assert player["emailVerifiedAt"]
    game = rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]
    assert game["playersCount"] == 1
    mapping = rig["dynamo"].Table("verification").get_item(
        Key={"id": identity_id(SECRET, GAME_ID, EMAIL)}
    )["Item"]
    assert mapping["playerId"] == player["playerId"]
    record = rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{challenge['challengeId']}"}
    )["Item"]
    assert record["state"] == "completed"
    assert record["playerId"] == player["playerId"]


def test_same_challenge_retry_is_idempotent_and_preserves_stack_attempts_and_progress(rig, monkeypatch):
    import stand_common.stack as stack

    game = rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]
    game["gameType"] = "STACK"
    game["participantFields"] = default_participant_fields("STACK")
    rig["dynamo"].seed("games", game)
    monkeypatch.setattr(assign_mod, "ASSIGNERS", {
        "STACK": lambda _event: ({
            "type": {"STACK": {"attempts": 0, "progress": {"level": 0, "score": 0}}},
        }, "", []),
    })
    monkeypatch.setattr(stack, "public_assignment", lambda *_args: {"score": 0})
    challenge = response_body(request_code())
    code = sent_code(rig)
    assert join(rig, challenge["challengeId"], code)["statusCode"] == 200
    player = rig["dynamo"].rows("players")[0]
    player["type"]["STACK"].update(attempts=4, progress={"level": 3, "score": 88})
    rig["dynamo"].seed("players", player)
    before = rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{challenge['challengeId']}"}
    )["Item"]
    retry = join(rig, challenge["challengeId"], code)
    after = rig["dynamo"].Table("verification").get_item(
        Key={"id": f"challenge:{challenge['challengeId']}"}
    )["Item"]
    assert retry["statusCode"] == 200
    assert response_body(retry)["alreadyJoined"] is True
    assert after["attempts"] == before["attempts"] == 0
    assert after["verifiedAt"] == before["verifiedAt"]
    assert len(rig["dynamo"].rows("players")) == 1
    persisted = rig["dynamo"].Table("players").get_item(
        Key={"gameId": GAME_ID, "playerId": player["playerId"]}
    )["Item"]["type"]["STACK"]
    assert persisted["attempts"] == 4
    assert persisted["progress"] == {"level": 3, "score": 88}
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1


def test_new_challenge_recovers_existing_player_after_lost_response(rig, monkeypatch):
    clock = [1_700_000_000]
    monkeypatch.setattr(verification.time, "time", lambda: clock[0])
    first = response_body(request_code())
    assert join(rig, first["challengeId"], sent_code(rig))["statusCode"] == 200
    clock[0] += verification.RESEND_SECONDS
    second = response_body(request_code())
    recovered = join(rig, second["challengeId"], sent_code(rig, 1))

    assert recovered["statusCode"] == 200
    assert response_body(recovered)["alreadyJoined"] is True
    assert len(rig["dynamo"].rows("players")) == 1
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1


def test_simultaneous_confirmed_joins_create_at_most_one_player(rig):
    challenge = response_body(request_code())
    event = api_event("join", {
        "gameId": GAME_ID, "challengeId": challenge["challengeId"], "code": sent_code(rig),
    })
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda _: public_mod.lambda_handler(event, None), range(8)))

    assert all(response["statusCode"] == 200 for response in responses)
    assert sum(not response_body(response)["alreadyJoined"] for response in responses) == 1
    assert len(rig["dynamo"].rows("players")) == 1
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1


def test_capacity_failure_rolls_back_player_and_identity_transaction(rig):
    game = rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]
    game.update(maxPlayers=1, playersCount=1)
    rig["dynamo"].seed("games", game)
    challenge = response_body(request_code())
    response = join(rig, challenge["challengeId"], sent_code(rig))

    assert response["statusCode"] == 403
    assert response_body(response)["error"] == "player_limit_reached"
    assert rig["dynamo"].rows("players") == []
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1
    assert rig["dynamo"].Table("verification").get_item(
        Key={"id": identity_id(SECRET, GAME_ID, EMAIL)}
    ) == {}


def test_retry_recovers_atomic_join_if_challenge_finalization_fails(rig, monkeypatch):
    challenge = response_body(request_code())
    code = sent_code(rig)
    table = rig["dynamo"].Table("verification")
    original_update = table.update_item
    raised = [False]

    def fail_completion_once(**kwargs):
        if not raised[0] and ":completed" in (kwargs.get("ExpressionAttributeValues") or {}):
            raised[0] = True
            raise RuntimeError("simulated completion write failure")
        return original_update(**kwargs)

    monkeypatch.setattr(table, "update_item", fail_completion_once)
    first = join(rig, challenge["challengeId"], code)
    assert first["statusCode"] != 200
    assert len(rig["dynamo"].rows("players")) == 1
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1

    retry = join(rig, challenge["challengeId"], code)
    assert retry["statusCode"] == 200
    assert response_body(retry)["alreadyJoined"] is True
    assert len(rig["dynamo"].rows("players")) == 1
    assert rig["dynamo"].Table("games").get_item(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 1


def test_ses_failure_is_explicit_and_returns_no_challenge_or_code(rig):
    rig["ses"].exception = FakeDynamoError("MessageRejected")
    response = request_code()
    body = response_body(response)

    assert response["statusCode"] == 502
    assert body == {"ok": False, "error": "EmailDeliveryFailed", "retryAfter": 60}
    assert "challengeId" not in body and "code" not in body
    challenge = next(row for row in rig["dynamo"].rows("verification") if row["id"].startswith("challenge:"))
    assert challenge["state"] == "failed"
    assert len([row for row in rig["dynamo"].rows("verification") if row["id"].startswith("rate:")]) == 3


@pytest.mark.parametrize("signature", ["", "A" * 64, "g" * 64, "0" * 63, "\u00e9" * 64])
def test_malformed_receipt_signatures_are_rejected_not_internal_errors(rig, signature):
    challenge = response_body(request_code())
    result = assign_mod.lambda_handler({
        "source": "web", "game_id": GAME_ID, "email": EMAIL,
        "email_verification": challenge["challengeId"] + "." + signature,
    }, None)
    assert result == {"ok": False, "reason": "VerificationRequired"}
    assert rig["dynamo"].rows("players") == []


@pytest.mark.parametrize(
    ("raw", "function_error"),
    [
        ('{"ok": true}', "Unhandled"),
        ("", None),
        ("null", None),
        ("[]", None),
        ('{"ok": "true"}', None),
        ('{"assignment": {}}', None),
        ("not-json", None),
    ],
)
def test_assignment_invocation_errors_never_return_a_success_session(rig, monkeypatch, raw, function_error):
    challenge = response_body(request_code())

    def invoke(**_kwargs):
        result = {"Payload": io.BytesIO(raw.encode("utf-8"))}
        if function_error:
            result["FunctionError"] = function_error
        return result

    monkeypatch.setattr(rig["invoke"], "invoke", invoke)
    response = join(rig, challenge["challengeId"], sent_code(rig))
    assert response["statusCode"] == 500
    assert response_body(response) == {"ok": False, "error": "InternalError"}
    assert rig["dynamo"].rows("players") == []


@pytest.mark.parametrize(
    ("game_change", "status", "error"),
    [("inactive", 403, "GameInactive"), ("deleted", 404, "GameNotFound")],
)
def test_game_changes_before_registration_transaction_do_not_consume_capacity(
    rig, monkeypatch, game_change, status, error
):
    challenge = response_body(request_code())
    table = rig["dynamo"].Table("verification")
    transact = table.meta.client.transact_write_items
    game_table = rig["dynamo"].Table("games")
    original_get = game_table.get_item
    reads = []

    def get_game(**kwargs):
        reads.append(kwargs)
        return original_get(**kwargs)

    def change_game_before_commit(TransactItems):
        if any(action.get("Update", {}).get("TableName") == "games" for action in TransactItems):
            with rig["dynamo"].lock:
                if game_change == "inactive":
                    game_table.items[(GAME_ID,)]["isActive"] = False
                else:
                    del game_table.items[(GAME_ID,)]
        return transact(TransactItems=TransactItems)

    monkeypatch.setattr(game_table, "get_item", get_game)
    monkeypatch.setattr(table.meta.client, "transact_write_items", change_game_before_commit)
    response = join(rig, challenge["challengeId"], sent_code(rig))
    assert response["statusCode"] == status
    assert response_body(response)["error"] == error
    assert any(read.get("ConsistentRead") is True for read in reads)
    assert rig["dynamo"].rows("players") == []
    assert table.get_item(Key={"id": identity_id(SECRET, GAME_ID, EMAIL)}) == {}
    if game_change == "inactive":
        assert original_get(Key={"gameId": GAME_ID})["Item"]["playersCount"] == 0
