import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError


ROOT = Path(__file__).parent.parent


def _load(name, *parts):
    path = ROOT.joinpath(*parts)
    import sys

    if str(path.parent) not in sys.path:
        sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _http_event(method, body=None, *, sub="owner", query=None):
    return {
        "httpMethod": method,
        "queryStringParameters": query or {},
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": sub}}}},
        "body": json.dumps(body) if body is not None else None,
    }


def test_instagram_post_rejects_missing_and_bad_signatures(monkeypatch):
    webhook = _load(
        "audit_instagram_webhook",
        "src",
        "stand_prod_webhook_fn_instagram",
        "lambda_function.py",
    )
    webhook.IG_APP_SECRET = "app-secret"
    webhook.lambda_client = MagicMock()
    body = '{"entry":[]}'

    missing = webhook.lambda_handler({"httpMethod": "POST", "body": body}, None)
    expected = hmac.new(b"app-secret", body.encode(), hashlib.sha256).hexdigest()
    bad = webhook.lambda_handler(
        {"httpMethod": "POST", "body": body, "headers": {"X-Hub-Signature-256": "sha256=bad"}},
        None,
    )
    assert missing["statusCode"] == bad["statusCode"] == 401
    webhook.lambda_client.invoke.assert_not_called()

    monkeypatch.setattr(webhook, "_get_ig_profile_for_assign", lambda _psid: (None, None))
    webhook.lambda_client.invoke.side_effect = RuntimeError("Lambda API unavailable")
    dispatch_body = json.dumps(
        {"entry": [{"messaging": [{"sender": {"id": "psid"}, "message": {"text": "ABC123"}}]}]}
    )
    dispatch_signature = hmac.new(b"app-secret", dispatch_body.encode(), hashlib.sha256).hexdigest()
    failed_dispatch = webhook.lambda_handler(
        {
            "httpMethod": "POST",
            "body": dispatch_body,
            "headers": {"X-Hub-Signature-256": f"sha256={dispatch_signature}"},
        },
        None,
    )
    assert failed_dispatch["statusCode"] == 503


def test_instagram_get_verification_fails_closed_without_configured_token():
    webhook = _load(
        "audit_instagram_verify",
        "src",
        "stand_prod_webhook_fn_instagram",
        "lambda_function.py",
    )
    webhook.VERIFY_TOKEN = ""
    response = webhook.lambda_handler(
        {
            "httpMethod": "GET",
            "queryStringParameters": {"hub.challenge": "challenge", "hub.verify_token": "anything"},
        },
        None,
    )
    assert response["statusCode"] == 503


def test_quiz_owner_only_for_http_reads_and_configuration(monkeypatch):
    quiz = _load("audit_quiz", "src", "stand_prod_game_fn_quiz", "lambda_function.py")
    monkeypatch.setattr(quiz, "_get_quiz_meta", lambda _game_id: ([], {}, {"ownerUserId": "owner"}))

    get_response = quiz.lambda_handler(_http_event("GET", sub="intruder", query={"gameId": "GAME01"}), None)
    post_response = quiz.lambda_handler(
        _http_event("POST", {"gameId": "GAME01", "questions": [{"id": "q1"}]}, sub="intruder"),
        None,
    )
    assert get_response["statusCode"] == post_response["statusCode"] == 403


def test_raffle_and_score_require_game_owner(monkeypatch):
    raffle = _load("audit_raffle", "src", "stand_prod_game_fn_raffle", "lambda_function.py")
    monkeypatch.setattr(raffle, "_get_game", lambda _game_id: {"ownerUserId": "owner"})
    raffle_response = raffle.lambda_handler(
        _http_event("POST", {"gameId": "GAME01", "numberOfWinners": 1}, sub="intruder"),
        None,
    )
    assert raffle_response["statusCode"] == 403

    score = _load("audit_score", "src", "stand_prod_game_score", "lambda_function.py")
    monkeypatch.setattr(score, "_get_game_meta", lambda _game_id: {"ownerUserId": "owner", "gameType": "T1MER"})
    score.t1mer.store_score = MagicMock()
    score_response = score.lambda_handler(
        _http_event("POST", {"gameId": "GAME01", "playerId": 1, "timer": 1, "score": 1}, sub="intruder"),
        None,
    )
    assert score_response["statusCode"] == 403
    score.t1mer.store_score.assert_not_called()


def test_quiz_sqs_reports_failures_for_retry(monkeypatch):
    quiz = _load("audit_quiz_sqs", "src", "stand_prod_game_fn_quiz", "lambda_function.py")
    monkeypatch.setattr(quiz, "_get_quiz_meta", lambda _game_id: ([], {}, None))
    monkeypatch.setattr(quiz, "_send_dm", lambda *_args: None)
    response = quiz.lambda_handler(
        {
            "Records": [
                {"messageId": "failed-start", "body": json.dumps({"kind": "quiz_start", "gameId": "GAME01", "psid": ["p1"]})},
                {"messageId": "ignored", "body": json.dumps({"kind": "unknown"})},
            ]
        },
        None,
    )
    assert response == {"batchItemFailures": [{"itemIdentifier": "failed-start"}]}


def test_semaforo_validation_returns_only_required_game_and_player_data():
    semaforo = _load(
        "audit_semaforo",
        "src",
        "stand_prod_game_validate",
        "validators",
        "semaforo.py",
    )
    players = {
        1111: {
            "playerId": 1,
            "instagramUsername": "@one",
            "instagramPSID": "private-psid-1",
            "validationCode": 1111,
            "type": {"SEMAFORO": {"quizCompleted": True, "quizAnswers": {"q1": "a"}, "secret": "private"}},
        },
        2222: {
            "playerId": 2,
            "instagramUsername": "@two",
            "instagramPSID": "private-psid-2",
            "validationCode": 2222,
            "type": {"SEMAFORO": {"quizCompleted": True, "quizAnswers": {"q1": "b"}, "secret": "private"}},
        },
    }
    ctx = {
        "gameId": "GAME01",
        "gameType": "SEMAFORO",
        "codes": ["1111", "2222"],
        "gameMeta": {
            "gameId": "GAME01",
            "ownerUserId": "private-owner",
            "raffleWinners": [{"instagramPSID": "private-winner"}],
            "quizOrder": ["q1"],
            "quizQuestions": {
                "q1": {
                    "text": "Question",
                    "correctAnswerId": "private-answer",
                    "options": [{"title": "Answer", "payload": "a", "answerId": "private-answer"}],
                }
            },
        },
        "to_int_code": int,
        "query_players_by_code": lambda _game_id, code: [players[code]],
        "set_validated": lambda *_args: True,
        "inc_validated_count": None,
        "log": lambda *_args: None,
    }

    result = semaforo.validate_semaforo(ctx)
    assert result["game"] == {
        "gameId": "GAME01",
        "quizOrder": ["q1"],
        "quizQuestions": {"q1": {"text": "Question", "options": [{"title": "Answer", "payload": "a"}]}},
    }
    assert all("instagramPSID" not in player and "validationCode" not in player for player in result["players"])
    assert all("secret" not in player["type"]["SEMAFORO"] for player in result["players"])


def test_user_media_bootstrap_preserves_concurrently_created_paid_user():
    media = _load("audit_user_media", "src", "stand_prod_user_fn_media", "lambda_function.py")
    table = MagicMock()
    paid_user = {"userId": "user-1", "plan": "EVENT_24H", "activeUntil": "2099-01-01T00:00:00Z"}
    table.get_item.side_effect = [{}, {"Item": paid_user}]
    table.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}},
        "PutItem",
    )

    user, _ = media._load_user_with_media(table, "user-1")
    assert user["plan"] == "EVENT_24H"
    assert user["activeUntil"] == paid_user["activeUntil"]
    table.put_item.assert_called_once()


def test_user_media_rejects_over_limit_upload_size():
    media = _load("audit_media_size", "src", "stand_prod_user_fn_media", "lambda_function.py")
    media.USERS_TABLE = "users"
    media.USER_MEDIA_BUCKET = "bucket"
    media.dynamodb = MagicMock()
    table = MagicMock()
    table.get_item.return_value = {"Item": {"userId": "owner", "media": media._empty_media()}}
    media.dynamodb.Table.return_value = table
    media.s3 = MagicMock()

    response = media.lambda_handler(
        _http_event(
            "POST",
            {"files": [{"kind": "logo", "fileName": "logo.png", "contentType": "image/png", "sizeBytes": media.MAX_LOGO_SIZE + 1}]},
            query={"action": "upload-url"},
        ),
        None,
    )
    assert response["statusCode"] == 400
    media.s3.generate_presigned_post.assert_not_called()


def test_user_media_registration_rejects_foreign_s3_key(monkeypatch):
    media = _load("audit_media_key", "src", "stand_prod_user_fn_media", "lambda_function.py")
    table = MagicMock()
    monkeypatch.setattr(media, "_load_user_with_media", lambda *_args: ({"userId": "owner"}, media._empty_media()))
    response = media._handle_register(
        {"body": json.dumps({"items": [{"id": "m1", "kind": "logo", "key": "users/other/logos/x.png"}]})},
        table,
        "owner",
    )
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_infocards_processing_rejects_keys_from_other_games():
    process = _load("audit_infocards_process", "src", "stand_prod_infocards_fn_process", "lambda_function.py")
    process.GAMES_TABLE = "games"
    process.games_table = MagicMock()
    process.games_table.get_item.return_value = {"Item": {"gameId": "GAME01", "ownerUserId": "owner"}}
    process.s3 = MagicMock()
    response = process.lambda_handler(
        _http_event(
            "POST",
            {"gameId": "GAME01", "fileKeys": ["infocards/GAME02/secret.pdf"]},
        ),
        None,
    )
    assert response["statusCode"] == 400
    process.s3.get_object.assert_not_called()


def test_t1mer_score_response_uses_submitted_timer():
    t1mer = _load("audit_t1mer", "src", "stand_prod_game_score", "games", "t1mer.py")
    table = MagicMock()
    table.get_item.return_value = {"Item": {"instagramUsername": "@one"}}
    table.query.return_value = {
        "Items": [
            {"playerId": 1, "type": {"T1MER": {"score": 50, "timer": 4.5}}},
            {"playerId": 2, "type": {"T1MER": {"score": 1, "timer": 99}}},
        ]
    }

    result = t1mer.store_score(
        "GAME01",
        "T1MER",
        table,
        {"playerId": 1, "timer": "4.5", "score": 50},
        lambda *_args: None,
    )
    assert result["timer"] == 4.5
    assert result["rank"] == 2


def test_t1mer_rejects_non_finite_timer_before_writing():
    t1mer = _load("audit_t1mer_invalid", "src", "stand_prod_game_score", "games", "t1mer.py")
    table = MagicMock()
    result = t1mer.store_score(
        "GAME01",
        "T1MER",
        table,
        {"playerId": 1, "timer": "NaN", "score": 1},
        lambda *_args: None,
    )
    assert result["error"] == "invalid_timer"
    table.update_item.assert_not_called()


def test_validation_codes_are_unique_and_reserved_atomically(monkeypatch):
    assign = _load(
        "audit_assign_codes",
        "src",
        "stand_prod_game_fn_assign",
        "lambda_function.py",
    )
    assign.games_table = MagicMock()
    assign.games_table.get_item.return_value = {
        "Item": {"validationCodesInitialized": True, "validationCodeOffset": 2500}
    }
    counter = 0
    reserved = set()

    def update_item(**kwargs):
        nonlocal counter
        if "validationCodeCounter =" in kwargs["UpdateExpression"]:
            counter += 1
            return {"Attributes": {"validationCodeCounter": counter}}
        code = int(kwargs["ExpressionAttributeNames"]["#code"])
        if code in reserved:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
        reserved.add(code)
        return {}

    monkeypatch.setattr(assign.games_table, "update_item", update_item)
    codes = [assign._code4("GAME01") for _ in range(2)]
    assert codes == [3500, 3501]
    assert len(reserved) == 2


def test_stripe_grant_uses_atomic_dedupe_and_entitlement_transaction():
    webhook = _load("audit_stripe", "src", "stand_prod_webhook_fn_stripe", "lambda_function.py")
    webhook.USERS_TABLE = "users"
    webhook.STRIPE_EVENTS_TABLE = "stripe-events"
    users = MagicMock()
    events = MagicMock()
    client = MagicMock()
    users.meta.client = client
    users.get_item.return_value = {"Item": {"userId": "u1"}}
    webhook.dynamodb = MagicMock()
    webhook.dynamodb.Table.side_effect = lambda name: {"users": users, "stripe-events": events}[name]

    assert webhook._grant_paid_access("u1", "evt_1", {"id": "cs_1"})
    transaction = client.transact_write_items.call_args.kwargs["TransactItems"]
    assert transaction[0]["Put"]["Item"]["eventId"] == {"S": "evt_1"}
    assert transaction[1]["Update"]["ConditionExpression"] == "attribute_not_exists(activeUntil)"
    assert transaction[1]["Update"]["ExpressionAttributeValues"][":lastCheckoutSessionId"] == {"S": "cs_1"}


def test_stripe_duplicate_event_is_acknowledged_without_second_grant():
    webhook = _load("audit_stripe_duplicate", "src", "stand_prod_webhook_fn_stripe", "lambda_function.py")
    webhook.USERS_TABLE = "users"
    webhook.STRIPE_EVENTS_TABLE = "stripe-events"
    users = MagicMock()
    events = MagicMock()
    users.get_item.return_value = {"Item": {"userId": "u1"}}
    users.meta.client.transact_write_items.side_effect = ClientError(
        {"Error": {"Code": "TransactionCanceledException"}},
        "TransactWriteItems",
    )
    events.get_item.return_value = {"Item": {"eventId": "evt_1"}}
    webhook.dynamodb = MagicMock()
    webhook.dynamodb.Table.side_effect = lambda name: {"users": users, "stripe-events": events}[name]

    assert not webhook._grant_paid_access("u1", "evt_1", {})
    users.meta.client.transact_write_items.assert_called_once()


def test_checkout_passes_stripe_idempotency_key():
    checkout = _load("audit_checkout", "src", "stand_prod_billing_fn_checkout", "lambda_function.py")
    checkout.USERS_TABLE = "users"
    checkout.STRIPE_SECRET_KEY = "sk_test"
    checkout.STRIPE_PRICE_ID = "price"
    checkout.STRIPE_SUCCESS_URL = "https://success"
    checkout.STRIPE_CANCEL_URL = "https://cancel"
    checkout.dynamodb = MagicMock()
    table = MagicMock()
    table.get_item.return_value = {"Item": {"userId": "u1", "plan": "FREE"}}
    checkout.dynamodb.Table.return_value = table
    checkout.stripe.checkout.Session.create.return_value = {"id": "cs_1", "url": "https://checkout"}
    event = _http_event("POST", sub="u1")
    event["requestContext"]["requestId"] = "request-1"

    response = checkout.handle_checkout(event)
    assert response["statusCode"] == 200
    assert checkout.stripe.checkout.Session.create.call_args.kwargs["idempotency_key"] == "checkout_u1_request-1"
