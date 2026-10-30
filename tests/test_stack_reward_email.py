import json
from email import policy
from email.parser import BytesParser
from unittest.mock import MagicMock

import pytest
import qrcode

from stand_common import stack_prize_api, stack_prizes as prizes
from stand_common.email_verification import RegistrationError, registration_values
from stand_common.reward_email import build_message
from stand_common.utils import normalize_participant_fields, web_participant_fields
from tests.email_fake import FakeDynamoError
from tests.test_stack_prize_rules import (
    rig, meta, player, delivery_module, PROGRAM, ATTEMPT,
)


@pytest.mark.parametrize("channel", ["wallet", "email"])
@pytest.mark.parametrize("phone,error", [(None, "missing_phone"), ("", "missing_phone"),
                                       ("not-a-phone", "invalid_phone"), ("+34\n1234", "invalid_phone")])
def test_stack_always_requires_phone(channel, phone, error):
    game = {"gameType": "STACK", "participantFields": {"phone": {"enabled": False}},
            "stackPrizeProgram": {**PROGRAM, "deliveryChannel": channel}}
    with pytest.raises(RegistrationError, match=error):
        registration_values(game, {"username": "Player", "email": "player@example.test", "phone": phone})
    assert web_participant_fields("STACK", game["participantFields"])["phone"] == {
        "enabled": True, "required": True,
    }


def test_stack_rejects_disabling_phone_on_settings_write_but_reads_old_settings():
    fields = {"phone": {"enabled": False, "required": False}}
    with pytest.raises(ValueError, match="STACK requires"):
        normalize_participant_fields("STACK", fields, strict=True)
    assert normalize_participant_fields("STACK", fields)["phone"]["required"] is True
    assert web_participant_fields("T1MER", fields)["phone"]["enabled"] is False


def test_program_channel_validation_and_compatibility(monkeypatch):
    assert prizes.upgrade_program({**PROGRAM, "deliveryChannel": "wallet"})["deliveryChannel"] == "wallet"
    old = {k: v for k, v in PROGRAM.items() if k != "deliveryChannel"}
    assert prizes.upgrade_program(old)["deliveryChannel"] == "wallet"
    for channel in ("both", None, [], True):
        with pytest.raises(prizes.PrizeError, match="InvalidStackDeliveryChannel"):
            prizes.validate_program({**PROGRAM, "deliveryChannel": channel}, {}, None, [], True)
    with pytest.raises(prizes.PrizeError, match="RewardEmailUnavailable"):
        prizes.validate_program({**PROGRAM, "deliveryChannel": "email"}, {}, None, [], True)
    monkeypatch.setenv("STACK_REWARD_EMAIL_ENABLED", "true")
    monkeypatch.setattr(prizes.registration, "account_game_settings", lambda *_: {"enabled": True})
    result = prizes.validate_program({**PROGRAM, "deliveryChannel": "email"}, {}, {"locationId": "loc"},
                                     [{"id": "r1", "name": "Coffee"}], True)
    assert result["deliveryChannel"] == "email"


def test_channel_is_pinned_at_start_not_at_award_or_game_close(rig):
    game = {**meta(rig), "stackPrizeProgram": {**PROGRAM, "deliveryChannel": "email"}}
    rig.Table("games").put_item(Item=game)
    prizes.start(rig, game, player(rig), ATTEMPT)
    game["stackPrizeProgram"]["deliveryChannel"] = "wallet"
    rig.Table("games").put_item(Item=game)
    current = player(rig)
    for condition in ("completed", "top_finish", "top_final", "raffle"):
        row = prizes.award_units(game, current, {"id": "rule", "condition": condition},
                                 PROGRAM["catalog"][1], 1)[0]
        assert row["deliveryChannel"] == "email"
    assert prizes.wallet_link(rig, game, current) is None


@pytest.fixture
def email_rig(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    monkeypatch.setenv("STACK_REWARD_EMAIL_ENABLED", "true")
    monkeypatch.setenv("EMAIL_SENDER_ADDRESS", "no-reply@example.test")
    monkeypatch.setattr(worker, "BotoCoreError", OSError)
    current = player(rig)
    current.update(email="verified@example.test", emailVerifiedAt="2026-10-30T00:00:00Z",
                   phone="+34 111111111")
    current["type"]["STACK"]["attempt"] = {"prizeProgram": {**PROGRAM, "deliveryChannel": "email"}}
    rig.Table("players").put_item(Item=current)
    row = prizes.award_units(meta(rig), current, PROGRAM["rules"][0], PROGRAM["catalog"][1], 1)[0]
    rig.Table("prizes").put_item(Item=row)
    client, ses = MagicMock(), MagicMock()
    client.earn_reward.return_value = {"success": True}
    client.find_customer.return_value = {
        "id": "customer", "is_active": True, "phone": "+34 999999999",
    }
    ses.send_raw_email.return_value = {"MessageId": "message-1"}
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    monkeypatch.setattr(worker.boto3, "client", lambda *_, **__: ses)
    return worker, rig, row, client, ses


def stored(rig, row):
    return rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]


def process(worker, row):
    event = {"Records": [{"messageId": "m1", "body": json.dumps({"gameId": row["gameId"], "id": row["id"]})}]}
    assert worker.lambda_handler(event, None) == {"batchItemFailures": []}


def test_email_is_sent_after_earn_once_using_kivly_phone_not_form(email_rig, monkeypatch):
    worker, rig, row, client, ses = email_rig
    payloads = []
    original = qrcode.QRCode.add_data
    def capture(self, data, *args, **kwargs):
        payloads.append(data)
        return original(self, data, *args, **kwargs)
    monkeypatch.setattr(qrcode.QRCode, "add_data", capture)
    process(worker, row)
    ses.send_raw_email.assert_not_called()
    assert stored(rig, row)["state"] == "confirmed"
    assert stored(rig, row)["work"] == "pending"
    process(worker, row)
    process(worker, row)
    assert payloads == ["+34 999999999"]
    assert stored(rig, row)["emailState"] == "accepted"
    assert "work" not in stored(rig, row)
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()
    sent = ses.send_raw_email.call_args.kwargs
    assert sent["Destinations"] == ["verified@example.test"]
    public = prizes.public_awards(rig, meta(rig), player(rig), lambda _: "token")
    assert public["walletUrl"] is None and public["walletExpected"] is False
    assert public["awards"][0]["emailState"] == "accepted"
    assert "+34" not in json.dumps(public)
    assert "verified@example.test" not in json.dumps(public)


def test_mail_has_inline_png_text_and_escaped_content():
    raw = build_message("sender@example.test", "player@example.test", "<script>name</script>",
                        {"name": "<b>Coffee</b>", "imageUrl": "https://images.example.test/photo.png"}, "+34123456789")
    message = BytesParser(policy=policy.default).parsebytes(raw)
    html = message.get_body(preferencelist=("html",)).get_content()
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "&lt;b&gt;Coffee&lt;/b&gt;" in html
    assert 'src="cid:reward-qr"' in html
    assert "No wallet needed" in html
    assert "Your reward is assigned" in message.get_body(preferencelist=("plain",)).get_content()
    image = next(p for p in message.walk() if p.get_content_type() == "image/png")
    assert image.get_content_disposition() == "inline"
    assert image["Content-ID"] == "<reward-qr>"
    assert image.get_payload(decode=True).startswith(b"\x89PNG\r\n\x1a\n")


@pytest.mark.parametrize("phone", [None, "", "invalid"])
def test_missing_kivly_phone_never_falls_back_to_form_or_reassigns(email_rig, phone):
    worker, rig, row, client, ses = email_rig
    client.find_customer.return_value["phone"] = phone
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["state"] == "confirmed"
    assert stored(rig, row)["emailError"] == "KivlyPhoneRequired"
    ses.send_raw_email.assert_not_called()
    client.earn_reward.assert_called_once()


def test_unverified_address_cannot_receive_reward_email(email_rig):
    worker, rig, row, client, ses = email_rig
    current = player(rig)
    del current["emailVerifiedAt"]
    rig.Table("players").put_item(Item=current)
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailError"] == "VerifiedEmailRequired"
    ses.send_raw_email.assert_not_called()


def test_ambiguous_ses_response_never_resends_or_reearns(email_rig):
    worker, rig, row, client, ses = email_rig
    ses.send_raw_email.side_effect = OSError("timeout")
    process(worker, row)
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "reconciliation_required"
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()


def test_ses_5xx_is_uncertain_not_safely_retryable(email_rig):
    worker, rig, row, client, ses = email_rig
    error = FakeDynamoError("ServiceUnavailable")
    error.response["ResponseMetadata"] = {"HTTPStatusCode": 503}
    ses.send_raw_email.side_effect = error
    process(worker, row)
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "reconciliation_required"
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()


def test_ses_sdk_does_not_retry_business_mutations_implicitly(email_rig, monkeypatch):
    worker, rig, row, client, ses = email_rig
    config = MagicMock()
    monkeypatch.setattr(worker, "Config", config)
    factory = MagicMock(return_value=ses)
    monkeypatch.setattr(worker.boto3, "client", factory)
    process(worker, row)
    process(worker, row)
    config.assert_called_once_with(
        retries={"mode": "standard", "total_max_attempts": 1}, connect_timeout=5, read_timeout=10,
    )
    factory.assert_called_once_with("ses", region_name="us-east-1", config=config.return_value)


def test_interrupted_email_requires_reconciliation_without_sending(email_rig):
    worker, rig, row, client, ses = email_rig
    row.update(state="confirmed", emailState="sending")
    rig.Table("prizes").put_item(Item=row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "reconciliation_required"
    ses.send_raw_email.assert_not_called()
    client.earn_reward.assert_not_called()


def test_throttling_retries_are_bounded_without_reassigning(email_rig):
    worker, rig, row, client, ses = email_rig
    ses.send_raw_email.side_effect = FakeDynamoError("Throttling")
    process(worker, row)
    for _ in range(4):
        process(worker, row)
    assert stored(rig, row)["emailState"] == "failed"
    assert stored(rig, row)["emailAttempts"] == 3
    assert ses.send_raw_email.call_count == 3
    client.earn_reward.assert_called_once()


def test_operator_can_retry_failed_email_after_fix_without_earning_again(email_rig, monkeypatch):
    worker, rig, row, client, ses = email_rig
    client.find_customer.return_value["phone"] = None
    process(worker, row)
    process(worker, row)
    monkeypatch.setattr(stack_prize_api, "_get_claims", lambda _: {"sub": "owner"})
    monkeypatch.setattr(stack_prize_api, "ClientError", FakeDynamoError)
    body = {"gameId": row["gameId"], "action": "retry_email", "awardId": row["id"]}
    event = {"body": json.dumps(body)}
    response = stack_prize_api.handle(event, "POST", rig, lambda _: True, None)
    assert response["statusCode"] == 200
    client.find_customer.return_value["phone"] = "+34123456789"
    process(worker, row)
    assert stored(rig, row)["emailState"] == "accepted"
    assert stack_prize_api.handle(event, "POST", rig, lambda _: True, None)["statusCode"] == 409
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()


def test_wallet_awards_never_send_email(email_rig):
    worker, rig, row, client, ses = email_rig
    row["deliveryChannel"] = "wallet"
    rig.Table("prizes").put_item(Item=row)
    process(worker, row)
    process(worker, row)
    assert "work" not in stored(rig, row)
    ses.send_raw_email.assert_not_called()


def test_disabled_email_remains_pending_and_does_not_send(email_rig, monkeypatch):
    worker, rig, row, client, ses = email_rig
    monkeypatch.setenv("STACK_REWARD_EMAIL_ENABLED", "false")
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "pending"
    assert stored(rig, row)["work"] == "pending"
    ses.send_raw_email.assert_not_called()
