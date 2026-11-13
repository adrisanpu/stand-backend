import json
from email import policy
from email.parser import BytesParser
from io import BytesIO
from unittest.mock import MagicMock

import pytest
import qrcode

from stand_common import stack_prize_api, stack_prizes as prizes
from stand_common.email_verification import RegistrationError, registration_values
from stand_common.reward_email import build_message, build_reward_message
from stand_common.utils import normalize_participant_fields, web_participant_fields
from tests.email_fake import FakeDynamoError
from tests.test_stack_prize_rules import (
    rig, meta, player, delivery_module, PROGRAM, ATTEMPT,
)

TEST_LOGO_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + b"\x00" * 13 + b"\x00" * 4
    + b"\x00\x00\x00\x00IEND\xaeB`\x82"
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


def test_stack_rejects_disabling_phone_and_normalizes_legacy_settings():
    fields = {"phone": {"enabled": False, "required": False}}
    with pytest.raises(ValueError, match="All games require name, email and phone"):
        normalize_participant_fields("STACK", fields, strict=True)
    assert normalize_participant_fields("STACK", fields)["phone"]["required"] is True
    assert web_participant_fields("T1MER", fields)["phone"]["enabled"] is True


def test_program_channel_is_legacy_only_and_normalizes_to_wallet():
    assert prizes.upgrade_program({**PROGRAM, "deliveryChannel": "wallet"})["deliveryChannel"] == "wallet"
    old = {k: v for k, v in PROGRAM.items() if k != "deliveryChannel"}
    assert prizes.upgrade_program(old)["deliveryChannel"] == "wallet"
    for channel in ("both", None, [], True):
        with pytest.raises(prizes.PrizeError, match="InvalidStackDeliveryChannel"):
            prizes.validate_program({**PROGRAM, "deliveryChannel": channel}, {}, None, [], True)
    result = prizes.validate_program({**PROGRAM, "deliveryChannel": "email"}, {}, {"locationId": "loc"},
                                     [{"id": "r1", "name": "Coffee"}], True)
    assert result["deliveryChannel"] == "wallet"


def test_legacy_email_channel_is_normalized_to_wallet(rig):
    game = {**meta(rig), "stackPrizeProgram": {**PROGRAM, "deliveryChannel": "email"}}
    rig.Table("games").put_item(Item=game)
    prizes.start(rig, game, player(rig), ATTEMPT)
    game["stackPrizeProgram"]["deliveryChannel"] = "wallet"
    rig.Table("games").put_item(Item=game)
    current = player(rig)
    for condition in ("completed", "top_finish", "top_final", "raffle"):
        row = prizes.award_units(game, current, {"id": "rule", "condition": condition},
                                 PROGRAM["catalog"][1], 1)[0]
        assert row["deliveryChannel"] == "wallet"
    assert prizes.wallet_link(rig, game, current) is None


@pytest.fixture
def email_rig(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    monkeypatch.setenv("STACK_REWARD_EMAIL_ENABLED", "true")
    monkeypatch.setenv("EMAIL_SENDER_ADDRESS", "no-reply@example.test")
    monkeypatch.setenv("PUBLIC_API_BASE_URL", "https://api.example.test/prod")
    monkeypatch.setenv("PLAYER_TOKEN_SECRET", "test-token-secret")
    monkeypatch.setattr(worker, "BotoCoreError", OSError)
    current = player(rig)
    current.update(email="verified@example.test", verifiedAt="2026-10-30T00:00:00Z",
                   verificationMethod="email-code", phone="+34 111111111")
    current["type"]["STACK"]["attempt"] = {"prizeProgram": {**PROGRAM, "deliveryChannel": "email"}}
    rig.Table("players").put_item(Item=current)
    row = prizes.award_units(meta(rig), current, PROGRAM["rules"][0], PROGRAM["catalog"][1], 1)[0]
    rig.Table("prizes").put_item(Item=row)
    client, ses = MagicMock(), MagicMock()
    client.earn_reward.return_value = {"success": True}
    client.get_customer.return_value = {
        "id": "customer", "is_active": True, "phone": "+34 999999999",
    }
    client.get_context.return_value = MagicMock(location_name="Club & Pizza", location_id="loc", owner_id="owner")
    ses.send_raw_email.return_value = {"MessageId": "message-1"}
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    branding = ("Club & Pizza",
                "https://example.kivly.store/add-to-wallet?customer_id=customer&location_id=loc",
                TEST_LOGO_PNG)
    monkeypatch.setattr(worker, "_email_branding", lambda *_: branding)
    monkeypatch.setattr(
        worker, "_kivly_wallet_link",
        lambda *_: "https://example.kivly.store/add-to-wallet?customer_id=customer&location_id=loc",
    )
    monkeypatch.setattr(worker.boto3, "client", lambda *_, **__: ses)
    return worker, rig, row, client, ses


def stored(rig, row):
    return rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]


def process(worker, row):
    event = {"Records": [{"messageId": "m1", "body": json.dumps({"gameId": row["gameId"], "id": row["id"]})}]}
    assert worker.lambda_handler(event, None) == {"batchItemFailures": []}


def test_survey_holds_kivly_assignment_and_email_then_releases_once(email_rig):
    worker, rig, row, client, ses = email_rig
    game = meta(rig)
    game["formConfig"] = {"quizMoment": "prize"}
    rig.Table("games").put_item(Item=game)
    process(worker, row)
    assert stored(rig, row)["state"] == "queued"
    assert stored(rig, row)["work"] == "pending"
    client.earn_reward.assert_not_called()
    ses.send_raw_email.assert_not_called()
    current = player(rig)
    current["type"]["STACK"]["quizCompleted"] = True
    rig.Table("players").put_item(Item=current)
    process(worker, row)
    process(worker, row)
    process(worker, row)
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()
    assert stored(rig, row)["emailState"] == "accepted"


def test_survey_holds_stand_email_without_losing_award(email_rig):
    worker, rig, _, _, ses = email_rig
    game = meta(rig)
    game["formConfig"] = {"quizMoment": "prize"}
    rig.Table("games").put_item(Item=game)
    row = prizes.award_units(game, player(rig), PROGRAM["rules"][0], PROGRAM["catalog"][0], 1)[0]
    rig.Table("prizes").put_item(Item=row)
    process(worker, row)
    assert stored(rig, row)["state"] == "available"
    assert stored(rig, row)["emailState"] == "pending"
    ses.send_raw_email.assert_not_called()
    with pytest.raises(prizes.PrizeError, match="QuizRequired"):
        prizes.redeem(rig, game["gameId"], 1, row["id"])


def test_email_is_sent_after_earn_once_with_kivly_wallet_link(email_rig):
    worker, rig, row, client, ses = email_rig
    process(worker, row)
    ses.send_raw_email.assert_not_called()
    assert stored(rig, row)["state"] == "confirmed"
    assert stored(rig, row)["work"] == "pending"
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "accepted"
    assert stored(rig, row)["emailMessageId"] == "message-1"
    assert stored(rig, row)["emailAcceptedAt"]
    assert "work" not in stored(rig, row)
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()
    client.get_customer.assert_called_with("loc", "owner", "customer")
    client.find_customer.assert_not_called()
    sent = ses.send_raw_email.call_args.kwargs
    assert sent["Destinations"] == ["verified@example.test"]
    message = BytesParser(policy=policy.default).parsebytes(sent["RawMessage"]["Data"])
    html = message.get_body(preferencelist=("html",)).get_content()
    assert "Kivly Wallet" in html
    assert "add-to-wallet?customer_id=customer&amp;location_id=loc" in html
    assert "cid:club-logo" not in html and "cid:reward-qr" not in html
    public = prizes.public_awards(rig, meta(rig), player(rig), lambda _: "token")
    assert public["walletUrl"] == "https://example.kivly.store/add-to-wallet?customer_id=customer&location_id=loc"
    assert public["walletExpected"] is True
    assert public["awards"][0]["emailState"] == "accepted"
    owner = json.loads(stack_prize_api.response(rig, meta(rig))["body"])
    assert owner["awards"][0]["emailAcceptedAt"] == stored(rig, row)["emailAcceptedAt"]
    assert "+34" not in json.dumps(public)
    assert "verified@example.test" not in json.dumps(public)


def test_stand_award_email_omits_wallet_passes_and_keeps_redeem_qr(email_rig):
    worker, rig, _, client, ses = email_rig
    row = prizes.award_units(
        meta(rig), player(rig), PROGRAM["rules"][0], PROGRAM["catalog"][0], 1,
    )[0]
    rig.Table("prizes").put_item(Item=row)

    process(worker, row)

    saved = stored(rig, row)
    assert saved["state"] == "available"
    assert saved["emailState"] == "accepted"
    client.earn_reward.assert_not_called()
    ses.send_raw_email.assert_called_once()
    sent = ses.send_raw_email.call_args.kwargs
    message = BytesParser(policy=policy.default).parsebytes(sent["RawMessage"]["Data"])
    html = message.get_body(preferencelist=("html",)).get_content()
    text = message.get_body(preferencelist=("plain",)).get_content()
    assert "Apple Wallet" not in html and "Google Wallet" not in html
    assert "/v1/public/wallet/pass?" not in text
    assert "Show this QR code" in text
    qr_image = next(part for part in message.walk()
                    if part.get_content_type() == "image/png"
                    and str(part["Content-ID"]) == "<reward-qr>")
    assert qr_image.get_payload(decode=True).startswith(b"\x89PNG\r\n\x1a\n")


@pytest.mark.parametrize("game_type", [
    "EMPAREJA2", "T1MER", "STACK", "RULET4", "L3TRAS",
    "CUPIDO", "SEMAFORO", "INFOCARDS", "IMPOSTOR",
])
def test_native_stand_reward_email_uses_shared_format_for_every_game(email_rig, game_type):
    worker, rig, _, _, ses = email_rig
    game = meta(rig)
    game["gameType"] = game_type
    rig.Table("games").put_item(Item=game)
    row = {
        "gameId": game["gameId"], "id": "award_" + "0" * 64,
        "playerId": 1, "ownerUserId": game["ownerUserId"],
        "prize": {"source": "stand", "name": "Coffee"},
        "state": "available", "emailState": "pending", "work": "pending", "dueAt": 0,
    }
    rig.Table("prizes").put_item(Item=row)

    process(worker, row)

    assert stored(rig, row)["emailState"] == "accepted"
    ses.send_raw_email.assert_called_once()
    message = BytesParser(policy=policy.default).parsebytes(
        ses.send_raw_email.call_args.kwargs["RawMessage"]["Data"],
    )
    html = message.get_body(preferencelist=("html",)).get_content()
    text = message.get_body(preferencelist=("plain",)).get_content()
    assert "Tu premio STAND / Your STAND reward: Coffee" in str(message["Subject"])
    assert "Coffee" in html
    assert "Your reward QR" in html
    assert "Apple Wallet" not in html and "Google Wallet" not in html
    assert "/v1/public/wallet/pass" not in text
    assert "Muestra este QR" in text


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


def test_spanish_club_mail_contains_wallet_link_and_phone_qr(monkeypatch):
    payloads = []
    original = qrcode.QRCode.add_data

    def capture(self, data, *args, **kwargs):
        payloads.append(data)
        return original(self, data, *args, **kwargs)

    monkeypatch.setattr(qrcode.QRCode, "add_data", capture)
    wallet = "https://club.kivly.store/add-to-wallet?customer_id=customer&location_id=loc"
    raw = build_message(
        "sender@example.test", "player@example.test", "<Player>",
        {"name": "Coffee"}, "+34123456789", club_name="Club & Pizza", wallet_url=wallet,
    )
    message = BytesParser(policy=policy.default).parsebytes(raw)
    html = message.get_body(preferencelist=("html",)).get_content()
    text = message.get_body(preferencelist=("plain",)).get_content()
    assert str(message["Subject"]) == "Tu premio en Club & Pizza: Coffee"
    assert wallet in text
    assert 'href="' + wallet.replace("&", "&amp;") + '"' in html
    assert "Club &amp; Pizza" in html and "&lt;Player&gt;" in html
    assert 'src="cid:reward-qr"' in html
    assert "Your reward" not in text
    assert "no necesitas instalar una wallet" in text
    assert payloads == ["+34123456789"]
    image = next(p for p in message.walk() if p.get_content_type() == "image/png")
    assert image["Content-ID"] == "<reward-qr>"
    assert image.get_payload(decode=True).startswith(b"\x89PNG\r\n\x1a\n")


def test_branded_mail_embeds_logo_and_qr_without_remote_images():
    original = BytesParser(policy=policy.default).parsebytes(build_message(
        "sender@example.test", "player@example.test", "Player",
        {"name": "Coffee"}, "+34123456789",
    ))
    logo = next(p for p in original.walk() if p.get_content_type() == "image/png")
    logo_bytes = logo.get_payload(decode=True)
    message = BytesParser(policy=policy.default).parsebytes(build_message(
        "sender@example.test", "player@example.test", "Player",
        {"name": "Coffee", "imageUrl": "https://images.example.test/photo.png"},
        "+34123456789", club_name="Club",
        wallet_url="https://club.kivly.store/add-to-wallet?customer_id=c&location_id=l",
        logo_png=logo_bytes,
    ))
    html = message.get_body(preferencelist=("html",)).get_content()
    assert 'src="cid:club-logo"' in html
    assert 'src="cid:reward-qr" width="280" height="280"' in html
    assert "Tu premio te espera" in html
    assert 'role="presentation"' in html
    assert "images.example.test" not in html
    images = [p for p in message.walk() if p.get_content_type() == "image/png"]
    assert {str(p["Content-ID"]) for p in images} == {"<club-logo>", "<reward-qr>"}
    assert all(p.get_content_disposition() == "inline" for p in images)
    embedded_logo = next(p for p in images if str(p["Content-ID"]) == "<club-logo>")
    assert embedded_logo.get_payload(decode=True) == logo_bytes


def test_club_and_stand_reward_emails_use_the_same_html_layout():
    club = BytesParser(policy=policy.default).parsebytes(build_message(
        "sender@example.test", "player@example.test", "Player",
        {"name": "Coffee"}, "+34123456789", club_name="Club",
    ))
    stand = BytesParser(policy=policy.default).parsebytes(build_reward_message(
        "sender@example.test", "player@example.test", "Player",
        {"name": "Coffee"}, qr_value="reward-token",
    ))
    club_html = club.get_body(preferencelist=("html",)).get_content()
    stand_html = stand.get_body(preferencelist=("html",)).get_content()
    for html in (club_html, stand_html):
        assert 'data-email-layout="reward-card"' in html
        assert "max-width:440px" in html
        assert "Tu premio te espera" in html
        assert "TU PREMIO" in html


def test_stand_reward_email_embeds_logo_prize_photo_and_larger_qr():
    message = BytesParser(policy=policy.default).parsebytes(build_reward_message(
        "sender@example.test", "player@example.test", "Player",
        {"name": "Coffee"}, qr_value="reward-token",
        logo_png=TEST_LOGO_PNG, prize_image_png=TEST_LOGO_PNG,
    ))
    html = message.get_body(preferencelist=("html",)).get_content()
    assert html.index('src="cid:club-logo"') < html.index("TU PREMIO")
    assert 'src="cid:prize-photo"' in html
    assert html.index('src="cid:prize-photo"') < html.index(">Coffee</p>")
    assert 'src="cid:reward-qr" width="280" height="280"' in html
    images = {str(part["Content-ID"]): part for part in message.walk()
              if part.get_content_maintype() == "image"}
    assert set(images) == {"<club-logo>", "<prize-photo>", "<reward-qr>"}
    assert all(part.get_content_disposition() == "inline" for part in images.values())
    assert all(part.get_payload(decode=True).startswith(b"\x89PNG\r\n\x1a\n")
               for part in images.values())


@pytest.mark.parametrize("logo", [b"", b"not PNG", "image.png", b"\x89PNG\r\n\x1a\n"])
def test_club_mail_rejects_invalid_logo(logo):
    with pytest.raises(ValueError, match="InvalidRewardEmailLogo"):
        build_message(
            "sender@example.test", "player@example.test", "Player",
            {"name": "Coffee"}, "+34123456789", club_name="Club", logo_png=logo,
        )


@pytest.mark.parametrize("wallet", [
    "javascript:alert(1)",
    "http://club.kivly.store/add-to-wallet?customer_id=c&location_id=l",
    "https://evil.test/add-to-wallet?customer_id=c&location_id=l",
    "https://club.kivly.store/login?customer_id=c&location_id=l",
    "https://club.kivly.store/add-to-wallet?customer_id=c",
    "https://club.kivly.store/add-to-wallet?customer_id=c&location_id=l&redirect=evil",
    "https://club.kivly.store/add-to-wallet?customer_id=c&customer_id=d&location_id=l",
    "https://club.kivly.store/add-to-wallet?customer_id=%0A&location_id=l",
    "https://club.kivly.store/add-to-wallet?customer_id=c&location_id=l#fragment",
])
def test_club_mail_rejects_invalid_wallet_link(wallet):
    with pytest.raises(ValueError, match="InvalidRewardWalletLink"):
        build_message(
            "sender@example.test", "player@example.test", "Player",
            {"name": "Coffee"}, "+34123456789", club_name="Club", wallet_url=wallet,
        )


@pytest.mark.parametrize("club", ["", " ", "Club\nInjected", "x" * 201, 123])
def test_club_mail_rejects_invalid_club_name(club):
    with pytest.raises(ValueError, match="InvalidRewardEmailClub"):
        build_message(
            "sender@example.test", "player@example.test", "Player",
            {"name": "Coffee"}, "+34123456789", club_name=club,
        )


@pytest.mark.parametrize("phone", [None, "", "invalid"])
def test_kivly_reward_email_does_not_require_phone(email_rig, phone):
    worker, rig, row, client, ses = email_rig
    client.get_customer.return_value["phone"] = phone
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["state"] == "confirmed"
    assert stored(rig, row)["emailState"] == "accepted"
    assert "emailError" not in stored(rig, row)
    ses.send_raw_email.assert_called_once()
    client.earn_reward.assert_called_once()


@pytest.mark.parametrize("method", [None, "sms-code", "email-code"])
def test_unverified_address_cannot_receive_reward_email(email_rig, method):
    worker, rig, row, client, ses = email_rig
    current = player(rig)
    if method == "email-code":
        del current["verifiedAt"]
    else:
        current["verificationMethod"] = method
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
    process(worker, row)
    original_link = worker._kivly_wallet_link
    monkeypatch.setattr(worker, "_kivly_wallet_link", lambda *_: (_ for _ in ()).throw(worker.RewardEmailAssetError()))
    process(worker, row)
    monkeypatch.setattr(worker, "_kivly_wallet_link", original_link)
    monkeypatch.setattr(stack_prize_api, "_get_claims", lambda _: {"sub": "owner"})
    monkeypatch.setattr(stack_prize_api, "ClientError", FakeDynamoError)
    body = {"gameId": row["gameId"], "action": "retry_email", "awardId": row["id"]}
    event = {"body": json.dumps(body)}
    response = stack_prize_api.handle(event, "POST", rig, lambda _: True, None)
    assert response["statusCode"] == 200
    process(worker, row)
    assert stored(rig, row)["emailState"] == "accepted"
    assert stack_prize_api.handle(event, "POST", rig, lambda _: True, None)["statusCode"] == 409
    client.earn_reward.assert_called_once()
    ses.send_raw_email.assert_called_once()


def test_kivly_awards_send_email_independently_of_wallet_channel(email_rig):
    worker, rig, row, client, ses = email_rig
    row["deliveryChannel"] = "wallet"
    rig.Table("prizes").put_item(Item=row)
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "accepted"
    ses.send_raw_email.assert_called_once()


def test_disabled_email_remains_pending_and_does_not_send(email_rig, monkeypatch):
    worker, rig, row, client, ses = email_rig
    monkeypatch.setenv("STACK_REWARD_EMAIL_ENABLED", "false")
    process(worker, row)
    process(worker, row)
    assert stored(rig, row)["emailState"] == "pending"
    assert stored(rig, row)["work"] == "pending"
    ses.send_raw_email.assert_not_called()


def test_email_is_blocked_until_confirmed_and_pending_marker_cannot_earn(email_rig):
    worker, rig, row, client, ses = email_rig
    row["customerId"] = "pending-reconciliation"
    rig.Table("players").put_item(Item={
        **player(rig), "kivly": {"membership": {
            "locationId": "loc", "customerId": "pending-reconciliation",
        }},
    })
    worker.deliver(row)
    saved = stored(rig, row)
    assert saved["state"] == "connection_required"
    assert saved["emailState"] == "blocked"
    assert saved["emailError"] == "KivlyMembershipRequired"
    public = prizes.public_awards(rig, meta(rig), player(rig), lambda _: "token")
    assert public["awards"][0]["emailState"] == "blocked"
    client.get_customer.assert_not_called()
    client.earn_reward.assert_not_called()
    ses.send_raw_email.assert_not_called()


def test_inactive_customer_by_confirmed_id_cannot_be_earned_or_emailed(email_rig):
    worker, rig, row, client, ses = email_rig
    client.get_customer.return_value["is_active"] = False
    worker.deliver(row)
    saved = stored(rig, row)
    assert saved["state"] == "connection_required"
    assert saved["emailState"] == "blocked"
    client.earn_reward.assert_not_called()
    ses.send_raw_email.assert_not_called()


def test_missing_branded_assets_fail_without_sending_or_fallback(email_rig, monkeypatch):
    worker, rig, row, client, ses = email_rig
    def missing_branding(*_):
        raise worker.RewardEmailAssetError()
    monkeypatch.setattr(worker, "_kivly_wallet_link", missing_branding)
    process(worker, row)
    process(worker, row)
    saved = stored(rig, row)
    assert saved["state"] == "confirmed"
    assert saved["emailState"] == "failed"
    assert saved["emailError"] == "RewardEmailAssetsUnavailable"
    assert client.earn_reward.call_count == 1
    ses.send_raw_email.assert_not_called()


def test_award_email_state_is_blocked_before_confirmation(rig):
    player_row = player(rig)
    player_row["type"]["STACK"]["attempt"] = {"prizeProgram": {"deliveryChannel": "email"}}
    row = prizes.award_units(meta(rig), player_row, {"id": "done", "condition": "completed"},
                             PROGRAM["catalog"][1], 1)[0]
    assert row["state"] == "queued"
    assert "emailState" not in row
    assert prizes.email_delivery_state(row) is None


def test_selected_owner_logo_is_required_and_owner_scoped(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    monkeypatch.setenv("USERS_TABLE", "users")
    monkeypatch.setenv("USER_MEDIA_BUCKET", "user-media")
    data = TEST_LOGO_PNG
    rig.Table("users").put_item(Item={
        "userId": "owner",
        "media": {"logo": {"selectedId": "selected", "items": [
            {"id": "selected", "key": "users/owner/logos/club.png"},
        ]}},
    })
    body = BytesIO(data)
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": body, "ContentType": "image/png", "ContentLength": len(data),
    }
    monkeypatch.setattr(worker.boto3, "client", lambda service: s3)

    assert worker._selected_logo(rig, "owner") == data
    s3.get_object.assert_called_once_with(
        Bucket="user-media", Key="users/owner/logos/club.png",
    )
    rig.Table("users").put_item(Item={
        "userId": "owner",
        "media": {"logo": {"selectedId": "other", "items": [
            {"id": "selected", "key": "users/owner/logos/club.png"},
        ]}},
    })
    with pytest.raises(worker.RewardEmailAssetError):
        worker._selected_logo(rig, "owner")


def test_email_branding_uses_selected_logo_and_resolved_wallet_for_owner(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    monkeypatch.setenv("USERS_TABLE", "users")
    monkeypatch.setenv("USER_MEDIA_BUCKET", "user-media")
    rig.Table("users").put_item(Item={
        "userId": "owner",
        "media": {"logo": {"selectedId": "selected", "items": [
            {"id": "selected", "key": "users/owner/logos/club.png"},
        ]}},
    })
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": BytesIO(TEST_LOGO_PNG), "ContentType": "image/png",
        "ContentLength": len(TEST_LOGO_PNG),
    }
    monkeypatch.setattr(worker.boto3, "client", lambda service: s3)

    branding = worker._email_branding(
        rig, {"gameId": "ABC123", "ownerUserId": "owner"},
        {"locationId": "loc", "ownerId": "owner"},
        MagicMock(location_id="loc", owner_id="owner", location_name="Club"),
        {"id": "customer"},
    )

    assert branding == (
        "Club",
        "https://example.kivly.store/add-to-wallet?customer_id=customer&location_id=loc",
        TEST_LOGO_PNG,
    )


def test_stand_email_prize_image_is_loaded_only_from_owner_game_asset(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    monkeypatch.setenv("GAMES_TABLE", "games")
    monkeypatch.setenv("USER_MEDIA_BUCKET", "user-media")
    game = meta(rig)
    rig.Table("games").put_item(Item=game)
    image_key = "games/owner/ABC123/ranking/01234567-89ab-cdef-0123-456789abcdef.png"
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": BytesIO(TEST_LOGO_PNG), "ContentType": "image/png",
        "ContentLength": len(TEST_LOGO_PNG),
    }
    monkeypatch.setattr(worker.boto3, "client", lambda service: s3)

    image = worker._stand_prize_image(
        rig, {"gameId": "ABC123", "ownerUserId": "owner"},
        {"source": "stand", "name": "Coffee", "imageKey": image_key},
    )

    assert image == TEST_LOGO_PNG
    s3.get_object.assert_called_once_with(Bucket="user-media", Key=image_key)
    with pytest.raises(worker.RewardEmailAssetError):
        worker._stand_prize_image(
            rig, {"gameId": "ABC123", "ownerUserId": "owner"},
            {"source": "stand", "name": "Coffee", "imageKey": "games/other/ABC123/ranking/prize.png"},
        )
    assert s3.get_object.call_count == 1
