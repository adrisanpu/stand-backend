"""Short-lived email challenges and server-only registration receipts."""
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from datetime import datetime, timezone
from botocore.exceptions import BotoCoreError, ClientError

from stand_common.utils import is_valid_email, normalize_email, web_participant_fields, mask_email, log
from stand_common.kivly_registration import consent_values, RegistrationError as KivlyRegistrationError

CODE_TTL = 600
RESEND_SECONDS = 60
MAX_ATTEMPTS = 5
_CHALLENGE_RE = re.compile(r"^[A-Za-z0-9_-]{32,80}$")


class VerificationError(Exception):
    def __init__(self, code, status=400, retry_after=None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.retry_after = retry_after


class RegistrationError(ValueError):
    pass


def registration_values(meta, body):
    fields = web_participant_fields(meta.get("gameType") or "", meta.get("participantFields"))
    values = {}
    for name, limit in (("email", 254), ("phone", 64)):
        if name == "phone" and not fields["phone"]["enabled"]:
            continue
        raw = body.get(name)
        if raw is not None and not isinstance(raw, str):
            raise RegistrationError(f"invalid_{name}")
        value = (raw or "").strip()
        if not value:
            if name == "phone":
                continue
            raise RegistrationError(f"missing_{name}")
        if len(value) > limit or re.search(r"[\x00-\x1f\x7f-\x9f]", value):
            raise RegistrationError(f"invalid_{name}")
        if name == "email":
            value = normalize_email(value)
            if not is_valid_email(value):
                raise RegistrationError("invalid_email")
        values[name] = value
    raw = body.get("username")
    if raw is not None and not isinstance(raw, str):
        raise RegistrationError("invalid_username")
    username = (raw or "").strip()
    if not username:
        raise RegistrationError("missing_username")
    if len(username) > 100 or re.search(r"[\x00-\x1f\x7f-\x9f]", username):
        raise RegistrationError("invalid_username")
    values["username"] = username
    try:
        values.update(consent_values(meta, body))
    except KivlyRegistrationError as exc:
        raise VerificationError(exc.code, exc.status) from None
    return values


def _key(secret):
    if not isinstance(secret, str) or len(secret) < 32:
        raise VerificationError("VerificationUnavailable", 503)
    return hmac.new(secret.encode(), b"stand:email-verification:v1", hashlib.sha256).digest()


def _digest(secret, purpose, value):
    return hmac.new(_key(secret), f"{purpose}:{value}".encode(), hashlib.sha256).hexdigest()


def _binding(game_id, values):
    return json.dumps({"gameId": game_id, "values": values}, sort_keys=True, separators=(",", ":"))


def _iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z")


def _error_code(exc):
    response = getattr(exc, "response", {})
    return response.get("Error", {}).get("Code") if isinstance(response, dict) else None


def verification_table(dynamo):
    name = os.environ.get("EMAIL_VERIFICATION_TABLE")
    if not name:
        raise VerificationError("VerificationUnavailable", 503)
    return dynamo.Table(name)


def _limit(name, default):
    try:
        value = int(os.environ.get(name, default))
    except ValueError as exc:
        raise VerificationError("VerificationUnavailable", 503) from exc
    if value < 1:
        raise VerificationError("VerificationUnavailable", 503)
    return value


def _recipient_id(secret, email):
    return "recipient:" + _digest(secret, "recipient", email)


def identity_id(secret, game_id, email):
    return "identity:" + _digest(secret, "identity", f"{game_id}:{email}")


def _send_code(ses, sender, email, code, language):
    if language == "en":
        subject = "Your STAND verification code"
        text = (
            f"Your code to access STAND is: {code}\n\n"
            "It expires in 10 minutes. Enter it on the page where you requested it.\n"
            "Do not share this code. If you did not request it, ignore this email."
        )
    else:
        subject = "Tu codigo de verificacion de STAND"
        text = (
            f"Tu codigo para acceder a STAND es: {code}\n\n"
            "Caduca en 10 minutos. Introducelo en la pagina donde lo solicitaste.\n"
            "No compartas este codigo. Si no lo solicitaste, ignora este correo."
        )
    ses.send_email(
        Source=sender,
        Destination={"ToAddresses": [email]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Text": {"Data": text, "Charset": "UTF-8"}},
        },
    )


def request_code(table, ses, secret, game_id, values, source_ip, language="es"):
    sender = os.environ.get("EMAIL_SENDER_ADDRESS")
    if not sender or not source_ip:
        raise VerificationError("VerificationUnavailable", 503)
    now = int(time.time())
    hour = now // 3600
    hour_end = (hour + 1) * 3600
    challenge_id = secrets.token_urlsafe(32)
    code = f"{secrets.randbelow(1000000):06d}"
    recipient = _recipient_id(secret, values["email"])
    challenge = {
        "id": f"challenge:{challenge_id}",
        "gameId": game_id,
        "registration": values,
        "consent": True,
        "recipientId": recipient,
        "codeHash": _digest(secret, "code", f"{challenge_id}:{code}"),
        "binding": _digest(secret, "binding", _binding(game_id, values)),
        "state": "sending",
        "attempts": 0,
        "createdAt": now,
        "expiresAt": now + CODE_TTL,
    }
    transactions = [{
        "Update": {
            "TableName": table.name,
            "Key": {"id": recipient},
            "UpdateExpression": "SET lastSentAt = :now, challengeId = :cid, expiresAt = :ttl",
            "ConditionExpression": "attribute_not_exists(lastSentAt) OR lastSentAt <= :before",
            "ExpressionAttributeValues": {
                ":now": now, ":cid": challenge_id, ":ttl": now + 3600,
                ":before": now - RESEND_SECONDS,
            },
        },
    }]
    for scope, identifier, limit in (
        ("email", _digest(secret, "email-rate", values["email"]), _limit("EMAIL_MAX_PER_HOUR", 5)),
        ("ip", _digest(secret, "ip-rate", source_ip), _limit("EMAIL_IP_MAX_PER_HOUR", 3000)),
        ("global", "all", _limit("EMAIL_GLOBAL_MAX_PER_HOUR", 20000)),
    ):
        transactions.append({"Update": {
            "TableName": table.name,
            "Key": {"id": f"rate:{scope}:{identifier}:{hour}"},
            "UpdateExpression": "SET expiresAt = :ttl ADD #count :one",
            "ConditionExpression": "attribute_not_exists(#count) OR #count < :limit",
            "ExpressionAttributeNames": {"#count": "count"},
            "ExpressionAttributeValues": {":ttl": hour_end + 3600, ":one": 1, ":limit": limit},
        }})
    transactions.append({"Put": {
        "TableName": table.name, "Item": challenge, "ConditionExpression": "attribute_not_exists(id)",
    }})
    try:
        table.meta.client.transact_write_items(TransactItems=transactions)
    except Exception as exc:
        if _error_code(exc) == "TransactionCanceledException":
            reasons = exc.response.get("CancellationReasons") or []
            if any(reason.get("Code") == "ConditionalCheckFailed" for reason in reasons[:4]):
                cooldown = bool(reasons and reasons[0].get("Code") == "ConditionalCheckFailed")
                raise VerificationError(
                    "EmailRateLimited", 429, RESEND_SECONDS if cooldown else hour_end - now,
                ) from exc
        raise
    try:
        _send_code(ses, sender, values["email"], code, language)
    except (BotoCoreError, ClientError) as exc:
        # Preserve quotas on delivery failure, so retrying a broken provider cannot send unbounded mail.
        log("email_verification_delivery_failed", {
            "errorType": type(exc).__name__, "providerCode": _error_code(exc), "gameId": game_id,
        })
        table.update_item(
            Key={"id": challenge["id"]},
            UpdateExpression="SET #state = :failed",
            ExpressionAttributeNames={"#state": "state"},
            ExpressionAttributeValues={":failed": "failed"},
        )
        raise VerificationError("EmailDeliveryFailed", 502, RESEND_SECONDS) from exc
    table.update_item(
        Key={"id": challenge["id"]},
        UpdateExpression="SET #state = :pending",
        ConditionExpression="#state = :sending",
        ExpressionAttributeNames={"#state": "state"},
        ExpressionAttributeValues={":pending": "pending", ":sending": "sending"},
    )
    return {
        "ok": True, "challengeId": challenge_id, "emailMasked": mask_email(values["email"]),
        "expiresAt": _iso(now + CODE_TTL), "resendAvailableAt": _iso(now + RESEND_SECONDS),
    }


def _get_challenge(table, challenge_id, game_id, now):
    if not isinstance(challenge_id, str) or not _CHALLENGE_RE.fullmatch(challenge_id):
        raise VerificationError("VerificationRequired", 403)
    record = table.get_item(Key={"id": f"challenge:{challenge_id}"}, ConsistentRead=True).get("Item")
    if not record or record.get("gameId") != game_id:
        raise VerificationError("VerificationRequired", 403)
    if int(record["expiresAt"]) <= now:
        raise VerificationError("VerificationExpired", 410)
    if record.get("state") not in ("pending", "completed"):
        raise VerificationError("VerificationUnavailable", 503)
    if int(record.get("attempts", 0)) >= MAX_ATTEMPTS:
        raise VerificationError("VerificationAttemptsExceeded", 429)
    return record


def confirm_code(table, secret, game_id, challenge_id, code):
    now = int(time.time())
    record = _get_challenge(table, challenge_id, game_id, now)
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code):
        correct = False
    else:
        correct = hmac.compare_digest(record["codeHash"], _digest(secret, "code", f"{challenge_id}:{code}"))
    if not correct:
        try:
            result = table.update_item(
                Key={"id": record["id"]},
                UpdateExpression="ADD attempts :one",
                ConditionExpression="attempts < :max AND expiresAt > :now",
                ExpressionAttributeValues={":one": 1, ":max": MAX_ATTEMPTS, ":now": now},
                ReturnValues="ALL_NEW",
            )
        except Exception as exc:
            if _error_code(exc) == "ConditionalCheckFailedException":
                raise VerificationError("VerificationAttemptsExceeded", 429) from exc
            raise
        exhausted = int(result["Attributes"]["attempts"]) >= MAX_ATTEMPTS
        raise VerificationError(
            "VerificationAttemptsExceeded" if exhausted else "InvalidVerificationCode",
            429 if exhausted else 400,
        )
    try:
        table.meta.client.transact_write_items(TransactItems=[
            {"ConditionCheck": {
                "TableName": table.name, "Key": {"id": record["recipientId"]},
                "ConditionExpression": "challengeId = :cid",
                "ExpressionAttributeValues": {":cid": challenge_id},
            }},
            {"Update": {
                "TableName": table.name, "Key": {"id": record["id"]},
                "UpdateExpression": "SET verifiedAt = if_not_exists(verifiedAt, :now)",
                "ConditionExpression": "attempts < :max AND expiresAt > :now AND #state IN (:pending, :completed)",
                "ExpressionAttributeNames": {"#state": "state"},
                "ExpressionAttributeValues": {
                    ":max": MAX_ATTEMPTS, ":now": now, ":pending": "pending", ":completed": "completed",
                },
            }},
        ])
    except Exception as exc:
        if _error_code(exc) == "TransactionCanceledException":
            reasons = exc.response.get("CancellationReasons") or []
            if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
                raise VerificationError("VerificationSuperseded", 409) from exc
            if len(reasons) > 1 and reasons[1].get("Code") == "ConditionalCheckFailed":
                _get_challenge(table, challenge_id, game_id, int(time.time()))
                raise VerificationError("VerificationUsed", 409) from exc
        raise
    record["verifiedAt"] = record.get("verifiedAt", now)
    receipt = challenge_id + "." + _digest(secret, "receipt", f"{challenge_id}:{record['binding']}")
    return record, receipt


def require_verified_registration(table, secret, game_id, values, receipt):
    if not isinstance(receipt, str) or receipt.count(".") != 1:
        raise VerificationError("VerificationRequired", 403)
    challenge_id, signature = receipt.split(".")
    if not re.fullmatch(r"[0-9a-f]{64}", signature):
        raise VerificationError("VerificationRequired", 403)
    record = _get_challenge(table, challenge_id, game_id, int(time.time()))
    binding = _digest(secret, "binding", _binding(game_id, values))
    expected = _digest(secret, "receipt", f"{challenge_id}:{binding}")
    if (not hmac.compare_digest(signature, expected) or record["binding"] != binding
            or "verifiedAt" not in record or record.get("consent") is not True):
        raise VerificationError("VerificationRequired", 403)
    latest = table.get_item(Key={"id": record["recipientId"]}, ConsistentRead=True).get("Item") or {}
    if latest.get("challengeId") != challenge_id:
        raise VerificationError("VerificationSuperseded", 409)
    return record


def complete_registration(table, record, player_id):
    table.update_item(
        Key={"id": record["id"]},
        UpdateExpression="SET #state = :completed, playerId = :pid",
        ConditionExpression="attribute_exists(verifiedAt) AND (attribute_not_exists(playerId) OR playerId = :pid)",
        ExpressionAttributeNames={"#state": "state"},
        ExpressionAttributeValues={":completed": "completed", ":pid": int(player_id)},
    )
