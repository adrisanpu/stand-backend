"""Owner connection, game consent and durable at-most-once customer creation."""
import hashlib
import hmac
import json
import os
import uuid
from functools import lru_cache
from urllib.parse import urlsplit

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from stand_common.kivly import KivlyClient, KivlyError
from stand_common.utils import _iso_now, log


class RegistrationError(Exception):
    def __init__(self, code, status=503, retry_after=None):
        super().__init__(code)
        self.code, self.status, self.retry_after = code, status, retry_after


def _code(error):
    return error.response.get("Error", {}).get("Code")


def _table(dynamo):
    name = os.environ.get("KIVLY_REGISTRATION_TABLE")
    if not name:
        raise RegistrationError("KivlyConfigurationInvalid")
    return dynamo.Table(name)


def connection(dynamo, owner):
    try:
        conn = dynamo.Table(os.environ["USERS_TABLE"]).get_item(
            Key={"userId": owner}, ConsistentRead=True,
            ProjectionExpression="kivlyConnection",
        ).get("Item", {}).get("kivlyConnection")
    except (BotoCoreError, ClientError):
        raise RegistrationError("KivlyUnavailable") from None
    if conn is not None and (
        not isinstance(conn, dict) or not all(isinstance(conn.get(key), str) and conn[key]
                                             for key in ("secretArn", "locationId", "ownerId", "version"))
    ):
        raise RegistrationError("KivlyLegacyConfiguration", 409)
    return conn


def account_game_settings(meta, conn):
    """Resolve the account-wide integration without persisting per-game switches."""
    if (meta.get("gameType") or "").upper() != "STACK":
        return {"enabled": False, "clubName": "", "privacyUrl": "", "termsUrl": ""}
    club = conn.get("clubSettings") if conn else None
    disabled = {"enabled": False, "clubName": "", "privacyUrl": "", "termsUrl": ""}
    if not club:
        return disabled
    if not isinstance(club, dict):
        raise RegistrationError("KivlyConfigurationInvalid")
    for key in ("privacyUrl", "termsUrl"):
        try:
            host = urlsplit(club.get(key, "")).hostname or ""
        except ValueError:
            raise RegistrationError("KivlyConfigurationInvalid") from None
        if host == "example.com" or host.endswith(".example.com"):
            return {**club, "enabled": False}
    return prepare_settings(meta, {**club, "enabled": True}, conn)


def apply_account_settings(dynamo, meta):
    if not meta or not meta.get("ownerUserId"):
        return meta
    if (meta.get("gameType") or "").upper() != "STACK":
        return {
            **meta,
            "kivly": {"enabled": False, "clubName": "", "privacyUrl": "", "termsUrl": ""},
        }
    settings = account_game_settings(meta, connection(dynamo, meta["ownerUserId"]))
    fields = dict(meta.get("participantFields") or {})
    if settings["enabled"]:
        fields["email"] = {"required": True}
    return {**meta, "kivly": settings, "participantFields": fields}


def game_settings(meta):
    settings = meta.get("kivly")
    if settings is None:
        return {"enabled": False, "clubName": "", "privacyUrl": "", "termsUrl": ""}
    if not isinstance(settings, dict) or type(settings.get("enabled")) is not bool:
        raise RegistrationError("KivlyConfigurationInvalid")
    if not settings["enabled"]:
        return settings
    if (
        not all(isinstance(settings.get(key), str) and settings[key].strip()
                   for key in ("clubName", "privacyUrl", "termsUrl", "locationId", "consentVersion"))
        or not (meta.get("participantFields") or {}).get("email", {}).get("required")
    ):
        raise RegistrationError("KivlyConfigurationInvalid")
    return settings


def consent_version(settings):
    data = {key: settings[key] for key in ("clubName", "privacyUrl", "termsUrl", "locationId")}
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def prepare_settings(meta, raw, conn):
    if not isinstance(raw, dict) or type(raw.get("enabled")) is not bool:
        raise RegistrationError("KivlyConfigurationInvalid", 400)
    if raw.get("rewardId") is not None:
        raise RegistrationError("KivlyCatalogUnverified", 409)
    if isinstance(conn, dict) and isinstance(conn.get("clubSettings"), dict):
        raw = {**raw, **conn["clubSettings"]}
    result = {"enabled": raw["enabled"]}
    for key, limit in (("clubName", 200), ("privacyUrl", 2048), ("termsUrl", 2048)):
        value = raw.get(key, "")
        if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 for c in value):
            raise RegistrationError("KivlyConfigurationInvalid", 400)
        result[key] = value.strip()
    if result["enabled"]:
        if not isinstance(conn, dict) or not isinstance(conn.get("locationId"), str) or not conn["locationId"].strip():
            raise RegistrationError("KivlyConnectionRequired", 409)
        for key in ("privacyUrl", "termsUrl"):
            try:
                url = urlsplit(result[key])
                valid = url.scheme in ("https", "http") and url.hostname and not url.username and not url.password
                if url.hostname == "example.com" or (url.hostname or "").endswith(".example.com"):
                    valid = False
            except ValueError:
                valid = False
            if not valid:
                raise RegistrationError("KivlyConfigurationInvalid", 400)
        if not result["clubName"]:
            raise RegistrationError("KivlyConfigurationInvalid", 400)
        result["locationId"] = conn["locationId"]
        result["consentVersion"] = consent_version(result)
    return result


def consent_values(meta, body):
    settings = game_settings(meta)
    if not settings["enabled"]:
        return {}
    if body.get("loyaltyConsentVersion") != settings["consentVersion"]:
        raise RegistrationError("KivlyConsentRequired", 400)
    return {"loyaltyConsentVersion": settings["consentVersion"]}


def _client(conn):
    return _cached_client(conn["secretArn"], conn["version"])


@lru_cache(maxsize=8)
def _cached_client(secret_arn, version):
    # A reconnect creates a new immutable credential version. Warm invocations
    # reuse its client/token without mixing owners or minting a token per player.
    try:
        data = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
        credentials = json.loads(data["SecretString"])
        return KivlyClient(credentials["client_id"], credentials["client_secret"])
    except (KeyError, TypeError, ValueError):
        raise RegistrationError("KivlyConfigurationInvalid") from None


def _identity(meta, conn, email):
    secret = os.environ.get("PLAYER_TOKEN_SECRET", "")
    if len(secret) < 32:
        raise RegistrationError("KivlyConfigurationInvalid")
    binding = json.dumps([meta["ownerUserId"], conn["locationId"], email.strip().casefold()])
    digest = hmac.new(secret.encode(), binding.encode(), hashlib.sha256).hexdigest()
    return "customer:" + digest


def _required_connection(dynamo, meta):
    settings = game_settings(meta)
    conn = connection(dynamo, meta["ownerUserId"])
    if not conn:
        raise RegistrationError("KivlyConnectionRequired", 409)
    if conn["locationId"] != settings["locationId"]:
        raise RegistrationError("KivlyActivationUnverified")
    return conn


def ensure_membership(dynamo, meta, values):
    try:
        return _ensure_membership(dynamo, meta, values)
    except (BotoCoreError, ClientError):
        raise RegistrationError("KivlyUnavailable") from None


def _ensure_membership(dynamo, meta, values):
    settings = game_settings(meta)
    if not settings["enabled"]:
        return None
    consent_values(meta, values)
    conn = _required_connection(dynamo, meta)
    table = _table(dynamo)
    key = _identity(meta, conn, values["email"])
    record = table.get_item(Key={"id": key}, ConsistentRead=True).get("Item")
    if record and record.get("state") == "confirmed":
        if not isinstance(record.get("customerId"), str) or not record["customerId"]:
            raise RegistrationError("KivlyReconciliationRequired", 409)
        return _membership(settings, record["customerId"])
    if record and (record.get("state") != "creating" or not record.get("externalId")):
        raise RegistrationError("KivlyReconciliationRequired", 409)
    client = _client(conn)
    customer = client.find_customer(conn["locationId"], conn["ownerId"], values["email"])
    if record:
        # Recover a lost POST response only through the exact attempt identity.
        if not customer or customer.get("external_id") != record.get("externalId"):
            raise RegistrationError("KivlyReconciliationRequired", 409)
    elif customer is None:
        record = {"id": key, "state": "creating", "externalId": "stand-" + uuid.uuid4().hex, "createdAt": _iso_now()}
        try:
            table.put_item(Item=record, ConditionExpression="attribute_not_exists(id)")
        except ClientError as exc:
            if _code(exc) == "ConditionalCheckFailedException":
                raise RegistrationError("KivlyRegistrationBusy", 409) from None
            raise
        try:
            customer = client.create_customer(
                conn["locationId"], conn["ownerId"], values["username"], values["email"], record["externalId"],
                **({"phone": values["phone"]} if values.get("phone") else {}),
            )
        except KivlyError:
            # The intent is never removed or expired, even if the response was lost.
            raise RegistrationError("KivlyReconciliationRequired", 409) from None
    if customer.get("is_active") is not True:
        raise RegistrationError("KivlyCustomerInvalid", 409)
    # Existing customers are reused, never modified. Concurrent lookups of an
    # existing customer can safely converge on the same confirmed identity.
    if record:
        table.update_item(
            Key={"id": key},
            UpdateExpression="SET #s = :s, customerId = :c, confirmedAt = :at",
            ConditionExpression="externalId = :external",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":s": "confirmed", ":c": customer["id"], ":at": _iso_now(),
                                       ":external": record["externalId"]},
        )
    else:
        try:
            table.put_item(
                Item={"id": key, "state": "confirmed", "customerId": customer["id"], "confirmedAt": _iso_now()},
                ConditionExpression="attribute_not_exists(id)",
            )
        except ClientError as exc:
            if _code(exc) == "ConditionalCheckFailedException":
                raise RegistrationError("KivlyRegistrationBusy", 409) from None
            raise
    # Re-read owner connection so a concurrent disconnect cannot grant access.
    current = _required_connection(dynamo, meta)
    if current["version"] != conn["version"]:
        raise RegistrationError("KivlyConnectionChanged", 409)
    return _membership(settings, customer["id"])


def _membership(settings, customer_id):
    return {"locationId": settings["locationId"], "customerId": customer_id,
            "consentVersion": settings["consentVersion"], "consentedAt": _iso_now()}


def require_player(dynamo, meta, player):
    settings = game_settings(meta)
    if not settings["enabled"]:
        return
    _required_connection(dynamo, meta)
    membership = player.get("kivlyMembership") or {}
    if (
        membership.get("locationId") != settings["locationId"]
        or membership.get("consentVersion") != settings["consentVersion"]
        or not membership.get("customerId")
    ):
        raise RegistrationError("KivlyMembershipRequired", 403)


def error_response(error):
    if isinstance(error, KivlyError):
        status = 429 if error.http_status == 429 else 503
        code = "KivlyAccessDenied" if error.http_status in (401, 403) else error.code
    elif isinstance(error, RegistrationError):
        status, code = error.status, error.code
    else:
        status, code = 503, "KivlyUnavailable"
    log("kivly_operation_failed", {"code": code})
    payload = {"ok": False, "error": code}
    if isinstance(error, (KivlyError, RegistrationError)) and error.retry_after is not None:
        payload["retryAfter"] = error.retry_after
    return status, payload


ERRORS = (RegistrationError, KivlyError, BotoCoreError, ClientError)
