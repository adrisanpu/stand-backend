# handler: webhook_stripe.lambda_handler
import os
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import stripe
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _iso_now, _parse_iso

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")

# ========= ENV =========
USERS_TABLE = (os.environ.get("USERS_TABLE", "") or "").strip()
STRIPE_EVENTS_TABLE = (os.environ.get("STRIPE_EVENTS_TABLE", "") or "").strip()

# Stripe keys solo desde Secrets Manager (STRIPE_SECRET_NAME obligatorio)
STRIPE_SECRET_KEY = ""
STRIPE_WEBHOOK_SECRET = ""
_stripe_secret_name = os.environ.get("STRIPE_SECRET_NAME", "").strip()
if _stripe_secret_name:
    try:
        sm = boto3.client("secretsmanager")
        raw = sm.get_secret_value(SecretId=_stripe_secret_name)
        data = json.loads(raw.get("SecretString", "{}"))
        if data:
            STRIPE_SECRET_KEY = (data.get("STRIPE_SECRET_KEY") or data.get("SECRET_KEY") or "").strip()
            STRIPE_WEBHOOK_SECRET = (data.get("STRIPE_WEBHOOK_SECRET") or data.get("WEBHOOK_SECRET") or "").strip()
    except Exception as e:
        print(json.dumps({"msg": "stripe_secret_load_failed", "error": repr(e)}))

stripe.api_key = STRIPE_SECRET_KEY
_serializer = TypeSerializer()


def _raw_body(event: dict) -> bytes:
    body = (event or {}).get("body") or ""
    if (event or {}).get("isBase64Encoded"):
        return base64.b64decode(body)
    return body.encode("utf-8")


def _get_header(event: dict, name: str) -> Optional[str]:
    headers = (event or {}).get("headers") or {}
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def _iso_from_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _plus_24h_from(active_until: Optional[str]) -> str:
    """
    Extiende 24h desde:
    - activeUntil si está en el futuro
    - si no, desde ahora
    """
    now = datetime.now(timezone.utc)
    base = now
    if active_until:
        try:
            t = _parse_iso(active_until)
            if t > now:
                base = t
        except Exception:
            pass
    return _iso_from_dt(base + timedelta(hours=24))


def _serialize_item(item: dict) -> dict:
    return {key: _serializer.serialize(value) for key, value in item.items()}


def _grant_paid_access(user_id: str, event_id: str, obj: dict) -> bool:
    users = dynamodb.Table(USERS_TABLE)
    events = dynamodb.Table(STRIPE_EVENTS_TABLE)
    client = users.meta.client

    for attempt in range(5):
        current = users.get_item(Key={"userId": user_id}, ConsistentRead=True).get("Item") or {}
        now = _iso_now()
        active_until = _plus_24h_from(current.get("activeUntil"))
        values = {
            ":plan": "EVENT_24H",
            ":until": active_until,
            ":now": now,
            ":event": event_id,
        }
        condition = "attribute_not_exists(activeUntil)"
        if "activeUntil" in current:
            values[":previous"] = current["activeUntil"]
            condition = "activeUntil = :previous"

        names = {
            "#plan": "plan",
            "#activeUntil": "activeUntil",
            "#updatedAt": "updatedAt",
            "#lastStripeEventId": "lastStripeEventId",
            "#createdAt": "createdAt",
        }
        update = (
            "SET #plan=:plan, #activeUntil=:until, #updatedAt=:now, "
            "#lastStripeEventId=:event, #createdAt=if_not_exists(#createdAt, :now)"
        )
        for attr, key in (("customer", "#stripeCustomerId"), ("payment_intent", "#lastPaymentIntentId"), ("id", "#lastCheckoutSessionId")):
            if obj.get(attr):
                names[key] = key[1:]
                value_name = f":{key[1:]}"
                values[value_name] = obj[attr]
                update += f", {key}={value_name}"

        expiry = int(datetime.now(timezone.utc).timestamp()) + 400 * 24 * 60 * 60
        try:
            client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": STRIPE_EVENTS_TABLE,
                            "Item": _serialize_item({
                                "eventId": event_id,
                                "userId": user_id,
                                "processedAt": now,
                                "expiresAt": expiry,
                            }),
                            "ConditionExpression": "attribute_not_exists(eventId)",
                        }
                    },
                    {
                        "Update": {
                            "TableName": USERS_TABLE,
                            "Key": _serialize_item({"userId": user_id}),
                            "UpdateExpression": update,
                            "ExpressionAttributeNames": names,
                            "ExpressionAttributeValues": _serialize_item(values),
                            "ConditionExpression": condition,
                        }
                    },
                ]
            )
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "TransactionCanceledException":
                raise
            processed = events.get_item(
                Key={"eventId": event_id},
                ConsistentRead=True,
            ).get("Item")
            if processed:
                return False
            if attempt == 4:
                raise
    raise RuntimeError("Stripe event transaction retries exhausted")


def lambda_handler(event, context):
    log("webhook:start", {"requestId": getattr(context, "aws_request_id", None)})

    missing = [k for k, v in {
        "STRIPE_SECRET_KEY": STRIPE_SECRET_KEY,
        "STRIPE_WEBHOOK_SECRET": STRIPE_WEBHOOK_SECRET,
        "USERS_TABLE": USERS_TABLE,
        "STRIPE_EVENTS_TABLE": STRIPE_EVENTS_TABLE,
    }.items() if not v]
    if missing:
        log("webhook:error_missing_env", missing)
        return _resp(500, {"error": f"Missing env vars: {', '.join(missing)}"})

    sig = _get_header(event, "Stripe-Signature")
    if not sig:
        log("webhook:missing_signature")
        return _resp(400, {"error": "Missing Stripe-Signature"})

    payload = _raw_body(event)

    try:
        evt = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except Exception as e:
        log("webhook:signature_verification_failed", str(e))
        return _resp(400, {"error": "Invalid signature"})

    event_type = evt.get("type")
    obj = (evt.get("data") or {}).get("object") or {}
    evt_id = evt.get("id")

    log("webhook:event_received", {"type": event_type, "id": evt_id})

    # Solo nos interesa el pago completado
    if event_type != "checkout.session.completed":
        return _resp(200, {"ok": True})

    try:
        payment_status = obj.get("payment_status")  # paid/unpaid/no_payment_required
        if payment_status not in ("paid", "no_payment_required"):
            log("webhook:session_not_paid_yet", {"payment_status": payment_status})
            return _resp(200, {"ok": True})

        metadata = obj.get("metadata") or {}
        user_id = metadata.get("userId") or obj.get("client_reference_id")
        if not user_id:
            log("webhook:missing_userId", {
                "metadata": metadata,
                "client_reference_id": obj.get("client_reference_id"),
            })
            return _resp(200, {"ok": True})

        if not evt_id:
            return _resp(400, {"error": "Missing Stripe event ID"})
        if not _grant_paid_access(user_id, evt_id, obj):
            log("webhook:duplicate_event_ignored", {"eventId": evt_id})
            return _resp(200, {"ok": True})

        log("webhook:plan_activated", {"userId": user_id})
        return _resp(200, {"ok": True})

    except ClientError as e:
        log("webhook:dynamo_error", {
            "error": str(e),
            "response": getattr(e, "response", None),
        })
        return _resp(500, {"error": "Webhook processing failed"})

    except Exception as e:
        log("webhook:error_processing_completed", str(e))
        return _resp(500, {"error": "Webhook processing failed"})
