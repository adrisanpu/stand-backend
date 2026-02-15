# handler: webhook_stripe.lambda_handler
import os
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import stripe
from botocore.exceptions import ClientError

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")

# ========= ENV =========
STRIPE_SECRET_KEY = (os.environ.get("STRIPE_SECRET_KEY", "") or "").strip()
STRIPE_WEBHOOK_SECRET = (os.environ.get("STRIPE_WEBHOOK_SECRET", "") or "").strip()
USERS_TABLE = (os.environ.get("USERS_TABLE", "") or "").strip()

stripe.api_key = STRIPE_SECRET_KEY

HEADERS = {"Content-Type": "application/json"}


def log(msg, data=None):
    print(json.dumps({"msg": msg, "data": data}, ensure_ascii=False))


def _resp(status: int, body: dict):
    return {
        "statusCode": int(status),
        "headers": HEADERS,
        "body": json.dumps(body, ensure_ascii=False),
    }


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


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


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


def lambda_handler(event, context):
    log("webhook:start", {"requestId": getattr(context, "aws_request_id", None)})

    missing = [k for k, v in {
        "STRIPE_SECRET_KEY": STRIPE_SECRET_KEY,
        "STRIPE_WEBHOOK_SECRET": STRIPE_WEBHOOK_SECRET,
        "USERS_TABLE": USERS_TABLE,
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

        table = dynamodb.Table(USERS_TABLE)
        now = _iso_now()

        # Leer activeUntil actual para extender correctamente
        current = table.get_item(Key={"userId": user_id}).get("Item") or {}
        current_until = current.get("activeUntil")
        active_until = _plus_24h_from(current_until)

        # Construir UpdateExpression (upsert)
        update_expr = """
        SET #plan=:plan,
            #activeUntil=:until,
            #updatedAt=:now,
            #lastStripeEventId=:eid,
            #createdAt=if_not_exists(#createdAt, :now)
        """
        expr_names = {
            "#plan": "plan",
            "#activeUntil": "activeUntil",
            "#updatedAt": "updatedAt",
            "#lastStripeEventId": "lastStripeEventId",
            "#createdAt": "createdAt",
        }
        expr_vals = {
            ":plan": "EVENT_24H",
            ":until": active_until,
            ":now": now,
            ":eid": evt_id,
        }

        # Campos útiles para debug
        customer_id = obj.get("customer")
        payment_intent = obj.get("payment_intent")
        session_id = obj.get("id")

        if customer_id:
            update_expr += ", #stripeCustomerId=:cid"
            expr_names["#stripeCustomerId"] = "stripeCustomerId"
            expr_vals[":cid"] = customer_id

        if payment_intent:
            update_expr += ", #lastPaymentIntentId=:pi"
            expr_names["#lastPaymentIntentId"] = "lastPaymentIntentId"
            expr_vals[":pi"] = payment_intent

        if session_id:
            update_expr += ", #lastCheckoutSessionId=:sid"
            expr_names["#lastCheckoutSessionId"] = "lastCheckoutSessionId"
            expr_vals[":sid"] = session_id

        # Idempotencia: procesar cada Stripe event una sola vez
        table.update_item(
            Key={"userId": user_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_vals,
            ConditionExpression="attribute_not_exists(lastStripeEventId) OR lastStripeEventId <> :eid",
        )

        log("webhook:plan_activated", {"userId": user_id, "activeUntil": active_until})
        return _resp(200, {"ok": True})

    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            log("webhook:duplicate_event_ignored", {"eventId": evt_id})
            return _resp(200, {"ok": True})

        log("webhook:dynamo_error", {
            "error": str(e),
            "response": getattr(e, "response", None),
        })
        return _resp(500, {"error": "Webhook processing failed"})

    except Exception as e:
        log("webhook:error_processing_completed", str(e))
        return _resp(500, {"error": "Webhook processing failed"})
