# handler: billing_checkout.lambda_handler
import os
import json
from datetime import datetime, timezone
from typing import Optional

import boto3
import stripe
from botocore.exceptions import ClientError

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")

# ========= ENV =========
USERS_TABLE = (os.environ.get("USERS_TABLE", "") or "").strip()

# URLs a las que vuelve Stripe tras pagar/cancelar (solo env, no están en el secret)
STRIPE_SUCCESS_URL = (os.environ.get("STRIPE_SUCCESS_URL", "") or "").strip()
STRIPE_CANCEL_URL = (os.environ.get("STRIPE_CANCEL_URL", "") or "").strip()

# Stripe keys solo desde Secrets Manager (STRIPE_SECRET_NAME obligatorio)
STRIPE_SECRET_KEY = ""
STRIPE_PRICE_ID = ""
_stripe_secret_name = os.environ.get("STRIPE_SECRET_NAME", "").strip()
if _stripe_secret_name:
    try:
        sm = boto3.client("secretsmanager")
        raw = sm.get_secret_value(SecretId=_stripe_secret_name)
        data = json.loads(raw.get("SecretString", "{}"))
        if data:
            STRIPE_SECRET_KEY = (data.get("STRIPE_SECRET_KEY") or data.get("SECRET_KEY") or "").strip()
            STRIPE_PRICE_ID = (data.get("STRIPE_PRICE_ID") or data.get("PRICE_ID") or "").strip()
    except Exception as e:
        print(json.dumps({"msg": "stripe_secret_load_failed", "error": repr(e)}))

stripe.api_key = STRIPE_SECRET_KEY


def log(msg, data=None):
    print(json.dumps({"msg": msg, "data": data}, ensure_ascii=False))


def _resp(status: int, body: dict):
    return {
        "statusCode": int(status),
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, ensure_ascii=False),
    }


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _is_expired(active_until: Optional[str]) -> bool:
    if not active_until:
        return False
    try:
        return datetime.now(timezone.utc) > _parse_iso(active_until)
    except Exception:
        return False


def _get_http_method(event: dict) -> str:
    method = event.get("requestContext", {}).get("http", {}).get("method")
    if method:
        return method.upper()
    return (event.get("httpMethod") or "").upper()


def _get_claims(event: dict) -> dict:
    rc = (event or {}).get("requestContext") or {}
    auth = rc.get("authorizer") or {}
    jwt = auth.get("jwt") or {}
    return jwt.get("claims") or auth.get("claims") or {}


def _normalize_user(item: dict) -> dict:
    user = dict(item or {})
    user.setdefault("plan", "FREE")
    user.setdefault("activeUntil", None)
    return user


def _get_or_create_user(table, user_id: str, email: Optional[str]) -> dict:
    now = _iso_now()
    existing = table.get_item(Key={"userId": user_id}).get("Item")
    if existing:
        return _normalize_user(existing)

    # Bootstrap mínimo (FREE)
    user = {
        "userId": user_id,
        "plan": "FREE",
        "createdAt": now,
        "updatedAt": now,
    }
    if email:
        user["email"] = email

    try:
        table.put_item(Item=user, ConditionExpression="attribute_not_exists(userId)")
        return _normalize_user(user)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            existing = table.get_item(Key={"userId": user_id}).get("Item") or {}
            return _normalize_user(existing)
        raise


def handle_checkout(event: dict):
    missing = [k for k, v in {
        "USERS_TABLE": USERS_TABLE,
        "STRIPE_SECRET_KEY": STRIPE_SECRET_KEY,
        "STRIPE_PRICE_ID": STRIPE_PRICE_ID,
        "STRIPE_SUCCESS_URL": STRIPE_SUCCESS_URL,
        "STRIPE_CANCEL_URL": STRIPE_CANCEL_URL,
    }.items() if not v]

    if missing:
        log("checkout:missing_env", missing)
        return _resp(500, {"error": f"Missing env vars: {', '.join(missing)}"})

    claims = _get_claims(event)
    user_id = claims.get("sub")
    email = claims.get("email")

    if not user_id:
        return _resp(401, {"error": "Missing sub claim (unauthorized)"})

    table = dynamodb.Table(USERS_TABLE)

    # Asegura que el user existe (bootstrap silent)
    user = _get_or_create_user(table, user_id, email)

    # Si ya tiene plan activo, no le hacemos pagar otra vez (MVP)
    if user.get("plan") == "EVENT_24H" and not _is_expired(user.get("activeUntil")):
        return _resp(200, {
            "ok": True,
            "alreadyActive": True,
            "user": {"userId": user_id, "plan": "EVENT_24H", "activeUntil": user.get("activeUntil")},
        })

    now = _iso_now()

    try:
        # Idempotencia: evita duplicar sesiones si el user spamea el botón (por requestId)
        req_id = (event.get("requestContext", {}) or {}).get("requestId") or ""
        idempotency_key = f"checkout_{user_id}_{req_id}" if req_id else f"checkout_{user_id}"

        session = stripe.checkout.Session.create(
            mode="payment",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            success_url=STRIPE_SUCCESS_URL,
            cancel_url=STRIPE_CANCEL_URL,
            client_reference_id=user_id,
            customer_email=email if email else None,
            metadata={"userId": user_id},
        )

        # Guardamos sessionId (debug)
        table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET #updatedAt=:now, #lastCheckoutSessionId=:sid",
            ExpressionAttributeNames={"#updatedAt": "updatedAt", "#lastCheckoutSessionId": "lastCheckoutSessionId"},
            ExpressionAttributeValues={":now": now, ":sid": session.get("id")},
        )

        return _resp(200, {
            "ok": True,
            "checkoutUrl": session.get("url"),
            "sessionId": session.get("id"),
        })

    except Exception as e:
        log("checkout:error", str(e))
        return _resp(500, {"error": "Failed to create checkout session"})


def lambda_handler(event, context):
    method = _get_http_method(event)

    # HTTP API maneja CORS, pero OPTIONS puede llegar si lo llamas manualmente
    if method == "OPTIONS":
        return _resp(200, {"ok": True})

    if method != "POST":
        return _resp(405, {"error": f"Method {method} not allowed"})

    log("checkout:start", {"requestId": getattr(context, "aws_request_id", None)})
    return handle_checkout(event)
