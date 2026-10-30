"""Verify raw Kivly webhook bytes. Deduplication/persistence belongs to the receiver."""
import hashlib
import hmac
import json
import time


def verify_webhook(raw_body: bytes, headers: dict, secret: str, *, now=None, tolerance=300):
    normalized = {key.lower():value for key,value in headers.items()}
    timestamp = normalized.get('x-kivly-timestamp','')
    signature = normalized.get('x-kivly-signature-v2','')
    if not secret:
        raise ValueError('Webhook secret is required')
    try:
        stamp = int(timestamp)
    except (ValueError,TypeError) as exc:
        raise ValueError('Invalid webhook timestamp') from exc
    if abs((time.time() if now is None else now)-stamp) > tolerance:
        raise ValueError('Expired webhook delivery')
    expected = hmac.new(secret.encode(),timestamp.encode()+b'.'+raw_body,hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError('Invalid webhook signature')
    event = json.loads(raw_body)
    if not event.get('id') or normalized.get('x-kivly-delivery-id') != event['id'] or normalized.get('x-kivly-event') != event.get('event'):
        raise ValueError('Webhook headers and payload do not match')
    return event
