"""Personal one-use access links (Instagram DM -> mobile play).

Link format: {PUBLIC_APP_ORIGIN}/play/{gameId}#access={token}. The token lives in the
URL fragment (never sent to servers/crawlers) and is exchanged once via
POST /v1/public/access for a regular participant session (playerToken).
Only sha256(nonce) is stored on the existing player record; issuing a new link
replaces it, and the exchange removes it with a conditional update.
Never log links or tokens.
"""
import os
import secrets
from urllib.parse import quote, urlsplit

from stand_common.utils import hash_access_nonce, issue_access_token, _iso_now

DEFAULT_ACCESS_LINK_TTL_SECONDS = 900


def require_access_secret(secret: str) -> None:
    if not isinstance(secret, str) or len(secret) < 32:
        raise RuntimeError("A configured PLAYER_TOKEN_SECRET of at least 32 characters is required")


def access_link_ttl_seconds() -> int:
    try:
        ttl = int(os.environ.get("ACCESS_LINK_TTL_SECONDS", DEFAULT_ACCESS_LINK_TTL_SECONDS))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("ACCESS_LINK_TTL_SECONDS must be an integer") from exc
    if not 60 <= ttl <= 3600:
        raise RuntimeError("ACCESS_LINK_TTL_SECONDS must be between 60 and 3600")
    return ttl


def public_app_origin() -> str:
    origin = (os.environ.get("PUBLIC_APP_ORIGIN") or "").strip().rstrip("/")
    return _validate_origin(origin)


def _validate_origin(origin: str) -> str:
    parsed = urlsplit(origin)
    if (not parsed.hostname or parsed.username or parsed.password or parsed.path
            or parsed.query or parsed.fragment):
        raise RuntimeError("PUBLIC_APP_ORIGIN must be an origin without a path")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1")
    ):
        raise RuntimeError("PUBLIC_APP_ORIGIN must use HTTPS (except local development)")
    return origin


def _error_code(exc) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    return (response.get("Error") or {}).get("Code")


def issue_access_link(gp_table, game_id: str, player_id: int, secret: str, *,
                      origin: str | None = None, ttl_seconds: int | None = None) -> str:
    """Store a fresh nonce hash on the existing player and return the personal link."""
    origin = public_app_origin() if origin is None else _validate_origin(origin.strip().rstrip("/"))
    require_access_secret(secret)
    ttl = access_link_ttl_seconds() if ttl_seconds is None else int(ttl_seconds)
    nonce = secrets.token_urlsafe(24)
    gp_table.update_item(
        Key={"gameId": game_id, "playerId": int(player_id)},
        UpdateExpression="SET accessNonceHash = :h, accessIssuedAt = :t",
        ConditionExpression="attribute_exists(gameId) AND attribute_exists(playerId)",
        ExpressionAttributeValues={":h": hash_access_nonce(nonce), ":t": _iso_now()},
    )
    token = issue_access_token(game_id, int(player_id), nonce, secret, ttl)
    return f"{origin}/play/{quote(game_id, safe='')}#access={token}"


def consume_access_nonce(gp_table, game_id: str, player_id: int, nonce: str) -> bool:
    """Atomically consume the stored nonce; False if already used or superseded."""
    try:
        gp_table.update_item(
            Key={"gameId": game_id, "playerId": int(player_id)},
            UpdateExpression="REMOVE accessNonceHash, accessIssuedAt",
            ConditionExpression="attribute_exists(playerId) AND accessNonceHash = :h",
            ExpressionAttributeValues={":h": hash_access_nonce(nonce)},
        )
        return True
    except Exception as exc:
        if _error_code(exc) == "ConditionalCheckFailedException":
            return False
        raise


def access_link_message(psid: str, url: str, ttl_seconds: int | None = None) -> dict:
    """Sender message (template 'access_link'): button only, URL never in plain text."""
    minutes = max(1, (access_link_ttl_seconds() if ttl_seconds is None else int(ttl_seconds)) // 60)
    return {"psid": psid, "template": "access_link", "url": url, "ttlMinutes": minutes}


ACCESS_LINK_FAILED_TEXT = (
    "No he podido generar tu enlace personal 😅\n"
    "Vuelve a enviarme el código de la partida para recibir uno nuevo."
)