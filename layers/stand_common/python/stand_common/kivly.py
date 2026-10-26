"""Server-only Kivly client with bounded, explicitly scoped customer operations."""
import json
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from urllib.parse import quote, urlencode

BASE_URL = "https://api.getkivly.com"
MAX_RESPONSE_BYTES = 1_048_576
TIMEOUT_SECONDS = 8
RENEWAL_MARGIN_SECONDS = 30


class KivlyError(Exception):
    def __init__(self, code, http_status=None, retry_after=None):
        super().__init__(code)
        self.code = code
        self.http_status = http_status
        self.retry_after = retry_after


@dataclass(frozen=True, repr=False)
class IntegrationContext:
    location_id: str
    location_name: str
    owner_id: str
    token_expires_in_max: int
    requests_per_minute: int
    requests_per_hour: int
    resources: tuple[str, ...]


@dataclass(frozen=True)
class _Token:
    value: str = field(repr=False)
    location_id: str = field(repr=False)
    owner_id: str = field(repr=False)
    deadline: float


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _text(value, maximum):
    return (
        isinstance(value, str) and bool(value.strip()) and len(value) <= maximum
        and not any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value)
    )


def _positive_integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _retry_after(headers):
    value = headers.get("Retry-After", "") if headers is not None else ""
    return int(value) if value.isascii() and value.isdigit() and len(value) <= 10 else None


class KivlyClient:
    """One credential pair per instance; tokens remain only in that instance."""

    def __init__(self, client_id, client_secret, *, ca_bundle=None):
        if not _text(client_id, 512) or not _text(client_secret, 1024):
            raise KivlyError("KivlyCredentialsInvalid")
        self._credentials = {"client_id": client_id, "client_secret": client_secret}
        self._token = None
        try:
            context = ssl.create_default_context(cafile=ca_bundle)
        except (OSError, ValueError):
            raise KivlyError("KivlyTLSConfigurationInvalid") from None
        self._opener = build_opener(_NoRedirect(), HTTPSHandler(context=context))

    def _request(self, path, *, token=None):
        # Deliberately not a general-purpose HTTP client or arbitrary URL proxy.
        if path not in ("/api/v1/auth/token", "/api/v1/integration/context"):
            raise KivlyError("KivlyRequestNotAllowed")
        authenticating = path == "/api/v1/auth/token"
        headers = {"Accept": "application/json", "User-Agent": "Stand-Kivly/1.0"}
        data = None
        if authenticating:
            headers["Content-Type"] = "application/json"
            data = json.dumps(self._credentials).encode("utf-8")
        else:
            headers["Authorization"] = f"Bearer {token}"
        request = Request(
            BASE_URL + path, method="POST" if authenticating else "GET",
            headers=headers, data=data,
        )
        return self._send_request(request)

    def _send_request(self, request, *, expected_status=200, response_type=dict):
        try:
            with self._opener.open(request, timeout=TIMEOUT_SECONDS) as response:
                if response.status != expected_status:
                    raise KivlyError("KivlyResponseInvalid")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise KivlyError("KivlyResponseTooLarge")
                result = json.loads(raw)
        except HTTPError as exc:
            status = exc.code
            retry_after = _retry_after(exc.headers)
            exc.close()
            if status == 401:
                self._token = None
            code = {
                401: "KivlyUnauthorized", 403: "KivlyForbidden",
                422: "KivlyValidationFailed", 429: "KivlyRateLimited",
            }.get(status, "KivlyUnavailable" if status >= 500 else "KivlyRequestFailed")
            if 300 <= status < 400:
                code = "KivlyRedirectRejected"
            raise KivlyError(code, status, retry_after) from None
        except (URLError, OSError, HTTPException):
            raise KivlyError("KivlyUnavailable") from None
        except (ValueError, UnicodeError):
            raise KivlyError("KivlyResponseInvalid") from None
        if response_type is not None and not isinstance(result, response_type):
            raise KivlyError("KivlyResponseInvalid")
        return result

    def _authenticate(self):
        started = time.monotonic()
        if self._token is not None and self._token.deadline > started + RENEWAL_MARGIN_SECONDS:
            return self._token
        self._token = None
        result = self._request("/api/v1/auth/token")
        data = result.get("data")
        if (
            not _text(result.get("id"), 512) or not isinstance(data, dict)
            or ("success" in result and result["success"] is not True)
            or ("status" in result and (
                type(result["status"]) is not int or result["status"] != 200
            ))
        ):
            raise KivlyError("KivlyTokenResponseInvalid")
        token, ttl = data.get("token"), data.get("expires_in", 3600)
        if (
            not _text(token, 65536) or not token.isascii()
            or any(char.isspace() for char in token)
            or data.get("token_type", "Bearer") != "Bearer"
            or not _positive_integer(ttl)
            or not _text(data.get("location_id"), 512)
            or not _text(data.get("owner"), 512)
            or not isinstance(data.get("expires_at"), str)
        ):
            raise KivlyError("KivlyTokenResponseInvalid")
        try:
            expires_at = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00"))
            if expires_at.tzinfo is None:
                raise ValueError("Timezone required")
            remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
        except ValueError:
            raise KivlyError("KivlyTokenResponseInvalid") from None
        deadline = started + min(ttl, remaining)
        if deadline <= time.monotonic():
            raise KivlyError("KivlyTokenExpired")
        self._token = _Token(token, data["location_id"], data["owner"], deadline)
        return self._token

    def get_context(self):
        token = self._authenticate()
        result = self._request("/api/v1/integration/context", token=token.value)
        if (
            not all(_text(result.get(key), 512) for key in (
                "location_id", "location_name", "owner_id",
            ))
            or not all(_positive_integer(result.get(key)) for key in (
                "token_expires_in_max", "requests_per_minute", "requests_per_hour",
            ))
            or not isinstance(result.get("resources"), list)
            or not all(_text(resource, 512) for resource in result["resources"])
        ):
            raise KivlyError("KivlyContextResponseInvalid")
        if result["location_id"] != token.location_id or result["owner_id"] != token.owner_id:
            self._token = None
            raise KivlyError("KivlyScopeMismatch")
        return IntegrationContext(
            location_id=result["location_id"], location_name=result["location_name"],
            owner_id=result["owner_id"], token_expires_in_max=result["token_expires_in_max"],
            requests_per_minute=result["requests_per_minute"],
            requests_per_hour=result["requests_per_hour"], resources=tuple(result["resources"]),
        )

    def _customers(self, location_id, owner_id, *, page=None, customer=None):
        token = self._authenticate()
        if token.location_id != location_id or token.owner_id != owner_id:
            raise KivlyError("KivlyScopeMismatch")
        headers = {
            "Accept": "application/json", "User-Agent": "Stand-Kivly/1.0",
            "Authorization": f"Bearer {token.value}",
        }
        url = BASE_URL + "/api/v1/customers/"
        data = None
        if customer is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(customer).encode("utf-8")
        else:
            url += "?" + urlencode({"page": page, "limit": 100, "include_balance": "false"})
        return self._send_request(
            Request(url, method="POST" if customer is not None else "GET",
                    headers=headers, data=data),
            expected_status=201 if customer is not None else 200,
        )

    def _scoped_request(self, location_id, owner_id, path, *, body=None, response_type=dict):
        token = self._authenticate()
        if token.location_id != location_id or token.owner_id != owner_id:
            raise KivlyError("KivlyScopeMismatch")
        base_path = path.partition("?")[0] if isinstance(path, str) else ""
        if not (base_path == "/api/v1/rewards/"
                or base_path.startswith("/api/v1/redemptions/customer/")
                or base_path == "/api/v1/redemptions/rewards/earn"):
            raise KivlyError("KivlyRequestNotAllowed")
        headers = {
            "Accept": "application/json", "User-Agent": "Stand-Kivly/1.0",
            "Authorization": f"******",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        return self._send_request(
            Request(BASE_URL + path, method="POST" if body is not None else "GET",
                    headers=headers, data=json.dumps(body).encode() if body is not None else None),
            response_type=response_type,
        )

    def list_rewards(self, location_id, owner_id):
        rewards, total = [], None
        for page in range(1, 6):
            result = self._scoped_request(
                location_id, owner_id,
                "/api/v1/rewards/?" + urlencode({"skip": (page - 1) * 100, "limit": 100, "status": "active"}),
            )
            rows = result.get("data")
            if (not isinstance(rows, list) or len(rows) > 100
                    or type(result.get("total")) is not int or result["total"] < 0
                    or type(result.get("page")) is not int or result["page"] != page
                    or type(result.get("limit")) is not int or result["limit"] != 100
                    or type(result.get("has_next")) is not bool
                    or type(result.get("has_previous")) is not bool
                    or result["has_previous"] != (page > 1)):
                raise KivlyError("KivlyCatalogInvalid")
            if total is None:
                total = result["total"]
            if total != result["total"]:
                raise KivlyError("KivlyCatalogInvalid")
            for row in rows:
                if not isinstance(row, dict) or not _text(row.get("id"), 512) or not _text(row.get("title"), 200):
                    raise KivlyError("KivlyCatalogInvalid")
                if row.get("status") != "active" or any(item["id"] == row["id"] for item in rewards):
                    raise KivlyError("KivlyCatalogInvalid")
                rewards.append({"id": row["id"], "name": row["title"]})
            if not result["has_next"]:
                if len(rewards) != total:
                    raise KivlyError("KivlyCatalogInvalid")
                return rewards
            if not rows:
                raise KivlyError("KivlyCatalogInvalid")
        raise KivlyError("KivlyCatalogIncomplete")

    def customer_rewards(self, location_id, owner_id, customer_id):
        if not _text(customer_id, 512):
            raise KivlyError("KivlyCustomerInvalid")
        rewards, seen = [], set()
        for page in range(1, 6):
            rows = self._scoped_request(
                location_id, owner_id,
                "/api/v1/redemptions/customer/" + quote(customer_id, safe="") + "?"
                + urlencode({"page": page, "limit": 100}), response_type=list,
            )
            if len(rows) > 100:
                raise KivlyError("KivlyRewardsInvalid")
            for row in rows:
                if (not isinstance(row, dict) or not _text(row.get("id"), 512)
                        or row["id"] in seen or row.get("customer_id") != customer_id
                        or not _text(row.get("reward_id"), 512)):
                    raise KivlyError("KivlyRewardsInvalid")
                seen.add(row["id"])
                rewards.append(row)
            if len(rows) < 100:
                return rewards
        raise KivlyError("KivlyRewardsIncomplete")

    def earn_reward(self, location_id, owner_id, customer_id, reward_id):
        if not _text(customer_id, 512) or not _text(reward_id, 512):
            raise KivlyError("KivlyRewardInvalid")
        self._scoped_request(
            location_id, owner_id, "/api/v1/redemptions/rewards/earn",
            body={"customer_id": customer_id, "reward_ids": [reward_id]}, response_type=None,
        )

    def find_customer(self, location_id, owner_id, email):
        # Filtered search has returned an inconsistent total in a controlled test.
        # Only a complete, bounded unfiltered listing may establish absence.
        seen, matches = set(), []
        total = None
        for page in range(1, 6):
            result = self._customers(location_id, owner_id, page=page)
            rows = result.get("data")
            if (
                not isinstance(rows, list) or len(rows) > 100
                or type(result.get("total")) is not int or result["total"] < 0
                or type(result.get("page")) is not int or result["page"] != page
                or type(result.get("limit")) is not int or result["limit"] != 100
                or type(result.get("has_next")) is not bool
                or type(result.get("has_previous")) is not bool
                or result["has_previous"] != (page > 1)
            ):
                raise KivlyError("KivlySearchIncomplete")
            if total is None:
                total = result["total"]
            if total != result["total"]:
                raise KivlyError("KivlySearchIncomplete")
            for row in rows:
                if (
                    not isinstance(row, dict) or not _text(row.get("id"), 512)
                    or row["id"] in seen
                    or (row.get("email") is not None and not isinstance(row["email"], str))
                ):
                    raise KivlyError("KivlyResponseInvalid")
                seen.add(row["id"])
                if (row.get("email") or "").strip().casefold() == email.strip().casefold():
                    matches.append(row)
            if not result["has_next"]:
                if len(seen) != total:
                    raise KivlyError("KivlySearchIncomplete")
                if len(matches) > 1:
                    raise KivlyError("KivlyDuplicateCustomers")
                return matches[0] if matches else None
            if not rows or len(seen) >= total:
                raise KivlyError("KivlySearchIncomplete")
        raise KivlyError("KivlySearchIncomplete")

    def create_customer(self, location_id, owner_id, name, email, external_id, phone=None):
        if not all(_text(value, limit) for value, limit in (
            (name, 100), (email, 254), (external_id, 128),
        )):
            raise KivlyError("KivlyCustomerInvalid")
        customer = {
            "name": name, "email": email, "external_id": external_id, "source": "stand",
        }
        if phone is not None:
            if not _text(phone, 64):
                raise KivlyError("KivlyCustomerInvalid")
            customer["phone"] = phone
        result = self._customers(location_id, owner_id, customer=customer)
        if (
            not _text(result.get("id"), 512)
            or not isinstance(result.get("email"), str)
            or result["email"].strip().casefold() != email.strip().casefold()
            or result.get("is_active") is not True
            or result.get("external_id") != external_id
        ):
            raise KivlyError("KivlyCustomerInvalid")
        return result
