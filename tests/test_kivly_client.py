"""Contract tests with fake transport only; no external credentials or network."""
import io
import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from urllib.error import HTTPError, URLError

import pytest

from stand_common import kivly
from stand_common.kivly import KivlyClient, KivlyError, _Token


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(kivly, "build_opener", lambda *_: MagicMock())
    return KivlyClient("test-client", "test-secret")


def token_response(**changes):
    return {"id": "token-id", "data": {
        "token": "test-access-token", "location_id": "location", "owner": "owner",
        "expires_in": 3600,
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        **changes,
    }}


def context_response(**changes):
    return {
        "location_id": "location", "location_name": "Club", "owner_id": "owner",
        "token_expires_in_max": 3600, "requests_per_minute": 60, "requests_per_hour": 1000,
        "resources": ["customers"], **changes,
    }


def customer_page(rows, *, page=1, total=None, has_next=False):
    return {
        "data": rows, "total": len(rows) if total is None else total, "page": page,
        "limit": 100, "has_next": has_next, "has_previous": page > 1,
    }


@pytest.mark.parametrize("image,expected", [
    ("https://cdn.kivly.store/rewards/coffee.png", "https://cdn.kivly.store/rewards/coffee.png"),
    (None, None), ("javascript:alert(1)", None), ("http://cdn.test/image.png", None),
    ("https://user:password@cdn.test/image.png", None), ("https://[invalid", None),
])
def test_reward_catalog_only_maps_safe_documented_image_field(client, monkeypatch, image, expected):
    result = customer_page([{"id": "coffee", "title": "Coffee", "status": "active",
                             "image": image, "imageUrl": "https://invented.test/ignore.png"}])
    monkeypatch.setattr(client, "_scoped_request", lambda *_: result)
    rewards = client.list_rewards("location", "owner")
    assert rewards[0].get("imageUrl") == expected


def test_create_reward_posts_scoped_contract_and_maps_response(client, monkeypatch):
    client._authenticate = lambda: _Token("token", "location", "owner", 99999)
    request = MagicMock(return_value={
        "id": "reward-1", "title": "Coffee", "description": "A coffee",
        "reward_type": "free_item", "value": 1, "point_type": "POINT",
        "points_required": 25, "status": "active",
    })
    monkeypatch.setattr(client, "_scoped_request", request)

    created = client.create_reward("location", "owner", {
        "title": " Coffee ", "description": " A coffee ", "rewardType": "free_item",
        "value": 1, "pointsRequired": 25, "image": "https://cdn.kivly.store/coffee.png",
        "termsAndConditions": " One per visit ",
        "validFrom": "2030-01-01T10:00:00Z", "validUntil": "2030-02-01T10:00:00+00:00",
        "maxRedemptions": 100, "maxRedemptionsPerUser": 1,
    })

    assert created == {"id": "reward-1", "name": "Coffee"}
    request.assert_called_once_with(
        "location", "owner", "/api/v1/rewards/",
        body={
            "title": "Coffee", "description": "A coffee", "rewardType": "free_item",
            "value": 1, "pointsRequired": 25,
            "image": "https://cdn.kivly.store/coffee.png",
            "termsAndConditions": "One per visit",
            "validFrom": "2030-01-01T10:00:00+00:00",
            "validUntil": "2030-02-01T10:00:00+00:00",
            "maxRedemptions": 100, "maxRedemptionsPerUser": 1,
        },
        expected_status=201,
    )


@pytest.mark.parametrize("reward", [
    {},
    {"title": "Coffee", "description": "A coffee", "rewardType": "other", "value": 1, "pointsRequired": 0},
    {"title": "Coffee", "description": "A coffee", "rewardType": "free_item", "value": True, "pointsRequired": 0},
    {"title": "Coffee", "description": "A coffee", "rewardType": "free_item", "value": 1, "pointsRequired": -1},
    {"title": "Coffee", "description": "A coffee", "rewardType": "free_item", "value": 1, "pointsRequired": 0,
     "validFrom": "2030-02-01T00:00:00Z", "validUntil": "2030-01-01T00:00:00Z"},
    {"title": "Coffee", "description": "A coffee", "rewardType": "free_item", "value": 1, "pointsRequired": 0,
     "image": "javascript:alert(1)"},
    {"title": "Coffee", "description": "A coffee", "rewardType": "free_item", "value": 1, "pointsRequired": 0,
     "unexpected": True},
])
def test_create_reward_rejects_invalid_input_before_post(client, monkeypatch, reward):
    client._authenticate = lambda: _Token("token", "location", "owner", 99999)
    request = MagicMock()
    monkeypatch.setattr(client, "_scoped_request", request)
    with pytest.raises(KivlyError, match="KivlyRewardInvalid"):
        client.create_reward("location", "owner", reward)
    request.assert_not_called()


def test_create_reward_does_not_repeat_uncertain_mutations(client, monkeypatch):
    client._authenticate = lambda: _Token("token", "location", "owner", 99999)
    request = MagicMock(side_effect=KivlyError("KivlyUnavailable"))
    monkeypatch.setattr(client, "_scoped_request", request)
    reward = {
        "title": "Coffee", "description": "A coffee", "rewardType": "free_item",
        "value": 1, "pointsRequired": 0,
    }

    with pytest.raises(KivlyError, match="KivlyRewardCreationUncertain"):
        client.create_reward("location", "owner", reward)
    request.assert_called_once()


def test_create_reward_marks_invalid_success_response_uncertain(client, monkeypatch):
    client._authenticate = lambda: _Token("token", "location", "owner", 99999)
    monkeypatch.setattr(client, "_scoped_request", lambda *args, **kwargs: {"id": "reward-1"})
    reward = {
        "title": "Coffee", "description": "A coffee", "rewardType": "free_item",
        "value": 1, "pointsRequired": 0,
    }

    with pytest.raises(KivlyError, match="KivlyRewardCreationUncertain"):
        client.create_reward("location", "owner", reward)


def test_scoped_reward_post_uses_http_201_and_json_body(client, monkeypatch):
    client._authenticate = lambda: _Token("token", "location", "owner", 99999)
    sender = MagicMock(return_value={"accepted": True})
    monkeypatch.setattr(client, "_send_request", sender)
    payload = {"title": "Coffee", "description": "A coffee"}

    client._scoped_request(
        "location", "owner", "/api/v1/rewards/", body=payload, expected_status=201,
    )

    request = sender.call_args.args[0]
    assert request.method == "POST"
    assert request.full_url == "https://api.getkivly.com/api/v1/rewards/"
    assert json.loads(request.data) == payload
    assert sender.call_args.kwargs["expected_status"] == 201


def test_authentication_cache_is_private_and_reused(client, monkeypatch):
    request = MagicMock(return_value=token_response())
    monkeypatch.setattr(client, "_request", request)
    first = client._authenticate()
    assert client._authenticate() is first
    request.assert_called_once_with("/api/v1/auth/token")
    assert "test-access-token" not in repr(first)


def test_near_expiry_token_is_renewed(client, monkeypatch):
    client._token = _Token("old", "location", "owner", time.monotonic() + 1)
    request = MagicMock(return_value=token_response())
    monkeypatch.setattr(client, "_request", request)
    assert client._authenticate().value == "test-access-token"
    request.assert_called_once()


@pytest.mark.parametrize("changes", [
    {"token": ""}, {"token": "bad token"}, {"token": "\nunsafe"}, {"token": "á"},
    {"token_type": "Other"}, {"expires_in": True}, {"expires_in": 0},
    {"expires_at": "invalid"}, {"expires_at": "2030-01-01T00:00:00"},
    {"expires_at": "2020-01-01T00:00:00Z"}, {"location_id": ""}, {"owner": None},
])
def test_authentication_rejects_invalid_or_expired_response(client, monkeypatch, changes):
    monkeypatch.setattr(client, "_request", lambda *_: token_response(**changes))
    with pytest.raises(KivlyError):
        client._authenticate()
    assert client._token is None


def test_context_must_match_token_scope(client, monkeypatch):
    responses = iter([token_response(), context_response(location_id="different")])
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: next(responses))
    with pytest.raises(KivlyError, match="KivlyScopeMismatch"):
        client.get_context()
    assert client._token is None


def test_context_requires_typed_contract(client, monkeypatch):
    responses = iter([token_response(), context_response(requests_per_minute=True)])
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: next(responses))
    with pytest.raises(KivlyError, match="KivlyContextResponseInvalid"):
        client.get_context()


def test_customer_operations_reject_scope_mismatch_before_network(client, monkeypatch):
    monkeypatch.setattr(client, "_authenticate", lambda: _Token("token", "location", "owner", 99999))
    with pytest.raises(KivlyError, match="KivlyScopeMismatch"):
        client._customers("other-location", "owner", page=1)
    client._opener.open.assert_not_called()


def test_customer_lookup_requires_complete_listing_and_normalizes_email(client, monkeypatch):
    pages = iter([
        customer_page([{"id": "unrelated", "email": None}], total=2, has_next=True),
        customer_page([{"id": "match", "email": " Player@Club.Test "}], total=2, page=2),
    ])
    lookup = MagicMock(side_effect=lambda *args, **kwargs: next(pages))
    monkeypatch.setattr(client, "_customers", lookup)
    assert client.find_customer("location", "owner", "player@club.test")["id"] == "match"
    assert lookup.call_count == 2
    lookup.assert_any_call("location", "owner", page=2)


@pytest.mark.parametrize("page", [
    customer_page([], total=1),
    customer_page([], has_next=True),
    customer_page([{"id": "one", "email": "player@club.test"}], total=0),
    customer_page([{"id": "one", "email": 123}]),
    customer_page([{"id": "one"}, {"id": "one"}]),
    {**customer_page([]), "total": True},
    {**customer_page([]), "has_next": 0},
    {**customer_page([]), "page": 2},
])
def test_incomplete_or_ambiguous_listing_never_proves_absence(client, monkeypatch, page):
    monkeypatch.setattr(client, "_customers", lambda *args, **kwargs: page)
    with pytest.raises(KivlyError):
        client.find_customer("location", "owner", "player@club.test")


def test_duplicate_matching_customers_fail_closed(client, monkeypatch):
    monkeypatch.setattr(client, "_customers", lambda *args, **kwargs: customer_page([
        {"id": "one", "email": "player@club.test"},
        {"id": "two", "email": "PLAYER@CLUB.TEST"},
    ]))
    with pytest.raises(KivlyError, match="KivlyDuplicateCustomers"):
        client.find_customer("location", "owner", "player@club.test")


def test_lookup_stops_at_five_pages_without_claiming_absence(client, monkeypatch):
    def page(*args, **kwargs):
        index = kwargs["page"]
        return customer_page([{"id": f"customer-{index}"}], page=index, total=1000, has_next=True)

    lookup = MagicMock(side_effect=page)
    monkeypatch.setattr(client, "_customers", lookup)
    with pytest.raises(KivlyError, match="KivlySearchIncomplete"):
        client.find_customer("location", "owner", "player@club.test")
    assert lookup.call_count == 5


@pytest.mark.parametrize("phone", [None, "+34 612345678"])
def test_create_payload_contains_only_registration_fields(client, monkeypatch, phone):
    request = MagicMock(return_value={
        "id": "customer", "email": "player@club.test", "external_id": "stand-attempt", "is_active": True,
    })
    monkeypatch.setattr(client, "_customers", request)
    client.create_customer("location", "owner", "Full Name", "player@club.test", "stand-attempt", phone)
    request.assert_called_once_with("location", "owner", customer={
        "name": "Full Name", "email": "player@club.test", "external_id": "stand-attempt", "source": "stand",
        **({"phone": phone} if phone else {}),
    })


@pytest.mark.parametrize("change", [
    {"id": ""}, {"email": "different@club.test"}, {"is_active": False}, {"external_id": "other-attempt"},
])
def test_create_requires_matching_active_customer_response(client, monkeypatch, change):
    monkeypatch.setattr(client, "_customers", lambda *args, **kwargs: {
        "id": "customer", "email": "player@club.test", "external_id": "stand-attempt",
        "is_active": True, **change,
    })
    with pytest.raises(KivlyError, match="KivlyCustomerInvalid"):
        client.create_customer("location", "owner", "Full Name", "player@club.test", "stand-attempt")


def test_http_transport_is_bounded_and_does_not_retry(client):
    response = MagicMock()
    response.status = 200
    response.read.return_value = json.dumps({"ok": True}).encode()
    client._opener.open.return_value.__enter__.return_value = response
    assert client._request("/api/v1/auth/token") == {"ok": True}
    request = client._opener.open.call_args.args[0]
    assert request.full_url == "https://api.getkivly.com/api/v1/auth/token"
    assert request.method == "POST"
    assert json.loads(request.data) == {"client_id": "test-client", "client_secret": "test-secret"}
    assert client._opener.open.call_args.kwargs["timeout"] == kivly.TIMEOUT_SECONDS
    response.read.assert_called_once_with(kivly.MAX_RESPONSE_BYTES + 1)


@pytest.mark.parametrize("status,code", [
    (301, "KivlyRedirectRejected"), (401, "KivlyUnauthorized"), (403, "KivlyForbidden"),
    (422, "KivlyValidationFailed"), (429, "KivlyRateLimited"), (500, "KivlyUnavailable"),
])
def test_http_failures_have_safe_codes_and_no_automatic_retry(client, status, code):
    client._opener.open.side_effect = HTTPError(
        "https://api.getkivly.com", status, "not returned to caller", {"Retry-After": "7"}, io.BytesIO(),
    )
    with pytest.raises(KivlyError) as caught:
        client._request("/api/v1/auth/token")
    assert caught.value.code == code
    assert caught.value.http_status == status
    assert caught.value.retry_after == 7
    client._opener.open.assert_called_once()


@pytest.mark.parametrize("error", [URLError("private network detail"), OSError("private path")])
def test_network_details_are_not_returned(client, error):
    client._opener.open.side_effect = error
    with pytest.raises(KivlyError, match="^KivlyUnavailable$"):
        client._request("/api/v1/auth/token")


def test_oversized_response_is_rejected(client):
    response = MagicMock()
    response.status = 200
    response.read.return_value = b" " * (kivly.MAX_RESPONSE_BYTES + 1)
    client._opener.open.return_value.__enter__.return_value = response
    with pytest.raises(KivlyError, match="KivlyResponseTooLarge"):
        client._request("/api/v1/auth/token")


def test_arbitrary_http_paths_are_not_available(client):
    with pytest.raises(KivlyError, match="KivlyRequestNotAllowed"):
        client._request("/unexpected")
    client._opener.open.assert_not_called()
