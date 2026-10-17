import importlib.util
import io
import json
import struct
import binascii
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class FakeClientError(Exception):
    def __init__(self, error_code="TestError"):
        self.response = {"Error": {"Code": error_code}}


ROOT = Path(__file__).parent.parent
MODULE_PATH = ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py"
spec = importlib.util.spec_from_file_location("game_prize_lambda", MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
with patch("boto3.resource"), patch("boto3.client"):
    spec.loader.exec_module(mod)
mod.ClientError = FakeClientError


def _png():
    def chunk(name, contents):
        raw = name + contents
        return (
            struct.pack(">I", len(contents))
            + raw
            + struct.pack(">I", binascii.crc32(raw) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(b"\x00\x00\x00\x00\xff"))
        + chunk(b"IEND", b"")
    )


def _event(method, body=None, path="/v1/game", action=None, sub="owner"):
    query = {"action": action} if action else {}
    return {
        "httpMethod": method,
        "rawPath": path,
        "queryStringParameters": query,
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": sub}}}},
        "body": json.dumps(body) if body is not None else None,
    }


def _setup(monkeypatch, game_type="EMPAREJA2", owner="owner", game_id="ABC123", catalog=None):
    table = MagicMock()
    item = {
        "gameId": game_id,
        "ownerUserId": owner,
        "gameType": game_type,
        "updatedAt": "2026-10-17T00:00:00Z",
        "type": {"EMPAREJA2": {"numPairs": 3, "maxValidations": 4}} if game_type == "EMPAREJA2" else {},
    }
    if catalog is not None:
        item["prizeCatalog"] = catalog
    table.get_item.return_value = {"Item": item}
    mod.games_table = table
    mod.USER_MEDIA_BUCKET = "media"
    mod.prize_s3 = MagicMock()
    mod.prize_s3.generate_presigned_url.return_value = "https://signed"
    mod._get_user = lambda _user_id: {"plan": "FREE"}
    return table, item


def test_put_catalog_persists_and_synchronizes_empareja2_limit_atomically(monkeypatch):
    table, _ = _setup(monkeypatch)
    key = "games/staged/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    data = _png()
    mod.prize_s3.head_object.return_value = {"ContentLength": len(data)}
    mod.prize_s3.get_object.return_value = {"Body": io.BytesIO(data)}

    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Regalo", "limit": 7, "imageKey": key}},
    }), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["prizeCatalog"]["validation"]["imageUrl"] == "https://signed"
    update = table.update_item.call_args.kwargs
    values = update["ExpressionAttributeValues"]
    committed_key = values[":catalog"]["validation"]["imageKey"]
    assert committed_key.startswith("games/owner/ABC123/validation/")
    assert committed_key != key
    assert payload["prizeCatalog"]["validation"]["imageKey"] == committed_key
    assert values[":maxValidations"] == 7
    assert "prizeCatalog = :catalog" in update["UpdateExpression"]
    assert "#type.#gameType.#maxValidations = :maxValidations" in update["UpdateExpression"]
    assert ":type" not in values
    assert "updatedAt = :updated" in update["UpdateExpression"]
    mod.prize_s3.put_object.assert_called_once_with(
        Bucket="media", Key=committed_key, Body=data, ContentLength=len(data), ContentType="image/png",
    )
    mod.prize_s3.delete_object_tagging.assert_called_once_with(Bucket="media", Key=key)


@pytest.mark.parametrize("bad_bytes", [b"not png", _png()[:-12]])
def test_put_invalid_png_preserves_existing_catalog(monkeypatch, bad_bytes):
    previous = {"validation": {"name": "Old", "limit": 4, "imageKey": None}}
    table, _ = _setup(monkeypatch, catalog=previous)
    key = "games/staged/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    mod.prize_s3.head_object.return_value = {"ContentLength": len(bad_bytes)}
    mod.prize_s3.get_object.return_value = {"Body": io.BytesIO(bad_bytes)}
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "New", "limit": 1, "imageKey": key}},
    }), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()
    assert previous["validation"]["name"] == "Old"


def test_put_missing_uploaded_object_returns_explicit_error(monkeypatch):
    table, _ = _setup(monkeypatch)
    key = "games/staged/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    mod.prize_s3.head_object.side_effect = FakeClientError("NoSuchKey")
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Gift", "limit": 1, "imageKey": key}},
    }), None)
    assert response["statusCode"] == 400
    assert "not found" in json.loads(response["body"])["error"]
    table.update_item.assert_not_called()


def test_put_rejects_wrong_size_and_foreign_key_before_persistence(monkeypatch):
    table, _ = _setup(monkeypatch)
    foreign_key = "games/someone-else/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Prize", "limit": 0, "imageKey": foreign_key}},
    }), None)
    assert response["statusCode"] == 400
    mod.prize_s3.head_object.assert_not_called()
    table.update_item.assert_not_called()

    own_key = "games/staged/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    mod.prize_s3.head_object.return_value = {"ContentLength": 5 * 1024 * 1024 + 1}
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Prize", "limit": 0, "imageKey": own_key}},
    }), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


def test_catalog_write_failure_keeps_existing_catalog_and_staging_tag(monkeypatch):
    table, _ = _setup(monkeypatch, catalog={
        "validation": {"name": "Existing", "limit": 2, "imageKey": None},
    })
    table.update_item.side_effect = FakeClientError("ConditionalCheckFailedException")
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Replacement", "limit": 1}},
    }), None)
    assert response["statusCode"] == 409
    mod.prize_s3.delete_object_tagging.assert_not_called()


def test_committed_copy_failure_does_not_write_catalog(monkeypatch):
    table, _ = _setup(monkeypatch, catalog={
        "validation": {"name": "Existing", "limit": 2, "imageKey": None},
    })
    key = "games/staged/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    data = _png()
    mod.prize_s3.head_object.return_value = {"ContentLength": len(data)}
    mod.prize_s3.get_object.return_value = {"Body": io.BytesIO(data)}
    mod.prize_s3.put_object.side_effect = RuntimeError("S3 unavailable")
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Replacement", "limit": 1, "imageKey": key}},
    }), None)
    assert response["statusCode"] == 500
    table.update_item.assert_not_called()


def test_failed_staging_tag_cleanup_is_reported_after_catalog_commit(monkeypatch):
    table, _ = _setup(monkeypatch)
    key = "games/staged/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    data = _png()
    mod.prize_s3.head_object.return_value = {"ContentLength": len(data)}
    mod.prize_s3.get_object.return_value = {"Body": io.BytesIO(data)}
    mod.prize_s3.delete_object_tagging.side_effect = RuntimeError("S3 unavailable")
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Gift", "limit": 1, "imageKey": key}},
    }), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["warnings"] == ["PrizeImageCleanupPending"]
    assert table.update_item.call_count == 1
    assert mod.prize_s3.delete_object_tagging.call_count == 2


def test_put_rejects_non_owner_and_keeps_existing_catalog(monkeypatch):
    table, _ = _setup(monkeypatch)
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Prize", "limit": None}},
    }, sub="not-owner"), None)
    assert response["statusCode"] == 403
    table.update_item.assert_not_called()


def test_put_can_remove_image_and_explicitly_save_empty_catalog(monkeypatch):
    table, _ = _setup(monkeypatch)
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Gift", "limit": None, "imageKey": None}},
    }), None)
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["prizeCatalog"]["validation"]["imageUrl"] is None
    saved = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":catalog"]
    assert saved["validation"]["imageKey"] is None

    table.reset_mock()
    response = mod.lambda_handler(_event("PUT", {"gameId": "ABC123", "prizeCatalog": {}}), None)
    assert response["statusCode"] == 200
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":catalog"] == {}


def test_null_validation_limit_atomically_removes_only_empareja2_cap(monkeypatch):
    table, item = _setup(monkeypatch)
    item["type"]["CUPIDO"] = {"unrelated": "preserve"}
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Gift", "limit": None}},
    }), None)
    assert response["statusCode"] == 200
    payload = json.loads(response["body"])
    update = table.update_item.call_args.kwargs
    values = update["ExpressionAttributeValues"]
    assert "REMOVE #type.#gameType.#maxValidations" in update["UpdateExpression"]
    assert "prizeCatalog = :catalog" in update["UpdateExpression"]
    assert ":type" not in values
    assert ":maxValidations" not in values
    assert "maxValidations" not in payload["game"]["type"]["EMPAREJA2"]
    assert item["type"]["CUPIDO"] == {"unrelated": "preserve"}


def test_omitted_image_key_preserves_existing_image_and_null_removes_it(monkeypatch):
    key = "games/owner/ABC123/validation/01234567-89ab-cdef-0123-456789abcdef.png"
    previous = {"validation": {"name": "Gift", "limit": 1, "imageKey": key}}
    table, _ = _setup(monkeypatch, catalog=previous)
    data = _png()
    mod.prize_s3.head_object.return_value = {"ContentLength": len(data)}
    mod.prize_s3.get_object.return_value = {"Body": io.BytesIO(data)}

    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Renamed gift", "limit": 2}},
    }), None)
    assert response["statusCode"] == 200
    persisted = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":catalog"]
    assert persisted["validation"]["imageKey"] == key
    mod.prize_s3.head_object.assert_called_once_with(Bucket="media", Key=key)

    table.reset_mock()
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {
            "name": "Renamed gift", "limit": 2, "imageKey": None,
        }},
    }), None)
    assert response["statusCode"] == 200
    persisted = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":catalog"]
    assert persisted["validation"]["imageKey"] is None


def test_raffle_catalog_obeys_existing_paid_plan_gate(monkeypatch):
    monkeypatch.delenv("STAGE", raising=False)
    table, _ = _setup(monkeypatch, game_type="EMPAREJA2")
    body = {
        "gameId": "ABC123",
        "prizeCatalog": {"raffle": {"name": "Raffle", "limit": None}},
    }
    response = mod.lambda_handler(_event("PUT", body), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()

    active_until = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    mod._get_user = lambda _user_id: {"plan": "EVENT_24H", "activeUntil": active_until}
    response = mod.lambda_handler(_event("PUT", body), None)
    assert response["statusCode"] == 200
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":catalog"] == {
        "raffle": {"name": "Raffle", "limit": None, "imageKey": None},
    }


def test_downgraded_plan_can_preserve_existing_raffle_while_editing_validation(monkeypatch):
    existing_raffle = {"name": "Raffle", "limit": 4, "imageKey": None}
    table, _ = _setup(monkeypatch, catalog={
        "validation": {"name": "Old validation", "limit": 2, "imageKey": None},
        "raffle": existing_raffle,
    })
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {
            "validation": {"name": "New validation", "limit": 3},
            "raffle": dict(existing_raffle),
        },
    }), None)
    assert response["statusCode"] == 200
    saved = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":catalog"]
    assert saved["validation"]["name"] == "New validation"
    assert saved["raffle"] == existing_raffle


@pytest.mark.parametrize("raffle_entry", [
    {"name": "Edited raffle", "limit": 4, "imageKey": None},
    {"name": "Raffle", "limit": 5, "imageKey": None},
    {"name": "Raffle", "limit": 4, "imageKey": "games/staged/owner/ABC123/raffle/01234567-89ab-cdef-0123-456789abcdef.png"},
    None,
])
def test_downgraded_plan_cannot_edit_or_drop_existing_raffle(monkeypatch, raffle_entry):
    existing_raffle = {"name": "Raffle", "limit": 4, "imageKey": None}
    table, _ = _setup(monkeypatch, catalog={"raffle": existing_raffle})
    catalog = {"validation": {"name": "New validation", "limit": 3}}
    if raffle_entry is not None:
        catalog["raffle"] = raffle_entry
    response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": catalog,
    }), None)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()
    mod.prize_s3.put_object.assert_not_called()


def test_authenticated_get_returns_catalog_only_when_explicitly_saved(monkeypatch):
    table, item = _setup(monkeypatch, catalog={
        "validation": {"name": "Gift", "limit": None, "imageKey": None},
    })
    mod._empareja2_catalog_pair_count = lambda: 0
    event = _event("GET", path="/v1/game", sub="owner")
    event["queryStringParameters"] = {"gameId": "ABC123"}
    response = mod.lambda_handler(event, None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["prizeCatalog"]["validation"]["imageUrl"] is None
    assert "prizeCatalog" not in payload["game"]

    item.pop("prizeCatalog")
    table.get_item.return_value = {"Item": item}
    response = mod.lambda_handler(event, None)
    assert "prizeCatalog" not in json.loads(response["body"])


def test_game_name_get_includes_catalog_at_top_level(monkeypatch):
    table, item = _setup(monkeypatch, catalog={
        "validation": {"name": "Gift", "limit": None, "imageKey": None},
    })
    item.update({"gameName": "Campaign", "playersCount": 2, "validatedCount": 1})
    table.query.return_value = {"Items": [item]}
    mod._empareja2_catalog_pair_count = lambda: 0
    event = _event("GET", path="/v1/game", sub="owner")
    event["queryStringParameters"] = {"gameName": "Campaign"}

    response = mod.lambda_handler(event, None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert payload["prizeCatalog"]["validation"]["imageUrl"] is None
    assert "prizeCatalog" not in payload["game"]


def test_catalog_is_persisted_and_available_to_a_later_get(monkeypatch):
    table, original = _setup(monkeypatch)
    stored = dict(original)

    def read_item(**_kwargs):
        return {"Item": dict(stored)}

    def update_item(**kwargs):
        values = kwargs["ExpressionAttributeValues"]
        stored["prizeCatalog"] = values[":catalog"]
        stored["updatedAt"] = values[":updated"]
        names = kwargs.get("ExpressionAttributeNames", {})
        if ":maxValidations" in values:
            stored["type"][names["#gameType"]][names["#maxValidations"]] = values[":maxValidations"]
        elif "REMOVE #type.#gameType.#maxValidations" in kwargs["UpdateExpression"]:
            stored["type"][names["#gameType"]].pop(names["#maxValidations"], None)
        elif ":gameConfig" in values:
            stored["type"][names["#gameType"]] = values[":gameConfig"]
        elif ":type" in values:
            stored["type"] = values[":type"]

    table.get_item.side_effect = read_item
    table.update_item.side_effect = update_item
    put_response = mod.lambda_handler(_event("PUT", {
        "gameId": "ABC123",
        "prizeCatalog": {"validation": {"name": "Persisted", "limit": 9}},
    }), None)
    assert put_response["statusCode"] == 200
    event = _event("GET", path="/v1/game", sub="owner")
    event["queryStringParameters"] = {"gameId": "ABC123"}
    get_response = mod.lambda_handler(event, None)
    assert json.loads(get_response["body"])["prizeCatalog"]["validation"]["name"] == "Persisted"
    assert stored["type"]["EMPAREJA2"]["maxValidations"] == 9


def test_upload_url_is_owner_and_slot_bound_and_requires_png_metadata(monkeypatch):
    _setup(monkeypatch)
    mod._get_game = lambda game_id: {
        "gameId": game_id, "ownerUserId": "owner", "gameType": "EMPAREJA2",
    }
    response = mod.lambda_handler(_event("POST", {
        "gameId": "ABC123",
        "slot": "validation",
        "fileName": "award.png",
        "contentType": "image/png",
        "sizeBytes": 100,
    }, path="/v1/game/prizes", action="upload-url"), None)
    payload = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert set(payload) == {"ok", "key", "uploadUrl"}
    assert payload["key"].startswith("games/staged/owner/ABC123/validation/")
    assert payload["key"].endswith(".png")
    params = mod.prize_s3.generate_presigned_url.call_args.kwargs["Params"]
    assert params["Tagging"] == "staged=true"

    mod.prize_s3.generate_presigned_url.reset_mock()
    response = mod.lambda_handler(_event("POST", {
        "gameId": "ABC123",
        "slot": "validation",
        "fileName": "boundary.png",
        "contentType": "image/png",
        "sizeBytes": 5 * 1024 * 1024,
    }, path="/v1/game/prizes", action="upload-url"), None)
    assert response["statusCode"] == 200
    mod.prize_s3.generate_presigned_url.reset_mock()

    response = mod.lambda_handler(_event("POST", {
        "gameId": "ABC123",
        "slot": "ranking",
        "fileName": "award.png",
        "contentType": "image/png",
        "sizeBytes": 100,
    }, path="/v1/game/prizes", action="upload-url"), None)
    assert response["statusCode"] == 400
    mod.prize_s3.generate_presigned_url.assert_not_called()

    response = mod.lambda_handler(_event("POST", {
        "gameId": "ABC123",
        "slot": "validation",
        "fileName": "award.jpg",
        "contentType": "image/jpeg",
        "sizeBytes": 100,
    }, path="/v1/game/prizes", action="upload-url"), None)
    assert response["statusCode"] == 400

    response = mod.lambda_handler(_event("POST", {
        "gameId": "ABC123",
        "slot": "validation",
        "fileName": "large.png",
        "contentType": "image/png",
        "sizeBytes": 5 * 1024 * 1024 + 1,
    }, path="/v1/game/prizes", action="upload-url"), None)
    assert response["statusCode"] == 400
    mod.prize_s3.generate_presigned_url.assert_not_called()


def test_upload_url_rejects_unauthenticated_and_foreign_games(monkeypatch):
    _setup(monkeypatch)
    mod._get_game = lambda _game_id: {
        "gameId": "ABC123", "ownerUserId": "another-user", "gameType": "EMPAREJA2",
    }
    body = {
        "gameId": "ABC123", "slot": "validation", "fileName": "a.png",
        "contentType": "image/png", "sizeBytes": 10,
    }
    event = _event("POST", body, path="/v1/game/prizes", action="upload-url", sub="")
    response = mod.lambda_handler(event, None)
    assert response["statusCode"] == 401
    response = mod.lambda_handler(
        _event("POST", body, path="/v1/game/prizes", action="upload-url"), None,
    )
    assert response["statusCode"] == 403
