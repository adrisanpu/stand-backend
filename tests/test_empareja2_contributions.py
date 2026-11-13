import io
from unittest.mock import MagicMock

import pytest

from stand_common import empareja2_contributions as contributions
from tests.test_game_prizes import _png


def _setup():
    game = {"gameId": "ABC123", "ownerUserId": "owner", "gameType": "EMPAREJA2",
            "type": {"EMPAREJA2": {"numPairs": 2, "maxValidations": 4}}}
    pairs = [[
        {"name": "Alice", "imageKey": "empareja2/staged/owner/ABC123/" + "a" * 32 + ".png"},
        {"name": "Bob", "imageKey": "empareja2/staged/owner/ABC123/" + "b" * 32 + ".png"},
    ]]
    body = {"operationId": "c" * 32, "numPairs": 1, "pairs": pairs}
    images = _png()
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": len(images)}
    s3.get_object.side_effect = lambda **_: {"Body": io.BytesIO(images)}
    catalog, games, dynamo = MagicMock(), MagicMock(), MagicMock()
    catalog.name, games.name = "catalog", "games"
    catalog.get_item.return_value = {}
    items = [{"pairGroupId": "1", "characterId": 1}, {"pairGroupId": "2", "characterId": 2}]
    return dynamo, games, catalog, s3, game, body, items


def _publish(data):
    dynamo, games, catalog, s3, game, body, items = data
    return contributions.publish(dynamo, games, catalog, s3, "staging", "characters", "owner", game, body, items)


def test_publish_writes_both_characters_and_game_in_one_transaction():
    data = _setup()
    result = _publish(data)
    dynamo, _, _, s3, _, _, _ = data
    assert result == {"ok": True, "alreadyPublished": False}
    operations = dynamo.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
    assert len(operations) == 5
    assert operations[0]["Update"]["ExpressionAttributeValues"][":next"]["EMPAREJA2"] == {
        "numPairs": 1, "maxValidations": 4,
    }
    rows = [operation["Put"]["Item"] for operation in operations[3:]]
    assert [row["characterName"] for row in rows] == ["Alice", "Bob"]
    assert rows[0]["pairGroupId"] == rows[1]["pairGroupId"] == "CONTRIB#00000000000000000001"
    assert [row["characterId"] for row in rows] == [-1, -2]
    assert all(row["imageKey"].startswith("contributions/") for row in rows)
    assert s3.put_object.call_count == 2


def test_retry_of_committed_operation_does_not_duplicate_pairs():
    data = _setup()
    _publish(data)
    dynamo, _, catalog, s3, _, _, _ = data
    marker = dynamo.meta.client.transact_write_items.call_args.kwargs["TransactItems"][1]["Put"]["Item"]
    catalog.get_item.return_value = {"Item": marker}
    dynamo.meta.client.transact_write_items.reset_mock()
    s3.reset_mock()
    assert _publish(data) == {"ok": True, "alreadyPublished": True}
    dynamo.meta.client.transact_write_items.assert_not_called()
    s3.put_object.assert_not_called()


def test_counter_collision_retries_with_new_sequence(monkeypatch):
    class FakeClientError(Exception):
        response = {"Error": {"Code": "TransactionCanceledException"}}

    monkeypatch.setattr(contributions, "ClientError", FakeClientError)
    data = _setup()
    dynamo, games, catalog, _, game, _, _ = data
    games.get_item.return_value = {"Item": game}
    catalog.get_item.side_effect = [{}, {}, {}, {"Item": {"value": 4}}]
    dynamo.meta.client.transact_write_items.side_effect = [FakeClientError(), {}]
    assert _publish(data)["ok"] is True
    operations = dynamo.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
    assert operations[3]["Put"]["Item"]["pairGroupId"] == "CONTRIB#00000000000000000005"


def test_rejects_invalid_image_before_publishing():
    data = _setup()
    data[3].get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"bad")}
    with pytest.raises(contributions.CatalogError):
        _publish(data)
    data[0].meta.client.transact_write_items.assert_not_called()


def test_invalid_second_image_does_not_leave_first_image_committed():
    data = _setup()
    image = _png()
    data[3].head_object.side_effect = [{"ContentLength": len(image)}, {"ContentLength": 3}]
    data[3].get_object.side_effect = [{"Body": io.BytesIO(image)}, {"Body": io.BytesIO(b"bad")}]
    with pytest.raises(contributions.CatalogError):
        _publish(data)
    data[3].put_object.assert_not_called()
    data[0].meta.client.transact_write_items.assert_not_called()


@pytest.mark.parametrize("change", ["bad_key", "duplicate", "missing_name", "too_many", "invalid_limit"])
def test_rejects_invalid_contributions_without_writes(change):
    data = _setup()
    body = data[5]
    if change == "bad_key":
        body["pairs"][0][0]["imageKey"] = "other/key.png"
    elif change == "duplicate":
        body["pairs"][0][1]["imageKey"] = body["pairs"][0][0]["imageKey"]
    elif change == "missing_name":
        body["pairs"][0][0]["name"] = " "
    elif change == "too_many":
        body["pairs"] *= 11
    else:
        body["numPairs"] = 3
    with pytest.raises(contributions.CatalogError):
        _publish(data)
    data[3].put_object.assert_not_called()
    data[0].meta.client.transact_write_items.assert_not_called()


def test_upload_policy_limits_image_size():
    s3 = MagicMock()
    s3.generate_presigned_post.return_value = {"url": "https://s3", "fields": {"key": "value"}}
    result = contributions.upload_url(s3, "stage", "owner", "ABC123", {"uploadId": "a" * 32, "sizeBytes": 10})
    assert result["key"].startswith("empareja2/staged/owner/ABC123/")
    assert ["content-length-range", 1, contributions.MAX_PRIZE_IMAGE_BYTES] in s3.generate_presigned_post.call_args.kwargs["Conditions"]
