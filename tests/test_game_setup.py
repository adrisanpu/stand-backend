"""Initial setup review is owner-only metadata, not a gameplay lifecycle gate."""
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from tests.email_fake import FakeDynamoError
from tests.test_game_players import event, game, response_body


@pytest.fixture
def table():
    table = MagicMock()
    table.item = {
        "gameId": "ABC123", "gameName": "Event", "gameType": "T1MER",
        "ownerUserId": "owner", "isActive": True,
        "setupReview": game._initial_setup_review(),
    }
    table.get_item.side_effect = lambda **_: {"Item": deepcopy(table.item)}
    table.query.side_effect = lambda **_: {"Items": [deepcopy(table.item)]}

    def update(**kwargs):
        values = kwargs["ExpressionAttributeValues"]
        item = table.item
        if (item.get("ownerUserId") != values[":owner"]
                or item.get("setupReview", {}).get("revision") != values[":expected"]
                or not item.get("isActive", True)
                or item.get("lifecycleState", "active") != "active"
                or item.get("stackPrizeState") in {"closing", "closed"}
                or any(key in item for key in ("archivedAt", "deletedAt", "deletionStartedAt"))):
            raise FakeDynamoError("ConditionalCheckFailedException")
        table.item["setupReview"] = deepcopy(values[":review"])
        return {"Attributes": {"setupReview": deepcopy(table.item["setupReview"])}}

    table.update_item.side_effect = update
    with patch.object(game, "games_table", table):
        yield table


def put(table, update, owner="owner", **extra):
    return game.lambda_handler(event("PUT", "/v1/game", body={
        "gameId": "ABC123", "setupReview": update, **extra,
    }, owner=owner), None)


def confirm(table, section, confirmed=True):
    return put(table, {
        "section": section, "confirmed": confirmed,
        "expectedRevision": table.item["setupReview"]["revision"],
    })


def finish(table):
    return put(table, {"finish": True, "expectedRevision": table.item["setupReview"]["revision"]})


def test_creation_initializes_review_in_same_conditional_put():
    table = MagicMock()
    table.query.return_value = {"Items": []}
    with patch.object(game, "games_table", table), patch.object(game, "_is_user_pro", return_value=True), \
            patch.object(game, "_get_user", return_value={}), \
            patch.object(game, "_load_default_quiz_from_catalog", return_value=([], {})):
        result = game.lambda_handler(event("POST", "/v1/game", body={
            "gameType": "T1MER", "gameName": "New",
            "setupReview": {"completed": True},
        }), None)
    assert result["statusCode"] == 201
    item = response_body(result)["game"]
    assert item["isActive"] is True
    assert game.game_lifecycle.state(item) == "active"
    assert item["setupReview"] == {
        "version": 1, "revision": item["setupReview"]["revision"],
        "confirmedSections": [], "completed": False,
    }
    assert item["setupReview"]["revision"]
    assert table.put_item.call_args.kwargs["Item"]["setupReview"] == item["setupReview"]
    assert table.put_item.call_args.kwargs["ConditionExpression"] == "attribute_not_exists(gameId)"


@pytest.mark.parametrize("query", [{}, {"gameId": "ABC123"}, {"gameId": "ABC123", "configuration": "true"}])
def test_owner_reads_expose_review_and_legacy_events_remain_exempt(table, query):
    for legacy in (False, True):
        if legacy:
            del table.item["setupReview"]
        response = game.lambda_handler(event("GET", "/v1/game", query=query), None)
        assert response["statusCode"] == 200
        payload = response_body(response)
        item = payload["game"] if "gameId" in query else payload["games"][0]
        assert ("setupReview" in item) is not legacy
        if not legacy:
            assert item["setupReview"] == table.item["setupReview"]
    assert "setupReview" not in game._public_game_shape({
        **table.item, "setupReview": game._initial_setup_review(),
    })
    response = put(table, {"section": "appearance", "confirmed": True, "expectedRevision": "old"})
    assert response_body(response)["error"] == "SetupReviewNotRequired"
    table.update_item.assert_not_called()


@pytest.mark.parametrize(("owner", "status"), [("", 401), ("someone-else", 403)])
def test_review_write_requires_owner(table, owner, status):
    response = put(table, {"section": "appearance", "confirmed": True, "expectedRevision": "r"}, owner=owner)
    assert response["statusCode"] == status
    table.update_item.assert_not_called()


@pytest.mark.parametrize(("owner", "status"), [("", 401), ("someone-else", 403)])
@pytest.mark.parametrize("configuration", [False, True])
def test_review_reads_require_owner(table, owner, status, configuration):
    query = {"gameId": "ABC123"}
    if configuration:
        query["configuration"] = "true"
    response = game.lambda_handler(event("GET", "/v1/game", query=query, owner=owner), None)
    assert response["statusCode"] == status
    assert "setupReview" not in response_body(response)


@pytest.mark.parametrize("update", [
    None, [], {}, {"finish": False, "expectedRevision": "r"},
    {"section": "unknown", "confirmed": True, "expectedRevision": "r"},
    {"section": [], "confirmed": True, "expectedRevision": "r"},
    {"section": "form", "confirmed": 1, "expectedRevision": "r"},
    {"section": "form", "confirmed": True},
    {"section": "form", "confirmed": True, "expectedRevision": ""},
    {"section": "form", "confirmed": True, "expectedRevision": 3},
    {"section": "form", "confirmed": True, "expectedRevision": "r", "finish": True},
])
def test_invalid_updates_are_rejected_without_writing(table, update):
    assert put(table, update)["statusCode"] == 400
    table.update_item.assert_not_called()


def test_review_cannot_be_combined_with_other_updates(table):
    response = put(table, {
        "section": "form", "confirmed": True,
        "expectedRevision": table.item["setupReview"]["revision"],
    }, isActive=False)
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()


@pytest.mark.parametrize("lifecycle", [
    {"isActive": False}, {"lifecycleState": "closing"}, {"lifecycleState": "inactive"},
    {"stackPrizeState": "closed"}, {"archivedAt": "now"},
    {"deletedAt": "now"}, {"deletionStartedAt": "now"},
])
def test_inactive_or_deleting_events_cannot_change_review(table, lifecycle):
    table.item.update(lifecycle)
    assert confirm(table, "form")["statusCode"] == 409
    table.update_item.assert_not_called()


def test_persistent_mark_unmark_explicit_finish_and_reset(table):
    initial_revision = table.item["setupReview"]["revision"]
    assert finish(table)["statusCode"] == 409
    assert response_body(finish(table))["error"] == "SetupReviewIncomplete"
    for section in reversed(game.SETUP_REVIEW_SECTIONS):
        response = confirm(table, section)
        assert response["statusCode"] == 200
        assert response_body(response)["setupReview"] == table.item["setupReview"]
        assert table.item["setupReview"]["completed"] is False
    assert table.item["setupReview"]["revision"] != initial_revision
    assert table.item["setupReview"]["confirmedSections"] == list(game.SETUP_REVIEW_SECTIONS)
    assert finish(table)["statusCode"] == 200
    assert table.item["setupReview"]["completed"] is True
    assert table.item["isActive"] is True
    assert confirm(table, "form", False)["statusCode"] == 200
    assert table.item["setupReview"]["completed"] is False
    assert "form" not in table.item["setupReview"]["confirmedSections"]
    assert finish(table)["statusCode"] == 409
    assert confirm(table, "form")["statusCode"] == 200
    assert table.item["setupReview"]["completed"] is False
    assert finish(table)["statusCode"] == 200
    kwargs = table.update_item.call_args.kwargs
    assert kwargs["ReturnValues"] == "UPDATED_NEW"
    condition = kwargs["ConditionExpression"]
    for predicate in (
        "attribute_exists(gameId)", "ownerUserId = :owner", "#review.#revision = :expected",
        "attribute_not_exists(archivedAt)", "attribute_not_exists(deletedAt)",
        "attribute_not_exists(deletionStartedAt)", "isActive = :active",
        "lifecycleState = :activeState", "stackPrizeState <> :closing",
    ):
        assert predicate in condition
    assert kwargs["UpdateExpression"] == "SET #review = :review, updatedAt = :now"


def test_stale_revision_does_not_overwrite_newer_progress(table):
    stale = table.item["setupReview"]["revision"]
    assert confirm(table, "appearance")["statusCode"] == 200
    table.update_item.reset_mock()
    response = put(table, {"section": "form", "confirmed": True, "expectedRevision": stale})
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "SetupReviewChanged"
    assert table.item["setupReview"]["confirmedSections"] == ["appearance"]
    table.update_item.assert_not_called()


@pytest.mark.parametrize("race", [
    {"ownerUserId": "other"}, {"isActive": False}, {"deletionStartedAt": "now"},
    {"setupReview": {"revision": "newer"}},
])
def test_conditional_write_rejects_concurrent_changes(table, race):
    previous_read = table.get_item.side_effect

    def read_then_race(**kwargs):
        snapshot = previous_read(**kwargs)
        table.item.update(race)
        return snapshot

    table.get_item.side_effect = read_then_race
    response = confirm(table, "form")
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "SetupReviewChanged"


def test_storage_failures_are_reported_not_confirmed(table):
    table.update_item.side_effect = FakeDynamoError("InternalServerError")
    response = confirm(table, "form")
    assert response["statusCode"] == 500
    assert response_body(response)["error"] == "SetupReviewUpdateFailed"
    assert table.item["setupReview"]["confirmedSections"] == []


def test_finish_does_not_overwrite_a_concurrently_unconfirmed_section(table):
    for section in game.SETUP_REVIEW_SECTIONS:
        assert confirm(table, section)["statusCode"] == 200
    previous_read = table.get_item.side_effect

    def read_then_reset(**kwargs):
        snapshot = previous_read(**kwargs)
        table.item["setupReview"] = game._initial_setup_review()
        return snapshot

    table.get_item.side_effect = read_then_reset
    response = finish(table)
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "SetupReviewChanged"
    assert table.item["setupReview"]["completed"] is False


@pytest.mark.parametrize("progress", [
    None, {}, {"version": 1, "revision": "", "confirmedSections": [], "completed": False},
    {"version": 1, "revision": "r", "confirmedSections": ["form"], "completed": True},
    {"version": 1, "revision": "r", "confirmedSections": ["form", "form"], "completed": False},
])
def test_invalid_persisted_state_is_not_silently_replaced(table, progress):
    table.item["setupReview"] = progress
    response = put(table, {"section": "form", "confirmed": True, "expectedRevision": "r"})
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "InvalidSetupReviewState"
    table.update_item.assert_not_called()
