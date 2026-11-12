"""Archived lists, inactive restoration and scoped, resumable local deletion."""
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from stand_common import game_deletion as deletion, game_lifecycle as lifecycle, stack_prizes as prizes
from stand_common.email_verification import identity_id
from stand_common.utils import GAME_MUTABLE_CONDITION, set_game_type_blob
from tests.email_fake import FakeDynamoError
from tests.stack_prize_fake import Condition
from tests.test_game_players import game, event, response_body, _sdk_transaction_payload
from tests.test_stack_prize_rules import rig, meta, player


class Media:
    def __init__(self):
        self.keys = {
            "players/ABC123/1/profile.png",
            "games/owner/ABC123/ranking/prize.png",
            "games/staged/owner/ABC123/ranking/staged.png",
            "players/OTHER/1/profile.png", "games/owner/OTHER/prize.png", "users/owner/logos/logo.png",
            "infocards/ABC123/source.pdf", "infocards/OTHER/source.pdf",
        }
        self.fail = False

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken="", **_):
        assert Bucket == ("infocards" if Prefix.startswith("infocards/") else "media")
        keys = [key for key in sorted(self.keys) if key.startswith(Prefix) and key > ContinuationToken]
        page = {"Contents": [{"Key": key} for key in keys[:2]], "IsTruncated": len(keys) > 2}
        if len(keys) > 2:
            page["NextContinuationToken"] = keys[1]
        return page

    def delete_objects(self, Bucket, Delete):
        if self.fail:
            return {"Errors": [{"Code": "AccessDenied", "Key": Delete["Objects"][0]["Key"]}]}
        self.keys.difference_update(row["Key"] for row in Delete["Objects"])
        return {}


@pytest.fixture
def archived(rig, monkeypatch):
    monkeypatch.setattr(deletion, "Attr", Condition)
    monkeypatch.setattr(game, "Key", Condition)
    monkeypatch.setattr(game, "PLAYER_TOKEN_SECRET", "test-email-verification-secret-at-least-32-bytes")
    for key, value in {
        "EMAIL_VERIFICATION_TABLE": "verification", "KIVLY_REGISTRATION_TABLE": "registration",
        "CLIENT_INDEX_TABLE": "client-index",
        "USER_MEDIA_BUCKET": "media", "INFOCARDS_BUCKET": "infocards",
    }.items():
        monkeypatch.setenv(key, value)
    saved = meta(rig)
    saved.update(gameName="Event", isActive=False, lifecycleState="inactive", archivedAt="2026-01-01T00:00:00Z")
    rig.Table("games").put_item(Item=saved)
    return rig


def invoke(dynamo, method, body=None, query=None, owner="owner", media=None):
    with patch.object(game, "games_table", dynamo.Table("games")), patch.object(game, "dynamodb", dynamo), \
            patch.object(game, "prize_s3", media or Media()):
        return game.lambda_handler(event(method, "/v1/game", body=body, query=query, owner=owner), None)


def award(dynamo, **changes):
    row = {
        "gameId": "ABC123", "id": "award_1", "playerId": 1,
        "prize": {"source": "stand", "name": "Medal"}, "state": "delivered",
        **changes,
    }
    dynamo.Table("prizes").put_item(Item=row)
    return row


def test_archived_filter_is_explicit_and_never_lists_deleted_codes(archived):
    archived.Table("games").put_item(Item={
        "gameId": "OTHER", "ownerUserId": "owner", "gameName": "Current", "gameType": "STACK",
    })
    archived.Table("games").put_item(Item={"gameId": "DELETED", "deletedAt": "2026-01-01", "isActive": False})
    assert [row["gameId"] for row in response_body(invoke(archived, "GET"))["games"]] == ["OTHER"]
    response = invoke(archived, "GET", query={"archived": "true"})
    assert response["statusCode"] == 200
    assert [row["gameId"] for row in response_body(response)["games"]] == ["ABC123"]
    assert response_body(response)["games"][0]["archivedAt"]
    assert invoke(archived, "GET", query={"archived": "yes"})["statusCode"] == 400


def test_restore_preserves_everything_but_the_archive_marker(archived):
    before = deepcopy(meta(archived))
    participant = player(archived)
    award(archived)
    original_rows = prizes.records(archived, "ABC123")
    result = invoke(archived, "PUT", {"gameId": "ABC123", "archived": False})
    assert result["statusCode"] == 200
    restored = response_body(result)["game"]
    assert restored["isActive"] is False and restored["lifecycleState"] == "inactive"
    assert "archivedAt" not in restored
    for key, value in before.items():
        if key != "archivedAt":
            assert restored[key] == value
    assert player(archived) == participant
    assert prizes.records(archived, "ABC123") == original_rows
    assert response_body(invoke(archived, "GET"))["count"] == 1


@pytest.mark.parametrize("body", [
    {"gameId": "ABC123", "archived": True},
    {"gameId": "ABC123", "archived": False, "isActive": True},
    {"gameId": "ABC123", "archived": "false"},
])
def test_restore_requires_a_separate_strict_action(archived, body):
    assert invoke(archived, "PUT", body)["statusCode"] == 400
    assert meta(archived)["archivedAt"]


@pytest.mark.parametrize("owner,status", [("", 401), ("someone-else", 403)])
def test_restore_requires_owner(archived, owner, status):
    assert invoke(archived, "PUT", {"gameId": "ABC123", "archived": False}, owner=owner)["statusCode"] == status
    assert meta(archived)["archivedAt"]


def test_restore_and_deletion_cannot_race(archived):
    stale = meta(archived)
    prizes.transact(archived, [prizes.game_update(
        stale, ", deletionStartedAt = :now", {":now": "2026-02-01T00:00:00Z"})])
    with pytest.raises(prizes.PrizeError):
        lifecycle.unarchive(archived, stale)
    result = invoke(archived, "PUT", {"gameId": "ABC123", "archived": False})
    assert response_body(result)["error"] == "GameDeletionInProgress"
    listed = response_body(invoke(archived, "GET", query={"archived": "true"}))["games"][0]
    assert listed["deletionStartedAt"]


@pytest.mark.parametrize("changes", [
    {"state": "available"},
    {"state": "delivered", "work": "pending"},
    {"prize": {"source": "kivly"}, "state": "queued"},
    {"prize": {"source": "kivly"}, "state": "connection_required"},
    {"prize": {"source": "kivly"}, "state": "reconciliation_required"},
    {"prize": {"source": "kivly"}, "state": "confirmed", "deliveryChannel": "email", "emailState": "sending"},
    {"prize": {"source": "kivly"}, "state": "confirmed", "deliveryChannel": "email", "emailState": "failed"},
])
def test_pending_prizes_do_not_block_permanent_deletion(archived, changes):
    award(archived, **changes)
    media = Media()
    response = invoke(archived, "DELETE", {"gameId": "ABC123", "permanent": True, "confirmation": "Event"}, media=media)
    assert response["statusCode"] == 200
    assert prizes.records(archived, "ABC123") == []
    assert prizes.players(archived, "ABC123") == []
    assert media.keys == {"players/OTHER/1/profile.png", "games/owner/OTHER/prize.png", "users/owner/logos/logo.png", "infocards/OTHER/source.pdf"}


@pytest.mark.parametrize("row", [
    {"id": "registration-request", "gameId": "ABC123", "kind": "registration", "state": "pending"},
    {"id": "registration-request", "gameId": "ABC123", "kind": "registration", "state": "review_required"},
    {"id": "registration-request", "gameId": "ABC123", "kind": "registration", "state": "failed", "createStarted": True},
    {"id": "reward:ABC123:1", "state": "pending"},
])
def test_pending_or_uncertain_registrations_do_not_block_deletion(archived, row):
    archived.Table("registration").put_item(Item=row)
    response = invoke(archived, "DELETE", {"gameId": "ABC123", "permanent": True, "confirmation": "Event"})
    assert response["statusCode"] == 200
    assert archived.Table("registration").rows == {}
    assert prizes.players(archived, "ABC123") == []


def test_permanent_deletion_cleans_only_this_events_local_data(archived):
    p = player(archived)
    p["email"] = "player@example.test"
    archived.Table("players").put_item(Item=p)
    mapping = identity_id(game.PLAYER_TOKEN_SECRET, "ABC123", p["email"])
    archived.Table("verification").put_item(Item={"id": mapping, "playerId": 1})
    archived.Table("verification").put_item(Item={"id": "challenge:a", "gameId": "ABC123"})
    archived.Table("verification").put_item(Item={"id": "rate:recipient", "count": 1})
    archived.Table("verification").put_item(Item={"id": "challenge:other", "gameId": "OTHER"})
    archived.Table("registration").put_item(Item={"id": "request", "gameId": "ABC123", "state": "confirmed"})
    archived.Table("registration").put_item(Item={"id": "customer:shared", "state": "confirmed", "customerId": "external"})
    archived.Table("registration").put_item(Item={"id": "request:other", "gameId": "OTHER", "state": "pending"})
    archived.Table("players").put_item(Item={"gameId": "OTHER", "playerId": 1})
    archived.Table("prizes").put_item(Item={"gameId": "OTHER", "id": "award_other", "state": "available"})
    award(archived)
    media = Media()
    response = invoke(archived, "DELETE", {"gameId": "ABC123", "permanent": True, "confirmation": "Event"}, media=media)
    assert response["statusCode"] == 200
    assert response_body(response)["message"] == "Game permanently deleted successfully"
    assert prizes.players(archived, "ABC123") == [] and prizes.records(archived, "ABC123") == []
    assert meta(archived) == {"gameId": "ABC123", "deletedAt": meta(archived)["deletedAt"], "isActive": False}
    assert [row["id"] for row in archived.Table("verification").rows.values()] == ["rate:recipient", "challenge:other"]
    assert [row["id"] for row in archived.Table("registration").rows.values()] == ["customer:shared", "request:other"]
    assert media.keys == {"players/OTHER/1/profile.png", "games/owner/OTHER/prize.png", "users/owner/logos/logo.png", "infocards/OTHER/source.pdf"}
    assert prizes.players(archived, "OTHER") and prizes.records(archived, "OTHER")
    assert response_body(invoke(archived, "GET", query={"archived": "true"}))["count"] == 0
    assert invoke(archived, "GET", query={"gameId": "ABC123"})["statusCode"] == 404


@pytest.mark.parametrize("channel", ["wallet", "email"])
def test_settled_external_awards_do_not_require_external_collection(archived, channel):
    award(archived, prize={"source": "kivly", "id": "legacy_kivly"}, state="confirmed",
          deliveryChannel=channel, **({"emailState": "accepted"} if channel == "email" else {}))
    archived.Table("registration").put_item(Item={"id": "reward:ABC123:1", "state": "pending"})
    assert invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    })["statusCode"] == 200


def test_permanent_delete_preserves_inventory_and_shared_images(archived, monkeypatch):
    from stand_common import prize_inventory
    monkeypatch.setenv("PRIZE_INVENTORY_TABLE", "inventory")
    monkeypatch.setattr(prize_inventory, "ClientError", FakeDynamoError)
    monkeypatch.setattr(prize_inventory, "Key", Condition)
    image = "games/owner/ABC123/ranking/prize.png"
    archived.Table("users").put_item(Item={
        "userId": "owner", "sharedPrizeCatalog": [{"id": "shared", "name": "Medal", "imageKey": image}],
    })
    award(archived, ownerUserId="owner", condition="validation",
          prize={"source": "stand", "name": "Medal", "sharedPrizeId": "shared", "imageKey": image})
    media = Media()
    response = invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    }, media=media)
    assert response["statusCode"] == 200
    assert prizes.records(archived, "ABC123") == []
    stock = prize_inventory.balance(archived, "owner", "shared")
    assert stock["historicalAssigned"] == stock["historicalRedeemed"] == 1
    movement = prize_inventory.page(archived, "owner", "MOVE#shared#")["items"][0]
    assert movement["gameId"] == "ABC123" and movement["gameName"] == "Event"
    assert image in media.keys


def test_unresolved_history_blocks_deletion_instead_of_losing_counts(archived, monkeypatch):
    from stand_common import prize_inventory
    monkeypatch.setenv("PRIZE_INVENTORY_TABLE", "inventory")
    monkeypatch.setattr(prize_inventory, "ClientError", FakeDynamoError)
    award(archived, ownerUserId="owner", condition="validation")
    archived.Table("users").put_item(Item={"userId": "owner", "sharedPrizeCatalog": []})
    response = invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    })
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "InventoryHistoryImportRequired"
    assert prizes.records(archived, "ABC123")
    assert not meta(archived).get("deletionStartedAt")


def test_partial_media_failure_is_visible_retryable_and_cannot_be_restored(archived):
    media = Media()
    media.fail = True
    body = {"gameId": "ABC123", "permanent": True, "confirmation": "Event"}
    response = invoke(archived, "DELETE", body, media=media)
    assert response["statusCode"] == 503
    assert response_body(response)["error"] == "GameMediaDeletionFailed"
    assert meta(archived)["deletionStartedAt"] and player(archived)
    assert response_body(invoke(archived, "PUT", {"gameId": "ABC123", "archived": False}))["error"] == "GameDeletionInProgress"
    media.fail = False
    assert invoke(archived, "DELETE", body, media=media)["statusCode"] == 200


def test_partial_row_failure_keeps_metadata_and_allows_safe_retry(archived):
    award(archived)
    table = archived.Table("prizes")
    with patch.object(table, "delete_item", side_effect=FakeDynamoError("InternalServerError")):
        response = invoke(archived, "DELETE", {"gameId": "ABC123", "permanent": True, "confirmation": "Event"})
    assert response["statusCode"] == 500
    assert meta(archived)["deletionStartedAt"] and prizes.records(archived, "ABC123")
    assert invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    })["statusCode"] == 200


@pytest.mark.parametrize("body,status", [
    ({"gameId": "ABC123", "permanent": True}, 400),
    ({"gameId": "ABC123", "permanent": True, "confirmation": "Wrong"}, 400),
    ({"gameId": "ABC123", "permanent": "true", "confirmation": "Event"}, 400),
    ({"gameId": "ABC123", "permanent": False, "confirmation": "Event"}, 400),
])
def test_irreversible_deletion_needs_explicit_correct_confirmation(archived, body, status):
    assert invoke(archived, "DELETE", body)["statusCode"] == status
    assert not meta(archived).get("deletionStartedAt")


def test_permanent_deletion_requires_archive_and_owner(archived):
    body = {"gameId": "ABC123", "permanent": True, "confirmation": "Event"}
    assert invoke(archived, "DELETE", body, owner="someone-else")["statusCode"] == 404
    assert invoke(archived, "DELETE", body, owner="")["statusCode"] == 401
    lifecycle.unarchive(archived, meta(archived))
    assert response_body(invoke(archived, "DELETE", body))["error"] == "GameMustBeArchived"


def test_auxiliary_scans_read_later_pages_even_when_first_filtered_page_is_empty(archived):
    table = archived.Table("registration")
    with patch.object(table, "scan", side_effect=[
        {"Items": [], "LastEvaluatedKey": {"id": "unrelated"}},
        {"Items": [{"id": "later", "gameId": "ABC123", "state": "pending"}]},
    ]) as scan:
        result = invoke(archived, "DELETE", {"gameId": "ABC123", "permanent": True, "confirmation": "Event"})
    assert result["statusCode"] == 200
    assert scan.call_args_list[1].kwargs["ExclusiveStartKey"] == {"id": "unrelated"}


def test_bounded_cleanup_makes_progress_and_finishes_on_explicit_retry(archived):
    for number in range(2, 602):
        archived.Table("players").put_item(Item={"gameId": "ABC123", "playerId": number})
    body = {"gameId": "ABC123", "permanent": True, "confirmation": "Event"}
    response = invoke(archived, "DELETE", body)
    assert response["statusCode"] == 409 and response_body(response)["error"] == "GameDeletionInProgress"
    assert 0 < len(prizes.players(archived, "ABC123")) < 601
    assert meta(archived)["deletionStartedAt"]
    assert invoke(archived, "DELETE", body)["statusCode"] == 200
    assert prizes.players(archived, "ABC123") == []


def test_media_cleanup_follows_continuation_tokens_after_deleting_previous_pages(archived):
    media = Media()
    media.keys.update(f"players/ABC123/1/photo-{number}.png" for number in range(8))
    assert invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    }, media=media)["statusCode"] == 200
    assert media.keys == {"players/OTHER/1/profile.png", "games/owner/OTHER/prize.png", "users/owner/logos/logo.png", "infocards/OTHER/source.pdf"}


def test_preflight_deadline_never_starts_an_unchecked_deletion(archived):
    with patch.object(deletion.time, "monotonic", side_effect=[0, 19]):
        response = invoke(archived, "DELETE", {
            "gameId": "ABC123", "permanent": True, "confirmation": "Event",
        })
    assert response_body(response)["error"] == "GameDeletionCheckIncomplete"
    assert not meta(archived).get("deletionStartedAt") and player(archived)


def test_missing_identity_configuration_fails_before_deletion_starts(archived):
    participant = player(archived)
    participant["email"] = "player@example.test"
    archived.Table("players").put_item(Item=participant)
    with patch.object(game, "PLAYER_TOKEN_SECRET", "invalid"):
        response = invoke(archived, "DELETE", {
            "gameId": "ABC123", "permanent": True, "confirmation": "Event",
        })
    assert response["statusCode"] == 503
    assert response_body(response)["error"] == "GameDeletionUnavailable"
    assert not meta(archived).get("deletionStartedAt") and player(archived)


def test_pending_legacy_write_is_deleted_even_if_reward_does_not_match(archived):
    award(archived, prize={"source": "kivly", "id": "legacy_kivly", "rewardId": "another"}, state="confirmed")
    archived.Table("registration").put_item(Item={"id": "reward:ABC123:1", "state": "pending", "rewardId": "original"})
    response = invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    })
    assert response["statusCode"] == 200
    assert archived.Table("registration").rows == {}
    assert prizes.records(archived, "ABC123") == []


def test_pending_coordinator_without_awards_is_deleted(archived):
    archived.Table("prizes").put_item(Item={
        "gameId": "ABC123", "id": "lifecycle_cycle", "kind": "lifecycle", "work": "pending",
    })
    response = invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    })
    assert response["statusCode"] == 200
    assert prizes.records(archived, "ABC123") == []


def test_type_writes_keep_owner_and_archive_guards_on_both_steps():
    table = MagicMock()
    set_game_type_blob(table, "ABC123", "INFOCARDS", {"cards": []},
                       condition_expression="ownerUserId = :owner", condition_values={":owner": "owner"})
    assert table.update_item.call_count == 2
    for call in table.update_item.call_args_list:
        assert call.kwargs["ConditionExpression"] == GAME_MUTABLE_CONDITION + " AND (ownerUserId = :owner)"
        assert call.kwargs["ExpressionAttributeValues"][":owner"] == "owner"


def test_type_write_surfaces_a_midflight_archive_instead_of_reporting_success():
    table = MagicMock()
    table.update_item.side_effect = [{}, FakeDynamoError("ConditionalCheckFailedException")]
    with pytest.raises(FakeDynamoError):
        set_game_type_blob(table, "ABC123", "INFOCARDS", {"cards": []})
    assert table.update_item.call_args.kwargs["ConditionExpression"] == GAME_MUTABLE_CONDITION


def test_final_tombstone_is_serialized_by_the_real_sdk_without_network_calls(archived):
    assert invoke(archived, "DELETE", {
        "gameId": "ABC123", "permanent": True, "confirmation": "Event",
    })["statusCode"] == 200
    wire = _sdk_transaction_payload(archived.last_transaction)
    assert wire[0]["Put"]["Item"]["gameId"] == {"S": "ABC123"}
    assert wire[0]["Put"]["Item"]["isActive"] == {"BOOL": False}
    assert wire[0]["Put"]["ExpressionAttributeValues"][":revision"] == {"N": "1"}
    assert set(wire[0]["Put"]["Item"]) == {"gameId", "deletedAt", "isActive"}
