import json
from copy import deepcopy
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from stand_common import prize_inventory as inventory, stack_prizes as prizes
from stand_common import game_lifecycle, kivly_registration, shared_prizes
from tests.email_fake import FakeDynamoError
from tests.stack_prize_fake import PrizeDynamo, Condition


@pytest.fixture
def db(monkeypatch):
    for key, name in {
        "PRIZE_INVENTORY_TABLE": "inventory", "STACK_PRIZES_TABLE": "awards",
        "GAMES_TABLE": "games", "USERS_TABLE": "users", "GAMEPLAYER_TABLE": "players",
    }.items():
        monkeypatch.setenv(key, name)
    monkeypatch.setattr(inventory, "ClientError", FakeDynamoError)
    monkeypatch.setattr(prizes, "ClientError", FakeDynamoError)
    monkeypatch.setattr(inventory, "Key", Condition)
    monkeypatch.setattr(prizes, "Key", Condition)
    monkeypatch.setattr(shared_prizes, "Key", Condition)
    monkeypatch.setattr(kivly_registration, "connection", lambda *_: None)
    monkeypatch.setattr(prizes, "enqueue", lambda *_: True)
    dynamo = PrizeDynamo()
    dynamo.Table("users").put_item(Item={
        "userId": "owner", "sharedPrizeCatalog": [{"id": "gift", "name": "Gift", "imageKey": None}],
    })
    dynamo.Table("games").put_item(Item={
        "gameId": "ABC123", "ownerUserId": "owner", "gameName": "First", "isActive": True,
    })
    return dynamo


def adjustment(quantity, action="initialize", request_id="first", revision=0):
    return {"action": action, "prizeId": "gift", "quantity": quantity, "reason": "Physical inventory count",
            "requestId": request_id, "expectedRevision": revision}


def reserve(db, owner, pid, payload):
    meta = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    catalog = meta.setdefault("stackPrizeProgram", {"version": 3, "catalog": [], "rules": []})["catalog"]
    catalog.append({"id": pid, "name": "Gift", "source": "stand",
                    "sharedPrizeId": pid, "stockTotal": payload["quantity"]})
    db.Table("games").put_item(Item=meta)

@pytest.mark.parametrize("blocked", [None, "linked", "initialized", "history", "movement"])
def test_delete_unused_kivly_reward_is_scoped_and_preserves_used_prizes(db, monkeypatch, blocked):
    from stand_common import utils
    from stand_common.kivly import KivlyError
    monkeypatch.setattr(utils, "_get_claims", lambda _: {"sub": "owner"})
    conn = {"locationId": "club", "ownerId": "business"}
    monkeypatch.setattr(kivly_registration, "connection", lambda *_: conn)
    client = MagicMock()
    client.list_rewards.return_value = [{"id": "coffee", "name": "Coffee"}]
    monkeypatch.setattr(kivly_registration, "_client", lambda *_: client)
    prize = {"source": "kivly", "locationId": "club", "rewardId": "coffee", "name": "Coffee"}
    pid = inventory.prize_id(prize)
    row = inventory.balance(db, "owner", pid)
    if blocked == "linked":
        saved = configured_reservation(db, "ABC123", None, active=False)
        saved["stackPrizeProgram"]["catalog"] = [{**prize, "id": "coffee", "stockTotal": None}]
        db.Table("games").put_item(Item=saved)
    if blocked == "initialized":
        row["initialized"] = True
    if blocked == "history":
        row["historicalAssigned"] = 1
    db.Table("inventory").put_item(Item=row)
    if blocked == "movement":
        db.Table("inventory").put_item(Item={
            "ownerId": "owner", "key": "MOVE#" + pid + "#old",
            "kind": "assignment", "quantity": 1, "prizeId": pid,
        })
    event = {"body": json.dumps({"action": "delete", "prizeId": pid})}
    result = inventory.handle(event, "POST", db)
    if blocked:
        assert result["statusCode"] == 409, result
        client.delete_reward.assert_not_called()
        return
    assert result["statusCode"] == 200, result
    client.delete_reward.assert_called_once_with("club", "business", "coffee")
    client.list_rewards.return_value = []
    assert pid not in {p["id"] for p in inventory_response(db, monkeypatch)["catalog"]}
    # An ambiguous external failure remains explicit and is not automatically repeated.
    client.list_rewards.return_value = [{"id": "coffee", "name": "Coffee"}]
    client.delete_reward.side_effect = KivlyError("KivlyRewardDeletionUncertain")
    result = inventory.handle(event, "POST", db)
    assert result["statusCode"] == 503
    assert json.loads(result["body"])["error"] == "KivlyRewardDeletionUncertain"
    assert client.delete_reward.call_count == 2


def activate(db):
    db.Table("inventory").put_item(Item={"ownerId": "owner", "key": "CONTROL", "mode": "active"})


def award(identity="award_one", game_id="ABC123", source="stand"):
    return {
        "gameId": game_id, "id": identity, "ownerUserId": "owner", "playerId": 1,
        "condition": "completed", "state": "available" if source == "stand" else "queued",
        "createdAt": "2026-11-04T12:00:00Z",
        "prize": {"id": "local-gift", "sharedPrizeId": "gift", "source": source, "name": "Gift"},
    }


def concede(db, row):
    prizes.transact(db, [prizes.put(row)])






def test_award_consumes_once_and_redemption_does_not_consume_again(db):
    reserve(db, "owner", "gift", adjustment(3))
    activate(db)
    row = award()
    concede(db, row)
    saved = inventory.balance(db, "owner", "gift")
    assert (saved["available"], saved["assigned"], saved["redeemed"]) == (0, 1, 0)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        concede(db, deepcopy(row))
    prizes.redeem(db, "ABC123", 1, row["id"])
    saved = inventory.balance(db, "owner", "gift")
    assert (saved["available"], saved["assigned"], saved["redeemed"]) == (0, 1, 1)
    with pytest.raises(prizes.PrizeError, match="AlreadyDelivered"):
        prizes.redeem(db, "ABC123", 1, row["id"])
    ledger = inventory.page(db, "owner", "MOVE#gift#")["items"]
    assigned = next(r for r in ledger if r["kind"] == "assignment")
    assert assigned["units"][row["id"]]["state"] == "delivered"


def test_unreserved_event_cannot_borrow_another_events_units(db):
    reserve(db, "owner", "gift", adjustment(1))
    activate(db)
    db.Table("games").put_item(Item={"gameId": "DEF456", "ownerUserId": "owner"})
    concede(db, award("award_first"))
    with pytest.raises(prizes.PrizeError, match="EventReservationRequired"):
        concede(db, award("award_second", "DEF456"))
    assert inventory.balance(db, "owner", "gift")["available"] == 0
    assert "Item" not in db.Table("awards").get_item(Key={"gameId": "DEF456", "id": "award_second"})


def test_competing_snapshots_cannot_overassign(db):
    reserve(db, "owner", "gift", adjustment(1))
    activate(db)
    meta = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    first, second = award("award_first"), award("award_second")
    one = inventory.award_operations(db, meta, [first])
    two = inventory.award_operations(db, meta, [second])
    inventory.write(db, [prizes.put(first), *one])
    with pytest.raises(inventory.InventoryError, match="InventoryChanged"):
        inventory.write(db, [prizes.put(second), *two])
    assert inventory.balance(db, "owner", "gift")["available"] == 0
    assert "Item" not in db.Table("awards").get_item(Key={"gameId": "ABC123", "id": second["id"]})


def test_missing_reservation_rejects_award_with_explicit_reason(db):
    activate(db)
    with pytest.raises(prizes.PrizeError, match="EventReservationRequired"):
        concede(db, award())
    available = inventory.capacity(db, "owner", [award()["prize"]])
    assert available["local-gift"]["reason"] == "EventReservationRequired"


def test_legacy_assignments_and_import_do_not_debit_manual_available(db):
    reserve(db, "owner", "gift", adjustment(10))
    concede(db, award("award_live_legacy"))
    assert inventory.balance(db, "owner", "gift")["available"] == 0
    assert inventory.balance(db, "owner", "gift")["assigned"] == 1
    old = award("award_old")
    old["state"] = "delivered"
    db.Table("awards").put_item(Item=old)
    meta = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    inventory.import_award(db, meta, old)
    inventory.import_award(db, meta, old)
    stock = inventory.balance(db, "owner", "gift")
    assert (stock["available"], stock["assigned"], stock["historicalAssigned"], stock["historicalRedeemed"]) == (0, 1, 1, 1)
    historical = next(m for m in inventory.page(db, "owner", "MOVE#gift#")["items"] if old["id"] in m.get("units", {}))
    assert "deliveredAt" not in historical["units"][old["id"]]






def test_owner_isolation_for_balance_and_cursor(db):
    reserve(db, "owner", "gift", adjustment(4))
    assert inventory.balance(db, "someone-else", "gift")["initialized"] is False
    assert inventory.page(db, "someone-else", "MOVE#gift#")["items"] == []
    import base64
    cursor = base64.urlsafe_b64encode(json.dumps({"ownerId": "owner", "key": "MOVE#gift#row"}).encode()).decode()
    with pytest.raises(inventory.InventoryError, match="InvalidInventoryCursor"):
        inventory.page(db, "someone-else", "MOVE#gift#", cursor)


def test_multiple_local_ids_share_one_inventory_write(db):
    reserve(db, "owner", "gift", adjustment(3))
    activate(db)
    first, second = award("award_first"), award("award_second")
    second["prize"]["id"] = "another-local-id"
    prizes.transact(db, [prizes.put(first), prizes.put(second)])
    assert inventory.balance(db, "owner", "gift")["assigned"] == 2
    assert first["inventoryMovementKey"] == second["inventoryMovementKey"]
    assert len(db.last_transaction) == 4  # two awards, one balance, one movement


def test_kivly_identity_is_location_and_reward_scoped_not_name(db):
    assert inventory.prize_id({"source": "kivly", "locationId": "one", "rewardId": "reward", "name": "Gift"}) != inventory.prize_id(
        {"source": "kivly", "locationId": "two", "rewardId": "reward", "name": "Gift"})
    assert inventory.prize_id({"source": "kivly", "locationId": "one", "rewardId": "reward", "name": "Old"}) == inventory.prize_id(
        {"source": "kivly", "locationId": "one", "rewardId": "reward", "name": "New"})


def test_kivly_delivery_failure_never_refunds_stock(db):
    row = award(source="kivly")
    row["prize"].update(locationId="loc", rewardId="coffee")
    pid = inventory.prize_id(row["prize"])
    reserve(db, "owner", pid, {**adjustment(2), "prizeId": pid})
    meta = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    meta["stackPrizeProgram"]["catalog"] = [{**row["prize"], "stockTotal": 2}]
    db.Table("games").put_item(Item=meta)
    activate(db)
    concede(db, row)
    inventory.update_delivery(db, row, "reconciliation_required", {
        "Key": {"gameId": row["gameId"], "id": row["id"]},
        "UpdateExpression": "SET #s = :state",
        "ExpressionAttributeNames": {"#s": "state"},
        "ExpressionAttributeValues": {":state": "reconciliation_required"},
    })
    stock = inventory.balance(db, "owner", pid)
    assert (stock["available"], stock["assigned"], stock["redeemed"]) == (0, 1, 0)
    movement = next(m for m in inventory.page(db, "owner", "MOVE#" + pid + "#")["items"] if m["kind"] == "assignment")
    assert movement["units"][row["id"]]["state"] == "reconciliation_required"


def test_native_identity_survives_catalog_rename():
    assert game_lifecycle.native_identity("validation", {"sharedPrizeId": "gift", "name": "Old"}) == game_lifecycle.native_identity(
        "validation", {"sharedPrizeId": "gift", "name": "New", "imageKey": "different.png"})


def test_balances_and_history_survive_removing_event_awards(db):
    reserve(db, "owner", "gift", adjustment(2))
    activate(db)
    row = award()
    concede(db, row)
    db.Table("awards").delete_item(Key={"gameId": row["gameId"], "id": row["id"]})
    db.Table("games").delete_item(Key={"gameId": row["gameId"]})
    assert inventory.balance(db, "owner", "gift")["assigned"] == 1
    history = inventory.page(db, "owner", "MOVE#gift#")["items"]
    assert next(m for m in history if m["kind"] == "assignment")["gameName"] == "First"


def test_worst_case_eighty_units_fits_transaction_limit(db):
    activate(db)
    rows = []
    for index in range(8):
        pid = f"prize-{index}"
        reserve(db, "owner", pid, {**adjustment(10, request_id=pid), "prizeId": pid})
        for unit in range(10):
            row = award(f"award_{index}_{unit}")
            row["prize"]["sharedPrizeId"] = pid
            rows.append(row)
    prizes.transact(db, [
        {"Put": {"TableName": "awards", "Item": {"gameId": "ABC123", "id": "allocation_one"}}},
        prizes.game_update({"gameId": "ABC123"}, ""), *[prizes.put(row) for row in rows],
    ])
    assert len(db.last_transaction) == 98


def legacy_game(db):
    meta = {"gameId": "ABC123", "ownerUserId": "owner", "gameType": "EMPAREJA2", "isActive": True,
            "prizeCatalog": {"validation": {"sharedPrizeId": "gift", "name": "Gift", "limit": None}}}
    db.Table("games").put_item(Item=meta)
    return meta


def test_import_is_paginated_resumable_and_rejects_skipped_cursors(db):
    meta = legacy_game(db)
    for index in range(25):
        db.Table("awards").put_item(Item=award("award_" + str(index).zfill(3)))
    first = inventory.import_page(db, "owner", meta)
    assert first["cursor"].startswith("awards:")
    with pytest.raises(inventory.InventoryError, match="InvalidInventoryCursor"):
        inventory.import_page(db, "owner", meta, "awards:award_024")
    second = inventory.import_page(db, "owner", meta, first["cursor"])
    assert second["cursor"] == "players:"
    assert inventory.import_page(db, "owner", meta, second["cursor"])["cursor"] is None
    assert inventory.balance(db, "owner", "gift")["historicalAssigned"] == 25


def test_import_page_retry_after_lost_response_returns_same_progress(db):
    meta = legacy_game(db)
    for index in range(25):
        db.Table("awards").put_item(Item=award("award_" + str(index).zfill(3)))
    first = inventory.import_page(db, "owner", meta)
    second = inventory.import_page(db, "owner", meta, first["cursor"])
    revision = db.Table("inventory").get_item(Key={"ownerId": "owner", "key": "IMPORT#ABC123"})["Item"]["revision"]
    assert inventory.import_page(db, "owner", meta, first["cursor"]) == second
    assert db.Table("inventory").get_item(Key={"ownerId": "owner", "key": "IMPORT#ABC123"})["Item"]["revision"] == revision
    final = inventory.import_page(db, "owner", meta, second["cursor"])
    assert final["cursor"] is None
    assert inventory.import_page(db, "owner", meta, second["cursor"]) == final
    assert inventory.balance(db, "owner", "gift")["historicalAssigned"] == 25
    assert inventory.import_page(db, "owner", meta)["cursor"].startswith("awards:")
    assert inventory.balance(db, "owner", "gift")["historicalAssigned"] == 25


def test_pre_outbox_player_flags_are_imported_and_old_qr_redeems_once(db):
    meta = legacy_game(db)
    player = {"gameId": "ABC123", "playerId": 1, "validated": True, "type": {"EMPAREJA2": {}}}
    db.Table("players").put_item(Item=player)
    reserve(db, "owner", "gift", adjustment(8))
    first = inventory.import_page(db, "owner", meta)
    assert inventory.import_page(db, "owner", meta, first["cursor"])["cursor"] is None
    activate(db)
    claim = inventory.legacy_claim(db, meta, player, "validation")
    prizes.redeem(db, meta["gameId"], 1, claim["id"])
    with pytest.raises(prizes.PrizeError, match="AlreadyDelivered"):
        prizes.redeem(db, meta["gameId"], 1, claim["id"])
    balance = inventory.balance(db, "owner", "gift")
    assert (balance["available"], balance["historicalAssigned"], balance["historicalRedeemed"]) == (0, 1, 1)


def test_post_cutover_legacy_flags_cannot_mint_unassigned_prizes(db):
    meta = legacy_game(db)
    activate(db)
    player = {"gameId": "ABC123", "playerId": 1, "validated": True, "type": {"EMPAREJA2": {}}}
    assert inventory.legacy_claim(db, meta, player, "validation") is None
    assert not prizes.records(db, meta["gameId"])
    inventory.project_legacy_player(db, meta, player)
    assert inventory.legacy_claim(db, meta, player, "validation") is not None


def test_legacy_redeemed_flag_is_not_counted_as_a_new_redemption(db):
    meta = legacy_game(db)
    player = {"gameId": "ABC123", "playerId": 1, "validated": True,
              "type": {"EMPAREJA2": {"validationPrizeDelivered": True}}}
    inventory.project_legacy_player(db, meta, player)
    inventory.project_legacy_player(db, meta, player)
    stock = inventory.balance(db, "owner", "gift")
    assert stock["historicalAssigned"] == stock["historicalRedeemed"] == 1
    assert stock["initialized"] is False
    claim = inventory.legacy_claim(db, meta, player, "validation")
    with pytest.raises(prizes.PrizeError, match="AlreadyDelivered"):
        prizes.redeem(db, meta["gameId"], 1, claim["id"])


def test_movement_pagination_has_no_duplicates_and_is_scope_bound(db):
    reserve(db, "owner", "gift", adjustment(4))
    for index in range(4):
        concede(db, award(f"award_{index}"))
    first = inventory.page(db, "owner", "MOVE#gift#", limit=2)
    second = inventory.page(db, "owner", "MOVE#gift#", first["cursor"], limit=2)
    assert first["cursor"] and second["cursor"] is None
    assert len({m["key"] for m in first["items"] + second["items"]}) == 4
    with pytest.raises(inventory.InventoryError, match="InvalidInventoryCursor"):
        inventory.page(db, "owner", "MOVE#different#", first["cursor"])


def test_native_denial_is_visible_and_event_cap_is_separate(db):
    meta = legacy_game(db)
    player = {"playerId": 1, "validated": True, "type": {"EMPAREJA2": {}}}
    activate(db)
    assert inventory.native_unavailable(db, meta, player) == [{"condition": "validation", "reason": "EventReservationRequired"}]
    meta["prizeCatalog"]["validation"]["limit"] = 0
    assert inventory.native_unavailable(db, meta, player) == [{"condition": "validation", "reason": "EventLimitReached"}]
    assert inventory.balance(db, "owner", "gift")["available"] == 0




def configured_reservation(db, game_id, quantity, active=True):
    program = stack_program(quantity)
    meta = {"gameId": game_id, "gameName": game_id, "ownerUserId": "owner", "gameType": "STACK",
            "isActive": active, "stackPrizeProgram": program}
    db.Table("games").put_item(Item=meta)
    return meta


def inventory_response(db, monkeypatch):
    from stand_common import utils
    monkeypatch.setattr(utils, "_get_claims", lambda _: {"sub": "owner"})
    response = inventory.handle({}, "GET", db)
    assert response["statusCode"] == 200, response
    return json.loads(response["body"])


@pytest.mark.parametrize("active_mode", [False, True])
def test_exclusive_reservations_work_without_manual_initialization(db, monkeypatch, active_mode):
    if active_mode:
        activate(db)
    first = configured_reservation(db, "ABC123", 2)
    second = configured_reservation(db, "DEF456", 3)
    for player_id in (1, 2):
        prizes.allocate_rule(db, first, {"playerId": player_id}, first["stackPrizeProgram"],
                             first["stackPrizeProgram"]["rules"][0], f"first-{player_id}")
    assert len([row for row in prizes.records(db, "ABC123") if row["id"].startswith("award_")]) == 2
    assert inventory.capacity(db, "owner", second["stackPrizeProgram"]["catalog"], second)["local-gift"]["remaining"] == 3
    prizes.allocate_rule(db, second, {"playerId": 1}, second["stackPrizeProgram"],
                         second["stackPrizeProgram"]["rules"][0], "second-1")
    projected = next(prize for prize in inventory_response(db, monkeypatch)["catalog"] if prize["id"] == "gift")
    assert projected["reservation"] == {"total": 5, "available": 1, "assigned": 4, "redeemed": 0, "error": None}
    assert {game["gameId"]: game["remaining"] for game in projected["games"]} == {"ABC123": 0, "DEF456": 1}
    assert inventory.balance(db, "owner", "gift")["initialized"] is False
    # Neither reads nor refreshes require or create a physical-stock initialization.
    assert inventory_response(db, monkeypatch)["catalog"][0]["reservation"]["total"] == 5
    assert not any(row["kind"] == "initialize" for row in inventory.page(db, "owner", "MOVE#gift#")["items"])


def test_reservation_sum_includes_inactive_events_and_prizes_without_rules(db, monkeypatch):
    first = configured_reservation(db, "ABC123", 10)
    second = configured_reservation(db, "DEF456", 20, active=False)
    second["stackPrizeProgram"]["rules"] = []
    db.Table("games").put_item(Item=second)
    projected = next(prize for prize in inventory_response(db, monkeypatch)["catalog"] if prize["id"] == "gift")
    assert projected["reservation"]["total"] == projected["reservation"]["available"] == 30
    assert first["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 10
    assert sorted(game["limit"] for game in projected["games"]) == [10, 20]


def test_reservation_stock_tracks_history_and_redemption_without_reinitializing(db, monkeypatch):
    meta = configured_reservation(db, "ABC123", 10)
    old = award("award_imported")
    db.Table("awards").put_item(Item=old)
    inventory.import_award(db, meta, old)
    inventory.import_award(db, meta, old)
    before = next(prize for prize in inventory_response(db, monkeypatch)["catalog"] if prize["id"] == "gift")
    assert before["reservation"] == {"total": 10, "available": 9, "assigned": 1, "redeemed": 0, "error": None}
    prizes.redeem(db, meta["gameId"], 1, old["id"])
    db.Table("awards").delete_item(Key={"gameId": meta["gameId"], "id": old["id"]})
    after = next(prize for prize in inventory_response(db, monkeypatch)["catalog"] if prize["id"] == "gift")
    assert after["reservation"] == {"total": 10, "available": 9, "assigned": 1, "redeemed": 1, "error": None}
    assert inventory.capacity(db, "owner", meta["stackPrizeProgram"]["catalog"], meta)["local-gift"]["remaining"] == 9


def test_reservation_refresh_reflects_edits_without_accumulating_or_changing_balances(db, monkeypatch):
    meta = configured_reservation(db, "ABC123", 10)
    configured_reservation(db, "DEF456", 20)
    first = inventory_response(db, monkeypatch)
    meta["stackPrizeProgram"]["catalog"][0]["stockTotal"] = 12
    db.Table("games").put_item(Item=meta)
    second = inventory_response(db, monkeypatch)
    third = inventory_response(db, monkeypatch)
    assert first["catalog"][0]["reservation"]["total"] == 30
    assert second["catalog"][0]["reservation"]["total"] == third["catalog"][0]["reservation"]["total"] == 32
    assert inventory.balance(db, "owner", "gift")["revision"] == 0


@pytest.mark.parametrize("source", ["stand", "kivly"])
@pytest.mark.parametrize("quantity", [10, None])
def test_rule_removal_updates_admin_reservations_using_durable_history(db, monkeypatch, source, quantity):
    from stand_common import stack_prize_api, reward_conditions
    meta = configured_reservation(db, "ABC123", quantity)
    if source == "kivly":
        meta["stackPrizeProgram"]["catalog"][0] = {
            "id": "local-gift", "source": "kivly", "name": "Gift", "stockTotal": quantity,
            "locationId": "club", "rewardId": "reward",
        }
        conn = {"locationId": "club", "ownerId": "external-owner"}
        monkeypatch.setattr(kivly_registration, "connection", lambda *_: conn)
        monkeypatch.setattr(kivly_registration, "account_game_settings", lambda *_: {"enabled": True})
        client = MagicMock()
        client.list_rewards.return_value = [{"id": "reward", "name": "Gift"}]
        monkeypatch.setattr(kivly_registration, "_client", lambda *_: client)
    db.Table("games").put_item(Item=meta)
    prize = meta["stackPrizeProgram"]["catalog"][0]
    for index in range(2):
        row = {**award(f"award_{index}", source=source), "prize": deepcopy(prize)}
        db.Table("awards").put_item(Item=deepcopy(row))
        inventory.import_award(db, meta, row)
        db.Table("awards").delete_item(Key={"gameId": row["gameId"], "id": row["id"]})
    # Even absent award rows and pre-existing stock counters must not erase consumption.
    before = inventory_response(db, monkeypatch)
    monkeypatch.setattr(stack_prize_api, "_get_claims", lambda _: {"sub": "owner"})
    event = {"body": json.dumps({
        "gameId": "ABC123", "action": "rules", "rules": [],
        "expectedRevision": reward_conditions.revision(prizes.effective_program(db, meta)),
    })}
    response = stack_prize_api.handle(event, "PUT", db, lambda _: True, lambda _: b"")
    assert response["statusCode"] == 200, response
    saved = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    assert saved["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 2
    assert saved["stackPrizeStock"]["local-gift"] == {"total": 2, "remaining": 0}
    after = inventory_response(db, monkeypatch)
    pid = inventory.prize_id(prize)
    projected = next(item for item in after["catalog"] if item["id"] == pid)
    assert projected["reservation"] == {
        "total": 2, "available": 0, "assigned": 2,
        "redeemed": None if source == "kivly" else 0, "error": None,
    }
    assert projected["games"][0]["limit"] == 2
    assert projected["games"][0]["active"] is False
    previous = next(item for item in before["catalog"] if item["id"] == pid)
    assert projected["inventory"] == previous["inventory"]
    assert inventory.page(db, "owner", "MOVE#" + pid + "#")["items"]


def test_reserved_competing_transactions_cannot_spend_another_events_units(db):
    meta = configured_reservation(db, "ABC123", 1)
    other = configured_reservation(db, "DEF456", 100)
    first, second = award("award_first"), award("award_second")
    one, two = inventory.award_operations(db, meta, [first]), inventory.award_operations(db, meta, [second])
    inventory.write(db, [prizes.put(first), *one])
    with pytest.raises(inventory.InventoryError, match="InventoryChanged"):
        inventory.write(db, [prizes.put(second), *two])
    with pytest.raises(inventory.InventoryError, match="EventLimitReached"):
        inventory.award_operations(db, meta, [second])
    assert inventory.capacity(db, "owner", other["stackPrizeProgram"]["catalog"], other)["local-gift"]["remaining"] == 100


def test_missing_numeric_substock_is_explicit_not_guessed(db, monkeypatch):
    meta = configured_reservation(db, "ABC123", None)
    configured_reservation(db, "DEF456", 20)
    projected = next(prize for prize in inventory_response(db, monkeypatch)["catalog"] if prize["id"] == "gift")
    assert projected["reservation"]["total"] is None
    assert projected["reservation"]["error"] == "EventReservationRequired"
    assert inventory.capacity(db, "owner", meta["stackPrizeProgram"]["catalog"], meta)["local-gift"]["reason"] == "EventReservationRequired"
    with pytest.raises(inventory.InventoryError, match="EventReservationRequired"):
        inventory.award_operations(db, meta, [award()])




def stack_program(cap=None):
    return {"version": 3, "catalog": [
        {"id": "local-gift", "source": "stand", "name": "Gift", "sharedPrizeId": "gift", "stockTotal": cap},
    ], "rules": [{"id": "completion", "condition": "completed", "groups": [
        {"allocations": [{"prizeId": "local-gift", "quantity": 2}]},
    ]}]}


def allocate(db, program, player_id=1):
    meta = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    meta["stackPrizeProgram"] = deepcopy(program)
    db.Table("games").put_item(Item=meta)
    prizes.allocate_rule(db, meta, {"gameId": meta["gameId"], "playerId": player_id},
                         program, program["rules"][0], f"finish-{player_id}")


@pytest.mark.parametrize("cap,assigned,reason", [(None, 0, "EventReservationRequired"), (0, 0, "EventLimitReached"), (1, 1, "EventLimitReached")])
def test_numeric_event_caps_are_required_and_enforced(db, cap, assigned, reason):
    activate(db)
    allocate(db, stack_program(cap))
    stock = inventory.balance(db, "owner", "gift")
    assert (stock["assigned"], stock["available"]) == (assigned, 0)
    ledger = next(r for r in prizes.records(db, "ABC123") if r.get("kind") == "allocation")
    assert ledger["inventoryReasons"].get("local-gift") == reason




def test_or_rule_keeps_initialized_available_alternative(db):
    reserve(db, "owner", "alternative", {**adjustment(2), "prizeId": "alternative"})
    activate(db)
    program = stack_program()
    program["catalog"].append({**program["catalog"][0], "id": "alternative-local", "sharedPrizeId": "alternative", "stockTotal": 2})
    program["rules"][0]["groups"].append({"allocations": [{"prizeId": "alternative-local", "quantity": 1}]})
    allocate(db, program)
    rows = [r for r in prizes.records(db, "ABC123") if r["id"].startswith("award_")]
    assert len(rows) == 1 and rows[0]["prize"]["id"] == "alternative-local"
    assert inventory.balance(db, "owner", "gift")["assigned"] == 0




def test_pre_inventory_native_award_is_reused_after_shared_id_migration(db):
    meta = legacy_game(db)
    old = award()
    old["condition"] = "validation"
    old["prize"].pop("sharedPrizeId")
    db.Table("awards").put_item(Item=old)
    activate(db)
    assert game_lifecycle.issue_native_award(db, meta, {"playerId": 1}, "validation")["id"] == old["id"]
    assert inventory.balance(db, "owner", "gift")["assigned"] == 0


@pytest.mark.parametrize("active", [True, False])
def test_old_public_qr_updates_award_counter_and_legacy_flag_atomically(db, monkeypatch, active):
    from tests.test_public_web import public_mod
    from stand_common.utils import issue_prize_token
    meta = legacy_game(db)
    meta["isActive"] = active
    player = {"gameId": "ABC123", "playerId": 1, "validated": True, "type": {"EMPAREJA2": {}}}
    db.Table("players").put_item(Item=player)
    inventory.project_legacy_player(db, meta, player)
    activate(db)
    monkeypatch.setattr(public_mod, "dynamo_r", db)
    monkeypatch.setattr(public_mod, "_get_game", lambda _: meta)
    monkeypatch.setattr(public_mod, "_get_player", lambda gid, pid: db.Table("players").get_item(
        Key={"gameId": gid, "playerId": pid})["Item"])
    monkeypatch.setattr(kivly_registration, "require_player", lambda *_: None)
    token = issue_prize_token("ABC123", 1, "v", public_mod.PLAYER_TOKEN_SECRET)
    event = {"body": json.dumps({"prizeToken": token})}
    assert public_mod._handle_redeem(event)["statusCode"] == 200
    assert public_mod._handle_redeem(event)["statusCode"] == 409
    assert inventory.balance(db, "owner", "gift")["historicalRedeemed"] == 1
    assert db.Table("players").get_item(Key={"gameId": "ABC123", "playerId": 1})["Item"]["type"]["EMPAREJA2"]["validationPrizeDelivered"]


def test_historical_kivly_assignment_does_not_imply_redemption_or_earn_again(db, monkeypatch):
    from stand_common import utils
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "registration")
    monkeypatch.setattr(utils, "_get_claims", lambda _: {"sub": "owner"})
    meta = {"gameId": "ABC123", "ownerUserId": "owner", "gameType": "STACK",
            "kivlyReward": {"id": "coffee", "locationId": "loc", "name": "Coffee"}}
    db.Table("games").put_item(Item=meta)
    db.Table("registration").put_item(Item={
        "id": "reward:ABC123:1", "state": "confirmed", "name": "Coffee",
        "rewardId": "coffee", "locationId": "loc",
    })
    inventory.project_legacy_player(db, meta, {"playerId": 1})
    inventory.project_legacy_player(db, meta, {"playerId": 1})
    response = inventory.handle({}, "GET", db)
    entry = next(p for p in json.loads(response["body"])["catalog"] if p["source"] == "kivly")
    assert entry["canjeados"] is None
    assert entry["inventory"]["historicalAssigned"] == 1
    assert entry["inventory"]["historicalRedeemed"] == 0
    assert not any(row.get("work") for row in prizes.records(db, "ABC123"))




def test_eighty_units_with_legacy_player_guard_fits_one_hundred_operations(db, monkeypatch):
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "registration")
    meta = {"gameId": "ABC123", "ownerUserId": "owner", "gameType": "STACK"}
    participant = {"gameId": "ABC123", "playerId": 1, "type": {"STACK": {}},
                   "kivly": {"membership": {"customerId": "customer", "locationId": "loc"}}}
    db.Table("games").put_item(Item=meta)
    db.Table("players").put_item(Item=participant)
    program = {"version": 3, "catalog": [], "rules": [{
        "id": "complete", "condition": "completed", "groups": [{"allocations": []}],
    }]}
    for index in range(8):
        prize = {"id": "legacy_ranking" if index == 0 else "legacy_kivly" if index == 1 else f"local-{index}",
                 "name": f"Gift {index}", "stockTotal": 10, "source": "kivly" if index == 1 else "stand"}
        prize.update({"locationId": "loc", "rewardId": "coffee"} if index == 1 else {"sharedPrizeId": f"shared-{index}"})
        pid = inventory.prize_id(prize)
        reserve(db, "owner", pid, {**adjustment(10, request_id=f"init-{index}"), "prizeId": pid})
        program["catalog"].append(prize)
        program["rules"][0]["groups"][0]["allocations"].append({"prizeId": prize["id"], "quantity": 10})
    activate(db)
    meta["stackPrizeProgram"] = program
    db.Table("games").put_item(Item=meta)
    prizes.allocate_rule(db, meta, participant, program, program["rules"][0], "finish")
    assert len(db.last_transaction) == 99
    targets = [(next(iter(op.values()))["TableName"],
                json.dumps(next(iter(op.values())).get("Key") or {
                    k: v for k, v in next(iter(op.values()))["Item"].items()
                    if k in ("ownerId", "key", "gameId", "id")
                }, sort_keys=True)) for op in db.last_transaction]
    assert len(set(targets)) == 99
    assert len([r for r in prizes.records(db, "ABC123") if r["id"].startswith("award_")]) == 80


def test_same_prize_active_in_any_native_slot_is_an_active_game_association(db):
    meta = legacy_game(db)
    meta["prizeCatalog"]["validation"]["limit"] = 0
    meta["prizeCatalog"]["raffle"] = {"name": "Gift", "sharedPrizeId": "gift", "limit": 3}
    db.Table("games").put_item(Item=meta)
    entries, _ = inventory.definitions(db, "owner")
    assert entries["gift"]["games"] == [{"gameId": "ABC123", "name": "ABC123", "active": True, "limit": 3}]


def test_same_prize_under_multiple_stack_ids_preserves_active_association(db):
    program = stack_program(0)
    program["catalog"].append({**program["catalog"][0], "id": "second-local", "stockTotal": 4})
    program["rules"][0]["groups"][0]["allocations"].append({"prizeId": "second-local", "quantity": 1})
    meta = {"gameId": "ABC123", "gameType": "STACK", "ownerUserId": "owner", "stackPrizeProgram": program}
    db.Table("games").put_item(Item=meta)
    entries, _ = inventory.definitions(db, "owner")
    assert entries["gift"]["games"][0]["active"] is True
    assert entries["gift"]["games"][0]["limit"] == 4


def test_owner_api_cannot_read_or_adjust_another_operators_stock(db, monkeypatch):
    from stand_common import utils
    reserve(db, "owner", "gift", adjustment(7))
    monkeypatch.setattr(utils, "_get_claims", lambda _: {"sub": "different-owner"})
    assert json.loads(inventory.handle({}, "GET", db)["body"])["catalog"] == []
    detail = inventory.handle({"queryStringParameters": {"prizeId": "gift"}}, "GET", db)
    assert json.loads(detail["body"])["items"] == []
    response = inventory.handle({"body": json.dumps(adjustment(2))}, "POST", db)
    assert response["statusCode"] == 410
    assert inventory.balance(db, "owner", "gift")["available"] == 0


@pytest.mark.parametrize("pid", [None, True, [], {}, "x" * 81])
def test_owner_api_rejects_malformed_prize_ids(db, monkeypatch, pid):
    from stand_common import utils
    monkeypatch.setattr(utils, "_get_claims", lambda _: {"sub": "owner"})
    event = {"body": json.dumps({**adjustment(1), "prizeId": pid})}
    response = inventory.handle(event, "POST", db)
    assert response["statusCode"] == 410
    assert json.loads(response["body"])["error"] == "InventoryActionRetired"


def test_owner_api_requires_authentication(db, monkeypatch):
    from stand_common import utils
    monkeypatch.setattr(utils, "_get_claims", lambda _: {})
    assert inventory.handle({}, "GET", db)["statusCode"] == 401
