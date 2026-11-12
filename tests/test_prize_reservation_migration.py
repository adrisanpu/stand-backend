from copy import deepcopy
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.test_prize_inventory import db, award, configured_reservation
from stand_common import prize_inventory as inventory

spec = importlib.util.spec_from_file_location(
    "reservation_migration", Path(__file__).parents[1] / "scripts" / "migrate_prize_reservations.py",
)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


@pytest.fixture
def session(db, monkeypatch):
    tables = {
        "GAMES_TABLE": "games", "USERS_TABLE": "users", "GAMEPLAYER_TABLE": "players",
        "STACK_PRIZES_TABLE": "awards", "PRIZE_INVENTORY_TABLE": "inventory",
        "KIVLY_REGISTRATION_TABLE": "registration",
    }
    monkeypatch.setattr(migration, "TABLES", tables)
    for name in tables.values():
        db.Table(name).table_arn = f"arn:aws:dynamodb:{migration.REGION}:{migration.ACCOUNT}:table/{name}"
    session = MagicMock(region_name=migration.REGION)
    session.client.return_value.get_caller_identity.return_value = {"Account": migration.ACCOUNT}
    session.resource.return_value = db
    game = configured_reservation(db, "ABC123", 70)
    game["gameName"] = "Target"
    db.Table("games").put_item(Item=game)
    db.Table("inventory").put_item(Item={
        **inventory.balance(db, "owner", "gift"), "available": 51, "initialized": True, "revision": 1,
    })
    db.Table("inventory").put_item(Item={"ownerId": "owner", "key": "CONTROL", "mode": "active"})
    db.Table("inventory").put_item(Item={"ownerId": "owner", "key": "REQUEST#one", "quantity": 51})
    db.Table("awards").put_item(Item={**award("award_old"), "state": "delivered"})
    return session


def snapshot(db):
    return {name: deepcopy(table.rows) for name, table in db.tables.items()}


def test_dry_run_is_read_only_and_apply_is_idempotent(db, session):
    other = configured_reservation(db, "DEF456", 12)
    before = snapshot(db)
    result = migration.migrate(session, "Target", "Gift", 50)
    assert result["blockers"] == []
    assert result["awardsToImport"] == result["balancesToRetire"] == result["eventUpdates"] == 1
    assert snapshot(db) == before
    result = migration.migrate(session, "Target", "Gift", 50, apply=True)
    assert result["verified"] and result["preservedAwards"] == 1
    saved = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    assert saved["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 50
    assert saved["stackPrizeStock"]["local-gift"] == {"total": 50, "remaining": 49}
    assert db.Table("games").get_item(Key={"gameId": "DEF456"})["Item"] == other
    balance = inventory.balance(db, "owner", "gift")
    assert balance["historicalAssigned"] == balance["historicalRedeemed"] == 1
    assert balance["available"] == 0 and not balance["initialized"]
    assert balance["retiredManualInventory"] == {"available": 51, "initialized": True}
    assert db.Table("inventory").get_item(Key={"ownerId": "owner", "key": "RETIRED#CONTROL"})["Item"]["mode"] == "active"
    assert not db.Table("inventory").get_item(Key={"ownerId": "owner", "key": "CONTROL"})
    after = snapshot(db)
    assert migration.migrate(session, "Target", "Gift", 50, apply=True)["verified"]
    assert snapshot(db) == after


def test_imports_orphan_awards_without_recreating_catalog_or_changing_snapshot(db, session):
    row = award("award_orphan", "DELETED")
    row["prize"].pop("sharedPrizeId")
    row["prize"]["name"] = "Removed prize"
    db.Table("awards").put_item(Item=deepcopy(row))
    user = db.Table("users").get_item(Key={"userId": "owner"})["Item"]
    assert migration.migrate(session, "Target", "Gift", 50, apply=True)["preservedAwards"] == 2
    after = db.Table("awards").get_item(Key={"gameId": "DELETED", "id": row["id"]})["Item"]
    assert after["prize"] == row["prize"]
    assert after["inventoryPrizeId"].startswith("historical_")
    assert after["state"] == row["state"]
    assert db.Table("users").get_item(Key={"userId": "owner"})["Item"] == user


def test_cap_below_durable_history_blocks_before_any_mutation(db, session):
    game = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    row = award("award_removed")
    db.Table("awards").put_item(Item=row)
    inventory.import_award(db, game, row)
    db.Table("awards").delete_item(Key={"gameId": "ABC123", "id": row["id"]})
    before = snapshot(db)
    assert migration.migrate(session, "Target", "Gift", 0)["blockers"]
    with pytest.raises(migration.MigrationError, match="Preflight blocked"):
        migration.migrate(session, "Target", "Gift", 0, apply=True)
    assert snapshot(db) == before


@pytest.mark.parametrize("account,region", [("production", migration.REGION), (migration.ACCOUNT, "eu-west-1")])
def test_wrong_account_or_region_cannot_write(db, session, account, region):
    session.region_name = region
    session.client.return_value.get_caller_identity.return_value = {"Account": account}
    before = snapshot(db)
    with pytest.raises(migration.MigrationError, match="approved dev"):
        migration.migrate(session, "Target", "Gift", 50, apply=True)
    assert snapshot(db) == before


@pytest.mark.parametrize("action", ["activate", "initialize", "replenish", "adjust"])
def test_retired_inventory_actions_return_gone_without_writes(db, monkeypatch, action):
    import json
    from stand_common import utils
    monkeypatch.setattr(utils, "_get_claims", lambda _: {"sub": "owner"})
    before = snapshot(db)
    response = inventory.handle({"body": json.dumps({"action": action})}, "POST", db)
    assert response["statusCode"] == 410
    assert json.loads(response["body"])["error"] == "InventoryActionRetired"
    assert snapshot(db) == before
