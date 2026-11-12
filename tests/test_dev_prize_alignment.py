from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
from unittest.mock import MagicMock

import pytest

from tests.test_prize_inventory import db, award, configured_reservation
from stand_common import prize_inventory as inventory

scripts = Path(__file__).parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("dev_prize_alignment", scripts / "align_dev_prize_reservations.py")
alignment = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
spec.loader.exec_module(alignment)
sys.path.remove(str(scripts))


@pytest.fixture
def setup(db, monkeypatch):
    tables = {"GAMES_TABLE": "games", "USERS_TABLE": "users", "STACK_PRIZES_TABLE": "awards",
              "PRIZE_INVENTORY_TABLE": "inventory", "GAMEPLAYER_TABLE": "players",
              "KIVLY_REGISTRATION_TABLE": "registration"}
    monkeypatch.setattr(alignment, "TABLES", tables)
    for name in tables.values():
        db.Table(name).table_arn = f"arn:aws:dynamodb:{alignment.REGION}:{alignment.ACCOUNT}:table/{name}"
    game = configured_reservation(db, "ABC123", 50)
    unused = {"id": "unused-local", "source": "stand", "name": "Removed",
              "sharedPrizeId": "removed", "stockTotal": 20}
    game["stackPrizeProgram"]["catalog"].append(unused)
    game["stackPrizeStock"] = {"local-gift": {"total": 50, "remaining": 49},
                              "unused-local": {"total": 20, "remaining": 19}}
    db.Table("games").put_item(Item=game)
    user = db.Table("users").get_item(Key={"userId": "owner"})["Item"]
    user["sharedPrizeCatalog"].append({"id": "removed", "name": "Removed"})
    db.Table("users").put_item(Item=user)
    keep, remove, orphan = award("award_keep"), award("award_remove"), award("award_orphan", "DELETED")
    remove["prize"] = unused
    for row in (keep, remove, orphan):
        db.Table("awards").put_item(Item=deepcopy(row))
        inventory.import_award(db, {**game, "gameId": row["gameId"]}, row)
    db.Table("awards").put_item(Item={
        "gameId": "ABC123", "id": "allocation_one", "kind": "allocation",
        "requested": [{"prizeId": "local-gift", "quantity": 1}, {"prizeId": "unused-local", "quantity": 1}],
        "awarded": [{"prizeId": "unused-local", "quantity": 1}],
    })
    for n in range(101):
        db.Table("awards").put_item(Item={
            "gameId": "ABC123", "id": f"grant_{n}", "kind": "grant", "state": "confirmed",
            "program": deepcopy(game["stackPrizeProgram"]),
        })
    session = MagicMock(region_name=alignment.REGION)
    session.client.return_value.get_caller_identity.return_value = {"Account": alignment.ACCOUNT}
    session.resource.return_value = db
    return session


def snapshot(db):
    return {name: deepcopy(table.rows) for name, table in db.tables.items()}


def test_cleanup_preserves_configured_totals_and_retained_awards_and_is_idempotent(db, setup):
    before = snapshot(db)
    result = alignment.run(setup)
    assert result["awardsDeleted"] == 2 and result["awardsRetained"] == 1
    assert snapshot(db) == before
    saved_award = db.Table("awards").get_item(Key={"gameId": "ABC123", "id": "award_keep"})["Item"]
    result = alignment.run(setup, apply=True)
    assert result["verified"]
    game = db.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]
    assert len(game["stackPrizeProgram"]["catalog"]) == 1
    assert game["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 50
    assert game["stackPrizeStock"] == {"local-gift": {"total": 50, "remaining": 49}}
    assert db.Table("awards").get_item(Key={"gameId": "ABC123", "id": "award_keep"})["Item"] == saved_award
    assert not db.Table("awards").get_item(Key={"gameId": "ABC123", "id": "award_remove"})
    assert not db.Table("awards").get_item(Key={"gameId": "DELETED", "id": "award_orphan"})
    assert inventory.balance(db, "owner", "gift")["historicalAssigned"] == 1
    assert len(db.Table("users").get_item(Key={"userId": "owner"})["Item"]["sharedPrizeCatalog"]) == 1
    assert all(len(r["program"]["catalog"]) == 1 for r in db.Table("awards").rows.values() if r.get("kind") == "grant")
    after = snapshot(db)
    assert alignment.run(setup, apply=True)["verified"]
    assert snapshot(db) == after


def test_unsettled_grant_blocks_without_writes(db, setup):
    db.Table("awards").put_item(Item={
        "gameId": "ABC123", "id": "grant_pending", "kind": "grant", "state": "pending",
        "program": {"version": 3, "catalog": [], "rules": []},
    })
    before = snapshot(db)
    with pytest.raises(alignment.MigrationError, match="unsettled"):
        alignment.run(setup, apply=True)
    assert snapshot(db) == before


def test_guard_rejects_production_before_writes(db, setup):
    setup.client.return_value.get_caller_identity.return_value = {"Account": "production"}
    before = snapshot(db)
    with pytest.raises(alignment.MigrationError, match="approved dev"):
        alignment.run(setup, apply=True)
    assert snapshot(db) == before
