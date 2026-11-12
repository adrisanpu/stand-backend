"""Retire manual inventory in dev; dry-run by default, never calls Kivly."""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import sys

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))

from stand_common import prize_inventory as inventory, stack_prizes as prizes
from stand_common.utils import _iso_now

ACCOUNT = "781283585341"
REGION = "us-east-1"
TABLES = {
    "GAMES_TABLE": "stand-dev-game-table",
    "USERS_TABLE": "stand-dev-user-table",
    "GAMEPLAYER_TABLE": "stand-dev-gameplayer-table",
    "STACK_PRIZES_TABLE": "stand-dev-stack-prizes",
    "PRIZE_INVENTORY_TABLE": "stand-dev-prize-inventory",
    "KIVLY_REGISTRATION_TABLE": "stand-dev-kivly-registration-table",
}


class MigrationError(ValueError):
    pass


def scan(table):
    options = {"ConsistentRead": True}
    while True:
        page = table.scan(**options)
        yield from page.get("Items", [])
        if not page.get("LastEvaluatedKey"):
            return
        options["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def plan(dynamo, target_game, target_prize, target_total):
    games = list(scan(dynamo.Table(TABLES["GAMES_TABLE"])))
    rows = list(scan(dynamo.Table(TABLES["PRIZE_INVENTORY_TABLE"])))
    awards = list(scan(dynamo.Table(TABLES["STACK_PRIZES_TABLE"])))
    by_game = {game["gameId"]: game for game in games}
    changes, pending, blockers = [], [], []
    matched = 0
    for game in games:
        program = game.get("stackPrizeProgram")
        catalog = program.get("catalog", []) if isinstance(program, dict) else list((game.get("prizeCatalog") or {}).values())
        for prize in catalog:
            if not prize.get("name"):
                continue
            if inventory._resolve(dynamo, game["ownerUserId"], prize) is None:
                blockers.append("Unlinked configured prize")
            value = prize.get("stockTotal") if isinstance(program, dict) else prize.get("limit")
            if value is None or not inventory.integer(value):
                blockers.append("Missing numeric event reservation; explicit quantity required")
        if game.get("gameName") == target_game:
            if not isinstance(program, dict):
                blockers.append("Target event is not a STACK program")
                continue
            updated = prizes.upgrade_program(program)
            for prize in updated["catalog"]:
                if prize.get("name") == target_prize:
                    matched += 1
                    prize["stockTotal"] = target_total
            records = [row for row in awards if row.get("gameId") == game["gameId"]]
            try:
                stock = prizes.updated_stock(game, updated, records)
            except prizes.PrizeError as error:
                blockers.append(error.code)
                continue
            for pid, limit in inventory.reservation_limits(dynamo, game["ownerUserId"], {
                **game, "stackPrizeProgram": updated,
            }).items():
                used = inventory.event_usage(dynamo, game["ownerUserId"], pid).get(game["gameId"], {}).get("assigned", 0)
                unmarked = sum(
                    row.get("id", "").startswith("award_") and not row.get("inventoryMovementKey")
                    and inventory._resolve(dynamo, game["ownerUserId"], row.get("prize") or {}) == pid
                    for row in records
                )
                if limit is None or used + unmarked > limit:
                    blockers.append("Requested sub-stock is below recorded assignments")
            if updated != program or stock != game.get("stackPrizeStock"):
                changes.append((game, updated, stock))
    if matched != 1:
        blockers.append("Target event/prize must match exactly once")
    for row in awards:
        if not row.get("id", "").startswith("award_") or row.get("inventoryMovementKey"):
            continue
        game = by_game.get(row["gameId"])
        if not game and row.get("ownerUserId"):
            game = {"gameId": row["gameId"], "ownerUserId": row["ownerUserId"], "gameName": row["gameId"]}
        if not game:
            blockers.append("Unimported award without an owner")
            continue
        if not inventory._resolve(dynamo, game["ownerUserId"], row.get("prize") or {}):
            prize = row.get("prize") or {}
            if prize.get("source") != "stand" or not prize.get("name"):
                blockers.append("Unlinked historical award")
                continue
            # Archive-only identity: preserve the original award snapshot without recreating a catalog prize.
            row = {**row, "prize": {**prize, "sharedPrizeId": "historical_" + inventory.digest(
                [game["ownerUserId"], prize["name"], prize.get("imageKey")])}}
        pending.append((game, row))
    return games, rows, changes, pending, blockers


def retire_balance(row):
    updated = deepcopy(row)
    if row.get("initialized") or row.get("available", 0):
        updated.setdefault("retiredManualInventory", {
            "initialized": row.get("initialized", False),
            "available": row.get("available", 0),
        })
        updated.update(initialized=False, available=0)
    return updated


def migrate(session, target_game, target_prize, target_total, apply=False):
    if not inventory.integer(target_total) or target_total > 1000:
        raise MigrationError("Target total must be between 0 and 1000")
    if session.region_name != REGION or session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise MigrationError("Only the approved dev account and region are allowed")
    os.environ.update(TABLES)
    dynamo = session.resource("dynamodb")
    for name in TABLES.values():
        if dynamo.Table(name).table_arn != f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{name}":
            raise MigrationError("Unexpected table ARN")
    games, rows, changes, pending, blockers = plan(dynamo, target_game, target_prize, target_total)
    summary = {
        "mode": "apply" if apply else "dry-run", "games": len(games),
        "eventUpdates": len(changes), "awardsToImport": len(pending),
        "balancesToRetire": sum(retire_balance(row) != row for row in rows if row["key"].startswith("BAL#")),
        "controlsToRetire": sum(row["key"] == "CONTROL" or row["key"].startswith("REQUEST#") for row in rows),
        "blockers": sorted(set(blockers)),
    }
    if not apply:
        return summary
    if blockers:
        raise MigrationError("Preflight blocked: " + "; ".join(sorted(set(blockers))))
    original_awards = {
        (row["gameId"], row["id"]): deepcopy(row)
        for row in scan(dynamo.Table(TABLES["STACK_PRIZES_TABLE"]))
        if row.get("id", "").startswith("award_")
    }
    for game, row in pending:
        inventory.import_award(dynamo, game, row)
    for before, program, _ in changes:
        current = dynamo.Table(TABLES["GAMES_TABLE"]).get_item(
            Key={"gameId": before["gameId"]}, ConsistentRead=True,
        )["Item"]
        if current != before:
            raise MigrationError("Event changed during migration; rerun dry-run")
        records = prizes.records(dynamo, before["gameId"])
        stock = prizes.updated_stock(before, program, records)
        limits = inventory.reservation_limits(dynamo, before["ownerUserId"], {**before, "stackPrizeProgram": program})
        for pid, limit in limits.items():
            used = inventory.event_usage(dynamo, before["ownerUserId"], pid).get(before["gameId"], {}).get("assigned", 0)
            if limit is None or used > limit:
                raise MigrationError("Requested sub-stock is below recorded assignments")
        prizes.transact(dynamo, [prizes.game_update(
            before, ", stackPrizeProgram = :program, stackPrizeStock = :stock",
            {":program": program, ":stock": stock},
        )])
    table = dynamo.Table(TABLES["PRIZE_INVENTORY_TABLE"])
    # Read again after imports so balance revisions and counts are current.
    for row in scan(table):
        if row["key"].startswith("BAL#"):
            updated = retire_balance(row)
            if updated != row:
                inventory.write(dynamo, [inventory.snapshot_put(updated, row)])
        elif row["key"] == "CONTROL" or row["key"].startswith("REQUEST#"):
            archived = {**row, "key": "RETIRED#" + row["key"], "retiredAt": _iso_now()}
            names = {f"#a{n}": key for n, key in enumerate(row)}
            values = {f":v{n}": value for n, value in enumerate(row.values())}
            inventory.write(dynamo, [
                {"Put": {
                    "TableName": TABLES["PRIZE_INVENTORY_TABLE"], "Item": archived,
                    "ConditionExpression": "attribute_not_exists(#key)", "ExpressionAttributeNames": {"#key": "key"},
                }},
                {"Delete": {
                    "TableName": TABLES["PRIZE_INVENTORY_TABLE"],
                    "Key": {"ownerId": row["ownerId"], "key": row["key"]},
                    "ConditionExpression": " AND ".join(f"#a{n} = :v{n}" for n in range(len(row))),
                    "ExpressionAttributeNames": names, "ExpressionAttributeValues": values,
                }},
            ])
    _, final_rows, remaining, pending, blockers = plan(dynamo, target_game, target_prize, target_total)
    if remaining or pending or blockers or any(
        row["key"] == "CONTROL" or row["key"].startswith("REQUEST#")
        or row["key"].startswith("BAL#") and retire_balance(row) != row for row in final_rows
    ):
        raise MigrationError("Post-migration verification failed; rerun dry-run")
    current_awards = {
        (row["gameId"], row["id"]): row
        for row in scan(dynamo.Table(TABLES["STACK_PRIZES_TABLE"]))
        if row.get("id", "").startswith("award_")
    }
    for key, before in original_awards.items():
        after = current_awards.get(key, {})
        if any(after.get(field) != value for field, value in before.items()):
            raise MigrationError("An existing award changed during migration; inspect concurrent writes")
    for row in rows:
        if row["key"].startswith("MOVE#"):
            after = table.get_item(Key={"ownerId": row["ownerId"], "key": row["key"]}, ConsistentRead=True).get("Item")
            if after != row:
                raise MigrationError("Existing movement history changed during migration")
    summary["verified"] = True
    summary["preservedAwards"] = len(original_awards)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="stand-deploy-cli")
    parser.add_argument("--target-game", required=True)
    parser.add_argument("--target-prize", required=True)
    parser.add_argument("--target-total", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not 0 <= args.target_total <= 1000:
        parser.error("target-total must be between 0 and 1000")
    try:
        result = migrate(boto3.Session(profile_name=args.profile, region_name=REGION),
                         args.target_game, args.target_prize, args.target_total, args.apply)
    except (MigrationError, prizes.PrizeError, inventory.InventoryError) as error:
        parser.exit(1, f"Migration stopped: {error}\n")
    except ClientError as error:
        parser.exit(1, f"AWS request failed: {error.response['Error']['Code']}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
