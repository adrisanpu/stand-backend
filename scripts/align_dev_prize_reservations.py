"""Destructive dev-only alignment to current conditions; dry-run unless --apply."""
import argparse
from collections import Counter
from copy import deepcopy
import json
import os

import boto3

from migrate_prize_reservations import ACCOUNT, REGION, TABLES, MigrationError, scan
from stand_common import prize_inventory as inventory


def key_for(table, row):
    fields = {
        "GAMES_TABLE": ("gameId",), "USERS_TABLE": ("userId",),
        "STACK_PRIZES_TABLE": ("gameId", "id"),
        "PRIZE_INVENTORY_TABLE": ("ownerId", "key"),
    }[table]
    return {field: row[field] for field in fields}


def checked(table, before, after=None, check=False):
    names = {f"#a{n}": field for n, field in enumerate(before)}
    values = {f":v{n}": value for n, value in enumerate(before.values())}
    data = {
        "TableName": TABLES[table],
        "ConditionExpression": " AND ".join(f"#a{n} = :v{n}" for n in range(len(before))),
        "ExpressionAttributeNames": names, "ExpressionAttributeValues": values,
    }
    if after is not None:
        return {"Put": {**data, "Item": after}}
    return {"ConditionCheck" if check else "Delete": {**data, "Key": key_for(table, before)}}


def trim_program(program, allowed):
    result = deepcopy(program)
    result["catalog"] = [p for p in result.get("catalog", []) if p["id"] in allowed]
    rules = []
    for rule in result.get("rules", []):
        groups = []
        for group in rule.get("groups", []):
            allocations = [a for a in group.get("allocations", []) if a["prizeId"] in allowed]
            if allocations:
                groups.append({**group, "allocations": allocations})
        if groups:
            rules.append({**rule, "groups": groups})
    result["rules"] = rules
    return result


def make_plan(dynamo):
    data = {name: list(scan(dynamo.Table(TABLES[name]))) for name in (
        "GAMES_TABLE", "USERS_TABLE", "STACK_PRIZES_TABLE", "PRIZE_INVENTORY_TABLE",
    )}
    games = {g["gameId"]: g for g in data["GAMES_TABLE"]
             if g.get("ownerUserId") and not g.get("deletedAt")}
    allowed, canonical, changes, counts = {}, set(), [], Counter()
    for gid, game in games.items():
        program = game.get("stackPrizeProgram")
        if isinstance(program, dict):
            ids = {a["prizeId"] for r in program.get("rules", []) for group in r.get("groups", [])
                   for a in group.get("allocations", [])}
            catalog = [p for p in program.get("catalog", []) if p["id"] in ids]
            if ids - {p["id"] for p in catalog}:
                raise MigrationError("A current condition references a missing prize")
            allowed[gid] = ids
            updated = trim_program(program, ids)
            after = deepcopy(game)
            after["stackPrizeProgram"] = updated
            if "stackPrizeStock" in game:
                after["stackPrizeStock"] = {pid: stock for pid, stock in game["stackPrizeStock"].items() if pid in ids}
            if after != game:
                after["stackPrizeRevision"] = game.get("stackPrizeRevision", 0) + 1
                changes.append(("GAMES_TABLE", game, after))
                counts["eventCatalogsUpdated"] += 1
        else:
            catalog = list((game.get("prizeCatalog") or {}).values())
            allowed[gid] = None
        for prize in catalog:
            pid = inventory._resolve(dynamo, game["ownerUserId"], prize)
            total = prize.get("stockTotal") if isinstance(program, dict) else prize.get("limit")
            if not pid or not inventory.integer(total):
                raise MigrationError("A configured prize lacks identity or numeric sub-stock")
            canonical.add((game["ownerUserId"], gid, pid))

    def retained(row):
        gid = row["gameId"]
        if gid not in games:
            return False
        if allowed[gid] is not None:
            return row.get("prize", {}).get("id") in allowed[gid]
        pid = inventory._resolve(dynamo, games[gid]["ownerUserId"], row.get("prize") or {})
        return (games[gid]["ownerUserId"], gid, pid) in canonical

    awards = {(r["gameId"], r["id"]): r for r in data["STACK_PRIZES_TABLE"] if r["id"].startswith("award_")}
    removed = {aid for aid, row in awards.items() if not retained(row)}
    for row in data["STACK_PRIZES_TABLE"]:
        after = deepcopy(row)
        identity = row["gameId"], row["id"]
        if row["gameId"] not in games or identity in removed:
            after = None
        elif row.get("kind") == "grant" and isinstance(row.get("program"), dict):
            if row.get("state") != "confirmed":
                raise MigrationError("An unsettled grant job requires coordination before cleanup")
            if allowed[row["gameId"]] is not None:
                after["program"] = trim_program(row["program"], allowed[row["gameId"]])
        elif row.get("kind") == "allocation" and allowed[row["gameId"]] is not None:
            ids = allowed[row["gameId"]]
            for field in ("requested", "awarded"):
                if field in after:
                    after[field] = [a for a in after[field] if a["prizeId"] in ids]
            if "inventoryReasons" in after:
                after["inventoryReasons"] = {p: reason for p, reason in after["inventoryReasons"].items() if p in ids}
        if after != row:
            changes.append(("STACK_PRIZES_TABLE", row, after))
            counts["awardsDeleted" if identity in removed else
                   "jobsLedgersDeleted" if after is None else "jobsLedgersPruned"] += 1
    totals = {}
    used = {(owner, pid) for owner, _, pid in canonical}
    for row in data["PRIZE_INVENTORY_TABLE"]:
        if not row["key"].startswith("MOVE#"):
            continue
        after = deepcopy(row)
        if (row["ownerId"], row.get("gameId"), row.get("prizeId")) not in canonical:
            after = None
        elif row.get("kind") != "assignment":
            after = None
        else:
            units = row.get("units", {})
            after["units"] = {aid: unit for aid, unit in units.items() if (row["gameId"], aid) not in removed}
            if len(units) != row["quantity"]:
                raise MigrationError("Movement quantity does not match unit evidence")
            after["quantity"] = len(after["units"])
            if not after["quantity"]:
                after = None
        if after:
            key = row["ownerId"], row["prizeId"]
            values = totals.setdefault(key, Counter())
            prefix = "historical" if row.get("historical") else ""
            values[prefix + "Assigned" if prefix else "assigned"] += after["quantity"]
            values[prefix + "Redeemed" if prefix else "redeemed"] += sum(
                unit.get("state") == "delivered" for unit in after["units"].values())
        if after != row:
            changes.append(("PRIZE_INVENTORY_TABLE", row, after))
            counts["movementsDeleted" if after is None else "movementsPruned"] += 1
    for row in data["PRIZE_INVENTORY_TABLE"]:
        k = row["key"]
        if k.startswith("MOVE#"):
            continue
        after = deepcopy(row)
        if k.startswith("BAL#"):
            pid = row["prizeId"]
            if (row["ownerId"], pid) not in used:
                after = None
            else:
                values = totals.get((row["ownerId"], pid), {})
                for field in ("assigned", "redeemed", "historicalAssigned", "historicalRedeemed"):
                    after[field] = values.get(field, 0)
                after.update(initialized=False, available=0)
                after.pop("retiredManualInventory", None)
                if after != row:
                    after["revision"] = row.get("revision", 0) + 1
        elif k.startswith("IMPORT#"):
            game = games.get(k[len("IMPORT#"):])
            if not game or game["ownerUserId"] != row["ownerId"]:
                after = None
        elif k.startswith(("CONTROL", "REQUEST#", "RETIRED#")):
            after = None
        else:
            raise MigrationError("Unknown inventory row type")
        if after != row:
            changes.append(("PRIZE_INVENTORY_TABLE", row, after))
            counts["inventoryMetadataDeleted" if after is None else "balancesUpdated"] += 1
    for user in data["USERS_TABLE"]:
        if "sharedPrizeCatalog" not in user:
            continue
        catalog = [p for p in user["sharedPrizeCatalog"] if (user["userId"], p["id"]) in used]
        if catalog != user["sharedPrizeCatalog"]:
            after = {**user, "sharedPrizeCatalog": catalog,
                     "sharedPrizeCatalogRevision": user.get("sharedPrizeCatalogRevision", 0) + 1}
            changes.append(("USERS_TABLE", user, after))
            counts["standDefinitionsDeleted"] += len(user["sharedPrizeCatalog"]) - len(catalog)
    counts["awardsRetained"] = len(awards) - len(removed)
    return games, changes, dict(counts)


def run(session, apply=False):
    if session.region_name != REGION or session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise MigrationError("Only the approved dev account and region are allowed")
    os.environ.update(TABLES)
    dynamo = session.resource("dynamodb")
    for name in TABLES.values():
        if dynamo.Table(name).table_arn != f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{name}":
            raise MigrationError("Unexpected table ARN")
    games, changes, counts = make_plan(dynamo)
    if len(games) >= 100:
        raise MigrationError("Too many events for guarded cleanup")
    if apply:
        original_awards = {
            (row["gameId"], row["id"]): row for row in scan(dynamo.Table(TABLES["STACK_PRIZES_TABLE"]))
            if row["id"].startswith("award_")
        }
        deleted_awards = {(before["gameId"], before["id"]) for table, before, after in changes
                          if table == "STACK_PRIZES_TABLE" and after is None and before["id"].startswith("award_")}
        # Remove catalog references first; every later batch checks those event snapshots.
        first = [change for change in changes if change[0] == "GAMES_TABLE"]
        if first:
            inventory.write(dynamo, [checked(*change) for change in first])
        snapshots = {gid: next((after for table, before, after in first if before["gameId"] == gid), game)
                     for gid, game in games.items()}
        rest = [change for change in changes if change[0] != "GAMES_TABLE"]
        size = 100 - len(snapshots)
        for offset in range(0, len(rest), size):
            inventory.write(dynamo, [
                *[checked("GAMES_TABLE", game, check=True) for game in snapshots.values()],
                *[checked(*change) for change in rest[offset:offset + size]],
            ])
        _, remaining, final = make_plan(dynamo)
        if remaining:
            raise MigrationError("Cleanup incomplete; rerun dry-run before retrying")
        for identity, before in original_awards.items():
            after = dynamo.Table(TABLES["STACK_PRIZES_TABLE"]).get_item(
                Key={"gameId": identity[0], "id": identity[1]}, ConsistentRead=True,
            ).get("Item")
            if after != (None if identity in deleted_awards else before):
                raise MigrationError("Award preservation/deletion verification failed")
        counts["verified"] = True
        counts["awardsRetained"] = final["awardsRetained"]
    return {"mode": "apply" if apply else "dry-run", **counts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="stand-deploy-cli")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(boto3.Session(profile_name=args.profile, region_name=REGION), args.apply), sort_keys=True))
