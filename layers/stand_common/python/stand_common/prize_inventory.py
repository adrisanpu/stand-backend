"""Owner-scoped inventory and immutable movements, independent of event retention."""
import base64
import hashlib
import json
import os
from collections import defaultdict
from decimal import Decimal

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from stand_common.utils import _iso_now, _json_sanitize

MAX_UNITS = 9007199254740991


class InventoryError(Exception):
    def __init__(self, code, status=409):
        super().__init__(code)
        self.code, self.status = code, status


def enabled():
    return bool(os.environ.get("PRIZE_INVENTORY_TABLE"))


def table(dynamo):
    return dynamo.Table(os.environ["PRIZE_INVENTORY_TABLE"])


def digest(value):
    return hashlib.sha256(json.dumps(_json_sanitize(value), sort_keys=True).encode()).hexdigest()


def prize_id(prize):
    if prize.get("source") == "kivly":
        if prize.get("locationId") and prize.get("rewardId"):
            return "kivly_" + digest([prize["locationId"], prize["rewardId"]])
        return None
    return prize.get("sharedPrizeId")


def balance(dynamo, owner, pid):
    return table(dynamo).get_item(
        Key={"ownerId": owner, "key": "BAL#" + pid}, ConsistentRead=True,
    ).get("Item") or {
        "ownerId": owner, "key": "BAL#" + pid, "prizeId": pid,
        "initialized": False, "available": 0, "assigned": 0, "redeemed": 0,
        "historicalAssigned": 0, "historicalRedeemed": 0, "revision": 0,
    }


def snapshot_put(row, previous):
    revision = int(previous.get("revision", 0))
    return {"Put": {
        "TableName": os.environ["PRIZE_INVENTORY_TABLE"],
        "Item": {**row, "revision": revision + 1},
        "ConditionExpression": "#revision = :revision" if revision else "attribute_not_exists(#revision)",
        "ExpressionAttributeNames": {"#revision": "revision"},
        **({"ExpressionAttributeValues": {":revision": revision}} if revision else {}),
    }}


def write(dynamo, operations):
    if len(operations) > 100:
        raise InventoryError("InventoryTransactionTooLarge", 400)
    try:
        dynamo.meta.client.transact_write_items(TransactItems=operations)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            raise InventoryError("InventoryChanged") from None
        raise


def integer(value, minimum=0):
    return (type(value) in (int, Decimal) and not isinstance(value, bool)
            and (not isinstance(value, Decimal) or value.is_finite())
            and value == int(value) and minimum <= value <= MAX_UNITS)


def reservation_limits(dynamo, owner, meta):
    """Configured event quantities are exclusive, including inactive rule selections."""
    program = meta.get("stackPrizeProgram")
    catalog = program.get("catalog", []) if isinstance(program, dict) else list((meta.get("prizeCatalog") or {}).values())
    limits = {}
    for prize in catalog:
        if not prize.get("name"):
            continue
        pid = _resolve(dynamo, owner, prize)
        if not pid:
            continue
        value = prize.get("stockTotal") if isinstance(program, dict) else prize.get("limit")
        if value is not None and not integer(value):
            raise InventoryError("InvalidEventReservation", 400)
        previous = limits.get(pid, 0)
        limits[pid] = None if value is None or previous is None else previous + int(value)
        if limits[pid] is not None and limits[pid] > MAX_UNITS:
            raise InventoryError("InventoryTooLarge", 400)
    return limits


def event_usage(dynamo, owner, pid):
    """Immutable movements preserve event consumption even after awards are deleted."""
    usage, cursor = {}, None
    while True:
        result = page(dynamo, owner, "MOVE#" + pid + "#", cursor, 100)
        for movement in result["items"]:
            if movement.get("kind") != "assignment" or not movement.get("gameId"):
                continue
            entry = usage.setdefault(movement["gameId"], {"assigned": 0, "redeemed": 0})
            entry["assigned"] += int(movement["quantity"])
            entry["redeemed"] += sum(unit.get("state") == "delivered" for unit in movement.get("units", {}).values())
        cursor = result["cursor"]
        if not cursor:
            return usage


def capacity(dynamo, owner, catalog, meta=None):
    """Return shared availability for selection; the transaction rechecks snapshots."""
    if not enabled():
        return None
    limits = reservation_limits(dynamo, owner, meta) if meta else {}
    result = {}
    usage = {}
    for prize in catalog:
        pid = _resolve(dynamo, owner, prize)
        if pid in limits:
            if limits[pid] is None:
                remaining, reason = 0, "EventReservationRequired"
            else:
                if pid not in usage:
                    usage[pid] = event_usage(dynamo, owner, pid).get(meta["gameId"], {}).get("assigned", 0)
                remaining = max(0, limits[pid] - usage[pid])
                reason = "EventLimitReached" if remaining == 0 else None
            result[prize["id"]] = {"prizeId": pid, "remaining": remaining, "reason": reason}
            continue
        result[prize["id"]] = {
            "prizeId": pid, "remaining": 0,
            "reason": "InventoryUnlinked" if not pid else "EventReservationRequired",
        }
    return result


def _resolve(dynamo, owner, prize):
    pid = prize_id(prize)
    if pid:
        return pid
    user = dynamo.Table(os.environ["USERS_TABLE"]).get_item(
        Key={"userId": owner}, ConsistentRead=True,
    ).get("Item") or {}
    matches = [p for p in user.get("sharedPrizeCatalog", [])
               if p["name"] == prize.get("name") and p.get("imageKey") == prize.get("imageKey")]
    return matches[0]["id"] if len(matches) == 1 else None


def award_operations(dynamo, meta, rows, historical=False):
    """At most two additional writes per distinct prize, not per awarded unit."""
    if not enabled() or not rows:
        return []
    owner = meta["ownerUserId"]
    limits = reservation_limits(dynamo, owner, meta)
    grouped = defaultdict(list)
    for row in rows:
        pid = _resolve(dynamo, owner, row["prize"])
        if not pid:
            raise InventoryError("InventoryUnlinked")
        grouped[pid].append(row)
    operations = []
    for pid, units in grouped.items():
        old = balance(dynamo, owner, pid)
        if not historical:
            if limits.get(pid) is None:
                raise InventoryError("EventReservationRequired")
            used = event_usage(dynamo, owner, pid).get(meta["gameId"], {}).get("assigned", 0)
            if used + len(units) > limits[pid]:
                raise InventoryError("EventLimitReached")
        count = len(units)
        now = min(row.get("createdAt") or _iso_now() for row in units)
        key = f"MOVE#{pid}#{now}#{digest(sorted(row['id'] for row in units))}"
        redeemed = sum(row.get("state") == "delivered" and row["prize"]["source"] == "stand" for row in units)
        movement = {
            "ownerId": owner, "key": key, "prizeId": pid, "kind": "assignment",
            "quantity": count, "createdAt": now, "historical": historical,
            "gameId": meta["gameId"], "gameName": meta.get("gameName") or meta["gameId"],
            "source": units[0]["prize"].get("source", "stand"), "name": units[0]["prize"]["name"],
            "units": {row["id"]: {"state": row["state"], "condition": row["condition"],
                                **({"deliveredAt": row["deliveredAt"]} if row.get("deliveredAt") else {})}
                      for row in units},
        }
        new = {**old}
        new.update(name=units[0]["prize"]["name"], source=units[0]["prize"].get("source", "stand"))
        field = "historicalAssigned" if historical else "assigned"
        new[field] = old[field] + count
        field = "historicalRedeemed" if historical else "redeemed"
        new[field] = old[field] + redeemed
        operations.extend([
            snapshot_put(new, old),
            {"Put": {"TableName": os.environ["PRIZE_INVENTORY_TABLE"], "Item": movement,
                     "ConditionExpression": "attribute_not_exists(#key)", "ExpressionAttributeNames": {"#key": "key"}}},
        ])
        for row in units:
            row.update(inventoryPrizeId=pid, inventoryMovementKey=key,
                       inventoryEra="historical" if historical else "general")
    return operations


def redemption_operations(dynamo, row, now):
    if not enabled() or not row.get("inventoryMovementKey"):
        return []
    owner, pid = row["ownerUserId"], row["inventoryPrizeId"]
    old = balance(dynamo, owner, pid)
    field = "historicalRedeemed" if row["inventoryEra"] == "historical" else "redeemed"
    return [
        snapshot_put({**old, field: old[field] + 1}, old),
        {"Update": {
            "TableName": os.environ["PRIZE_INVENTORY_TABLE"],
            "Key": {"ownerId": owner, "key": row["inventoryMovementKey"]},
            "UpdateExpression": "SET units.#award.#state = :delivered, units.#award.deliveredAt = :now",
            "ConditionExpression": "units.#award.#state = :available",
            "ExpressionAttributeNames": {"#award": row["id"], "#state": "state"},
            "ExpressionAttributeValues": {":delivered": "delivered", ":available": "available", ":now": now},
        }},
    ]


def update_delivery(dynamo, row, state, update):
    """Keep assignment evidence with the authoritative delivery transition."""
    if not enabled() or not row.get("inventoryMovementKey"):
        dynamo.Table(os.environ["STACK_PRIZES_TABLE"]).update_item(**update)
        return
    try:
        write(dynamo, [
            {"Update": {"TableName": os.environ["STACK_PRIZES_TABLE"], **update}},
            {"Update": {
                "TableName": os.environ["PRIZE_INVENTORY_TABLE"],
                "Key": {"ownerId": row["ownerUserId"], "key": row["inventoryMovementKey"]},
                "UpdateExpression": "SET units.#award.#state = :state",
                "ConditionExpression": "attribute_exists(units.#award)",
                "ExpressionAttributeNames": {"#award": row["id"], "#state": "state"},
                "ExpressionAttributeValues": {":state": state},
            }},
        ])
    except InventoryError as exc:
        from stand_common.stack_prizes import PrizeError
        raise PrizeError(exc.code, exc.status) from None


def import_award(dynamo, meta, row):
    if not enabled() or row.get("inventoryMovementKey"):
        return
    previous_state = row["state"]
    operations = award_operations(dynamo, meta, [row], historical=True)
    operations.append({"Update": {
        "TableName": os.environ["STACK_PRIZES_TABLE"],
        "Key": {"gameId": row["gameId"], "id": row["id"]},
        "UpdateExpression": "SET inventoryMovementKey = :movement, inventoryPrizeId = :prize, inventoryEra = :era",
        "ConditionExpression": "attribute_not_exists(inventoryMovementKey) AND #state = :state",
        "ExpressionAttributeNames": {"#state": "state"},
        "ExpressionAttributeValues": {":movement": row["inventoryMovementKey"], ":prize": row["inventoryPrizeId"],
                                      ":era": "historical", ":state": previous_state},
    }})
    write(dynamo, operations)


def page(dynamo, owner, prefix, cursor=None, limit=30):
    args = {"KeyConditionExpression": Key("ownerId").eq(owner) & Key("key").begins_with(prefix),
            "Limit": limit, "ConsistentRead": True, "ScanIndexForward": False}
    if cursor:
        try:
            if not isinstance(cursor, str) or len(cursor) > 2048:
                raise ValueError()
            start = json.loads(base64.urlsafe_b64decode(cursor.encode()))
            if (set(start) != {"ownerId", "key"} or start["ownerId"] != owner
                    or not isinstance(start["key"], str) or not start["key"].startswith(prefix)):
                raise ValueError()
            args["ExclusiveStartKey"] = start
        except (ValueError, TypeError, UnicodeError):
            raise InventoryError("InvalidInventoryCursor", 400) from None
    result = table(dynamo).query(**args)
    last = result.get("LastEvaluatedKey")
    return {"items": result.get("Items", []),
            "cursor": base64.urlsafe_b64encode(json.dumps(last).encode()).decode() if last else None}


def project_legacy_player(dynamo, meta, player, existing_records=None):
    """Preserve confirmed pre-outbox claims, never mint a claim after cutover."""
    from stand_common import stack_prizes, game_lifecycle
    from stand_common.utils import get_game_type_blob
    records = stack_prizes.records(dynamo, meta["gameId"]) if existing_records is None else existing_records
    existing = [row for row in records
                if row.get("playerId") == player["playerId"] and row.get("id", "").startswith("award_")]
    blob = get_game_type_blob(player, meta.get("gameType", ""))
    candidates = []
    catalog = {} if meta.get("lifecycleState") or meta.get("stackPrizeProgram") else meta.get("prizeCatalog") or {}
    rank = None
    if "ranking" in catalog and meta.get("gameType") == "STACK":
        from stand_common.stack import get_ranking
        rank = next((r["rank"] for r in get_ranking(
            meta["gameId"], dynamo.Table(os.environ["GAMEPLAYER_TABLE"]), None,
        )["results"] if r["playerId"] == player["playerId"]), None)
    for slot, prize in catalog.items():
        delivered_attr = {"validation": "validationPrizeDelivered", "raffle": "prizeDelivered",
                          "ranking": "rankingPrizeDelivered"}.get(slot)
        eligible = (slot == "validation" and player.get("validated")
                    or slot == "raffle" and blob.get("raffleWin")
                    or slot == "ranking" and (blob.get(delivered_attr) or rank is not None
                        and rank <= prize.get("topX", 0) and (prize.get("limit") is None or rank <= prize["limit"])))
        if not eligible or not prize.get("name"):
            continue
        if any(game_lifecycle.native_matches(slot, prize, row)
               or meta.get("gameType") == "STACK" and row["prize"].get("id") == "legacy_" + slot
               for row in existing):
            continue
        identity = game_lifecycle.native_identity(slot, prize)
        candidates.append({
            "gameId": meta["gameId"], "id": "award_" + digest([identity, int(player["playerId"]), "historical"]),
            "ownerUserId": meta["ownerUserId"], "playerId": player["playerId"],
            "condition": slot, "ruleId": "native_" + slot,
            "state": "delivered" if blob.get(delivered_attr) else "available",
            "prize": {**prize, "source": "stand",
                      "id": "legacy_" + slot if meta.get("gameType") == "STACK" else identity},
        })
    registration_table = os.environ.get("KIVLY_REGISTRATION_TABLE")
    if registration_table and meta.get("gameType") == "STACK":
        reservation = dynamo.Table(registration_table).get_item(
            Key={"id": f"reward:{meta['gameId']}:{player['playerId']}"}, ConsistentRead=True,
        ).get("Item")
        if reservation and not any(row["prize"].get("id") == "legacy_kivly" for row in existing):
            candidates.append({
                "gameId": meta["gameId"], "id": "award_" + digest([reservation["id"], "historical"]),
                "ownerUserId": meta["ownerUserId"], "playerId": player["playerId"],
                "condition": "completed", "ruleId": "legacy_kivly",
                "state": "confirmed" if reservation["state"] == "confirmed" else "reconciliation_required",
                "createdAt": reservation.get("createdAt") or _iso_now(),
                "prize": {"id": "legacy_kivly", "source": "kivly", "name": reservation["name"],
                          "locationId": reservation["locationId"], "rewardId": reservation["rewardId"]},
            })
    for row in candidates:
        row.setdefault("createdAt", _iso_now())
        operations = award_operations(dynamo, meta, [row], historical=True)
        write(dynamo, [stack_prizes.put(row), *operations])


def legacy_claim(dynamo, meta, player, condition):
    """Resolve old signed tokens to the same auditable award used by new claims."""
    from stand_common import stack_prizes
    rows = stack_prizes.records(dynamo, meta["gameId"])
    return next((row for row in rows if row.get("id", "").startswith("award_")
                 and row.get("playerId") == player["playerId"] and row["prize"].get("source") == "stand"
                 and (row.get("condition") == condition
                      or condition == "ranking" and row["prize"].get("id") == "legacy_ranking")), None)


def native_unavailable(dynamo, meta, player):
    from stand_common import stack_prizes, game_lifecycle
    from stand_common.utils import get_game_type_blob
    catalog = meta.get("prizeCatalog") or {}
    stock = capacity(dynamo, meta["ownerUserId"], [{**p, "id": slot} for slot, p in catalog.items()], meta)
    if stock is None:
        return []
    blob = get_game_type_blob(player, meta.get("gameType", ""))
    rows = [r for r in stack_prizes.records(dynamo, meta["gameId"]) if r.get("id", "").startswith("award_")]
    result = []
    for slot, prize in catalog.items():
        if not (slot == "validation" and player.get("validated") or slot == "raffle" and blob.get("raffleWin")):
            continue
        if any(r["playerId"] == player["playerId"] and r.get("condition") == slot for r in rows):
            continue
        reason = stock[slot]["reason"]
        used = sum(bool(game_lifecycle.native_matches(slot, prize, r)) for r in rows)
        if prize.get("limit") is not None and used >= prize["limit"]:
            reason = "EventLimitReached"
        if reason:
            result.append({"condition": slot, "reason": reason})
    return result


def import_page(dynamo, owner, meta, cursor=None):
    from stand_common import stack_prizes
    game_id = meta["gameId"]
    previous = table(dynamo).get_item(
        Key={"ownerId": owner, "key": "IMPORT#" + game_id}, ConsistentRead=True,
    ).get("Item") or {}
    if isinstance(cursor, str) and cursor and previous.get("inputCursor") == cursor:
        return {"ok": True, "unresolved": int(previous.get("pageUnresolved", 0)),
                "cursor": previous.get("nextCursor")}
    if cursor and (not isinstance(cursor, str) or len(cursor) > 140
                   or previous.get("nextCursor") != cursor or previous.get("complete")):
        raise InventoryError("InvalidInventoryCursor", 400)
    stage, position = cursor.split(":", 1) if cursor else ("awards", "")
    args = {"KeyConditionExpression": Key("gameId").eq(game_id), "ConsistentRead": True, "Limit": 20}
    if stage == "awards":
        source = stack_prizes.table(dynamo)
        if position:
            args["ExclusiveStartKey"] = {"gameId": game_id, "id": position}
    else:
        source = dynamo.Table(os.environ["GAMEPLAYER_TABLE"])
        if position:
            args["ExclusiveStartKey"] = {"gameId": game_id, "playerId": int(position)}
    result = source.query(**args)
    unresolved = 0
    existing = stack_prizes.records(dynamo, game_id) if stage == "players" else None
    for row in result.get("Items", []):
        try:
            if stage == "players":
                project_legacy_player(dynamo, meta, row, existing)
            elif row.get("id", "").startswith("award_") and not row.get("inventoryMovementKey"):
                import_award(dynamo, meta, row)
        except InventoryError as exc:
            if exc.code != "InventoryUnlinked":
                raise
            unresolved += 1
    last = result.get("LastEvaluatedKey") or {}
    next_cursor = f"awards:{last['id']}" if stage == "awards" and last else (
        f"players:{last['playerId']}" if last else "players:" if stage == "awards" else None)
    marker = {"ownerId": owner, "key": "IMPORT#" + game_id, "complete": next_cursor is None,
              "nextCursor": next_cursor, "inputCursor": cursor, "pageUnresolved": unresolved,
              "unresolved": (int(previous.get("unresolved", 0)) if cursor else 0) + unresolved}
    write(dynamo, [snapshot_put(marker, previous)])
    return {"ok": True, "unresolved": unresolved, "cursor": next_cursor}


def owner_games(dynamo, owner):
    from stand_common.shared_prizes import _read_games
    result = []
    for candidate in _read_games(dynamo.Table(os.environ["GAMES_TABLE"]), owner):
        meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
            Key={"gameId": candidate["gameId"]}, ConsistentRead=True,
        ).get("Item") or {}
        if meta.get("ownerUserId") == owner and not meta.get("deletedAt"):
            result.append(meta)
    return result


def definitions(dynamo, owner, games=None):
    from stand_common import kivly_registration as registration
    from stand_common.stack_prizes import legacy_program, upgrade_program
    user = dynamo.Table(os.environ["USERS_TABLE"]).get_item(
        Key={"userId": owner}, ConsistentRead=True,
    ).get("Item") or {}
    entries = {p["id"]: {**p, "source": "stand", "games": []} for p in user.get("sharedPrizeCatalog", [])}
    conn = registration.connection(dynamo, owner)
    if conn:
        for reward in registration._client(conn).list_rewards(conn["locationId"], conn["ownerId"]):
            prize = {"source": "kivly", "locationId": conn["locationId"], "rewardId": reward["id"],
                     "name": reward["name"], "imageUrl": reward.get("imageUrl")}
            pid = prize_id(prize)
            entries[pid] = {**prize, "id": pid, "games": []}
    unresolved = 0
    for meta in owner_games(dynamo, owner) if games is None else games:
        program = meta.get("stackPrizeProgram")
        if meta.get("gameType") == "STACK":
            program = upgrade_program(program or legacy_program(meta))
        native = not isinstance(program, dict)
        catalog = list((meta.get("prizeCatalog") or {}).values()) if native else program.get("catalog", [])
        active_ids = None if native else {
            a["prizeId"] for r in program.get("rules", []) for g in r.get("groups", [])
            for a in g.get("allocations", [])
        }
        for prize in catalog:
            pid = _resolve(dynamo, owner, prize)
            if not pid:
                unresolved += 1
                continue
            if pid not in entries:
                entries[pid] = {**prize, "id": pid, "source": prize.get("source", "stand"), "games": []}
            game = {"gameId": meta["gameId"], "name": meta.get("gameName") or meta["gameId"],
                    "active": bool(meta.get("isActive", True)) and not meta.get("archivedAt")
                        and not meta.get("deletionStartedAt") and meta.get("lifecycleState", "active") == "active"
                        and (active_ids is None or prize.get("id") in active_ids) and prize.get("limit") != 0
                        and (native or prize.get("stockTotal") != 0),
                    "limit": prize.get("stockTotal") if not native else prize.get("limit")}
            associated = next((g for g in entries[pid]["games"] if g["gameId"] == game["gameId"]), None)
            if associated:
                associated["active"] = associated["active"] or game["active"]
                associated["limit"] = (None if associated["limit"] is None or game["limit"] is None
                                       else associated["limit"] + game["limit"])
            else:
                entries[pid]["games"].append(game)
    return entries, unresolved


def handle(event, method, dynamo):
    from stand_common.utils import _get_claims, _read_json_body, _resp, log
    from stand_common import stack_prizes
    from stand_common import kivly_registration as registration
    owner = _get_claims(event).get("sub")
    if not owner:
        return _resp(401, {"error": "Unauthorized"})
    if not enabled():
        return _resp(503, {"error": "InventoryUnavailable"})
    try:
        if method == "GET":
            query = event.get("queryStringParameters") or {}
            pid = query.get("prizeId")
            if pid:
                if not isinstance(pid, str) or not 1 <= len(pid) <= 80:
                    raise InventoryError("InvalidPrizeId", 400)
                result = page(dynamo, owner, "MOVE#" + pid + "#", query.get("cursor"))
                return _resp(200, _json_sanitize({"ok": True, **result}))
            games = owner_games(dynamo, owner)
            entries, unresolved = definitions(dynamo, owner, games)
            # Balance rows are bounded by catalog/reward definitions, not by award history.
            rows, cursor = [], None
            while True:
                result = page(dynamo, owner, "BAL#", cursor, 100)
                rows.extend(result["items"])
                cursor = result["cursor"]
                if not cursor:
                    break
            stored = {r["prizeId"]: r for r in rows}
            for pid in stored.keys() - entries.keys():
                if not stored[pid].get("initialized") and not any(
                    stored[pid].get(field, 0) for field in
                    ("available", "assigned", "redeemed", "historicalAssigned", "historicalRedeemed")
                ):
                    continue
                entries[pid] = {"id": pid, "name": pid, "source": "kivly" if pid.startswith("kivly_") else "stand", "games": []}
            catalog = []
            for pid, entry in entries.items():
                row = stored.get(pid) or balance(dynamo, owner, pid)
                if entry["name"] == pid:
                    entry.update(name=row.get("name", pid), source=row.get("source", entry["source"]))
                reservation = {"total": 0, "available": 0, "assigned": 0,
                               "redeemed": None if entry["source"] == "kivly" else 0, "error": None}
                if entry["games"] or row["assigned"] or row["historicalAssigned"]:
                    usage = event_usage(dynamo, owner, pid)
                    for game_id, consumed in usage.items():
                        if not any(game["gameId"] == game_id for game in entry["games"]):
                            entry["games"].append({
                                "gameId": game_id, "name": game_id, "active": False,
                                "limit": consumed["assigned"],
                            })
                    missing = any(game["limit"] is None for game in entry["games"])
                    for game in entry["games"]:
                        game.update(usage.get(game["gameId"], {"assigned": 0, "redeemed": 0}))
                        game["remaining"] = None if game["limit"] is None else max(0, game["limit"] - game["assigned"])
                    total = None if missing else sum(game["limit"] for game in entry["games"])
                    if total is not None and not integer(total):
                        raise InventoryError("InventoryTooLarge", 400)
                    reservation = {
                        "total": total,
                        "available": None if missing else sum(game["remaining"] for game in entry["games"]),
                        "assigned": sum(game["assigned"] for game in entry["games"]),
                        "redeemed": None if entry["source"] == "kivly" else sum(game["redeemed"] for game in entry["games"]),
                        "error": "EventReservationRequired" if missing else
                            "ReservationBelowAssigned" if any(game["assigned"] > game["limit"] for game in entry["games"]) else None,
                    }
                catalog.append({**entry, "inventory": row, "reservation": reservation,
                                "canjeados": None if entry["source"] == "kivly" else row["redeemed"] + row["historicalRedeemed"]})
            game_ids = sorted(game["gameId"] for game in games)
            import_rows = {gid: table(dynamo).get_item(
                Key={"ownerId": owner, "key": "IMPORT#" + gid}, ConsistentRead=True,
            ).get("Item", {}) for gid in game_ids}
            imports = {gid: bool(row.get("complete")) and not row.get("unresolved") for gid, row in import_rows.items()}
            unresolved += sum(int(row.get("unresolved", 0)) for row in import_rows.values())
            return _resp(200, _json_sanitize({"ok": True, "catalog": catalog,
                "mode": "reservations", "unresolved": unresolved, "imports": imports}))
        if method != "POST":
            return _resp(405, {"error": "MethodNotAllowed"})
        body = _read_json_body(event)
        if not isinstance(body, dict):
            raise InventoryError("InvalidInventoryRequest", 400)
        if body.get("action") not in ("import", "delete"):
            raise InventoryError("InventoryActionRetired", 410)
        if body.get("action") == "import" and set(body) <= {"action", "gameId", "cursor"}:
            game_id = body.get("gameId")
            if not isinstance(game_id, str):
                raise InventoryError("InvalidGameId", 400)
            meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(Key={"gameId": game_id}, ConsistentRead=True).get("Item")
            if not meta or meta.get("ownerUserId") != owner:
                raise InventoryError("Forbidden", 403)
            return _resp(200, import_page(dynamo, owner, meta, body.get("cursor")))
        pid = body.get("prizeId")
        if not isinstance(pid, str) or not 1 <= len(pid) <= 80:
            raise InventoryError("InvalidPrizeId", 400)
        entries, _ = definitions(dynamo, owner)
        if pid not in entries:
            raise InventoryError("PrizeNotFound", 404)
        if body.get("action") == "delete" and set(body) == {"action", "prizeId"}:
            entry = entries[pid]
            row = balance(dynamo, owner, pid)
            if entry["source"] != "kivly" or not entry.get("rewardId") or not entry.get("locationId"):
                raise InventoryError("InvalidPrizeDeletion", 400)
            if entry["games"] or row["initialized"] or any(
                row.get(field, 0) for field in
                ("available", "assigned", "redeemed", "historicalAssigned", "historicalRedeemed")
            ) or page(dynamo, owner, "MOVE#" + pid + "#", limit=1)["items"]:
                raise InventoryError("PrizeInUse")
            conn = registration.connection(dynamo, owner)
            if not conn or conn["locationId"] != entry["locationId"]:
                raise InventoryError("KivlyConnectionChanged")
            registration._client(conn).delete_reward(conn["locationId"], conn["ownerId"], entry["rewardId"])
            return _resp(200, {"ok": True})
        raise InventoryError("InvalidInventoryRequest", 400)
    except InventoryError as exc:
        return _resp(exc.status, {"error": exc.code})
    except stack_prizes.PrizeError as exc:
        return _resp(exc.status, {"error": exc.code})
    except ClientError as exc:
        log("prize_inventory_failed", {"ownerId": owner, "code": exc.response.get("Error", {}).get("Code")})
        return _resp(503, {"error": "InventoryUnavailable"})
    except registration.ERRORS as exc:
        status, payload = registration.error_response(exc)
        return _resp(status, payload)
