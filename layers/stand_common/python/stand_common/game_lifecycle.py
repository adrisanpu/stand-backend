"""Reversible event closure and archival, backed by the existing prize outbox."""
import hashlib
import json
import os
import uuid

from stand_common import stack_prizes as prizes
from stand_common import prize_inventory
from stand_common.utils import _iso_now, get_game_type_blob, log


def state(meta):
    if meta.get("lifecycleState"):
        return meta["lifecycleState"]
    if meta.get("stackPrizeState") == "closing":
        return "closing"
    return "active" if meta.get("isActive", True) else "inactive"


def current(dynamo, game_id):
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": game_id}, ConsistentRead=True,
    ).get("Item")
    if not meta:
        raise prizes.PrizeError("GameNotFound", 404)
    return meta


def change_active(dynamo, meta, active):
    if meta.get("archivedAt"):
        raise prizes.PrizeError("GameArchived")
    if active:
        if state(meta) == "closing" or pending_calculations(dynamo, meta["gameId"]):
            raise prizes.PrizeError("GameClosing")
        if state(meta) == "active":
            return meta
        expression = ", isActive = :active, lifecycleState = :state, updatedAt = :now"
        values = {":active": True, ":state": "active", ":now": _iso_now()}
        if meta.get("gameType") == "STACK":
            expression += ", stackPrizeState = :open"
            values[":open"] = "open"
        prizes.transact(dynamo, [prizes.game_update(meta, expression, values)])
    elif meta.get("lifecycleState") == "inactive":
        return meta
    elif state(meta) != "closing":
        cycle = uuid.uuid4().hex
        task = {"gameId": meta["gameId"], "id": "lifecycle_" + cycle,
                "kind": "lifecycle", "cycle": cycle, "work": "pending", "dueAt": 0,
                "prizeCatalog": meta.get("prizeCatalog") or {}}
        expression = (", isActive = :active, lifecycleState = :state, closeCycle = :cycle, "
                      "updatedAt = :now, closingStartedAt = :now")
        values = {":active": False, ":state": "closing", ":cycle": cycle, ":now": _iso_now()}
        if meta.get("gameType") == "STACK":
            expression += ", stackPrizeState = :closing"
            values[":closing"] = "closing"
            if "stackPrizeProgram" not in meta:
                program = prizes.upgrade_program(prizes.legacy_program(meta))
                expression += ", stackPrizeProgram = :program, stackPrizeStock = :stock"
                values.update({":program": program, ":stock": prizes.initial_stock(
                    program, prizes.records(dynamo, meta["gameId"]))})
        prizes.transact(dynamo, [prizes.game_update(meta, expression, values), prizes.put(task)])
        prizes.enqueue(task)
    return current(dynamo, meta["gameId"])


def pending_calculations(dynamo, game_id):
    return any(row.get("work") == "pending" and row.get("kind") in ("grant", "close")
               for row in prizes.records(dynamo, game_id))


def archive(dynamo, meta):
    lifecycle_state = state(meta)
    if lifecycle_state not in ("closing", "inactive") or meta.get("isActive") is not False:
        raise prizes.PrizeError("GameMustBeInactive")
    if meta.get("archivedAt"):
        return
    expression = ", archivedAt = :now, updatedAt = :now, lifecycleState = :inactiveState"
    values = {":now": _iso_now(), ":inactive": False, ":inactiveState": "inactive"}
    extra = " AND isActive = :inactive"
    if lifecycle_state == "closing":
        if meta.get("lifecycleState") == "closing" and meta.get("closeCycle"):
            values.update({":closing": "closing", ":cycle": meta["closeCycle"]})
            extra += " AND lifecycleState = :closing AND closeCycle = :cycle"
        if meta.get("gameType") == "STACK":
            expression += ", stackPrizeState = :closed"
            values.update({":closed": "closed", ":stackClosing": "closing"})
            extra += " AND stackPrizeState = :stackClosing"
        elif meta.get("lifecycleState") != "closing" or not meta.get("closeCycle"):
            raise prizes.PrizeError("GameMustBeInactive")
    elif meta.get("lifecycleState"):
        values[":currentState"] = "inactive"
        extra += " AND lifecycleState = :currentState"
    else:
        extra += " AND attribute_not_exists(lifecycleState)"
    prizes.transact(dynamo, [prizes.game_update(meta, expression, values, extra=extra)])


def unarchive(dynamo, meta):
    if meta.get("deletionStartedAt"):
        raise prizes.PrizeError("GameDeletionInProgress")
    if not meta.get("archivedAt") or state(meta) != "inactive" or meta.get("isActive") is not False:
        raise prizes.PrizeError("GameMustBeArchived")
    prizes.transact(dynamo, [prizes.game_update(
        meta, ", updatedAt = :now REMOVE archivedAt", {":now": _iso_now(), ":inactive": False},
        extra=" AND isActive = :inactive AND attribute_exists(archivedAt) AND attribute_not_exists(deletionStartedAt)",
    )])
    return current(dynamo, meta["gameId"])


def finish(dynamo, row):
    meta = current(dynamo, row["gameId"])
    if meta.get("archivedAt") or meta.get("deletionStartedAt") or meta.get("deletedAt"):
        complete(dynamo, row)
        return
    if meta.get("closeCycle") != row["cycle"] or state(meta) != "closing":
        complete(dynamo, row)
        return
    if meta.get("gameType") == "STACK":
        prizes.close(dynamo, meta, [])
        meta = current(dynamo, row["gameId"])
        if meta.get("stackPrizeState") != "closed":
            return
    else:
        settle_native(dynamo, {**meta, "prizeCatalog": row.get("prizeCatalog", meta.get("prizeCatalog"))})
        meta = current(dynamo, row["gameId"])
    if meta.get("archivedAt") or meta.get("deletionStartedAt") or meta.get("deletedAt"):
        complete(dynamo, row)
        return
    if pending_calculations(dynamo, row["gameId"]):
        return
    prizes.transact(dynamo, [prizes.game_update(
        meta, ", lifecycleState = :state, updatedAt = :now",
        {":state": "inactive", ":now": _iso_now(), ":cycle": row["cycle"], ":closing": "closing"},
        extra=" AND closeCycle = :cycle AND lifecycleState = :closing",
    )])
    complete(dynamo, row)


def complete(dynamo, row):
    prizes.table(dynamo).update_item(
        Key={"gameId": row["gameId"], "id": row["id"]},
        UpdateExpression="REMOVE #w, dueAt", ExpressionAttributeNames={"#w": "work"},
    )


def native_identity(slot, prize):
    return hashlib.sha256(json.dumps(
        [slot, prize["sharedPrizeId"]] if prize.get("sharedPrizeId")
        else [slot, prize["name"], prize.get("imageKey")], sort_keys=True,
    ).encode()).hexdigest()


def native_matches(slot, prize, row):
    previous = row.get("prize") or {}
    if previous.get("id") == native_identity(slot, prize):
        return True
    if row.get("condition") != slot:
        return False
    pid = prize.get("sharedPrizeId")
    return bool(pid) and (
        previous.get("sharedPrizeId") == pid or row.get("inventoryPrizeId") == pid
        or not previous.get("sharedPrizeId") and previous.get("name") == prize.get("name")
        and previous.get("imageKey") == prize.get("imageKey")
    )


def issue_native_award(dynamo, meta, player, slot, _attempt=0):
    """Persist a confirmed legacy prize immediately; closing remains the recovery path."""
    catalog = meta.get("prizeCatalog") or {}
    prize = catalog.get(slot) or {}
    if not prize.get("name") or state(meta) == "closing" or not meta.get("isActive", True):
        return None

    identity = native_identity(slot, prize)
    award_id = "award_" + hashlib.sha256(
        f"{identity}:{int(player['playerId'])}".encode()).hexdigest()
    table = prizes.table(dynamo)
    existing = table.get_item(
        Key={"gameId": meta["gameId"], "id": award_id}, ConsistentRead=True,
    ).get("Item")
    if existing:
        return existing

    records = prizes.records(dynamo, meta["gameId"])
    previous = next((row for row in records if row.get("playerId") == player["playerId"]
                     and row.get("id", "").startswith("award_") and native_matches(slot, prize, row)), None)
    if previous:
        return previous
    available = prize_inventory.capacity(dynamo, meta["ownerUserId"], [{**prize, "id": identity}], meta)
    if available is not None and not available[identity]["remaining"]:
        log("native_prize_not_awarded", {"gameId": meta["gameId"], "slot": slot, "reason": available[identity]["reason"]})
        return None
    existing_count = sum(1 for row in records if native_matches(slot, prize, row))
    limit = prize.get("limit")
    if limit is not None and existing_count >= int(limit):
        return None

    game_type = (meta.get("gameType") or "").upper()
    blob = get_game_type_blob(player, game_type)
    delivered_attr = (
        "validationPrizeDelivered" if slot == "validation"
        else "rankingPrizeDelivered" if slot == "ranking"
        else "prizeDelivered"
    )
    award = {
        "gameId": meta["gameId"], "id": award_id,
        "playerId": int(player["playerId"]), "ruleId": "native_" + slot,
        "condition": slot, "ownerUserId": meta["ownerUserId"], "createdAt": _iso_now(),
        "prize": {**prize, "id": identity, "source": "stand"},
        "state": "delivered" if blob.get(delivered_attr) else "available",
    }
    if award["state"] == "available":
        award.update(emailState="pending", work="pending", dueAt=0)

    claim_attribute = "nativePrizeClaim_" + identity
    expression = ", #claim = if_not_exists(#claim, :zero) + :one"
    values = {":zero": 0, ":one": 1, ":active": True}
    extra = " AND (attribute_not_exists(isActive) OR isActive = :active)"
    if limit is not None:
        values[":limit"] = int(limit)
        extra += " AND (attribute_not_exists(#claim) OR #claim < :limit)"

    try:
        prizes.transact(dynamo, [
            prizes.game_update(meta, expression, values, {"#claim": claim_attribute}, extra),
            prizes.put(award),
        ])
    except prizes.PrizeError as exc:
        existing = table.get_item(
            Key={"gameId": meta["gameId"], "id": award_id}, ConsistentRead=True,
        ).get("Item")
        if existing:
            return existing
        if _attempt < 2 and exc.code in ("StackPrizeConflict", "InventoryChanged", "EventLimitReached"):
            return issue_native_award(dynamo, current(dynamo, meta["gameId"]), player, slot, _attempt + 1)
        raise
    if award["state"] == "available":
        prizes.enqueue(award)
    return award


def settle_native(dynamo, meta):
    """Snapshot only configured rewards and already confirmed results."""
    catalog = meta.get("prizeCatalog") or {}
    if not catalog:
        return
    game_type = meta["gameType"]
    cycle = meta["closeCycle"]
    players = prizes.players(dynamo, meta["gameId"])
    cutoff = meta["closingStartedAt"]
    ranked = [p for p in players
              if get_game_type_blob(p, game_type).get("score") is not None
              and get_game_type_blob(p, game_type).get("timer") is not None
              and get_game_type_blob(p, game_type).get("scoreUpdatedAt", "") <= cutoff]
    ranked.sort(key=lambda p: (get_game_type_blob(p, game_type)["score"], int(p["playerId"])))
    ranks = {p["playerId"]: i for i, p in enumerate(ranked, 1)}
    raffle_dates = {}
    for winner in meta.get("raffleWinners", []):
        if winner.get("wonAt", "") <= cutoff:
            pid = winner.get("playerId")
            raffle_dates[pid] = min(raffle_dates.get(pid, winner.get("wonAt", "")), winner.get("wonAt", ""))
    for slot, prize in catalog.items():
        if not prize.get("name"):
            continue
        identity = native_identity(slot, prize)
        winners = []
        for player in players:
            blob = get_game_type_blob(player, game_type)
            if slot == "validation" and player.get("validated") and player.get("validatedAt", "") <= cutoff:
                winners.append(player)
            elif slot == "raffle" and (
                player["playerId"] in raffle_dates
                or blob.get("raffleWin") and blob.get("raffleWinAt", "") <= cutoff
            ):
                winners.append(player)
            elif slot == "ranking" and game_type == "T1MER" and ranks.get(player["playerId"], float("inf")) <= prize.get("topX", 3):
                winners.append(player)
        winners.sort(key=lambda p: (
            ranks.get(p["playerId"], 0) if slot == "ranking" else
            p.get("validatedAt", "") if slot == "validation" else
            raffle_dates.get(p["playerId"], get_game_type_blob(p, game_type).get("raffleWinAt", "")),
            int(p["playerId"]),
        ))
        existing = [r for r in prizes.records(dynamo, meta["gameId"]) if native_matches(slot, prize, r)]
        awarded = {r["playerId"] for r in existing}
        remaining = max(0, prize["limit"] - len(existing)) if prize.get("limit") is not None else len(winners)
        for player in winners:
            if player["playerId"] in awarded or not remaining:
                continue
            available = prize_inventory.capacity(dynamo, meta["ownerUserId"], [{**prize, "id": identity}], meta)
            if available is not None and not available[identity]["remaining"]:
                log("native_prize_not_awarded", {"gameId": meta["gameId"], "slot": slot, "reason": available[identity]["reason"]})
                break
            blob = get_game_type_blob(player, game_type)
            delivered_attr = "validationPrizeDelivered" if slot == "validation" else "rankingPrizeDelivered" if slot == "ranking" else "prizeDelivered"
            award = {
                "gameId": meta["gameId"], "id": "award_" + hashlib.sha256(
                    f"{identity}:{int(player['playerId'])}".encode()).hexdigest(),
                "playerId": int(player["playerId"]), "ruleId": "native_" + slot,
                "condition": slot, "ownerUserId": meta["ownerUserId"], "createdAt": _iso_now(),
                "prize": {**prize, "id": identity, "source": "stand"},
                "state": "delivered" if blob.get(delivered_attr) else "available",
            }
            if award["state"] == "available":
                award.update(emailState="pending", work="pending", dueAt=0)
            meta = current(dynamo, meta["gameId"])
            if (meta.get("archivedAt") or meta.get("deletionStartedAt") or meta.get("deletedAt")
                    or meta.get("closeCycle") != cycle or state(meta) != "closing"):
                return
            operations = [prizes.game_update(meta, ""), prizes.put(award)]
            if "type" in player:
                operations.append({"Update": {
                    "TableName": os.environ["GAMEPLAYER_TABLE"],
                    "Key": {"gameId": meta["gameId"], "playerId": player["playerId"]},
                    "UpdateExpression": "SET #t = :snapshot", "ConditionExpression": "#t = :snapshot",
                    "ExpressionAttributeNames": {"#t": "type"},
                    "ExpressionAttributeValues": {":snapshot": player["type"]},
                }})
            prizes.transact(dynamo, operations)
            remaining -= 1
