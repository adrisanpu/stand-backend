"""Account-wide prize definitions shared by game-specific prize assignments."""
import uuid

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


MAX_SHARED_PRIZES = 100
MAX_PRIZE_NAME_LENGTH = 120


def _identity(name, image_key):
    return name, image_key


def _owner_snapshot_condition(game, fields, owner_id, values):
    conditions = ["ownerUserId = :owner"]
    values[":owner"] = owner_id
    snapshots = {
        "prizeCatalog": "updatedAt",
        "stackPrizeProgram": "stackPrizeRevision",
    }
    for field in fields:
        snapshot = snapshots[field]
        if snapshot in game:
            marker = f":current_{snapshot}"
            values[marker] = game[snapshot]
            conditions.append(f"{snapshot} = {marker}")
        else:
            conditions.append(f"attribute_not_exists({snapshot})")
    return " AND ".join(conditions)


def _read_games(games_table, owner_id):
    items = []
    kwargs = {
        "IndexName": "gsi-ownerUserId",
        "KeyConditionExpression": Key("ownerUserId").eq(owner_id),
    }
    while True:
        response = games_table.query(**kwargs)
        items.extend(response.get("Items") or [])
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return items
        kwargs["ExclusiveStartKey"] = last_key


def migrate(users_table, games_table, owner_id):
    """Create the shared definitions from existing per-game STAND prize entries."""
    user = users_table.get_item(Key={"userId": owner_id}, ConsistentRead=True).get("Item")
    if not user:
        raise ValueError("Prize catalog owner was not found.")
    if user.get("sharedPrizeCatalogState") == "ready":
        return user.get("sharedPrizeCatalog") or []

    definitions = user.get("sharedPrizeCatalog")
    if not isinstance(definitions, list):
        definitions = []
        by_identity = {}
        for game in _read_games(games_table, owner_id):
            entries = []
            for prize in (game.get("prizeCatalog") or {}).values():
                if isinstance(prize, dict) and isinstance(prize.get("name"), str) and prize["name"]:
                    entries.append(prize)
            program = game.get("stackPrizeProgram") or {}
            for prize in program.get("catalog", []) if isinstance(program, dict) else []:
                if isinstance(prize, dict) and prize.get("source") == "stand" and isinstance(prize.get("name"), str) and prize["name"]:
                    entries.append(prize)
            for prize in entries:
                identity = _identity(prize["name"], prize.get("imageKey"))
                if identity not in by_identity:
                    by_identity[identity] = str(uuid.uuid4())
                    definitions.append({
                        "id": by_identity[identity],
                        "name": prize["name"],
                        "imageKey": prize.get("imageKey"),
                    })
        if len(definitions) > MAX_SHARED_PRIZES:
            raise ValueError("The existing catalog exceeds the shared prize limit.")
        try:
            users_table.update_item(
                Key={"userId": owner_id},
                UpdateExpression="SET sharedPrizeCatalog = :catalog, sharedPrizeCatalogRevision = :revision, sharedPrizeCatalogState = :pending",
                ConditionExpression="attribute_not_exists(sharedPrizeCatalogState)",
                ExpressionAttributeValues={
                    ":catalog": definitions,
                    ":revision": 1,
                    ":pending": "pending",
                },
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
            user = users_table.get_item(Key={"userId": owner_id}, ConsistentRead=True).get("Item") or {}
            definitions = user.get("sharedPrizeCatalog")
            if not isinstance(definitions, list):
                raise RuntimeError("Could not resume shared prize catalog migration.") from exc

    by_identity = {
        _identity(prize["name"], prize.get("imageKey")): prize["id"]
        for prize in definitions
        if isinstance(prize, dict) and isinstance(prize.get("name"), str) and isinstance(prize.get("id"), str)
    }
    for game in _read_games(games_table, owner_id):
        update = False
        catalog = game.get("prizeCatalog")
        if isinstance(catalog, dict):
            catalog = {**catalog}
            for slot, prize in catalog.items():
                if not isinstance(prize, dict) or prize.get("sharedPrizeId"):
                    continue
                shared_id = by_identity.get(_identity(prize.get("name"), prize.get("imageKey")))
                if shared_id:
                    catalog[slot] = {**prize, "sharedPrizeId": shared_id}
                    update = True
        program = game.get("stackPrizeProgram")
        if isinstance(program, dict) and isinstance(program.get("catalog"), list):
            program = {**program, "catalog": [dict(prize) if isinstance(prize, dict) else prize
                for prize in program["catalog"]]}
            for prize in program["catalog"]:
                if not isinstance(prize, dict) or prize.get("source") != "stand" or prize.get("sharedPrizeId"):
                    continue
                shared_id = by_identity.get(_identity(prize.get("name"), prize.get("imageKey")))
                if shared_id:
                    prize["sharedPrizeId"] = shared_id
                    update = True
        if update:
            values = {":owner": owner_id}
            expression = "SET "
            assignments = []
            if isinstance(catalog, dict):
                assignments.append("prizeCatalog = :catalog")
                values[":catalog"] = catalog
            if isinstance(program, dict):
                assignments.append("stackPrizeProgram = :program")
                values[":program"] = program
            expression += ", ".join(assignments)
            fields = [
                field for field, value in (
                    ("prizeCatalog", catalog), ("stackPrizeProgram", program),
                ) if isinstance(value, dict)
            ]
            try:
                games_table.update_item(
                    Key={"gameId": game["gameId"]},
                    UpdateExpression=expression,
                    ConditionExpression=_owner_snapshot_condition(game, fields, owner_id, values),
                    ExpressionAttributeValues=values,
                )
            except ClientError as exc:
                raise RuntimeError("Could not migrate existing prize assignments.") from exc

    users_table.update_item(
        Key={"userId": owner_id},
        UpdateExpression="SET sharedPrizeCatalogState = :ready",
        ExpressionAttributeValues={":ready": "ready"},
    )
    return definitions


def validate(raw):
    if not isinstance(raw, list) or len(raw) > MAX_SHARED_PRIZES:
        raise ValueError("prizes must be a list with no more than 100 entries.")
    result = []
    ids = set()
    for prize in raw:
        if not isinstance(prize, dict) or set(prize) - {"id", "name", "imageKey"}:
            raise ValueError("Invalid shared prize.")
        prize_id = prize.get("id")
        name = prize.get("name")
        image_key = prize.get("imageKey")
        if (not isinstance(prize_id, str) or not prize_id or len(prize_id) > 80
                or not isinstance(name, str) or not name.strip()
                or len(name.strip()) > MAX_PRIZE_NAME_LENGTH
                or (image_key is not None and not isinstance(image_key, str))):
            raise ValueError("Invalid shared prize.")
        if prize_id in ids:
            raise ValueError("Shared prize IDs must be unique.")
        ids.add(prize_id)
        result.append({"id": prize_id, "name": name.strip(), "imageKey": image_key})
    return result


def synchronize(games_table, owner_id, definitions):
    """Refresh denormalized display data while preserving game-specific limits and stock."""
    by_id = {prize["id"]: prize for prize in definitions}
    for game in _read_games(games_table, owner_id):
        catalog = game.get("prizeCatalog")
        next_catalog = None
        if isinstance(catalog, dict):
            next_catalog = {**catalog}
            for slot, prize in catalog.items():
                if not isinstance(prize, dict):
                    continue
                shared = by_id.get(prize.get("sharedPrizeId"))
                if shared and (prize.get("name") != shared["name"] or prize.get("imageKey") != shared.get("imageKey")):
                    next_catalog[slot] = {**prize, "name": shared["name"], "imageKey": shared.get("imageKey")}
        program = game.get("stackPrizeProgram")
        next_program = None
        if isinstance(program, dict) and isinstance(program.get("catalog"), list):
            next_program = {**program, "catalog": [dict(prize) if isinstance(prize, dict) else prize
                for prize in program["catalog"]]}
            for prize in next_program["catalog"]:
                if not isinstance(prize, dict) or prize.get("source") != "stand":
                    continue
                shared = by_id.get(prize.get("sharedPrizeId"))
                if shared and (prize.get("name") != shared["name"] or prize.get("imageKey") != shared.get("imageKey")):
                    prize.update({"name": shared["name"], "imageKey": shared.get("imageKey")})
        changes = {}
        if next_catalog is not None and next_catalog != catalog:
            changes["prizeCatalog"] = next_catalog
        if next_program is not None and next_program != program:
            changes["stackPrizeProgram"] = next_program
        if changes:
            update_expression = "SET " + ", ".join(f"{key} = :{key}" for key in changes)
            values = {f":{key}": value for key, value in changes.items()}
            condition = _owner_snapshot_condition(game, changes, owner_id, values)
            try:
                games_table.update_item(
                    Key={"gameId": game["gameId"]},
                    UpdateExpression=update_expression,
                    ConditionExpression=condition,
                    ExpressionAttributeValues=values,
                )
            except ClientError as exc:
                raise RuntimeError("Could not synchronize shared prize assignments.") from exc
