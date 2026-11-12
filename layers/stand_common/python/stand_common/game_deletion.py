"""Owner-confirmed, resumable removal of an archived event's local data."""
import os
import time

from boto3.dynamodb.conditions import Attr

from stand_common import client_index, game_lifecycle, stack_prizes as prizes, prize_inventory
from stand_common.email_verification import VerificationError, identity_id
from stand_common.utils import _iso_now, log


def _scoped_rows(table, game_id, check, legacy_rewards=False):
    condition = Attr("gameId").eq(game_id)
    if legacy_rewards:
        condition |= Attr("id").begins_with("reward:" + game_id + ":")
    query = {"FilterExpression": condition, "ConsistentRead": True}
    rows = []
    while True:
        check()
        page = table.scan(**query)
        rows.extend(page.get("Items") or [])
        if not page.get("LastEvaluatedKey"):
            return rows
        query["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def permanent_delete(dynamo, meta, s3, secret):
    game_id = meta["gameId"]
    if (not meta.get("archivedAt") or game_lifecycle.state(meta) != "inactive"
            or meta.get("isActive") is not False):
        raise prizes.PrizeError("GameMustBeArchived")
    required = (
        "EMAIL_VERIFICATION_TABLE", "KIVLY_REGISTRATION_TABLE", "CLIENT_INDEX_TABLE",
        "USER_MEDIA_BUCKET", "INFOCARDS_BUCKET",
    )
    if not all(os.environ.get(key) for key in required):
        raise prizes.PrizeError("GameDeletionUnavailable", 503)

    started = time.monotonic()
    deleting = bool(meta.get("deletionStartedAt"))
    operations = 0

    def check():
        nonlocal operations
        operations += 1
        if operations > 500 or time.monotonic() - started > 18:
            raise prizes.PrizeError("GameDeletionInProgress" if deleting else "GameDeletionCheckIncomplete")

    verification = dynamo.Table(os.environ["EMAIL_VERIFICATION_TABLE"])
    registration = dynamo.Table(os.environ["KIVLY_REGISTRATION_TABLE"])
    registration_rows = _scoped_rows(registration, game_id, check, legacy_rewards=True)
    rows = prizes.records(dynamo, game_id)
    if prize_inventory.enabled():
        for row in rows:
            if row.get("id", "").startswith("award_") and not row.get("inventoryMovementKey"):
                check()
                try:
                    prize_inventory.import_award(dynamo, meta, row)
                except prize_inventory.InventoryError as exc:
                    raise prizes.PrizeError("InventoryHistoryImportRequired") from exc
    participants = prizes.players(dynamo, game_id)
    try:
        identity_keys = {
            participant["playerId"]: identity_id(secret, game_id, participant["email"])
            for participant in participants if participant.get("email")
        }
    except VerificationError as exc:
        raise prizes.PrizeError("GameDeletionUnavailable", 503) from exc
    check()
    if not deleting:
        prizes.transact(dynamo, [prizes.game_update(
            meta, ", deletionStartedAt = :now, updatedAt = :now",
            {":now": _iso_now(), ":inactive": False},
            extra=" AND isActive = :inactive AND attribute_exists(archivedAt)",
        )])
        meta = game_lifecycle.current(dynamo, game_id)
        deleting = True

    media_bucket = os.environ["USER_MEDIA_BUCKET"]
    user = dynamo.Table(os.environ["USERS_TABLE"]).get_item(
        Key={"userId": meta["ownerUserId"]}, ConsistentRead=True,
    ).get("Item") or {}
    shared_images = {p["imageKey"] for p in user.get("sharedPrizeCatalog", []) if p.get("imageKey")}
    for bucket, prefix in (
        (media_bucket, f"players/{game_id}/"),
        (media_bucket, f"games/{meta['ownerUserId']}/{game_id}/"),
        (media_bucket, f"games/staged/{meta['ownerUserId']}/{game_id}/"),
        (os.environ["INFOCARDS_BUCKET"], f"infocards/{game_id}/"),
    ):
        query = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        while True:
            check()
            page = s3.list_objects_v2(**query)
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents") or [] if obj["Key"] not in shared_images]
            if objects:
                check()
                result = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
                if result.get("Errors"):
                    log("GameMediaDeletionFailed", {"gameId": game_id, "count": len(result["Errors"])})
                    raise prizes.PrizeError("GameMediaDeletionFailed", 503)
            if not page.get("IsTruncated"):
                break
            query["ContinuationToken"] = page["NextContinuationToken"]

    for row in _scoped_rows(verification, game_id, check):
        check()
        verification.delete_item(Key={"id": row["id"]})
    for participant in participants:
        check()
        client_index.remove_participation(
            dynamo, meta["ownerUserId"], game_id, participant, secret,
        )
        if participant["playerId"] in identity_keys:
            verification.delete_item(Key={"id": identity_keys[participant["playerId"]]})
        dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).delete_item(
            Key={"gameId": game_id, "playerId": participant["playerId"]})
    # Keep migration reservations until their authoritative awards are removed.
    # On a partial retry, a pending legacy reservation must not lose that evidence.
    for row in registration_rows:
        check()
        registration.delete_item(Key={"id": row["id"]})
    for row in rows:
        check()
        prizes.table(dynamo).delete_item(Key={"gameId": game_id, "id": row["id"]})

    check()
    # Reserve the join code without retaining event/player data: old signed
    # links must never resolve to a newly created event with the same code.
    prizes.transact(dynamo, [{"Put": {
        "TableName": os.environ["GAMES_TABLE"],
        "Item": {"gameId": game_id, "deletedAt": _iso_now(), "isActive": False},
        "ConditionExpression": "deletionStartedAt = :started AND stackPrizeRevision = :revision",
        "ExpressionAttributeValues": {
            ":started": meta["deletionStartedAt"], ":revision": meta["stackPrizeRevision"],
        },
    }}])
