"""Publish owner-contributed EMPAREJA2 characters."""
import hashlib
import json
import re
import struct
import time
import uuid

from botocore.exceptions import ClientError
from stand_common import empareja2_catalog
from stand_common.prizes import MAX_PRIZE_IMAGE_BYTES, validate_png
from stand_common.utils import _iso_now, log

META_ID = "EMPAREJA2#CONTRIBUTIONS#v1"
MAX_PAIRS = 10
_UUID = re.compile(r"^[0-9a-f]{32}$")


class CatalogError(Exception):
    def __init__(self, code, status=400):
        super().__init__(code)
        self.code = code
        self.status = status


def _token(value):
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise CatalogError("InvalidContributionId")
    return value


def upload_url(s3, bucket, owner, game_id, body):
    contribution = _token(body.get("uploadId"))
    size = body.get("sizeBytes")
    if type(size) is not int or not 1 <= size <= MAX_PRIZE_IMAGE_BYTES:
        raise CatalogError("InvalidImageSize")
    key = f"empareja2/staged/{owner}/{game_id}/{contribution}.png"
    result = s3.generate_presigned_post(
        Bucket=bucket, Key=key,
        Fields={"Content-Type": "image/png"},
        Conditions=[{"Content-Type": "image/png"}, ["content-length-range", 1, MAX_PRIZE_IMAGE_BYTES]],
        ExpiresIn=3600,
    )
    return {"ok": True, "key": key, "url": result["url"], "fields": result["fields"]}


def publish(dynamo, games, catalog, s3, bucket, char_bucket, owner, game, body, items):
    op = _token(body.get("operationId"))
    lock_key = {"catalogId": META_ID, "pairId": f"LOCK#{owner}#{op}"}
    token = uuid.uuid4().hex
    acquire_lock(catalog, lock_key, token)
    try:
        return _publish(dynamo, games, catalog, s3, bucket, char_bucket, owner, game, body, items, op)
    finally:
        release_lock(catalog, lock_key, token)


def acquire_lock(catalog, key, token, lease_seconds=180):
    now = int(time.time())
    try:
        catalog.update_item(
            Key=key,
            UpdateExpression="SET leaseUntil = :until, leaseToken = :token",
            ConditionExpression="attribute_not_exists(leaseUntil) OR leaseUntil < :now",
            ExpressionAttributeValues={":until": now + lease_seconds, ":token": token, ":now": now},
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise CatalogError("ContributionBusy", 409) from exc
        raise


def release_lock(catalog, key, token):
    try:
        catalog.delete_item(
            Key=key, ConditionExpression="leaseToken = :token",
            ExpressionAttributeValues={":token": token},
        )
    except ClientError as exc:
        log("empareja2_lock_release_failed", {"error": str(exc), "key": key["pairId"]})


def _publish(dynamo, games, catalog, s3, bucket, char_bucket, owner, game, body, items, op):
    pairs = body.get("pairs")
    num_pairs = body.get("numPairs")
    if not isinstance(pairs, list) or not 1 <= len(pairs) <= MAX_PAIRS:
        raise CatalogError("InvalidPairCount")
    if type(num_pairs) is not int or not 1 <= num_pairs <= len(empareja2_catalog.group_items(items)):
        raise CatalogError("InvalidDifficulty")
    normalized = []
    seen = set()
    prefix = f"empareja2/staged/{owner}/{game['gameId']}/"
    for pair in pairs:
        if not isinstance(pair, list) or len(pair) != 2:
            raise CatalogError("EachPairRequiresTwoCharacters")
        chars = []
        for character in pair:
            if not isinstance(character, dict) or set(character) != {"name", "imageKey"}:
                raise CatalogError("InvalidCharacter")
            name, key = character["name"], character["imageKey"]
            if not isinstance(name, str) or not 1 <= len(name.strip()) <= 80 or any(ord(c) < 32 for c in name):
                raise CatalogError("InvalidCharacterName")
            if not isinstance(key, str) or not key.startswith(prefix) or not key.endswith(".png"):
                raise CatalogError("InvalidImageKey")
            _token(key[len(prefix):-4])
            if key in seen:
                raise CatalogError("DuplicateImage")
            seen.add(key)
            chars.append({"name": name.strip(), "imageKey": key})
        normalized.append(chars)
    digest = hashlib.sha256(json.dumps({"pairs": normalized, "numPairs": num_pairs}, sort_keys=True).encode()).hexdigest()
    marker_key = {"catalogId": META_ID, "pairId": f"OP#{owner}#{op}"}

    def committed():
        marker = catalog.get_item(Key=marker_key, ConsistentRead=True).get("Item")
        if marker:
            if marker.get("digest") != digest or marker.get("gameId") != game["gameId"]:
                raise CatalogError("ContributionIdConflict", 409)
            return True
        return False

    if committed():
        return {"ok": True, "alreadyPublished": True}
    images = []
    for pair in normalized:
        for character in pair:
            key = character["imageKey"]
            head = s3.head_object(Bucket=bucket, Key=key)
            length = head["ContentLength"]
            if type(length) is not int or not 1 <= length <= MAX_PRIZE_IMAGE_BYTES:
                raise CatalogError("InvalidImageSize")
            stream = s3.get_object(Bucket=bucket, Key=key)["Body"]
            try:
                image = stream.read(MAX_PRIZE_IMAGE_BYTES + 1)
            finally:
                stream.close()
            if len(image) != length:
                raise CatalogError("InvalidImageSize")
            try:
                if len(image) < 24:
                    raise ValueError("Invalid PNG")
                width, height = struct.unpack(">II", image[16:24])
                if width > 1024 or height > 1024:
                    raise ValueError("Image dimensions exceed the limit")
                validate_png(image)
            except ValueError as exc:
                raise CatalogError("InvalidCharacterImage") from exc
            destination = f"contributions/{owner}/{game['gameId']}/{op}/{hashlib.sha256(key.encode() + image).hexdigest()}.png"
            character["destination"] = destination
            images.append((destination, image))

    for destination, image in images:
        s3.put_object(Bucket=char_bucket, Key=destination, Body=image, ContentType="image/png")

    counter_key = {"catalogId": META_ID, "pairId": "COUNTER"}
    for attempt in range(5):
        counter = catalog.get_item(Key=counter_key, ConsistentRead=True).get("Item")
        current = int(counter.get("value", 0)) if counter else 0
        if current < 0 or current + len(normalized) > 1_000_000_000:
            raise CatalogError("CatalogCapacityExceeded", 409)
        types = game.get("type") or {}
        next_types = {**types, "EMPAREJA2": {**(types.get("EMPAREJA2") or {}), "numPairs": num_pairs}}
        condition = "ownerUserId = :owner AND gameType = :gt AND attribute_not_exists(archivedAt) AND attribute_not_exists(deletedAt) AND attribute_not_exists(deletionStartedAt)"
        values = {":owner": owner, ":gt": "EMPAREJA2", ":next": next_types, ":now": _iso_now()}
        if "type" in game:
            condition += " AND #type = :previous"
            values[":previous"] = game["type"]
        else:
            condition += " AND attribute_not_exists(#type)"
        actions = [
            {"Update": {"TableName": games.name, "Key": {"gameId": game["gameId"]},
                        "UpdateExpression": "SET #type = :next, updatedAt = :now",
                        "ConditionExpression": condition, "ExpressionAttributeNames": {"#type": "type"},
                        "ExpressionAttributeValues": values}},
            {"Put": {"TableName": catalog.name, "Item": {**marker_key, "digest": digest, "gameId": game["gameId"]},
                     "ConditionExpression": "attribute_not_exists(catalogId)"}},
            {"Update": {"TableName": catalog.name, "Key": counter_key,
                        "UpdateExpression": "SET #v = :next",
                        "ConditionExpression": "#v = :old" if counter else "attribute_not_exists(#v)",
                        "ExpressionAttributeNames": {"#v": "value"},
                        "ExpressionAttributeValues": {":next": current + len(normalized), **({":old": current} if counter else {})}}},
        ]
        for index, pair in enumerate(normalized, 1):
            group = f"CONTRIB#{current + index:020d}"
            for position, character in enumerate(pair, 1):
                row = {"catalogId": "EMPAREJA2#CHARACTERS#v1", "pairId": f"{group}#CHAR#{position}",
                       "pairGroupId": group, "characterId": -((current + index - 1) * 2 + position),
                       "characterName": character["name"],
                       "imageKey": character["destination"]}
                actions.append({"Put": {"TableName": catalog.name, "Item": row,
                                         "ConditionExpression": "attribute_not_exists(catalogId)"}})
        try:
            dynamo.meta.client.transact_write_items(TransactItems=actions)
            return {"ok": True, "alreadyPublished": False}
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
                raise
            if committed():
                return {"ok": True, "alreadyPublished": True}
            latest = games.get_item(Key={"gameId": game["gameId"]}, ConsistentRead=True).get("Item")
            if not latest or latest.get("type") != game.get("type") or latest.get("ownerUserId") != owner:
                raise CatalogError("GameChanged", 409) from exc
    raise CatalogError("CatalogBusy", 409)
