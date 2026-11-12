"""Owner-scoped index derived from participants with verified email identities."""
import hashlib
import hmac
import os
import re

from botocore.exceptions import ClientError

from stand_common.email_verification import identity_id
from stand_common.utils import normalize_email

PROFILE_KEY = "PROFILE"
_CLIENT_ID_RE = re.compile(r"^[0-9a-f]{64}$")


class ClientIndexError(RuntimeError):
    pass


def client_id(secret, owner_id, email):
    if not isinstance(secret, str) or len(secret) < 32:
        raise ClientIndexError("ClientIndexUnavailable")
    if not isinstance(owner_id, str) or not owner_id.strip():
        raise ClientIndexError("ClientIndexUnavailable")
    normalized = normalize_email(email)
    if not normalized:
        raise ValueError("A verified email is required for the client index.")
    key = hmac.new(secret.encode(), b"stand:client-index:v1", hashlib.sha256).digest()
    value = hmac.new(key, f"{owner_id.strip()}:{normalized}".encode(), hashlib.sha256).hexdigest()
    return value


def valid_client_id(value):
    return isinstance(value, str) and bool(_CLIENT_ID_RE.fullmatch(value))


def has_verified_email(player):
    return (
        isinstance(player, dict)
        and isinstance(player.get("email"), str)
        and bool(normalize_email(player["email"]))
        and isinstance(player.get("verifiedAt"), str)
        and bool(player["verifiedAt"].strip())
        and player.get("verificationMethod") == "email-code"
    )


def reference_key(game_id, player_id):
    return f"GAME#{game_id}#PLAYER#{int(player_id):020d}"


def has_verified_email_mapping(dynamo, game_id, player, secret):
    email = player.get("email") if isinstance(player, dict) else None
    if not isinstance(email, str) or not email.strip():
        return False
    record = dynamo.Table(os.environ["EMAIL_VERIFICATION_TABLE"]).get_item(
        Key={"id": identity_id(secret, game_id, email)}, ConsistentRead=True,
    ).get("Item")
    try:
        return record is not None and int(record.get("playerId")) == int(player["playerId"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def index_player(dynamo, owner_id, game_id, player, secret, *, allow_legacy_verified=False):
    if not has_verified_email(player) and not allow_legacy_verified:
        return False
    table_name = os.environ["CLIENT_INDEX_TABLE"]
    participant_id = client_id(secret, owner_id, player["email"])
    player_id = int(player["playerId"])
    record_key = reference_key(game_id, player_id)
    name = player.get("username")
    phone = player.get("phone")
    expression = (
        "SET ownerUserId = :owner, email = if_not_exists(email, :email) "
        "ADD participationCount :one"
    )
    names = {}
    values = {
        ":owner": owner_id,
        ":email": normalize_email(player["email"]),
        ":one": 1,
    }
    if isinstance(name, str) and name.strip():
        expression = expression.replace(" ADD participationCount", ", #name = if_not_exists(#name, :name) ADD participationCount")
        names["#name"] = "name"
        values[":name"] = name.strip()
    if isinstance(phone, str) and phone.strip():
        expression = expression.replace(" ADD participationCount", ", phone = if_not_exists(phone, :phone) ADD participationCount")
        values[":phone"] = phone.strip()

    try:
        dynamo.meta.client.transact_write_items(TransactItems=[
            {"Put": {
                "TableName": table_name,
                "Item": {
                    "clientId": participant_id,
                    "recordKey": record_key,
                    "gameId": game_id,
                    "playerId": player_id,
                },
                "ConditionExpression": "attribute_not_exists(clientId)",
            }},
            {"Update": {
                "TableName": table_name,
                "Key": {"clientId": participant_id, "recordKey": PROFILE_KEY},
                "UpdateExpression": expression,
                "ConditionExpression": "attribute_not_exists(ownerUserId) OR ownerUserId = :owner",
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": values,
            }},
        ])
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
            raise
        reference = dynamo.Table(table_name).get_item(
            Key={"clientId": participant_id, "recordKey": record_key}, ConsistentRead=True,
        ).get("Item")
        profile = dynamo.Table(table_name).get_item(
            Key={"clientId": participant_id, "recordKey": PROFILE_KEY}, ConsistentRead=True,
        ).get("Item")
        if (reference and profile and profile.get("ownerUserId") == owner_id
                and reference.get("gameId") == game_id
                and int(reference.get("playerId", -1)) == player_id):
            return False
        raise
    return True


def delete_actions(dynamo, owner_id, game_id, player, secret):
    email = player.get("email")
    if not isinstance(email, str) or not email.strip():
        return []
    participant_id = client_id(secret, owner_id, email)
    table_name = os.environ["CLIENT_INDEX_TABLE"]
    key = {"clientId": participant_id, "recordKey": reference_key(game_id, player["playerId"])}
    reference = dynamo.Table(table_name).get_item(Key=key, ConsistentRead=True).get("Item")
    if not reference:
        return []
    profile_key = {"clientId": participant_id, "recordKey": PROFILE_KEY}
    profile = dynamo.Table(table_name).get_item(Key=profile_key, ConsistentRead=True).get("Item")
    if not profile or profile.get("ownerUserId") != owner_id:
        raise ClientIndexError("ClientIndexInconsistent")
    try:
        count = int(profile["participationCount"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ClientIndexError("ClientIndexInconsistent") from exc
    delete_reference = {
        "Delete": {
            "TableName": table_name,
            "Key": key,
            "ConditionExpression": "attribute_exists(clientId)",
        },
    }
    if count == 1:
        return [delete_reference, {
            "Delete": {
                "TableName": table_name,
                "Key": profile_key,
                "ConditionExpression": "ownerUserId = :owner AND participationCount = :one",
                "ExpressionAttributeValues": {":owner": owner_id, ":one": 1},
            },
        }]
    if count < 1:
        raise ClientIndexError("ClientIndexInconsistent")
    return [delete_reference, {
        "Update": {
            "TableName": table_name,
            "Key": profile_key,
            "UpdateExpression": "ADD participationCount :minus",
            "ConditionExpression": "ownerUserId = :owner AND participationCount = :expected",
            "ExpressionAttributeValues": {
                ":minus": -1, ":owner": owner_id, ":expected": count,
            },
        },
    }]


def remove_participation(dynamo, owner_id, game_id, player, secret):
    actions = delete_actions(dynamo, owner_id, game_id, player, secret)
    if not actions:
        return False
    try:
        dynamo.meta.client.transact_write_items(TransactItems=actions)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "TransactionCanceledException":
            raise
        email = player.get("email")
        participant_id = client_id(secret, owner_id, email)
        reference = dynamo.Table(os.environ["CLIENT_INDEX_TABLE"]).get_item(
            Key={
                "clientId": participant_id,
                "recordKey": reference_key(game_id, player["playerId"]),
            },
            ConsistentRead=True,
        ).get("Item")
        if not reference:
            return False
        raise ClientIndexError("ClientIndexChanged") from exc
    return True
