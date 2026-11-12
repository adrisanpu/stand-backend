"""One durable participant per Kivly identity and game."""
import hashlib
import json
import os

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


def _error(code):
    from stand_common.kivly_registration import RegistrationError
    raise RegistrationError(code, 409)


def claim_id(game_id, membership):
    if (not membership.get("locationId") or not membership.get("customerId")
            or membership["customerId"] == "pending-reconciliation"
            or membership.get("status", "confirmed") != "confirmed"):
        _error("KivlyMembershipRequired")
    binding = json.dumps([game_id, membership["locationId"], membership["customerId"]])
    return "participation:" + hashlib.sha256(binding.encode()).hexdigest()


def claim_item(game_id, player_id, membership):
    return {
        "id": claim_id(game_id, membership), "gameId": game_id, "playerId": player_id,
        "locationId": membership["locationId"], "customerId": membership["customerId"],
    }


def existing_player(dynamo, game_id, membership):
    table = dynamo.Table(os.environ["KIVLY_REGISTRATION_TABLE"])
    claim = table.get_item(Key={"id": claim_id(game_id, membership)}, ConsistentRead=True).get("Item")
    players = dynamo.Table(os.environ["GAMEPLAYER_TABLE"])
    if claim:
        player = players.get_item(
            Key={"gameId": game_id, "playerId": int(claim["playerId"])}, ConsistentRead=True,
        ).get("Item")
        if not player or not _matches(player, membership):
            _error("KivlyDuplicateParticipation")
        return player
    matches, last = [], None
    while True:
        kwargs = {"KeyConditionExpression": Key("gameId").eq(game_id), "ConsistentRead": True}
        if last:
            kwargs["ExclusiveStartKey"] = last
        page = players.query(**kwargs)
        matches.extend(p for p in page.get("Items", []) if _matches(p, membership))
        last = page.get("LastEvaluatedKey")
        if not last:
            break
    if len(matches) > 1:
        _error("KivlyDuplicateParticipation")
    if not matches:
        return None
    player = matches[0]
    try:
        table.put_item(
            Item=claim_item(game_id, player["playerId"], membership),
            ConditionExpression="attribute_not_exists(id)",
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        return existing_player(dynamo, game_id, membership)
    return player


def _matches(player, membership):
    saved = (player.get("kivly") or {}).get("membership") or {}
    return (saved.get("locationId") == membership["locationId"]
            and saved.get("customerId") == membership["customerId"])


def require_unique_player(dynamo, player):
    membership = (player.get("kivly") or {}).get("membership") or {}
    linked = existing_player(dynamo, player["gameId"], membership)
    if not linked or int(linked["playerId"]) != int(player["playerId"]):
        _error("KivlyDuplicateParticipation")


def bind_existing_player(dynamo, player, membership):
    table = dynamo.Table(os.environ["KIVLY_REGISTRATION_TABLE"])
    claim = table.get_item(
        Key={"id": claim_id(player["gameId"], membership)}, ConsistentRead=True,
    ).get("Item")
    if claim and int(claim["playerId"]) != int(player["playerId"]):
        _error("KivlyDuplicateParticipation")
    previous = player.get("kivly", {})
    if not isinstance(previous, dict):
        _error("KivlyMembershipRequired")
    update = {
        "TableName": os.environ["GAMEPLAYER_TABLE"],
        "Key": {"gameId": player["gameId"], "playerId": int(player["playerId"])},
        "UpdateExpression": "SET #kivly = :kivly",
        "ConditionExpression": (
            "attribute_exists(playerId) AND #kivly = :previous"
            if "kivly" in player else
            "attribute_exists(playerId) AND attribute_not_exists(#kivly)"
        ),
        "ExpressionAttributeNames": {"#kivly": "kivly"},
        "ExpressionAttributeValues": {":kivly": {**previous, "membership": membership}},
    }
    if "kivly" in player:
        update["ExpressionAttributeValues"][":previous"] = previous
    actions = [{"Update": update}]
    if not claim:
        actions.append({"Put": {
            "TableName": table.name,
            "Item": claim_item(player["gameId"], player["playerId"], membership),
            "ConditionExpression": "attribute_not_exists(id)",
        }})
    try:
        table.meta.client.transact_write_items(TransactItems=actions)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "TransactionCanceledException":
            raise
        current = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
            Key=update["Key"], ConsistentRead=True,
        ).get("Item")
        saved = table.get_item(
            Key={"id": claim_id(player["gameId"], membership)}, ConsistentRead=True,
        ).get("Item")
        if (not current or (current.get("kivly") or {}).get("membership") != membership
                or not saved or int(saved["playerId"]) != int(player["playerId"])):
            _error("KivlyDuplicateParticipation")


def require_joinable(player):
    stack = (player.get("type") or {}).get("STACK") or {}
    attempt = stack.get("attempt") or {}
    if stack.get("score") is not None or (attempt and attempt.get("status") != "started"):
        _error("KivlyAlreadyPlayed")
