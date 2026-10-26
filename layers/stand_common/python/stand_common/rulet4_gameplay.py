"""Server-selected, persistent RULET4 results shared by both play surfaces."""
import secrets

from botocore.exceptions import ClientError
from stand_common.rulet4 import game_config
from stand_common.utils import _iso_now, get_game_type_blob


def public_assignment(player, display_name, profile_pic_url=None):
    return {
        "gameType": "RULET4",
        "playerId": int(player["playerId"]),
        "displayName": display_name,
        "profilePicUrl": profile_pic_url,
        "result": get_game_type_blob(player, "RULET4").get("result"),
    }


def spin(meta, player, table):
    previous = get_game_type_blob(player, "RULET4").get("result")
    if previous is not None:
        return previous

    config = game_config(meta)
    index = secrets.randbelow(sum(panel["count"] for panel in config["panels"]))
    offset = index
    for panel in config["panels"]:
        if offset < panel["count"]:
            selected = panel
            break
        offset -= panel["count"]
    result = {
        "panel": selected,
        "index": index,
        "configSnapshot": config,
        "spunAt": _iso_now(),
    }
    key = {"gameId": player["gameId"], "playerId": int(player["playerId"])}
    try:
        table.update_item(
            Key=key,
            UpdateExpression="SET #type.#game.#result = :result",
            ConditionExpression=(
                "attribute_exists(gameId) AND attribute_exists(playerId) "
                "AND attribute_not_exists(#type.#game.#result)"
            ),
            ExpressionAttributeNames={"#type": "type", "#game": "RULET4", "#result": "result"},
            ExpressionAttributeValues={":result": result},
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise
        current = table.get_item(Key=key, ConsistentRead=True).get("Item")
        winner = get_game_type_blob(current or {}, "RULET4").get("result")
        if winner is None:
            raise
        return winner
    return result
