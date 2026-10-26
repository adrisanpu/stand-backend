"""One Kivly completion reward per STACK participant; never replay uncertain writes."""
from urllib.parse import urlsplit

from stand_common import kivly_registration as registration
from stand_common.utils import _iso_now, get_game_type_blob


def wallet_url(value):
    if value is None or value == "":
        return None
    if not isinstance(value, str) or len(value) > 16384 or any(ord(c) < 33 for c in value):
        raise registration.RegistrationError("KivlyWalletInvalid")
    try:
        url = urlsplit(value)
        host = url.hostname or ""
        valid = (url.scheme == "https" and not url.username and not url.password
                 and url.port in (None, 443)
                 and (host in ("pay.google.com", "wallet.google.com", "getkivly.com", "kivly.com")
                      or host.endswith((".getkivly.com", ".kivly.com"))))
    except ValueError:
        valid = False
    if not valid:
        raise registration.RegistrationError("KivlyWalletInvalid")
    return value


def configured_reward(meta, conn):
    reward = meta.get("kivlyReward")
    if (not isinstance(reward, dict) or not conn
            or reward.get("locationId") != conn["locationId"]):
        return None
    return reward


def _result(record, redemption=None):
    result = {"status": record["state"], "name": record["name"],
              "appleWalletUrl": None, "googleWalletUrl": None}
    if redemption:
        result["appleWalletUrl"] = wallet_url(redemption.get("appleWalletUrl"))
        result["googleWalletUrl"] = wallet_url(redemption.get("googleWalletUrl"))
    return result


def completion_reward(dynamo, meta, player):
    if meta.get("gameType") != "STACK":
        raise registration.RegistrationError("UnsupportedGameType", 400)
    blob = get_game_type_blob(player, "STACK")
    if blob.get("score") is None or (blob.get("attempt") or {}).get("status") != "finished":
        raise registration.RegistrationError("KivlyGameNotFinished", 409)
    if not registration.game_settings(meta)["enabled"]:
        raise registration.RegistrationError("KivlyConnectionRequired", 409)
    registration.require_player(dynamo, meta, player)
    conn = registration._required_connection(dynamo, meta)
    customer_id = player["kivlyMembership"]["customerId"]
    table = registration._table(dynamo)
    key = "reward:{}:{}".format(player["gameId"], player["playerId"])
    record = table.get_item(Key={"id": key}, ConsistentRead=True).get("Item")
    reward = configured_reward(meta, conn)
    if not record and not reward:
        return {"status": "none"}
    if record and (record["locationId"] != conn["locationId"] or record["customerId"] != customer_id):
        raise registration.RegistrationError("KivlyConnectionChanged", 409)
    client = registration._client(conn)
    if record:
        if record["state"] != "confirmed":
            return _result(record)
        rows = client.customer_rewards(conn["locationId"], conn["ownerId"], customer_id)
        redemption = next((row for row in rows if row["id"] == record["redemptionId"]
                           and row["reward_id"] == record["rewardId"]), None)
        if not redemption:
            raise registration.RegistrationError("KivlyRewardNotFound", 409)
        return _result(record, redemption)

    if not any(item["id"] == reward["id"] for item in client.list_rewards(conn["locationId"], conn["ownerId"])):
        raise registration.RegistrationError("KivlyRewardUnavailable", 409)
    before = {row["id"] for row in client.customer_rewards(conn["locationId"], conn["ownerId"], customer_id)}
    record = {"id": key, "state": "pending", "rewardId": reward["id"], "name": reward["name"],
              "locationId": conn["locationId"], "customerId": customer_id, "createdAt": _iso_now()}
    try:
        table.put_item(Item=record, ConditionExpression="attribute_not_exists(id)")
    except registration.ClientError as exc:
        if registration._code(exc) == "ConditionalCheckFailedException":
            raise registration.RegistrationError("KivlyRewardBusy", 409) from None
        raise
    try:
        client.earn_reward(conn["locationId"], conn["ownerId"], customer_id, reward["id"])
        rows = client.customer_rewards(conn["locationId"], conn["ownerId"], customer_id)
        new = [row for row in rows if row["id"] not in before and row["reward_id"] == reward["id"]]
        if len(new) != 1:
            registration.error_response(registration.RegistrationError("KivlyRewardReconciliationRequired", 409))
            return _result(record)
        current = registration._required_connection(dynamo, meta)
        if current["version"] != conn["version"]:
            raise registration.RegistrationError("KivlyConnectionChanged", 409)
        table.update_item(
            Key={"id": key}, UpdateExpression="SET #s = :s, redemptionId = :r",
            ConditionExpression="#s = :pending AND rewardId = :reward",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":s": "confirmed", ":r": new[0]["id"],
                                       ":pending": "pending", ":reward": reward["id"]},
        )
    except registration.ERRORS as exc:
        # A successful-looking response is not enough; the reservation survives all uncertainty.
        registration.error_response(exc)
        return _result(record)
    return _result({**record, "state": "confirmed"}, new[0])
