"""Authenticated owner connection endpoints; credentials are stored only in Secrets Manager."""
import hashlib
import json
import os
import uuid

import boto3
from botocore.exceptions import ClientError

from stand_common import kivly_registration as registration
from stand_common.kivly_rewards import configured_reward
from stand_common.kivly import KivlyClient
from stand_common.utils import _get_claims, _read_json_body, _resp, log


def _cleanup(secrets, arn):
    try:
        secrets.delete_secret(SecretId=arn, RecoveryWindowInDays=7)
        return {}
    except registration.ERRORS:
        log("kivly_secret_cleanup_required", {"code": "KivlySecretCleanupRequired"})
        return {"warning": "KivlySecretCleanupRequired"}


def _change_connection(owner, method, body, dynamo, conn):
    users = dynamo.Table(os.environ["USERS_TABLE"])
    secrets = boto3.client("secretsmanager")
    names = {"#c": "kivlyConnection"}
    values = {}
    condition = "attribute_not_exists(#c)"
    if conn:
        condition = "#c.#v = :old"
        names["#v"] = "version"
        values[":old"] = conn["version"]
    if method == "DELETE":
        if not conn:
            return _resp(200, {"ok": True})
        users.update_item(
            Key={"userId": owner}, UpdateExpression="REMOVE #c",
            ConditionExpression=condition, ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        return _resp(200, {"ok": True, **_cleanup(secrets, conn["secretArn"])})

    client_id, client_secret = body.get("clientId"), body.get("clientSecret")
    client = KivlyClient(client_id, client_secret)
    context = client.get_context()
    # Verify read access without changing any Kivly customer.
    client._customers(context.location_id, context.owner_id, page=1)
    version = uuid.uuid4().hex
    name = "stand/{}/kivly/{}/{}".format(
        os.environ["STAGE"], hashlib.sha256(owner.encode()).hexdigest(), version,
    )
    secret = secrets.create_secret(
        Name=name, SecretString=json.dumps({"client_id": client_id, "client_secret": client_secret}),
    )
    new = {"secretArn": secret["ARN"], "locationId": context.location_id,
           "ownerId": context.owner_id, "version": version,
           "clubSettings": {
               "clubName": context.location_name[:200],
               "privacyUrl": "https://playstand.app/pages/privacy-es.html",
               "termsUrl": "https://playstand.app/pages/legal.html",
           }}
    if conn and conn["locationId"] == context.location_id and "clubSettings" in conn:
        new["clubSettings"] = conn["clubSettings"]
    values[":new"] = new
    try:
        users.update_item(
            Key={"userId": owner}, UpdateExpression="SET #c = :new",
            ConditionExpression=condition, ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
    except registration.ERRORS as exc:
        if isinstance(exc, ClientError) and exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            _cleanup(secrets, new["secretArn"])
            raise registration.RegistrationError("KivlyConnectionChanged", 409) from None
        # A failed write may still have committed; keep the secret for reconciliation.
        raise registration.RegistrationError("KivlyConnectionReconciliationRequired") from None
    warning = _cleanup(secrets, conn["secretArn"]) if conn else {}
    return _resp(200, {"ok": True, **warning})


def _verify_connection(conn):
    if not conn:
        return
    client = registration._client(conn)
    context = client.get_context()
    if context.location_id != conn["locationId"] or context.owner_id != conn["ownerId"]:
        raise registration.RegistrationError("KivlyScopeUnverified", 409)
    client._customers(context.location_id, context.owner_id, page=1)
    return context


def handle(event, method, dynamo, games, *, catalog=False):
    try:
        claims = _get_claims(event)
        owner = claims.get("sub")
        if not owner:
            return _resp(401, {"ok": False, "error": "Unauthorized"})
        if method not in ("GET", "PUT", "DELETE"):
            return _resp(405, {"ok": False, "error": "MethodNotAllowed"})
        body = _read_json_body(event) if method == "PUT" else event.get("queryStringParameters") or {}
        if not isinstance(body, dict) or not isinstance(body.get("gameId"), str):
            return _resp(400, {"ok": False, "error": "InvalidGameId"})
        meta = games.get_item(Key={"gameId": body["gameId"]}, ConsistentRead=True).get("Item")
        if not meta or meta.get("ownerUserId") != owner:
            return _resp(403, {"ok": False, "error": "Forbidden"})
        conn = registration.connection(dynamo, owner)
        if catalog:
            if method != "GET":
                return _resp(405, {"ok": False, "error": "MethodNotAllowed"})
            if not conn:
                return _resp(409, {"ok": False, "error": "KivlyConnectionRequired"})
            return _resp(200, {"ok": True, "rewards": registration._client(conn).list_rewards(
                conn["locationId"], conn["ownerId"],
            )})
        if method == "GET":
            context = _verify_connection(conn)
            settings = registration.account_game_settings(meta, conn)
            if context and not settings.get("clubName") and not settings["enabled"]:
                settings = {**settings, "clubName": context.location_name[:200]}
            return _resp(200, {
                "ok": True, "connection": {
                    "connected": bool(conn),
                    **({"locationId": conn["locationId"]} if conn else {}),
                    "catalogValidated": False,
                },
                "settings": {key: settings.get(key, "") for key in ("enabled", "clubName", "privacyUrl", "termsUrl")},
                "reward": configured_reward(meta, conn),
            })
        if method == "PUT" and "rewardId" in body:
            if meta.get("gameType") != "STACK":
                return _resp(400, {"ok": False, "error": "UnsupportedGameType"})
            reward_id = body["rewardId"]
            reward = None
            if reward_id is not None:
                if not conn or not registration.account_game_settings(meta, conn)["enabled"]:
                    return _resp(409, {"ok": False, "error": "KivlyConnectionRequired"})
                if not isinstance(reward_id, str) or not reward_id.strip():
                    return _resp(400, {"ok": False, "error": "KivlyRewardInvalid"})
                rewards = registration._client(conn).list_rewards(conn["locationId"], conn["ownerId"])
                selected = next((item for item in rewards if item["id"] == reward_id), None)
                if not selected:
                    return _resp(409, {"ok": False, "error": "KivlyRewardUnavailable"})
                reward = {**selected, "locationId": conn["locationId"]}
            games.update_item(
                Key={"gameId": body["gameId"]},
                UpdateExpression="SET kivlyReward = :reward",
                ConditionExpression="ownerUserId = :owner",
                ExpressionAttributeValues={":reward": reward, ":owner": owner},
            )
            return _resp(200, {"ok": True})
        return _change_connection(owner, method, body, dynamo, conn)
    except registration.ERRORS as exc:
        status, payload = registration.error_response(exc)
        return _resp(status, payload)


def handle_account(event, method, dynamo):
    """Manage the owner's shared Kivly connection without a game context."""
    try:
        claims = _get_claims(event)
        owner = claims.get("sub")
        if not owner:
            return _resp(401, {"ok": False, "error": "Unauthorized"})
        if method not in ("GET", "PUT", "DELETE"):
            return _resp(405, {"ok": False, "error": "MethodNotAllowed"})

        body = _read_json_body(event) if method == "PUT" else {}
        if not isinstance(body, dict):
            return _resp(400, {"ok": False, "error": "InvalidRequest"})

        conn = registration.connection(dynamo, owner)
        if method == "GET":
            context = _verify_connection(conn)
            return _resp(200, {"ok": True, "connection": {
                "connected": bool(conn),
                "locationId": context.location_id if context else None,
                "locations": [{"id": context.location_id, "name": context.location_name}] if context else [],
                "clubSettings": conn.get("clubSettings") if conn else None,
            }})

        if method == "PUT" and "clubSettings" in body:
            if not conn:
                return _resp(409, {"ok": False, "error": "KivlyConnectionRequired"})
            raw = body["clubSettings"]
            if not isinstance(raw, dict) or set(raw) != {"clubName", "privacyUrl", "termsUrl"}:
                return _resp(400, {"ok": False, "error": "KivlyConfigurationInvalid"})
            settings = registration.prepare_settings(
                {"gameType": "STACK"}, {**raw, "enabled": False}, None,
            )
            club = {key: settings[key] for key in ("clubName", "privacyUrl", "termsUrl")}
            dynamo.Table(os.environ["USERS_TABLE"]).update_item(
                Key={"userId": owner},
                UpdateExpression="SET #c.#s = :settings, #c.#v = :new",
                ConditionExpression="#c.#v = :old",
                ExpressionAttributeNames={"#c": "kivlyConnection", "#s": "clubSettings", "#v": "version"},
                ExpressionAttributeValues={":settings": club, ":old": conn["version"], ":new": uuid.uuid4().hex},
            )
            return _resp(200, {"ok": True})

        return _change_connection(owner, method, body, dynamo, conn)
    except registration.ERRORS as exc:
        status, payload = registration.error_response(exc)
        return _resp(status, payload)
