"""Authenticated organizer approval of server-selected phone identity candidates."""
import os
import time

from stand_common import kivly_registration as registration
from stand_common import kivly_resolver as resolver
from stand_common.utils import _get_claims, _iso_now, _read_json_body, _resp, log


def handle(event, method, dynamo):
    try:
        actor = _get_claims(event).get("sub")
        if not actor:
            return _resp(401, {"ok": False, "error": "Unauthorized"})
        if method not in ("GET", "POST"):
            return _resp(405, {"ok": False, "error": "MethodNotAllowed"})
        game_id = (event.get("queryStringParameters") or {}).get("gameId")
        if not isinstance(game_id, str) or not game_id:
            raise registration.RegistrationError("InvalidGameId", 400)
        meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
            Key={"gameId": game_id}, ConsistentRead=True,
        ).get("Item")
        if not meta or meta.get("ownerUserId") != actor:
            return _resp(403, {"ok": False, "error": "Forbidden"})
        if (meta.get("gameType") or "").upper() != "STACK":
            raise registration.RegistrationError("UnsupportedGameType", 400)
        table = registration._table(dynamo)
        if method == "GET":
            requests, cursor = [], None
            while True:
                options = {
                    "FilterExpression": "#kind = :kind AND gameId = :game AND ownerUserId = :actor AND #state = :state",
                    "ExpressionAttributeNames": {"#kind": "kind", "#state": "state"},
                    "ExpressionAttributeValues": {":kind": "registration", ":game": game_id,
                                                  ":actor": actor, ":state": "review_required"},
                    "Limit": 1000,
                }
                if cursor:
                    options["ExclusiveStartKey"] = cursor
                result = table.scan(**options)
                for record in result.get("Items", []):
                    customer = record.get("customer") or {}
                    requests.append({
                        "requestId": record["id"], "state": "review_required",
                        "username": record.get("username"), "email": record.get("email"),
                        "phone": record.get("phone"),
                        "customer": {key: customer.get(key) for key in ("name", "email", "phone")},
                    })
                cursor = result.get("LastEvaluatedKey")
                if not cursor:
                    break
            return _resp(200, {"ok": True, "requests": requests})
        body = _read_json_body(event)
        if (not isinstance(body, dict) or set(body) != {"requestId"}
                or not isinstance(body["requestId"], str)
                or not body["requestId"].startswith("registration:") or len(body["requestId"]) != 77):
            raise registration.RegistrationError("InvalidRequest", 400)
        record = table.get_item(Key={"id": body["requestId"]}, ConsistentRead=True).get("Item")
        if (not record or record.get("kind") != "registration"
                or record.get("gameId") != game_id or record.get("ownerUserId") != actor):
            return _resp(403, {"ok": False, "error": "Forbidden"})
        if record.get("state") != "review_required":
            raise registration.RegistrationError("KivlyReviewNotPending", 409)
        resolver._current(dynamo, record)
        customer = record.get("customer") or {}
        if (not customer.get("id") or not resolver.phone_key(record.get("phone"))
                or resolver.phone_key(customer.get("phone")) != resolver.phone_key(record["phone"])):
            raise registration.RegistrationError("KivlyIdentityConflict", 409)
        approved = {
            **record, "state": "pending", "phase": "context",
            "revision": int(record["revision"]) + 1, "approvedBy": actor,
            "approvedAt": _iso_now(), "approvedCustomerId": customer["id"],
            "queuedAt": int(time.time()),
        }
        resolver._reset_scan(approved)
        try:
            table.put_item(
                Item=approved, ConditionExpression="revision = :revision AND #state = :review",
                ExpressionAttributeNames={"#state": "state"},
                ExpressionAttributeValues={":revision": record["revision"], ":review": "review_required"},
            )
        except registration.ClientError as error:
            if resolver._conditional(error):
                raise registration.RegistrationError("KivlyReviewNotPending", 409) from None
            raise
        log("kivly_identity_review_approved", {
            "actor": actor, "gameId": game_id, "requestId": record["id"],
            "customerId": customer["id"], "connectionVersion": record["connectionVersion"],
        })
        resolver.enqueue(record["id"])
        return _resp(200, {"ok": True})
    except registration.ERRORS as error:
        status, payload = registration.error_response(error)
        return _resp(status, payload)
