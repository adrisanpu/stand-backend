"""Authenticated owner-only client index and prize history reads."""
import base64
import json
import os
import unicodedata
from decimal import Decimal

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from stand_common import client_index, stack_prizes
from stand_common.utils import _get_claims, _resp, log

OWNER_INDEX = "owner-clients"


def _cursor_encode(key):
    if not key:
        return None
    raw = json.dumps(key, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _cursor_decode(value):
    if not value:
        return None
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("Invalid cursor.")
    try:
        padded = value + "=" * (-len(value) % 4)
        key = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Invalid cursor.") from exc
    if (not isinstance(key, dict) or set(key) != {"clientId", "recordKey", "ownerUserId"}
            or not all(isinstance(item, str) for item in key.values())):
        raise ValueError("Invalid cursor.")
    return key


def _page_size(params):
    raw = params.get("limit", "25")
    if not isinstance(raw, str) or not raw.isdigit():
        raise ValueError("Invalid page size.")
    size = int(raw)
    if not 1 <= size <= 100:
        raise ValueError("Invalid page size.")
    return min(size, 25)


def _list(event, dynamo, owner_id):
    params = event.get("queryStringParameters") or {}
    if "page" in params:
        return _directory(params, dynamo, owner_id)
    try:
        limit = _page_size(params)
        cursor = _cursor_decode(params.get("cursor"))
    except ValueError as exc:
        return _resp(400, {"ok": False, "error": str(exc)})
    options = {
        "IndexName": OWNER_INDEX,
        "KeyConditionExpression": Key("ownerUserId").eq(owner_id),
        "Limit": limit,
    }
    if cursor:
        if cursor["ownerUserId"] != owner_id:
            return _resp(400, {"ok": False, "error": "Invalid cursor."})
        options["ExclusiveStartKey"] = cursor
    try:
        page = dynamo.Table(os.environ["CLIENT_INDEX_TABLE"]).query(**options)
    except ClientError as exc:
        log("ClientIndexListFailed", {"error": str(exc), "ownerId": owner_id})
        return _resp(500, {"ok": False, "error": "ClientListFailed"})
    clients = []
    for row in page.get("Items") or []:
        if row.get("recordKey") != client_index.PROFILE_KEY or row.get("ownerUserId") != owner_id:
            continue
        clients.append({
            "id": row["clientId"],
            "name": row.get("name"),
            "email": row["email"],
            "phone": row.get("phone"),
            "participationCount": int(row.get("participationCount", 0)),
        })
    return _resp(200, {
        "ok": True, "clients": clients, "nextCursor": _cursor_encode(page.get("LastEvaluatedKey")),
    })


def _search_text(value):
    return "".join(char for char in unicodedata.normalize("NFKD", value.casefold())
                   if not unicodedata.combining(char))


def _directory(params, dynamo, owner_id):
    raw_page = params.get("page", "1")
    search = params.get("search", "")
    if (not isinstance(raw_page, str) or not raw_page.isascii() or not raw_page.isdigit()
            or len(raw_page) > 9 or int(raw_page) < 1
            or not isinstance(search, str) or len(search) > 200):
        return _resp(400, {"ok": False, "error": "Invalid client search or page."})
    requested_page = int(raw_page)
    terms = _search_text(search).split()
    options = {
        "IndexName": OWNER_INDEX,
        "KeyConditionExpression": Key("ownerUserId").eq(owner_id),
        "ProjectionExpression": "clientId, recordKey, ownerUserId, #name, email, phone, participationCount",
        "ExpressionAttributeNames": {"#name": "name"},
    }
    clients = []
    try:
        table = dynamo.Table(os.environ["CLIENT_INDEX_TABLE"])
        while True:
            batch = table.query(**options)
            for row in batch.get("Items") or []:
                if row.get("ownerUserId") != owner_id or row.get("recordKey") != client_index.PROFILE_KEY:
                    continue
                haystack = _search_text(" ".join(row.get(key) or "" for key in ("name", "email", "phone")))
                phone = "".join(char for char in (row.get("phone") or "") if char.isdigit())
                if terms and not all(term in haystack for term in terms):
                    digits = "".join(char for char in search if char.isdigit())
                    if not digits or any(char.isalpha() for char in search) or digits not in phone:
                        continue
                clients.append({
                    "id": row["clientId"], "name": row.get("name"), "email": row["email"],
                    "phone": row.get("phone"), "participationCount": int(row.get("participationCount", 0)),
                })
            if not batch.get("LastEvaluatedKey"):
                break
            options["ExclusiveStartKey"] = batch["LastEvaluatedKey"]
    except ClientError as exc:
        log("ClientIndexListFailed", {"error": str(exc), "ownerId": owner_id})
        return _resp(500, {"ok": False, "error": "ClientListFailed"})
    clients.sort(key=lambda row: (_search_text(row["name"] or row["email"]), row["id"]))
    total = len(clients)
    page = min(requested_page, max(1, (total + 24) // 25))
    start = (page - 1) * 25
    return _resp(200, {
        "ok": True, "clients": clients[start:start + 25],
        "page": page, "total": total, "pageSize": 25,
    })


def _award_view(row, game):
    prize = row.get("prize") or {}
    source = prize.get("source")
    state = row.get("state")
    is_stand = source == "stand"
    is_kivly = source == "kivly"
    return {
        "id": row["id"],
        "name": prize.get("name"),
        "imageUrl": stack_prizes.image_url(prize, game),
        "source": source,
        "condition": row.get("condition"),
        "awardedAt": row.get("createdAt"),
        "awardState": state,
        "kivlyAssignmentState": state if is_kivly else None,
        "emailState": row.get("emailState"),
        "deliveryChannel": row.get("deliveryChannel"),
        "collected": is_stand and state == "delivered",
        "collectedAt": row.get("deliveredAt") if is_stand and state == "delivered" else None,
    }


def _detail(dynamo, owner_id, requested_id, games_table, players_table):
    if not client_index.valid_client_id(requested_id):
        return _resp(400, {"ok": False, "error": "Invalid client id."})
    table = dynamo.Table(os.environ["CLIENT_INDEX_TABLE"])
    try:
        rows, last = [], None
        options = {
            "KeyConditionExpression": Key("clientId").eq(requested_id),
            "ConsistentRead": True,
        }
        while True:
            if last:
                options["ExclusiveStartKey"] = last
            page = table.query(**options)
            rows.extend(page.get("Items") or [])
            last = page.get("LastEvaluatedKey")
            if not last:
                break
        profile = next((row for row in rows
                        if row.get("recordKey") == client_index.PROFILE_KEY), None)
        if not profile or profile.get("ownerUserId") != owner_id:
            return _resp(404, {"ok": False, "error": "ClientNotFound"})
        references = [row for row in rows if row.get("recordKey") != client_index.PROFILE_KEY]
        game_ids = sorted({row.get("gameId") for row in references if isinstance(row.get("gameId"), str)})
        games, awards_by_game = {}, {}
        for game_id in game_ids:
            game = games_table.get_item(Key={"gameId": game_id}, ConsistentRead=True).get("Item")
            if not game or game.get("ownerUserId") != owner_id:
                return _resp(409, {"ok": False, "error": "ClientHistoryChanged"})
            games[game_id] = game
            awards_by_game[game_id] = [
                row for row in stack_prizes.records(dynamo, game_id)
                if isinstance(row.get("id"), str) and row["id"].startswith("award_")
            ]
        participations = []
        for reference in references:
            game_id = reference.get("gameId")
            player_id = reference.get("playerId")
            if (not isinstance(game_id, str) or isinstance(player_id, bool)
                    or not isinstance(player_id, (int, Decimal))):
                return _resp(409, {"ok": False, "error": "ClientHistoryChanged"})
            player = players_table.get_item(
                Key={"gameId": game_id, "playerId": int(player_id)}, ConsistentRead=True,
            ).get("Item")
            if not player:
                return _resp(409, {"ok": False, "error": "ClientHistoryChanged"})
            if (client_index.client_id(
                    os.environ["PLAYER_TOKEN_SECRET"], owner_id, player.get("email")) != requested_id
                    or not (client_index.has_verified_email(player)
                            or client_index.has_verified_email_mapping(
                                dynamo, game_id, player, os.environ["PLAYER_TOKEN_SECRET"],
                            ))):
                return _resp(409, {"ok": False, "error": "ClientHistoryChanged"})
            game = games[game_id]
            awards = [_award_view(row, game) for row in awards_by_game[game_id]
                      if row.get("playerId") is not None and int(row["playerId"]) == int(player_id)]
            awards.sort(key=lambda row: row.get("awardedAt") or "", reverse=True)
            participations.append({
                "gameId": game_id,
                "playerId": int(player_id),
                "gameName": game.get("gameName"),
                "gameType": game.get("gameType"),
                "joinedAt": player.get("joinedAt"),
                "name": player.get("username"),
                "phone": player.get("phone"),
                "awards": awards,
            })
        participations.sort(key=lambda row: row.get("joinedAt") or "", reverse=True)
    except ClientError as exc:
        log("ClientHistoryReadFailed", {"error": str(exc), "ownerId": owner_id})
        return _resp(500, {"ok": False, "error": "ClientHistoryUnavailable"})
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        log("ClientHistoryInvalid", {"error": str(exc), "ownerId": owner_id})
        return _resp(500, {"ok": False, "error": "ClientHistoryUnavailable"})
    return _resp(200, {
        "ok": True,
        "client": {
            "id": requested_id,
            "name": profile.get("name"),
            "email": profile.get("email"),
            "phone": profile.get("phone"),
            "participationCount": int(profile.get("participationCount", 0)),
        },
        "participations": participations,
    })


def handle(event, method, dynamo, games_table, players_table):
    owner_id = (_get_claims(event).get("sub") or "").strip()
    if not owner_id:
        return _resp(401, {"ok": False, "error": "Unauthorized"})
    if method != "GET":
        return _resp(405, {"ok": False, "error": "MethodNotAllowed"})
    path_params = event.get("pathParameters") or {}
    requested_id = path_params.get("clientId")
    if requested_id:
        return _detail(dynamo, owner_id, requested_id, games_table, players_table)
    return _list(event, dynamo, owner_id)
