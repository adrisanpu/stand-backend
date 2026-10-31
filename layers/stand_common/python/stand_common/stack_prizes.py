"""STACK-only prize rules and durable, individually redeemable awards."""
import hashlib
import json
import os
import re
import secrets
import time
import uuid
from copy import deepcopy
from decimal import Decimal
from urllib.parse import urlencode, urlsplit

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError

from stand_common import kivly_registration as registration
from stand_common.kivly import reward_image_url
from stand_common.prizes import prize_image_key
from stand_common.stack import get_ranking
from stand_common.utils import _iso_now, get_game_type_blob, log

CONDITIONS = {"completed", "top_finish", "top_final", "raffle", "score"}
SCORE_OPERATORS = {">", ">=", "=", "<=", "<", "between"}
MAX_SCORE = 9007199254740991
ID = re.compile(r"[a-zA-Z0-9_-]{1,80}", re.ASCII)


class PrizeError(Exception):
    def __init__(self, code, status=409):
        super().__init__(code)
        self.code, self.status = code, status


def table(dynamo):
    return dynamo.Table(os.environ["STACK_PRIZES_TABLE"])


def enqueue(row):
    queue_url = os.environ.get("STACK_PRIZES_QUEUE")
    if not queue_url:
        log("stack_prize_enqueue_failed", {"gameId": row["gameId"], "code": "MissingQueueConfiguration"})
        return False
    group = row.get("customerId") or row["gameId"]
    try:
        boto3.client("sqs").send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({"gameId": row["gameId"], "id": row["id"]}),
            MessageGroupId=hashlib.sha256(group.encode()).hexdigest(),
            MessageDeduplicationId=hashlib.sha256(
                f"{row['gameId']}:{row['id']}:{int(time.time()) // 60}".encode()).hexdigest(),
        )
    except (BotoCoreError, ClientError) as exc:
        # The committed outbox remains pending for scheduled recovery.
        log("stack_prize_enqueue_failed", {
            "gameId": row["gameId"], "code": type(exc).__name__,
        })
        return False
    return True


def players(dynamo, game_id):
    rows, last = [], None
    while True:
        args = {"KeyConditionExpression": Key("gameId").eq(game_id), "ConsistentRead": True}
        if last:
            args["ExclusiveStartKey"] = last
        page = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).query(**args)
        rows.extend(page.get("Items", []))
        last = page.get("LastEvaluatedKey")
        if not last:
            return rows


def records(dynamo, game_id):
    rows, last = [], None
    while True:
        args = {"KeyConditionExpression": Key("gameId").eq(game_id), "ConsistentRead": True}
        if last:
            args["ExclusiveStartKey"] = last
        page = table(dynamo).query(**args)
        rows.extend(page.get("Items", []))
        last = page.get("LastEvaluatedKey")
        if not last:
            return rows


def transact(dynamo, operations):
    # The resource client applies its Dynamo serializer to native Python values.
    try:
        dynamo.meta.client.transact_write_items(TransactItems=operations)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "TransactionCanceledException":
            raise PrizeError("StackPrizeConflict") from None
        raise


def put(row):
    return {"Put": {"TableName": os.environ["STACK_PRIZES_TABLE"], "Item": row,
                    "ConditionExpression": "attribute_not_exists(id)"}}


def game_update(meta, expression, values=None, names=None, extra=""):
    revision = meta.get("stackPrizeRevision", 0)
    condition = ("attribute_not_exists(stackPrizeRevision)" if revision == 0
                 else "stackPrizeRevision = :revision")
    vals = {":next": revision + 1, **(values or {})}
    if revision != 0:
        vals[":revision"] = revision
    return {"Update": {
        "TableName": os.environ["GAMES_TABLE"], "Key": {"gameId": meta["gameId"]},
        "UpdateExpression": "SET stackPrizeRevision = :next" + expression,
        "ConditionExpression": "attribute_exists(gameId) AND " + condition + extra,
        "ExpressionAttributeValues": vals,
        **({"ExpressionAttributeNames": names} if names else {}),
    }}


def storefront(value):
    if not isinstance(value, str) or len(value) > 300:
        raise PrizeError("InvalidKivlyStorefront", 400)
    try:
        parsed = urlsplit(value.strip())
        host = parsed.hostname or ""
        valid = (parsed.scheme == "https"
                 and re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+kivly\.store", host)
                 and not any(ord(c) < 32 for c in value)
                 and not parsed.username and not parsed.password and parsed.port in (None, 443)
                 and parsed.path in ("", "/") and not parsed.query and not parsed.fragment)
    except ValueError:
        valid = False
    if not valid:
        raise PrizeError("InvalidKivlyStorefront", 400)
    return "https://" + host


def legacy_program(meta):
    catalog, rules = [], []
    for slot, condition in (("ranking", "top_finish"), ("raffle", "raffle")):
        prize = (meta.get("prizeCatalog") or {}).get(slot)
        if not prize or not prize.get("name"):
            continue
        catalog.append({"id": "legacy_" + slot, "source": "stand", "name": prize["name"],
                        "stockTotal": 1000,
                        "imageKey": prize.get("imageKey"), "limit": prize.get("limit")})
        rule = {"id": "legacy_" + slot, "condition": condition, "mode": "all",
                "allocations": [{"prizeId": "legacy_" + slot, "quantity": 1}]}
        if slot == "ranking":
            rule["topX"] = prize.get("topX", 3)
        rules.append(rule)
    reward = meta.get("kivlyReward")
    if reward:
        catalog.append({"id": "legacy_kivly", "source": "kivly", "name": reward["name"],
                        "stockTotal": 1000,
                        "rewardId": reward["id"], "locationId": reward["locationId"]})
        rules.append({"id": "legacy_kivly", "condition": "completed", "mode": "all",
                      "allocations": [{"prizeId": "legacy_kivly", "quantity": 1}]})
    return {"version": 2, "catalog": catalog, "rules": rules, "storefrontUrl": ""}


def upgrade_program(raw):
    if not isinstance(raw, dict):
        raise PrizeError("InvalidStackPrizeProgram", 400)
    if raw.get("version") == 3:
        return {**deepcopy(raw), "deliveryChannel": raw.get("deliveryChannel", "wallet")}
    if raw.get("version") not in (1, 2):
        raise PrizeError("InvalidStackPrizeProgram", 400)
    if not isinstance(raw.get("catalog"), list) or not isinstance(raw.get("rules"), list):
        raise PrizeError("InvalidStackPrizeProgram", 400)
    catalog = []
    for prize in raw.get("catalog", []):
        if not isinstance(prize, dict):
            raise PrizeError("InvalidStackPrize", 400)
        catalog.append({**prize, **({"stockTotal": 1000} if raw["version"] == 1 else {})})
    rules = []
    for rule in raw.get("rules", []):
        if not isinstance(rule, dict):
            raise PrizeError("InvalidStackPrizeRule", 400)
        if raw["version"] == 1:
            if (set(rule) - {"id", "condition", "topX", "prizeIds"}
                    or not isinstance(rule.get("prizeIds"), list)):
                raise PrizeError("InvalidStackPrizeRule", 400)
            allocations = [{"prizeId": pid, "quantity": 1} for pid in rule["prizeIds"]]
            mode = "all"
        else:
            if (set(rule) - {"id", "condition", "topX", "mode", "allocations"}
                    or rule.get("mode") not in ("all", "random_one")
                    or not isinstance(rule.get("allocations"), list)
                    or not 1 <= len(rule["allocations"]) <= 8):
                raise PrizeError("InvalidStackPrizeRule", 400)
            allocations, mode = rule["allocations"], rule["mode"]
            if (any(not isinstance(a, dict) or not isinstance(a.get("prizeId"), str)
                    for a in allocations)
                    or len({a["prizeId"] for a in allocations}) != len(allocations)):
                raise PrizeError("InvalidStackRulePrizes", 400)
        rules.append({
            "id": rule.get("id"), "condition": rule.get("condition"),
            **({"topX": rule["topX"]} if "topX" in rule else {}),
            "groups": ([{"allocations": allocations}] if mode == "all"
                       else [{"allocations": [a]} for a in allocations]),
        })
    return {"version": 3, "catalog": catalog, "rules": rules,
            "storefrontUrl": raw.get("storefrontUrl", ""),
            "deliveryChannel": raw.get("deliveryChannel", "wallet")}


def validate_program(raw, meta, conn, rewards, raffles_enabled):
    if (not isinstance(raw, dict) or set(raw) - {"version", "catalog", "rules", "storefrontUrl", "deliveryChannel"}
            or not {"version", "catalog", "rules", "storefrontUrl"} <= set(raw)):
        raise PrizeError("InvalidStackPrizeProgram", 400)
    if type(raw["version"]) is not int:
        raise PrizeError("InvalidStackPrizeProgram", 400)
    raw = upgrade_program(raw)
    if raw["deliveryChannel"] not in ("wallet", "email"):
        raise PrizeError("InvalidStackDeliveryChannel", 400)
    if raw["deliveryChannel"] == "email":
        if os.environ.get("STACK_REWARD_EMAIL_ENABLED", "false").lower() != "true":
            raise PrizeError("RewardEmailUnavailable", 503)
        if not conn or not registration.account_game_settings(meta, conn)["enabled"]:
            raise PrizeError("KivlyConnectionRequired")
    if (raw["version"] != 3
            or not isinstance(raw["storefrontUrl"], str)
            or not isinstance(raw["catalog"], list) or len(raw["catalog"]) > 32):
        raise PrizeError("InvalidStackPrizeCatalog", 400)
    if not isinstance(raw["rules"], list) or len(raw["rules"]) > 8:
        raise PrizeError("InvalidStackPrizeRules", 400)
    result = {"version": 3, "catalog": [], "rules": [],
              "storefrontUrl": storefront(raw["storefrontUrl"]) if raw["storefrontUrl"] else "",
              "deliveryChannel": raw["deliveryChannel"]}
    previous = upgrade_program(meta.get("stackPrizeProgram") or legacy_program(meta))
    previous_catalog = {p["id"]: p for p in previous["catalog"]}
    previous_rules = {r["id"]: r for r in previous["rules"]}
    ids = set()
    for prize in raw["catalog"]:
        if (not isinstance(prize, dict) or not isinstance(prize.get("id"), str)
                or not ID.fullmatch(prize["id"]) or prize["id"] in ids):
            raise PrizeError("InvalidStackPrizeId", 400)
        ids.add(prize["id"])
        name = prize.get("name")
        if not isinstance(name, str) or not name.strip() or len(name) > 200 or any(ord(c) < 32 for c in name):
            raise PrizeError("InvalidStackPrizeName", 400)
        value = {"id": prize["id"], "source": prize.get("source"), "name": name.strip()}
        stock_total = prize.get("stockTotal")
        if type(stock_total) is not int or not 1 <= stock_total <= 1000:
            raise PrizeError("InvalidStackPrizeStock", 400)
        value["stockTotal"] = stock_total
        if value["source"] == "stand":
            if set(prize) - {"id", "source", "name", "imageKey", "imageUrl", "limit", "stockTotal"}:
                raise PrizeError("InvalidStackPrize", 400)
            key = prize.get("imageKey")
            if key and not any(prize_image_key(meta["ownerUserId"], meta["gameId"], slot, key)
                               for slot in ("ranking", "raffle", "validation")):
                raise PrizeError("InvalidStackPrizeImage", 400)
            limit = prize.get("limit")
            if limit is not None and (type(limit) is not int or limit < 0):
                raise PrizeError("InvalidStackPrizeLimit", 400)
            value.update(imageKey=key, limit=limit)
        elif value["source"] == "kivly":
            if set(prize) - {"id", "source", "name", "rewardId", "locationId", "stockTotal", "imageUrl", "imageKey"}:
                raise PrizeError("InvalidStackPrize", 400)
            if "imageKey" in prize:
                key = prize["imageKey"]
                if key is not None and not any(
                    prize_image_key(meta["ownerUserId"], meta["gameId"], slot, key)
                    for slot in ("ranking", "raffle", "validation")
                ):
                    raise PrizeError("InvalidStackPrizeImage", 400)
                value["imageKey"] = key
            if not conn or prize.get("locationId") != conn["locationId"]:
                raise PrizeError("KivlyConnectionChanged")
            selected = next((r for r in rewards if r["id"] == prize.get("rewardId")), None)
            if not selected:
                raise PrizeError("KivlyRewardUnavailable")
            value.update(name=selected["name"], rewardId=selected["id"], locationId=conn["locationId"])
            image = reward_image_url(selected.get("imageUrl"))
            if image:
                value["imageUrl"] = image
        else:
            raise PrizeError("InvalidStackPrizeSource", 400)
        # An award's identity cannot be reused for a different product.
        old = previous_catalog.get(value["id"])
        if old and any(old.get(k) != value.get(k) for k in ("source", "rewardId", "locationId")):
            raise PrizeError("StackPrizeIdentityChanged", 400)
        result["catalog"].append(value)
    rule_ids = set()
    for rule in raw["rules"]:
        if (not isinstance(rule, dict) or not isinstance(rule.get("id"), str)
                or not ID.fullmatch(rule["id"]) or rule["id"] in rule_ids
                or not isinstance(rule.get("condition"), str) or rule["condition"] not in CONDITIONS
                or set(rule) - {"id", "condition", "topX", "groups", "scoreOperator",
                                "scoreValue", "scoreMaxValue"}):
            raise PrizeError("InvalidStackPrizeRule", 400)
        rule_ids.add(rule["id"])
        groups = rule.get("groups")
        if not isinstance(groups, list) or not 1 <= len(groups) <= 8:
            raise PrizeError("InvalidStackRulePrizes", 400)
        for group in groups:
            if not isinstance(group, dict) or set(group) != {"allocations"}:
                raise PrizeError("InvalidStackRulePrizes", 400)
            allocations = group["allocations"]
            if (not isinstance(allocations, list) or not 1 <= len(allocations) <= 8
                    or any(not isinstance(a, dict) or set(a) != {"prizeId", "quantity"}
                           or not isinstance(a.get("prizeId"), str) or a["prizeId"] not in ids
                           or type(a.get("quantity")) is not int or not 1 <= a["quantity"] <= 10
                           for a in allocations)
                    or len({a["prizeId"] for a in allocations}) != len(allocations)):
                raise PrizeError("InvalidStackRulePrizes", 400)
        value = {"id": rule["id"], "condition": rule["condition"], "groups": groups}
        if rule["condition"].startswith("top_"):
            top = rule.get("topX")
            if type(top) is not int or not 1 <= top <= 100:
                raise PrizeError("InvalidStackPrizeTop", 400)
            value["topX"] = top
        if rule["condition"] == "score":
            operator = rule.get("scoreOperator")
            score_value = rule.get("scoreValue")
            if (not isinstance(operator, str) or operator not in SCORE_OPERATORS
                    or type(score_value) is not int or not 0 <= score_value <= MAX_SCORE):
                raise PrizeError("InvalidStackPrizeScore", 400)
            value.update(scoreOperator=operator, scoreValue=score_value)
            if operator == "between":
                maximum = rule.get("scoreMaxValue")
                if type(maximum) is not int or not score_value <= maximum <= MAX_SCORE:
                    raise PrizeError("InvalidStackPrizeScore", 400)
                value["scoreMaxValue"] = maximum
            elif "scoreMaxValue" in rule:
                raise PrizeError("InvalidStackPrizeScore", 400)
        elif any(key in rule for key in ("scoreOperator", "scoreValue", "scoreMaxValue")):
            raise PrizeError("InvalidStackPrizeRule", 400)
        if rule["condition"] == "raffle" and not raffles_enabled and rule != previous_rules.get(rule["id"]):
            raise PrizeError("RafflePlanRequired", 403)
        old = previous_rules.get(value["id"])
        if old and old != value:
            raise PrizeError("StackRuleIdentityChanged", 400)
        result["rules"].append(value)
    return result


def image_url(prize, meta=None):
    if prize.get("source") == "kivly" and "imageKey" not in prize:
        return reward_image_url(prize.get("imageUrl"))
    key = prize.get("imageKey")
    if prize.get("source") not in ("stand", "kivly") or (key and meta and not any(
        prize_image_key(meta["ownerUserId"], meta["gameId"], slot, key)
        for slot in ("ranking", "raffle", "validation")
    )):
        return None
    return boto3.client("s3").generate_presigned_url(
        "get_object", Params={"Bucket": os.environ["USER_MEDIA_BUCKET"], "Key": key}, ExpiresIn=3600,
    ) if key else None


def program_view(program, meta=None):
    program = upgrade_program(program)
    return {**program, "catalog": [{**p, **({"imageUrl": image_url(p, meta)}
                                         if p["source"] == "stand" or "imageKey" in p or p.get("imageUrl") else {})}
                                  for p in program["catalog"]]}


def effective_program(dynamo, meta, program=None):
    program = upgrade_program(program or meta.get("stackPrizeProgram") or legacy_program(meta))
    conn = registration.connection(dynamo, meta["ownerUserId"])
    return {**program, "storefrontUrl": resolved_storefront(program, conn)}


def resolved_storefront(program, conn):
    club = (conn or {}).get("clubSettings") or {}
    value = club["storefrontUrl"] if "storefrontUrl" in club else program.get("storefrontUrl", "")
    return storefront(value) if value != "" else ""


def promotional_rules(meta):
    program = upgrade_program(meta["stackPrizeProgram"])
    catalog = {p["id"]: p for p in program["catalog"]}
    result = []
    for rule in program["rules"]:
        ids = dict.fromkeys(a["prizeId"] for g in rule["groups"] for a in g["allocations"])
        configured = [catalog[pid] for pid in ids if catalog[pid].get("limit") != 0]
        if configured:
            result.append({
                "condition": rule["condition"],
                **({"topX": rule["topX"]} if "topX" in rule else {}),
                **({key: rule[key] for key in ("scoreOperator", "scoreValue", "scoreMaxValue")
                    if key in rule}),
                "names": [p["name"] for p in configured],
                "prizes": [{"id": p["id"], "name": p["name"], "imageUrl": image_url(p, meta)}
                           for p in configured],
            })
    return result


def eligible_allocations(group, catalog, rank):
    return [a for a in group["allocations"]
            if not (catalog[a["prizeId"]].get("limit") == 0
                    or (catalog[a["prizeId"]].get("limit") is not None
                        and rank is not None and rank > catalog[a["prizeId"]]["limit"]))]


def select_group(rule, catalog, stock, rank):
    groups = [eligible_allocations(g, catalog, rank) for g in rule["groups"]]
    available = [g for g in groups if any(
        stock.get(a["prizeId"], {}).get("remaining", 0) > 0 for a in g)]
    return secrets.choice(available) if len(available) > 1 else available[0] if available else []


def initial_stock(program, existing_awards=()):
    awarded = {}
    for row in existing_awards:
        prize = row.get("prize") or {}
        if row.get("id", "").startswith("award_") and isinstance(prize.get("id"), str):
            awarded[prize["id"]] = awarded.get(prize["id"], 0) + 1
    return {
        p["id"]: {"total": p["stockTotal"],
                  "remaining": max(0, p["stockTotal"] - awarded.get(p["id"], 0))}
        for p in upgrade_program(program)["catalog"]
    }


def updated_stock(meta, program, existing_awards=()):
    program = upgrade_program(program)
    previous_program = upgrade_program(meta.get("stackPrizeProgram") or legacy_program(meta))
    awarded = {}
    for row in existing_awards:
        prize = row.get("prize") or {}
        if row.get("id", "").startswith("award_") and isinstance(prize.get("id"), str):
            awarded[prize["id"]] = awarded.get(prize["id"], 0) + 1
    current = meta.get("stackPrizeStock") or initial_stock(previous_program, existing_awards)
    result = dict(current)
    for prize in program["catalog"]:
        old = current.get(prize["id"])
        if old:
            consumed = max(0, old["total"] - old["remaining"])
            if prize["stockTotal"] < consumed:
                raise PrizeError("StackPrizeStockBelowAwarded", 409)
            remaining = prize["stockTotal"] - consumed
        else:
            consumed = awarded.get(prize["id"], 0)
            if prize["stockTotal"] < consumed:
                raise PrizeError("StackPrizeStockBelowAwarded", 409)
            remaining = prize["stockTotal"] - consumed
        result[prize["id"]] = {"total": prize["stockTotal"], "remaining": remaining}
    return result


def score_matches(rule, score):
    if type(score) not in (int, Decimal) or (
            isinstance(score, Decimal) and not score.is_finite()):
        return False
    operator, value = rule["scoreOperator"], rule["scoreValue"]
    if operator == ">":
        return score > value
    if operator == ">=":
        return score >= value
    if operator == "=":
        return score == value
    if operator == "<=":
        return score <= value
    if operator == "<":
        return score < value
    return value <= score <= rule["scoreMaxValue"]


def award_rows(meta, player, program, conditions, rank=None, score=None):
    program = upgrade_program(program)
    catalog = {p["id"]: p for p in program["catalog"]}
    rows = []
    for rule in program["rules"]:
        if rule["condition"] not in conditions:
            continue
        if rule["condition"] == "score" and not score_matches(rule, score):
            continue
        if rule["condition"].startswith("top_") and (rank is None or rank > rule["topX"]):
            continue
        stock = meta.get("stackPrizeStock") or initial_stock(program)
        selected = select_group(rule, catalog, stock, rank)
        for allocation in selected:
            pid = allocation["prizeId"]
            prize = catalog[pid]
            if prize.get("limit") == 0 or (prize.get("limit") is not None and rank is not None and rank > prize["limit"]):
                continue
            rows.extend(award_units(meta, player, rule, prize, min(
                allocation["quantity"], stock[pid]["remaining"])))
    return rows


def award_units(meta, player, rule, prize, quantity, job_id=None):
    if (type(quantity) not in (int, Decimal)
            or (isinstance(quantity, Decimal) and not quantity.is_finite())
            or quantity != int(quantity) or not 0 <= quantity <= 10):
        raise PrizeError("InvalidStackPrizeQuantity", 400)
    rows = []
    for unit in range(int(quantity)):
        identity = hashlib.sha256(json.dumps([
            job_id or "legacy", int(player["playerId"]), rule["id"], prize["id"], unit,
        ]).encode()).hexdigest()
        row = {"gameId": meta["gameId"], "id": "award_" + identity,
               "playerId": int(player["playerId"]), "ruleId": rule["id"],
               "condition": rule["condition"], "prize": prize, "createdAt": _iso_now(),
               "ownerUserId": meta["ownerUserId"],
               "state": "available" if prize["source"] == "stand" else "queued"}
        if prize["source"] == "kivly":
            row["deliveryChannel"] = delivery_channel(player)
            if row["deliveryChannel"] == "email":
                row["emailState"] = "pending"
            membership = player.get("kivlyMembership") or {}
            if membership.get("locationId") != prize["locationId"] or not membership.get("customerId"):
                row["state"] = "connection_required"
            else:
                row.update(customerId=membership["customerId"], work="pending", dueAt=0)
        rows.append(row)
    return rows


def allocation_id(job_id, player_id, rule_id):
    digest = hashlib.sha256(json.dumps([job_id, int(player_id), rule_id]).encode()).hexdigest()
    return "allocation_" + digest


def allocate_rule(dynamo, meta, player, program, rule, job_id, rank=None, existing_records=None, score=None):
    program = upgrade_program(program)
    rule = next(r for r in program["rules"] if r["id"] == rule["id"])
    if rule["condition"] == "score" and not score_matches(rule, score):
        return
    if rule["condition"].startswith("top_") and (rank is None or rank > rule["topX"]):
        return
    key = {"gameId": meta["gameId"], "id": allocation_id(job_id, player["playerId"], rule["id"])}
    prize_table = table(dynamo)
    if prize_table.get_item(Key=key, ConsistentRead=True).get("Item"):
        return
    catalog = {p["id"]: p for p in program["catalog"]}
    current_meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": meta["gameId"]}, ConsistentRead=True,
    ).get("Item")
    if not current_meta:
        raise PrizeError("GameNotFound", 404)
    existing_records = existing_records if existing_records is not None else records(dynamo, meta["gameId"])
    stock = current_meta.get("stackPrizeStock") or initial_stock(program, existing_records)
    catalog = {p["id"]: p for p in program["catalog"]}
    prize_ids = {a["prizeId"] for g in rule["groups"] for a in g["allocations"]}
    already_awarded = [
        row for row in existing_records
        if row.get("id", "").startswith("award_")
        and int(row.get("playerId", -1)) == int(player["playerId"])
        and row.get("ruleId") == rule["id"]
        and (row.get("prize") or {}).get("id") in prize_ids
    ]
    if already_awarded:
        grouped = {}
        for row in already_awarded:
            prize_id = row["prize"]["id"]
            grouped[prize_id] = grouped.get(prize_id, 0) + 1
        ledger = {"gameId": meta["gameId"], "id": key["id"], "kind": "allocation",
                  "playerId": int(player["playerId"]), "ruleId": rule["id"],
                  "requested": [a for g in rule["groups"] for a in g["allocations"]], "awarded": [
                      {"prizeId": pid, "quantity": quantity, "name": catalog[pid]["name"]}
                      for pid, quantity in grouped.items()
                  ], "state": "allocated", "createdAt": _iso_now()}
        transact(dynamo, [
            game_update(current_meta, ", stackPrizeStock = :stock", {":stock": stock}),
            put(ledger),
        ])
        return
    rank_eligible = any(eligible_allocations(g, catalog, rank) for g in rule["groups"])
    selected = select_group(rule, catalog, stock, rank)
    new_stock = dict(stock)
    units = []
    requested = []
    for allocation in selected:
        pid = allocation["prizeId"]
        prize = catalog[pid]
        requested.append({"prizeId": pid, "quantity": allocation["quantity"]})
        available = stock.get(pid, {}).get("remaining", prize["stockTotal"])
        quantity = min(allocation["quantity"], max(0, available))
        if quantity == 0:
            continue
        new_stock[pid] = {"total": stock.get(pid, {}).get("total", prize["stockTotal"]),
                          "remaining": available - quantity}
        units.extend(award_units(meta, player, rule, prize, quantity, job_id))
    grouped = {}
    for row in units:
        pid = row["prize"]["id"]
        grouped[pid] = grouped.get(pid, 0) + 1
    ledger = {"gameId": meta["gameId"], "id": key["id"], "kind": "allocation",
              "playerId": int(player["playerId"]), "ruleId": rule["id"],
              "requested": requested, "awarded": [
                  {"prizeId": pid, "quantity": quantity, "name": catalog[pid]["name"]}
                  for pid, quantity in grouped.items()
              ], "state": "allocated" if units else "out_of_stock" if rank_eligible else "not_eligible",
              "createdAt": _iso_now()}
    # One rule can create at most 8 * 10 unit rows, leaving room below DynamoDB's
    # 100-operation transaction limit for the stock reservation and ledger.
    transact(dynamo, [
        game_update(current_meta, ", stackPrizeStock = :stock", {":stock": new_stock}),
        put(ledger), *[put(row) for row in units],
    ])
    for row in units:
        if row.get("work") == "pending":
            enqueue(row)


def start(dynamo, meta, player, attempt):
    if meta.get("stackPrizeState", "open") != "open":
        raise PrizeError("StackClosing")
    if "stackPrizeProgram" in meta:
        attempt = {**attempt, "prizeProgram": upgrade_program(meta["stackPrizeProgram"])}
    op = {"Update": {"TableName": os.environ["GAMEPLAYER_TABLE"],
        "Key": {"gameId": meta["gameId"], "playerId": player["playerId"]},
        "UpdateExpression": "SET #t.#g.#a = :attempt",
        "ConditionExpression": "attribute_exists(playerId) AND attribute_not_exists(#t.#g.#a)",
        "ExpressionAttributeNames": {"#t": "type", "#g": "STACK", "#a": "attempt"},
        "ExpressionAttributeValues": {":attempt": attempt}}}
    transact(dynamo, [game_update(meta, "", {":active": True},
                                  extra=" AND isActive = :active"), op])


def finish(dynamo, meta, player, final_attempt, score):
    if meta.get("stackPrizeState") == "closed":
        raise PrizeError("StackClosed")
    gp = dynamo.Table(os.environ["GAMEPLAYER_TABLE"])
    ranking = get_ranking(meta["gameId"], gp, None)["results"]
    candidate = {"playerId": int(player["playerId"]), "score": score,
                 "finishedAt": final_attempt["finishedAt"]}
    ranking = sorted([r for r in ranking if r["playerId"] != candidate["playerId"]] + [candidate],
                     key=lambda r: (-r["score"], r["finishedAt"], r["playerId"]))
    rank = next(i for i, row in enumerate(ranking, 1) if row["playerId"] == candidate["playerId"])
    program = upgrade_program(final_attempt.get("prizeProgram", meta.get("stackPrizeProgram") or legacy_program(meta)))
    values = {":attempt": final_attempt, ":score": score, ":rank": rank,
              ":now": final_attempt["finishedAt"], ":started": "started",
              ":id": final_attempt["attemptId"]}
    op = {"Update": {"TableName": os.environ["GAMEPLAYER_TABLE"],
        "Key": {"gameId": meta["gameId"], "playerId": player["playerId"]},
        "UpdateExpression": "SET #t.#g.#a = :attempt, #t.#g.score = :score, #t.#g.scoreUpdatedAt = :now, #t.#g.rankAtFinish = :rank",
        "ConditionExpression": "#t.#g.#a.#s = :started AND #t.#g.#a.attemptId = :id AND attribute_not_exists(#t.#g.score)",
        "ExpressionAttributeNames": {"#t": "type", "#g": "STACK", "#a": "attempt", "#s": "status"},
        "ExpressionAttributeValues": values}}
    operations = [game_update(meta, ""), op]
    task = None
    if any(rule["condition"] in {"completed", "top_finish", "score"} for rule in program["rules"]):
        task = job(meta, "finish", [{"playerId": int(player["playerId"]), "rank": rank, "score": score}])
        task["id"] = "job_" + hashlib.sha256(json.dumps([
            meta["gameId"], int(player["playerId"]), final_attempt["attemptId"],
        ]).encode()).hexdigest()
        task["program"] = program
        operations.append(put(task))
    transact(dynamo, operations)
    if task:
        enqueue(task)
    return rank


def job(meta, condition, winner_ids):
    return {"gameId": meta["gameId"], "id": "job_" + uuid.uuid4().hex, "kind": "grant",
            "program": upgrade_program(meta.get("stackPrizeProgram") or legacy_program(meta)), "condition": condition,
            "winners": winner_ids, "ownerUserId": meta["ownerUserId"], "work": "pending", "dueAt": 0}


def close(dynamo, meta, excluded):
    if meta.get("stackPrizeState") == "closed":
        return
    if meta.get("stackPrizeState", "open") == "open":
        task = {"gameId": meta["gameId"], "id": "close", "kind": "close", "work": "pending", "dueAt": 0}
        expression, values = ", stackPrizeState = :state", {":state": "closing"}
        if "stackPrizeProgram" not in meta:
            program = upgrade_program(legacy_program(meta))
            expression += ", stackPrizeProgram = :program, stackPrizeStock = :stock"
            values.update({":program": program, ":stock": initial_stock(
                program, records(dynamo, meta["gameId"]))})
        transact(dynamo, [game_update(meta, expression, values), put(task)])
        raise PrizeError("StackClosingStarted")
    all_players = players(dynamo, meta["gameId"])
    pending = [p for p in all_players if get_game_type_blob(p, "STACK").get("attempt", {}).get("status") == "started"]
    pending_ids = {int(p["playerId"]) for p in pending}
    if set(excluded) - pending_ids:
        raise PrizeError("StackPendingChanged")
    for player in pending:
        if int(player["playerId"]) not in excluded:
            continue
        op = {"Update": {"TableName": os.environ["GAMEPLAYER_TABLE"],
            "Key": {"gameId": meta["gameId"], "playerId": player["playerId"]},
            "UpdateExpression": "SET #t.#g.#a.#s = :excluded",
            "ConditionExpression": "#t.#g.#a.#s = :started",
            "ExpressionAttributeNames": {"#t": "type", "#g": "STACK", "#a": "attempt", "#s": "status"},
            "ExpressionAttributeValues": {":excluded": "excluded", ":started": "started"}}}
        transact(dynamo, [game_update(meta, ""), op])
        meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
            Key={"gameId": meta["gameId"]}, ConsistentRead=True)["Item"]
    if pending_ids - set(excluded):
        return
    ranking = get_ranking(meta["gameId"], dynamo.Table(os.environ["GAMEPLAYER_TABLE"]), None)["results"]
    task = job(meta, "top_final", [{"playerId": r["playerId"], "rank": r["rank"]}
                                 for r in ranking[:100]])
    transact(dynamo, [game_update(meta, ", stackPrizeState = :closed, stackClosedAt = :now, isActive = :inactive",
                                  {":closed": "closed", ":now": _iso_now(), ":inactive": False}), put(task)])


def delivery_channel(player):
    attempt = get_game_type_blob(player, "STACK").get("attempt") or {}
    return (attempt.get("prizeProgram") or {}).get("deliveryChannel", "wallet")


def wallet_link(dynamo, meta, player):
    if delivery_channel(player) == "email":
        return None
    membership = player.get("kivlyMembership") or {}
    conn = registration.connection(dynamo, meta["ownerUserId"])
    if not conn or membership.get("locationId") != conn["locationId"] or not membership.get("customerId"):
        return None
    origin = resolved_storefront(meta.get("stackPrizeProgram") or legacy_program(meta), conn)
    if not origin:
        return None
    return origin + "/add-to-wallet?" + urlencode({
        "customer_id": membership["customerId"], "location_id": conn["locationId"],
    })


def public_awards(dynamo, meta, player, issue_token):
    awards = []
    for row in records(dynamo, meta["gameId"]):
        if not row["id"].startswith("award_") or row["playerId"] != player["playerId"]:
            continue
        prize = row["prize"]
        awards.append({"id": row["id"], "source": prize["source"], "name": prize["name"],
                       "imageUrl": image_url(prize, meta), "condition": row["condition"], "state": row["state"],
                       "deliveryChannel": row.get("deliveryChannel", "wallet"),
                       "emailState": row.get("emailState"), "emailError": row.get("emailError"),
                       "token": issue_token(row["id"]) if prize["source"] == "stand" and row["state"] == "available" else None})
    channel = delivery_channel(player)
    return {"awards": awards, "walletUrl": wallet_link(dynamo, meta, player), "deliveryChannel": channel,
            "walletExpected": channel == "wallet" and (bool(player.get("kivlyMembership")) or any(
                p["source"] == "kivly" for p in meta["stackPrizeProgram"]["catalog"])),
            "state": meta.get("stackPrizeState", "open"),
            "finalPrizesPending": meta.get("stackPrizeState") != "closed" and any(
                r["condition"] == "top_final" for r in meta["stackPrizeProgram"]["rules"])}


def redeem(dynamo, game_id, player_id, award_id):
    key = {"gameId": game_id, "id": award_id}
    row = table(dynamo).get_item(Key=key, ConsistentRead=True).get("Item")
    if not row or row.get("playerId") != player_id or row["prize"]["source"] != "stand":
        raise PrizeError("NotEligible", 400)
    try:
        table(dynamo).update_item(
            Key=key, UpdateExpression="SET #s = :delivered, deliveredAt = :now",
            ConditionExpression="#s = :available",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":delivered": "delivered", ":available": "available", ":now": _iso_now()},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise PrizeError("AlreadyDelivered") from None
        raise
    return {"name": row["prize"]["name"], "limit": None, "imageUrl": image_url(row["prize"])}
