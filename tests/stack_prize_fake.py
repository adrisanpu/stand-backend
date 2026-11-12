"""Atomic native-value DynamoDB fake for the STACK prize lifecycle."""
import os
import re
from copy import deepcopy
from types import SimpleNamespace

from tests.email_fake import FakeDynamoError


class Condition:
    def __init__(self, name=None, operator=None, values=()):
        self.name, self.operator, self.values = name, operator, values

    def eq(self, value):
        return Condition(operator="=", values=(self, value))

    def begins_with(self, value):
        return Condition(operator="begins_with", values=(self, value))

    def __or__(self, other):
        return Condition(operator="OR", values=(self, other))

    def __and__(self, other):
        return Condition(operator="AND", values=(self, other))

    def get_expression(self):
        return {"operator": self.operator, "values": self.values}


class PrizeDynamo:
    def __init__(self):
        self.tables = {}
        self.meta = SimpleNamespace(client=self)

    def Table(self, name):
        if name not in self.tables:
            self.tables[name] = PrizeTable(self, name)
        return self.tables[name]

    def transact_write_items(self, TransactItems):
        self.last_transaction = deepcopy(TransactItems)
        before = {name: deepcopy(t.rows) for name, t in self.tables.items()}
        try:
            for op in TransactItems:
                kind, data = next(iter(op.items()))
                table = self.Table(data["TableName"])
                args = {k: v for k, v in data.items() if k != "TableName"}
                if kind == "ConditionCheck":
                    table.check(table.rows.get(table.key(args["Key"])), args["ConditionExpression"],
                                args.get("ExpressionAttributeNames", {}), args.get("ExpressionAttributeValues", {}))
                elif kind == "Put":
                    table.put_item(**args)
                elif kind == "Delete":
                    table.check(table.rows.get(table.key(args["Key"])), args.get("ConditionExpression", ""),
                                args.get("ExpressionAttributeNames", {}), args.get("ExpressionAttributeValues", {}))
                    table.delete_item(**args)
                else:
                    table.update_item(**args)
        except FakeDynamoError:
            for name, table in self.tables.items():
                table.rows = before.get(name, {})
            raise FakeDynamoError("TransactionCanceledException") from None


class PrizeTable:
    def __init__(self, dynamo, name):
        self.rows = {}
        self.meta = dynamo.meta
        self.name = name

    def key(self, item):
        if "ownerId" in item:
            return (item["ownerId"], item["key"])
        if "userId" in item:
            return (item["userId"],)
        if "id" in item:
            return (item.get("gameId"), item["id"]) if self.name == os.environ.get("STACK_PRIZES_TABLE") else (item["id"],)
        return tuple(item[k] for k in ("gameId", "playerId") if k in item)

    def get_item(self, Key, **_):
        item = self.rows.get(self.key(Key))
        return {"Item": deepcopy(item)} if item else {}

    @classmethod
    def matches(cls, item, condition):
        if condition is None:
            return True
        expr = condition.get_expression()
        if not isinstance(expr, dict):
            return True  # Older fixtures use the SDK condition stub from conftest.
        values = expr["values"]
        if expr["operator"] == "OR":
            return any(cls.matches(item, value) for value in values)
        if expr["operator"] == "AND":
            return all(cls.matches(item, value) for value in values)
        actual = item.get(values[0].name)
        return (str(actual or "").startswith(values[1]) if expr["operator"] == "begins_with"
                else actual == values[1])

    def query(self, KeyConditionExpression=None, Limit=None, ExclusiveStartKey=None, ScanIndexForward=None, **_):
        rows = [row for row in self.rows.values() if self.matches(row, KeyConditionExpression)]
        if Limit is not None or ScanIndexForward is not None or ExclusiveStartKey:
            rows.sort(key=self.key, reverse=ScanIndexForward is False)
        if ExclusiveStartKey:
            start = self.key(ExclusiveStartKey)
            rows = [row for row in rows if (self.key(row) < start if ScanIndexForward is False else self.key(row) > start)]
        result = {"Items": deepcopy(rows[:Limit] if Limit else rows)}
        if Limit and len(rows) > Limit:
            last = rows[Limit - 1]
            keys = ("ownerId", "key") if "ownerId" in last else (
                ("gameId", "id") if "id" in last else ("gameId", "playerId"))
            result["LastEvaluatedKey"] = {key: last[key] for key in keys}
        return result

    def scan(self, FilterExpression=None, **_):
        return {"Items": deepcopy([row for row in self.rows.values() if self.matches(row, FilterExpression)])}

    def put_item(self, Item, ConditionExpression="", ExpressionAttributeValues=None, ExpressionAttributeNames=None, **_):
        self.check(self.rows.get(self.key(Item)), ConditionExpression or "", ExpressionAttributeNames or {},
                   ExpressionAttributeValues or {})
        self.rows[self.key(Item)] = deepcopy(Item)

    def delete_item(self, Key, **_):
        self.rows.pop(self.key(Key), None)

    @staticmethod
    def path(value, names):
        return [names.get(k, k) for k in value.strip().split(".")]

    def read(self, item, path, names):
        for key in self.path(path, names):
            if not isinstance(item, dict) or key not in item:
                return None
            item = item[key]
        return item

    def check(self, item, ConditionExpression, names, values):
        for test in ConditionExpression.split(" AND "):
            test = test.strip()
            if not test:
                continue
            if test.startswith("attribute_not_exists("):
                ok = self.read(item, test[21:-1], names) is None
            elif test.startswith("attribute_exists("):
                ok = self.read(item, test[17:-1], names) is not None
            else:
                left, right = test.split(" = ")
                ok = self.read(item, left, names) == values[right]
            if not ok:
                raise FakeDynamoError("ConditionalCheckFailedException")

    def update_item(self, Key, UpdateExpression, ConditionExpression="", ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, **_):
        if re.search(r"(?<![#:])\bwork\b", UpdateExpression + " " + ConditionExpression, re.IGNORECASE):
            raise FakeDynamoError("ValidationException")
        names, values = ExpressionAttributeNames or {}, ExpressionAttributeValues or {}
        self.check(self.rows.get(self.key(Key)), ConditionExpression, names, values)
        item = self.rows.setdefault(self.key(Key), deepcopy(Key))
        for match in re.finditer(r"(SET|REMOVE) (.*?)(?= SET | REMOVE |$)", UpdateExpression):
            action, body = match.groups()
            for assignment in body.split(","):
                path, _, value = assignment.strip().partition(" = ")
                keys = self.path(path, names)
                target = item
                for key in keys[:-1]:
                    target = target.setdefault(key, {})
                if action == "REMOVE":
                    target.pop(keys[-1], None)
                else:
                    target[keys[-1]] = deepcopy(values[value])
