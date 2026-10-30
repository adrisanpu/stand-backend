"""Atomic native-value DynamoDB fake for the STACK prize lifecycle."""
import re
from copy import deepcopy
from types import SimpleNamespace

from tests.email_fake import FakeDynamoError


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
                table.put_item(**args) if kind == "Put" else table.update_item(**args)
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
        if "id" in item:
            return item["gameId"], item["id"]
        return tuple(item[k] for k in ("gameId", "playerId") if k in item)

    def get_item(self, Key, **_):
        item = self.rows.get(self.key(Key))
        return {"Item": deepcopy(item)} if item else {}

    def query(self, **_):
        return {"Items": deepcopy(list(self.rows.values()))}

    def put_item(self, Item, ConditionExpression=None, **_):
        if ConditionExpression and self.key(Item) in self.rows:
            raise FakeDynamoError("ConditionalCheckFailedException")
        self.rows[self.key(Item)] = deepcopy(Item)

    @staticmethod
    def path(value, names):
        return [names.get(k, k) for k in value.strip().split(".")]

    def read(self, item, path, names):
        for key in self.path(path, names):
            if not isinstance(item, dict) or key not in item:
                return None
            item = item[key]
        return item

    def update_item(self, Key, UpdateExpression, ConditionExpression="", ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, **_):
        if re.search(r"(?<![#:])\bwork\b", UpdateExpression + " " + ConditionExpression, re.IGNORECASE):
            raise FakeDynamoError("ValidationException")
        names, values = ExpressionAttributeNames or {}, ExpressionAttributeValues or {}
        item = self.rows.get(self.key(Key))
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
