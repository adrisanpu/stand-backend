"""Small stateful DynamoDB resource fake for email-verification tests."""
from copy import deepcopy
from threading import RLock


class FakeDynamoError(Exception):
    def __init__(self, code, reasons=None):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}
        if reasons is not None:
            self.response["CancellationReasons"] = reasons


class FakeDynamo:
    def __init__(self):
        self.tables = {}
        self.lock = RLock()

    def Table(self, name):
        return self.tables.setdefault(name, FakeTable(self, name))

    def seed(self, table_name, item):
        table = self.Table(table_name)
        with self.lock:
            table.items[self._key(table_name, item)] = deepcopy(item)

    def rows(self, table_name):
        with self.lock:
            return deepcopy(list(self.Table(table_name).items.values()))

    @staticmethod
    def _key(table_name, item):
        if "id" in item:
            return (item["id"],)
        if "gameId" in item and "playerId" in item:
            return item["gameId"], int(item["playerId"])
        if "gameId" in item:
            return (item["gameId"],)
        raise ValueError(f"Cannot determine key for {table_name}: {item!r}")


class FakeTable:
    def __init__(self, dynamo, name):
        self.dynamo = dynamo
        self.name = name
        self.items = {}
        self.meta = type("Meta", (), {})()
        self.meta.client = type("Client", (), {})()
        self.meta.client.transact_write_items = self._transact_write_items

    def _key(self, key):
        return self.dynamo._key(self.name, key)

    def get_item(self, Key, **kwargs):
        with self.dynamo.lock:
            item = self.items.get(self._key(Key))
            return {"Item": deepcopy(item)} if item is not None else {}

    def query(self, **kwargs):
        key_condition = kwargs.get("KeyConditionExpression")
        game_id = getattr(getattr(key_condition, "values", None), "gameId", None)
        if game_id is None:
            # boto3's condition stub is a MagicMock in this test environment.
            game_id = kwargs.get("game_id")
        with self.dynamo.lock:
            items = [deepcopy(item) for item in self.items.values()
                     if game_id is None or item.get("gameId") == game_id]
        items.sort(key=lambda item: int(item.get("playerId", 0)))
        if kwargs.get("ScanIndexForward") is False:
            items.reverse()
        if "Limit" in kwargs:
            items = items[:kwargs["Limit"]]
        return {"Items": items}

    def put_item(self, Item, ConditionExpression=None, **kwargs):
        with self.dynamo.lock:
            key = self._key(Item)
            if ConditionExpression and key in self.items:
                raise FakeDynamoError("ConditionalCheckFailedException")
            self.items[key] = deepcopy(Item)
        return {}

    def update_item(
        self, Key, UpdateExpression, ConditionExpression=None,
        ExpressionAttributeNames=None, ExpressionAttributeValues=None,
        ReturnValues=None, **kwargs
    ):
        values = ExpressionAttributeValues or {}
        names = ExpressionAttributeNames or {}
        with self.dynamo.lock:
            key = self._key(Key)
            item = self.items.setdefault(key, deepcopy(Key))
            if not self._condition(self.name, item, ConditionExpression, values, names):
                raise FakeDynamoError("ConditionalCheckFailedException")
            self._apply_update(item, UpdateExpression, values, names)
            result = {"Attributes": deepcopy(item)} if ReturnValues == "ALL_NEW" else {}
            return result

    def _transact_write_items(self, TransactItems):
        with self.dynamo.lock:
            staged = {name: deepcopy(table.items) for name, table in self.dynamo.tables.items()}
            reasons = []
            failed = False
            for action in TransactItems:
                kind, data = next(iter(action.items()))
                table_name = data["TableName"]
                table_items = staged.setdefault(table_name, {})
                table = self.dynamo.Table(table_name)
                key_data = data.get("Key") or data.get("Item")
                key = self.dynamo._key(table_name, key_data)
                item = table_items.get(key)
                values = data.get("ExpressionAttributeValues") or {}
                names = data.get("ExpressionAttributeNames") or {}
                condition = data.get("ConditionExpression")
                ok = self._condition(table_name, item, condition, values, names, kind, data)
                if not ok:
                    failed = True
                    reasons.append({"Code": "ConditionalCheckFailed"})
                    continue
                reasons.append({"Code": "None"})
                if kind == "Put":
                    table_items[key] = deepcopy(data["Item"])
                elif kind == "Update":
                    if item is None:
                        item = deepcopy(data["Key"])
                        table_items[key] = item
                    self._apply_update(item, data["UpdateExpression"], values, names)
            if failed:
                raise FakeDynamoError("TransactionCanceledException", reasons)
            for name, items in staged.items():
                self.dynamo.Table(name).items = items
        return {}

    @staticmethod
    def _condition(table_name, item, condition, values, names, kind=None, data=None):
        if not condition:
            return True
        exists = item is not None
        item = item or {}
        if "lastSentAt" in condition:
            return "lastSentAt" not in item or item["lastSentAt"] <= values[":before"]
        if "#count" in condition:
            return "count" not in item or item["count"] < values[":limit"]
        if "attribute_not_exists(id)" in condition:
            return not exists
        if "challengeId = :cid" in condition:
            return item is not None and item.get("challengeId") == values[":cid"]
        if "attempts < :max AND expiresAt > :now AND #state IN" in condition:
            return (
                item.get("attempts", 0) < values[":max"]
                and item.get("expiresAt", 0) > values[":now"]
                and item.get("state") in (values[":pending"], values[":completed"])
            )
        if "attempts < :max AND expiresAt > :now" in condition:
            return item.get("attempts", 0) < values[":max"] and item.get("expiresAt", 0) > values[":now"]
        if "attribute_exists(verifiedAt) AND expiresAt > :now" in condition:
            return "verifiedAt" in item and item.get("expiresAt", 0) > values[":now"] and item.get("attempts", 0) < values[":max"]
        if "attribute_exists(verifiedAt) AND (attribute_not_exists(playerId)" in condition:
            return (
                exists
                and "verifiedAt" in item
                and ("playerId" not in item or item.get("playerId") == values[":pid"])
            )
        if "attribute_not_exists(playerId)" in condition:
            return not exists
        if "attribute_exists(playerId)" in condition:
            return exists and "playerId" in item
        if "attribute_exists(gameId)" in condition:
            return (
                exists
                and ("playersCount" not in item or item["playersCount"] < values[":max"])
                and ("isActive" not in item or item["isActive"] is values[":active"])
            )
        if "attribute_not_exists(playersCount) OR playersCount < :max" in condition:
            return "playersCount" not in item or item["playersCount"] < values[":max"]
        if "attempts < :max" in condition:
            return item.get("attempts", 0) < values[":max"]
        return True

    @staticmethod
    def _apply_update(item, expression, values, names):
        if "ADD attempts :one" in expression:
            item["attempts"] = int(item.get("attempts", 0)) + int(values[":one"])
            return
        if "ADD #count :one" in expression:
            item["count"] = int(item.get("count", 0)) + int(values[":one"])
            item["expiresAt"] = values[":ttl"]
            return
        if "ADD playersCount :one" in expression:
            item["playersCount"] = int(item.get("playersCount", 0)) + int(values[":one"])
            item["lastJoinAt"] = values[":now"]
            item["updatedAt"] = values[":now"]
            return
        if "ADD playersCount :neg" in expression:
            item["playersCount"] = int(item.get("playersCount", 0)) + int(values[":neg"])
            item["updatedAt"] = values[":now"]
            return
        if "lastSentAt = :now" in expression:
            item.update(lastSentAt=values[":now"], challengeId=values[":cid"], expiresAt=values[":ttl"])
            return
        if "if_not_exists(verifiedAt, :now)" in expression:
            item.setdefault("verifiedAt", values[":now"])
            return
        if "emailVerifiedAt = if_not_exists" in expression:
            item.setdefault("emailVerifiedAt", values[":at"])
            item["emailVerificationMethod"] = values[":method"]
            return
        if "SET lastJoinAt" in expression:
            item.update(lastJoinAt=values[":now"], updatedAt=values[":now"])
            item["playersCount"] = int(item.get("playersCount", 0)) + int(values[":one"])
            return
        if "if_not_exists(playersCount, :z)" in expression:
            item.setdefault("playersCount", values[":z"])
            item.setdefault("validatedCount", values[":z"])
            return
        if "ADD playersCount :one SET lastJoinAt" in expression:
            item["playersCount"] = int(item.get("playersCount", 0)) + int(values[":one"])
            item["lastJoinAt"] = values[":now"]
            item["updatedAt"] = values[":now"]
            return
        if "SET" in expression:
            attr = names.get("#state", "state")
            if ":failed" in values:
                item[attr] = values[":failed"]
            elif ":pending" in values and ":sending" in values:
                item[attr] = values[":pending"]
            elif ":completed" in values:
                item[attr] = values[":completed"]
                item["playerId"] = int(values[":pid"])
            elif "lastSentAt" in expression:
                item.update(lastSentAt=values[":now"], challengeId=values[":cid"], expiresAt=values[":ttl"])
            return
