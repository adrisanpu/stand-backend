from copy import deepcopy
from decimal import Decimal
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.email_fake import FakeDynamoError

spec = importlib.util.spec_from_file_location(
    "gameplayer_migration",
    Path(__file__).parents[1] / "scripts" / "migrate_gameplayer_schema.py",
)
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def participant(**fields):
    return {
        "gameId": "ABC123", "playerId": Decimal(1),
        "emailVerificationMethod": "email-code",
        "emailVerifiedAt": "2026-01-01T00:00:00Z", "joinSource": "web",
        "kivlyMembership": {"customerId": "customer", "extra": {"preserved": True}},
        "kivlyRecoveryRun": "run-1", "recoveryDisposition": "duplicate",
        "recoveryDuplicateOf": Decimal(2), "phone": "+34 111111111",
        "validationCode": Decimal(1234), "validated": False,
        "type": {"STACK": {"score": Decimal(9)}}, **fields,
    }


def test_transform_preserves_values_and_removes_only_legacy_attributes():
    before = participant()
    result = migration.transform(before)
    assert result["verificationMethod"] == before["emailVerificationMethod"]
    assert result["verifiedAt"] == before["emailVerifiedAt"]
    assert result["kivly"] == {
        "membership": before["kivlyMembership"],
        "recovery": {"run": "run-1", "disposition": "duplicate", "duplicateOf": Decimal(2)},
    }
    assert not set(migration.LEGACY_FIELDS) & result.keys()
    for field in ("gameId", "playerId", "phone", "validationCode", "validated", "type"):
        assert result[field] == before[field]
    assert migration.transform(result) == result
    assert "emailVerifiedAt" in before


def test_missing_optional_fields_do_not_create_empty_objects():
    before = {"gameId": "ABC123", "playerId": 1, "validationCode": 1234}
    assert migration.transform(before) == before
    assert migration.transform({**before, "kivlyRecoveryRun": "run"})["kivly"] == {
        "recovery": {"run": "run"},
    }


def test_partial_new_maps_are_merged_without_losing_metadata():
    result = migration.transform(participant(kivly={
        "membership": {"customerId": "customer", "phone": "+34 222222222"},
        "recovery": {"run": "run-1", "audit": "keep"},
        "other": "keep",
    }))
    assert result["kivly"]["membership"]["phone"] == "+34 222222222"
    assert result["kivly"]["membership"]["extra"] == {"preserved": True}
    assert result["kivly"]["recovery"]["audit"] == "keep"
    assert result["kivly"]["other"] == "keep"


@pytest.mark.parametrize("fields", [
    {"verifiedAt": "2026-01-02T00:00:00Z"},
    {"verificationMethod": "sms-code"},
    {"kivly": {"membership": {"customerId": "different"}}},
    {"kivly": {"recovery": {"duplicateOf": True}}},
    {"kivly": {"recovery": {"run": "different"}}},
])
def test_conflicting_values_are_rejected(fields):
    with pytest.raises(migration.MigrationError, match="Conflicting"):
        migration.transform(participant(**fields))


@pytest.mark.parametrize("fields", [
    {"joinSource": "instagram"}, {"joinSource": None},
    {"kivly": "invalid"}, {"kivlyMembership": None}, {"kivlyMembership": {}},
    {"emailVerifiedAt": None}, {"emailVerificationMethod": ""},
    {"emailVerifiedAt": "not-a-date"}, {"emailVerifiedAt": "2026-01-01"},
    {"recoveryDuplicateOf": True}, {"recoveryDuplicateOf": Decimal("1.5")},
    {"kivlyRecoveryRun": None},
])
def test_invalid_formats_stop_the_migration(fields):
    with pytest.raises(migration.MigrationError):
        migration.transform(participant(**fields))


class Table:
    table_arn = f"arn:aws:dynamodb:{migration.REGION}:{migration.ACCOUNT}:table/{migration.TABLE}"

    def __init__(self, rows):
        self.rows = deepcopy(rows)
        self.calls = []
        self.scans = []

    def scan(self, **options):
        self.scans.append(options)
        index = 1 if options.get("ExclusiveStartKey") else 0
        page = {"Items": deepcopy(self.rows[index:index + 1])}
        if index + 1 < len(self.rows):
            page["LastEvaluatedKey"] = {
                name: self.rows[index][name] for name in ("gameId", "playerId")
            }
        return page

    def update_item(self, **options):
        row = next(r for r in self.rows if all(r[k] == v for k, v in options["Key"].items()))
        names, values = options["ExpressionAttributeNames"], options["ExpressionAttributeValues"]
        for condition in options["ConditionExpression"].split(" AND "):
            if condition.startswith("attribute_not_exists("):
                name = names[condition[len("attribute_not_exists("):-1]]
                valid = name not in row
            elif condition.startswith("attribute_exists("):
                valid = names[condition[len("attribute_exists("):-1]] in row
            else:
                token, old = condition.split(" = ")
                valid = names[token] in row and row[names[token]] == values[old]
            if not valid:
                raise FakeDynamoError("ConditionalCheckFailedException")
        self.calls.append(options)
        sets, _, removes = options["UpdateExpression"].partition("REMOVE ")
        if sets:
            for assignment in sets.removeprefix("SET ").strip().split(", "):
                token, value = assignment.split(" = ")
                row[names[token]] = deepcopy(values[value])
        for token in removes.split(", ") if removes else []:
            row.pop(names[token])


def session_for(table):
    session = MagicMock(region_name=migration.REGION)
    session.client.return_value.get_caller_identity.return_value = {"Account": migration.ACCOUNT}
    session.resource.return_value.Table.return_value = table
    return session


def test_dry_run_is_paginated_and_never_writes():
    table = Table([participant(), participant(playerId=Decimal(2))])
    result = migration.migrate(session_for(table))
    assert result["scanned"] == result["wouldUpdate"] == 2
    assert result["updated"] == 0
    assert not table.calls
    assert len(table.scans) == 2
    assert all(o["ConsistentRead"] for o in table.scans)


def test_apply_is_atomic_per_record_preserves_other_data_and_can_be_repeated():
    table = Table([participant(), participant(playerId=Decimal(2))])
    expected = [migration.transform(row) for row in table.rows]
    result = migration.migrate(session_for(table), apply=True)
    assert result["updated"] == 2
    assert table.rows == expected
    assert all("SET " in c["UpdateExpression"] and "REMOVE " in c["UpdateExpression"] for c in table.calls)
    assert migration.migrate(session_for(table), apply=True)["updated"] == 0
    assert len(table.calls) == 2


def test_preflight_validates_every_page_before_any_write():
    table = Table([participant(), participant(playerId=2, verifiedAt="invalid")])
    with pytest.raises(migration.MigrationError):
        migration.migrate(session_for(table), apply=True)
    assert not table.calls


@pytest.mark.parametrize("field,value", [
    ("kivlyRecoveryRun", "concurrent-run"), ("verifiedAt", "2026-01-02T00:00:00Z"),
])
def test_concurrent_schema_changes_stop_without_overwriting(monkeypatch, field, value):
    monkeypatch.setattr(migration, "ClientError", FakeDynamoError)
    table = Table([participant()])
    update = table.update_item

    def concurrent(**options):
        table.rows[0][field] = value
        update(**options)

    table.update_item = concurrent
    with pytest.raises(migration.MigrationError, match="Stopped after 0 updates"):
        migration.migrate(session_for(table), apply=True)
    assert not table.calls
    assert table.rows[0][field] == value


def test_verification_detects_unrelated_concurrent_changes():
    table = Table([participant()])
    update = table.update_item

    def concurrent(**options):
        update(**options)
        table.rows[0]["validated"] = True

    table.update_item = concurrent
    with pytest.raises(migration.MigrationError, match="Verification failed"):
        migration.migrate(session_for(table), apply=True)


@pytest.mark.parametrize("guard", ["account", "region", "table"])
def test_scope_guards_prevent_any_scan_or_write(guard):
    table = Table([participant()])
    session = session_for(table)
    if guard == "account":
        session.client.return_value.get_caller_identity.return_value = {"Account": "other"}
    elif guard == "region":
        session.region_name = "eu-west-1"
    else:
        table.table_arn = table.table_arn.replace("stand-dev-", "stand-prod-")
    with pytest.raises(migration.MigrationError):
        migration.migrate(session, apply=True)
    assert not table.scans
    assert not table.calls
