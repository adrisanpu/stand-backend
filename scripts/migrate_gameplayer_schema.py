"""Migrate dev participants; stop writers and workers before using --apply."""
import argparse
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import json

import boto3
from botocore.exceptions import ClientError

ACCOUNT = "781283585341"
REGION = "us-east-1"
TABLE = "stand-dev-gameplayer-table"
RENAMES = {
    "emailVerificationMethod": "verificationMethod",
    "emailVerifiedAt": "verifiedAt",
}
RECOVERY = {
    "kivlyRecoveryRun": "run",
    "recoveryDisposition": "disposition",
    "recoveryDuplicateOf": "duplicateOf",
}
LEGACY_FIELDS = (*RENAMES, "joinSource", "kivlyMembership", *RECOVERY)
FIELDS = (*LEGACY_FIELDS, *RENAMES.values(), "kivly")


class MigrationError(ValueError):
    pass


def _merge(target, name, value):
    if name not in target:
        target[name] = deepcopy(value)
    elif isinstance(target[name], dict) and isinstance(value, dict):
        for key, child in value.items():
            _merge(target[name], key, child)
    elif type(target[name]) is not type(value) or target[name] != value:
        raise MigrationError("Conflicting old and new participant attributes.")


def _validate(item):
    for name in ("verificationMethod", "verifiedAt"):
        if name in item and (not isinstance(item[name], str) or not item[name].strip()):
            raise MigrationError(f"Invalid {name}.")
    if "verifiedAt" in item:
        try:
            stamp = datetime.fromisoformat(item["verifiedAt"].replace("Z", "+00:00"))
        except ValueError:
            raise MigrationError("Invalid verifiedAt timestamp.") from None
        if stamp.tzinfo is None:
            raise MigrationError("verifiedAt must include a timezone.")
    if "kivly" not in item:
        return
    kivly = item["kivly"]
    if not isinstance(kivly, dict) or not kivly:
        raise MigrationError("Invalid kivly map.")
    for name in ("membership", "recovery"):
        if name in kivly and (not isinstance(kivly[name], dict) or not kivly[name]):
            raise MigrationError(f"Invalid kivly.{name} map.")
    recovery = kivly.get("recovery", {})
    for name in ("run", "disposition"):
        if name in recovery and (not isinstance(recovery[name], str) or not recovery[name].strip()):
            raise MigrationError(f"Invalid kivly.recovery.{name}.")
    if "duplicateOf" in recovery:
        value = recovery["duplicateOf"]
        if (isinstance(value, bool) or not isinstance(value, (int, Decimal))
                or value < 0 or value != int(value)):
            raise MigrationError("Invalid kivly.recovery.duplicateOf.")


def transform(item):
    result = deepcopy(item)
    if "joinSource" in result and result["joinSource"] != "web":
        raise MigrationError("Non-web joinSource requires an explicit migration decision.")
    result.pop("joinSource", None)
    for old, new in RENAMES.items():
        if old in result:
            _merge(result, new, result.pop(old))
    if "kivlyMembership" in result or any(name in result for name in RECOVERY):
        if "kivly" in result and not isinstance(result["kivly"], dict):
            raise MigrationError("Invalid kivly map.")
        kivly = result.setdefault("kivly", {})
        if "kivlyMembership" in result:
            membership = result.pop("kivlyMembership")
            if not isinstance(membership, dict) or not membership:
                raise MigrationError("Invalid legacy membership.")
            _merge(kivly, "membership", membership)
        recovery = {new: result.pop(old) for old, new in RECOVERY.items() if old in result}
        if recovery:
            _merge(kivly, "recovery", recovery)
    _validate(result)
    return result


def _scan(table):
    options = {"ConsistentRead": True}
    while True:
        page = table.scan(**options)
        yield from page.get("Items", [])
        if not page.get("LastEvaluatedKey"):
            return
        options["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _key(item):
    return item["gameId"], item["playerId"]


def _update(before, after):
    names = {"#game": "gameId", "#player": "playerId"}
    values, conditions, assignments, removals = {}, [
        "attribute_exists(#game)", "attribute_exists(#player)",
    ], [], []
    for index, field in enumerate(FIELDS):
        token = f"#f{index}"
        names[token] = field
        if field in before:
            old = f":old{index}"
            values[old] = before[field]
            conditions.append(f"{token} = {old}")
        else:
            conditions.append(f"attribute_not_exists({token})")
        if field in after and (field not in before or before[field] != after[field]):
            new = f":new{index}"
            values[new] = after[field]
            assignments.append(f"{token} = {new}")
        elif field in before and field not in after:
            removals.append(token)
    expression = []
    if assignments:
        expression.append("SET " + ", ".join(assignments))
    if removals:
        expression.append("REMOVE " + ", ".join(removals))
    return {
        "Key": {"gameId": before["gameId"], "playerId": before["playerId"]},
        "UpdateExpression": " ".join(expression),
        "ConditionExpression": " AND ".join(conditions),
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
    }


def migrate(session, apply=False):
    if session.region_name != REGION:
        raise MigrationError("Migration is restricted to us-east-1.")
    if session.client("sts").get_caller_identity()["Account"] != ACCOUNT:
        raise MigrationError("Unexpected AWS account.")
    table = session.resource("dynamodb").Table(TABLE)
    if table.table_arn != f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}":
        raise MigrationError("Unexpected DynamoDB table ARN.")
    rows = list(_scan(table))
    expected = {_key(row): transform(row) for row in rows}
    changes = [(row, expected[_key(row)]) for row in rows if row != expected[_key(row)]]
    updated = 0
    for before, after in changes:
        if apply:
            try:
                table.update_item(**_update(before, after))
            except ClientError as error:
                code = error.response["Error"]["Code"]
                raise MigrationError(
                    f"Stopped after {updated} updates ({code}); inspect and rerun while writers are paused."
                ) from None
            updated += 1
    if apply:
        actual = {_key(row): row for row in _scan(table)}
        if actual != expected:
            raise MigrationError("Verification failed: records differ from the expected migrated data.")
        if any(field in row for row in actual.values() for field in LEGACY_FIELDS):
            raise MigrationError("Verification failed: legacy attributes remain.")
    return {
        "table": TABLE, "mode": "apply" if apply else "dry-run", "scanned": len(rows),
        "wouldUpdate": len(changes), "updated": updated,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="stand-deploy-cli")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    try:
        result = migrate(session, apply=args.apply)
    except MigrationError as error:
        parser.exit(1, f"Migration failed: {error}\n")
    except ClientError as error:
        parser.exit(1, f"AWS request failed: {error.response['Error']['Code']}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
