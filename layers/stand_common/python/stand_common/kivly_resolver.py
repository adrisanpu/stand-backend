"""Durable, bounded Kivly identity resolution. No customer-directory cache."""
import hashlib
import hmac
import json
import math
import os
import time
import uuid

import boto3

from stand_common import kivly_registration as registration
from stand_common.utils import _iso_now, is_valid_phone, log

LEASE_SECONDS = 90
FRESH_SECONDS = 120
RETRY_SECONDS = 5
TERMINAL = {"confirmed", "failed", "review_required"}


def email_key(value):
    return (value or "").strip().casefold()


def phone_key(value):
    if not is_valid_phone(value):
        return ""
    digits = "".join(char for char in value if char.isascii() and char.isdigit())
    return digits[2:] if digits.startswith("00") else digits


def _valid_customer_id(value):
    return (
        isinstance(value, str) and bool(value.strip()) and len(value) <= 512
        and not any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value)
    )


def _digest(parts):
    secret = os.environ.get("PLAYER_TOKEN_SECRET", "")
    if len(secret) < 32:
        raise registration.RegistrationError("KivlyConfigurationInvalid")
    return hmac.new(secret.encode(), json.dumps(parts, sort_keys=True).encode(),
                    hashlib.sha256).hexdigest()


def request_id(meta, conn, settings, values):
    return "registration:" + _digest([
        meta["gameId"], meta["ownerUserId"], conn["ownerId"], conn["locationId"],
        conn["version"], settings["consentVersion"], email_key(values.get("email")),
        phone_key(values.get("phone")),
    ])


def enqueue(identifier, delay=0, **extra):
    queue = os.environ.get("KIVLY_REGISTRATION_QUEUE_URL") or os.environ.get("KIVLY_REGISTRATION_QUEUE")
    if not queue:
        raise registration.RegistrationError("KivlyConfigurationInvalid")
    boto3.client("sqs").send_message(
        QueueUrl=queue, MessageBody=json.dumps({"requestId": identifier, **extra}),
        DelaySeconds=min(900, max(0, math.ceil(delay))),
    )


def _conditional(error):
    return registration._code(error) == "ConditionalCheckFailedException"


def request_membership(dynamo, meta, conn, settings, values):
    if not email_key(values.get("email")) or not phone_key(values.get("phone")):
        raise registration.RegistrationError("KivlyPhoneRequired", 400)
    table, now = registration._table(dynamo), int(time.time())
    identifier = request_id(meta, conn, settings, values)
    record = table.get_item(Key={"id": identifier}, ConsistentRead=True).get("Item")
    if record is None:
        legacy = table.get_item(
            Key={"id": registration._identity(meta, conn, values["email"])}, ConsistentRead=True,
        ).get("Item") or {}
        record = {
            "id": identifier, "kind": "registration", "revision": 0, "state": "pending",
            "phase": "context", "gameId": meta["gameId"], "ownerUserId": meta["ownerUserId"],
            "locationId": conn["locationId"], "kivlyOwnerId": conn["ownerId"],
            "connectionVersion": conn["version"], "consentVersion": settings["consentVersion"],
            "username": values["username"], "email": email_key(values["email"]),
            "phone": values["phone"], "createdAt": _iso_now(), "queuedAt": now,
            "page": 1, "seenCount": 0, "matches": {}, "pageHashes": [],
        }
        if legacy.get("state") == "creating":
            if not legacy.get("externalId"):
                raise registration.RegistrationError("KivlyReconciliationRequired", 409)
            record.update(externalId=legacy["externalId"], createStarted=True)
        elif legacy:
            if legacy.get("state") != "confirmed" or not legacy.get("customerId"):
                raise registration.RegistrationError("KivlyReconciliationRequired", 409)
            record["mappedCustomerId"] = legacy["customerId"]
        try:
            table.put_item(Item=record, ConditionExpression="attribute_not_exists(id)")
        except registration.ClientError as error:
            if not _conditional(error):
                raise
            raise registration.RegistrationError("KivlyRegistrationPending", 503, RETRY_SECONDS) from None
        enqueue(identifier)
    if record["state"] == "confirmed":
        if now - int(record["validatedAt"]) > FRESH_SECONDS:
            # Revalidation is queued too; HTTP never performs a directory scan.
            refreshed = {**record, "state": "pending", "phase": "context", "queuedAt": now,
                         "revision": int(record["revision"]) + 1}
            if refreshed.get("createStarted") and refreshed.get("customerId"):
                refreshed["creationConfirmed"] = True
            _reset_scan(refreshed)
            try:
                table.put_item(Item=refreshed, ConditionExpression="revision = :revision",
                               ExpressionAttributeValues={":revision": record["revision"]})
            except registration.ClientError as error:
                if not _conditional(error):
                    raise
            enqueue(identifier)
        else:
            customer = record["customer"]
            _validate_identity(record, customer)
            membership = registration._membership(settings, customer["id"])
            membership["phone"] = customer["phone"]
            return membership
    elif record["state"] == "review_required":
        raise registration.RegistrationError("KivlyIdentityReviewRequired", 409)
    elif record["state"] == "failed":
        raise registration.RegistrationError(record.get("error", "KivlyReconciliationRequired"), 409)
    elif now - int(record.get("queuedAt", 0)) >= 60:
        enqueue(identifier)
    raise registration.RegistrationError("KivlyRegistrationPending", 503, RETRY_SECONDS)


def _project(customer):
    return {key: customer[key] for key in ("id", "name", "email", "phone", "external_id", "is_active")
            if key in customer}


def _validate_identity(record, customer):
    if (not isinstance(customer, dict) or customer.get("is_active") is not True
            or not _valid_customer_id(customer.get("id"))
            or customer["id"] == registration.PENDING_CUSTOMER_ID
            or not phone_key(customer.get("phone"))):
        raise registration.RegistrationError("KivlyCustomerInvalid", 409)
    if record.get("mappedCustomerId") and record["mappedCustomerId"] != customer["id"]:
        raise registration.RegistrationError("KivlyIdentityConflict", 409)
    if record.get("approvedCustomerId") and record["approvedCustomerId"] != customer["id"]:
        raise registration.RegistrationError("KivlyIdentityConflict", 409)
    if record.get("createStarted"):
        if (email_key(customer.get("email")) != record["email"]
                or phone_key(customer.get("phone")) != phone_key(record["phone"])):
            raise registration.RegistrationError("KivlyIdentityConflict", 409)
        if record.get("reusedExisting"):
            if customer.get("external_id") == record.get("externalId"):
                raise registration.RegistrationError("KivlyCustomerInvalid", 409)
        elif customer.get("external_id") != record.get("externalId"):
            raise registration.RegistrationError("KivlyReconciliationRequired", 409)
    elif email_key(customer.get("email")) != record["email"]:
        if (not record.get("approvedBy") or record.get("approvedCustomerId") != customer["id"]
                or phone_key(customer.get("phone")) != phone_key(record["phone"])):
            raise registration.RegistrationError("KivlyIdentityReviewRequired", 409)


def _acquire(table, identifier, now):
    record = table.get_item(Key={"id": identifier}, ConsistentRead=True).get("Item")
    if not record or record.get("kind") != "registration" or record.get("state") in TERMINAL:
        return None
    if int(record.get("leaseUntil", 0)) > now:
        return None
    acquired = {**record, "leaseOwner": uuid.uuid4().hex, "leaseUntil": now + LEASE_SECONDS,
                "revision": int(record["revision"]) + 1}
    try:
        table.put_item(
            Item=acquired,
            ConditionExpression="revision = :revision AND (attribute_not_exists(leaseUntil) OR leaseUntil <= :now)",
            ExpressionAttributeValues={":revision": record["revision"], ":now": now},
        )
    except registration.ClientError as error:
        if _conditional(error):
            return None
        raise
    return acquired


def _save(table, record):
    saved = {key: value for key, value in record.items() if key not in ("leaseOwner", "leaseUntil")}
    saved.update(revision=int(record["revision"]) + 1, queuedAt=int(time.time()))
    table.put_item(
        Item=saved, ConditionExpression="revision = :revision AND leaseOwner = :lease",
        ExpressionAttributeValues={":revision": record["revision"], ":lease": record["leaseOwner"]},
    )


def _take_clock(table, identifier, interval, now):
    try:
        table.update_item(
            Key={"id": identifier}, UpdateExpression="SET nextAllowedAt = :next",
            ConditionExpression="attribute_not_exists(nextAllowedAt) OR nextAllowedAt <= :now",
            ExpressionAttributeValues={":now": now, ":next": now + interval},
        )
        return 0
    except registration.ClientError as error:
        if not _conditional(error):
            raise
        clock = table.get_item(Key={"id": identifier}, ConsistentRead=True).get("Item") or {}
        return max(1, math.ceil((int(clock.get("nextAllowedAt", now + interval)) - now) / 1000))


def _reserve(table, record):
    now = int(time.time() * 1000)
    interval = int(record.get("intervalMs", 3000))
    for identifier in ("kivly-clock:global", "kivly-clock:" + _digest([
            record["locationId"], record["kivlyOwnerId"]])):
        delay = _take_clock(table, identifier, interval, now)
        if delay:
            return delay
    return 0


def _current(dynamo, record):
    meta = dynamo.Table(os.environ["GAMES_TABLE"]).get_item(
        Key={"gameId": record["gameId"]}, ConsistentRead=True,
    ).get("Item")
    if not meta or meta.get("ownerUserId") != record["ownerUserId"]:
        raise registration.RegistrationError("KivlyConnectionChanged", 409)
    meta = registration.apply_account_settings(dynamo, meta)
    if not registration.game_settings(meta)["enabled"]:
        raise registration.RegistrationError("KivlyConnectionChanged", 409)
    conn = registration._required_connection(dynamo, meta)
    if (not registration.game_settings(meta)["enabled"]
            or conn["version"] != record["connectionVersion"]
            or conn["ownerId"] != record["kivlyOwnerId"]
            or conn["locationId"] != record["locationId"]
            or meta["kivly"]["consentVersion"] != record["consentVersion"]):
        raise registration.RegistrationError("KivlyConnectionChanged", 409)
    return meta, conn


def _collect(record, rows):
    matches = record["matches"]
    for customer in rows:
        if (email_key(customer.get("email")) == record["email"]
                or phone_key(customer.get("phone")) == phone_key(record["phone"])
                or (record.get("externalId") and customer.get("external_id") == record["externalId"])):
            matches[customer["id"]] = _project(customer)
            if len(matches) > 4:
                raise registration.RegistrationError("KivlyIdentityConflict", 409)


def _select(record):
    rows = list(record["matches"].values())
    emails = [row for row in rows if email_key(row.get("email")) == record["email"]]
    phones = [row for row in rows if phone_key(row.get("phone")) == phone_key(record["phone"])]
    if (len(emails) > 1 or len(phones) > 1
            or (emails and phones and emails[0]["id"] != phones[0]["id"])):
        raise registration.RegistrationError("KivlyIdentityConflict", 409)
    if record.get("createStarted"):
        exact = [row for row in rows if row.get("external_id") == record.get("externalId")]
        if len(exact) != 1:
            raise registration.RegistrationError("KivlyReconciliationRequired", 409)
        customer = exact[0]
        _validate_identity(record, customer)
    else:
        customer = (emails or phones or [None])[0]
        if not customer:
            if record.get("approvedCustomerId"):
                raise registration.RegistrationError("KivlyIdentityConflict", 409)
            if record.get("mappedCustomerId"):
                raise registration.RegistrationError("KivlyReconciliationRequired", 409)
            return None
    if ((emails and emails[0]["id"] != customer["id"])
            or (phones and phones[0]["id"] != customer["id"])):
        raise registration.RegistrationError("KivlyIdentityConflict", 409)
    if customer.get("is_active") is not True or not phone_key(customer.get("phone")):
        raise registration.RegistrationError("KivlyCustomerInvalid", 409)
    record["customer"] = customer
    record["customerId"] = customer["id"]
    if not emails and not record.get("createStarted") and not record.get("approvedBy"):
        record["state"] = "review_required"
    else:
        record["phase"] = "validate"
    return customer


def _creation_lock(table, record):
    identifier = "kivly-create:" + _digest([record["kivlyOwnerId"], record["locationId"]])
    now = int(time.time())
    try:
        table.put_item(
            Item={"id": identifier, "requestId": record["id"], "leaseUntil": now + 600},
            ConditionExpression="attribute_not_exists(id) OR requestId = :request OR (attribute_not_exists(uncertain) AND leaseUntil <= :now)",
            ExpressionAttributeValues={":request": record["id"], ":now": now},
        )
        return True
    except registration.ClientError as error:
        if not _conditional(error):
            raise
        current = table.get_item(Key={"id": identifier}, ConsistentRead=True).get("Item") or {}
        if current.get("uncertain"):
            owner = table.get_item(Key={"id": current["requestId"]}, ConsistentRead=True).get("Item") or {}
            if owner.get("state") == "pending":
                return False
            raise registration.RegistrationError("KivlyReconciliationRequired", 409) from None
        return False


def _reset_scan(record):
    record.update(page=1, seenCount=0, matches={}, pageHashes=[])
    record.pop("total", None)


def _step(dynamo, table, record, conn):
    client = registration._client(conn)
    scope = (conn["locationId"], conn["ownerId"])
    phase = record["phase"]
    if phase == "context":
        context = client.get_context()
        if context.location_id != scope[0] or context.owner_id != scope[1]:
            raise registration.RegistrationError("KivlyScopeMismatch", 409)
        record["intervalMs"] = max(3000, math.ceil(60000 / context.requests_per_minute),
                                   math.ceil(3600000 / context.requests_per_hour))
        record["phase"] = ("validate" if record.get("creationConfirmed") else
                           "scan" if record.get("createStarted") else
                           "lookup_mapped" if record.get("mappedCustomerId") else "lookup_email")
    elif phase == "lookup_mapped":
        customer = client.get_customer(*scope, record["mappedCustomerId"])
        _validate_identity(record, customer)
        _collect(record, [customer])
        record.update(phase="lookup_phone", page=1)
    elif phase in ("lookup_email", "lookup_phone"):
        search = record["email"] if phase == "lookup_email" else record["phone"]
        result = client.list_customers_page(*scope, int(record["page"]), search=search)
        _collect(record, result["data"])
        if result["has_next"]:
            if record["page"] >= 100:
                raise registration.RegistrationError("KivlySearchIncomplete")
            record["page"] += 1
        elif phase == "lookup_email":
            record.update(phase="lookup_phone", page=1)
        else:
            if _select(record) is None:
                # Filtered misses are not proof of absence. The create endpoint
                # is the bounded path for a normal new registration.
                record.update(phase="recheck_email", page=1)
    elif phase == "scan":
        result = client.list_customers_page(*scope, int(record["page"]))
        if "total" in record and record["total"] != result["total"]:
            raise registration.RegistrationError("KivlySearchIncomplete")
        record["total"] = result["total"]
        page_hash = hashlib.sha256(json.dumps([row["id"] for row in result["data"]]).encode()).hexdigest()
        if page_hash in record["pageHashes"]:
            raise registration.RegistrationError("KivlySearchIncomplete")
        record["pageHashes"].append(page_hash)
        record["seenCount"] += len(result["data"])
        _collect(record, result["data"])
        if result["has_next"]:
            if record["seenCount"] >= record["total"] or record["page"] >= 1000:
                raise registration.RegistrationError("KivlySearchIncomplete")
            record["page"] += 1
        else:
            if record["seenCount"] != record["total"]:
                raise registration.RegistrationError("KivlySearchIncomplete")
            if _select(record) is None:
                record.update(phase="recheck_email", page=1)
    elif phase in ("recheck_email", "recheck_phone"):
        if not _creation_lock(table, record):
            return 10
        search = record["email"] if phase == "recheck_email" else record["phone"]
        result = client.list_customers_page(*scope, int(record["page"]), search=search)
        _collect(record, result["data"])
        if result["has_next"]:
            if record["page"] >= 100:
                raise registration.RegistrationError("KivlySearchIncomplete")
            record["page"] += 1
        elif phase == "recheck_email":
            record.update(phase="recheck_phone", page=1)
        elif _select(record) is None:
            record["phase"] = "create"
    elif phase == "create":
        if record.get("createStarted"):
            record["phase"] = "scan"
            _reset_scan(record)
            return 0
        lock_id = "kivly-create:" + _digest([record["kivlyOwnerId"], record["locationId"]])
        lock = table.get_item(Key={"id": lock_id}, ConsistentRead=True).get("Item") or {}
        if lock.get("requestId") != record["id"] or int(lock.get("leaseUntil", 0)) <= int(time.time()):
            record.update(phase="recheck_email", page=1)
            return 10
        if os.environ.get("KIVLY_EXTERNAL_WRITES_ENABLED", "true").lower() != "true":
            raise registration.RegistrationError("KivlyWritesDisabled")
        meta, _ = _current(dynamo, record)
        if not meta.get("isActive", True):
            raise registration.RegistrationError("GameInactive", 403)
        if int(meta.get("playersCount", 0)) >= int(meta.get("maxPlayers") or 9999):
            raise registration.RegistrationError("GameFull", 409)
        table.update_item(
            Key={"id": lock_id}, UpdateExpression="SET uncertain = :true",
            ConditionExpression="requestId = :request",
            ExpressionAttributeValues={":true": True, ":request": record["id"]},
        )
        external_id = "stand-" + uuid.uuid4().hex
        identity = registration._identity({"ownerUserId": record["ownerUserId"]}, conn, record["email"])
        try:
            table.put_item(
                Item={"id": identity, "state": "creating", "externalId": external_id,
                      "requestId": record["id"], "createdAt": _iso_now()},
                ConditionExpression="attribute_not_exists(id)",
            )
        except registration.ClientError as error:
            if not _conditional(error):
                raise
            previous = table.get_item(Key={"id": identity}, ConsistentRead=True).get("Item") or {}
            if previous.get("state") != "creating" or not previous.get("externalId"):
                raise registration.RegistrationError("KivlyReconciliationRequired", 409)
            record.update(createStarted=True, externalId=previous["externalId"], phase="scan")
            _reset_scan(record)
            return 0
        record.update(createStarted=True, externalId=external_id)
        # Commit before the non-idempotent POST, including before a process crash.
        checkpoint = {**record, "revision": int(record["revision"]) + 1}
        table.put_item(
            Item=checkpoint, ConditionExpression="revision = :revision AND leaseOwner = :lease",
            ExpressionAttributeValues={":revision": record["revision"], ":lease": record["leaseOwner"]},
        )
        record["revision"] = checkpoint["revision"]
        try:
            customer = client.create_customer(
                *scope, record["username"], record["email"], external_id,
                phone=record["phone"], allow_external_id_reuse=True,
            )
        except registration.KivlyError:
            record["phase"] = "scan"
            _reset_scan(record)
            return 0
        if (email_key(customer.get("email")) != record["email"]
                or phone_key(customer.get("phone")) != phone_key(record["phone"])):
            raise registration.RegistrationError("KivlyIdentityConflict", 409)
        record["reusedExisting"] = customer.get("external_id") != external_id
        _validate_identity(record, customer)
        record.update(customer=_project(customer), customerId=customer["id"], phase="validate")
    elif phase == "validate":
        customer = client.get_customer(*scope, record["customerId"])
        _validate_identity(record, customer)
        _current(dynamo, record)
        record.update(customer=_project(customer), state="confirmed", validatedAt=int(time.time()),
                      confirmedAt=_iso_now(), creationConfirmed=bool(record.get("createStarted")))
        # Historical email mappings are retained only for an email identity,
        # never for an organizer-approved different email.
        if email_key(customer.get("email")) == record["email"]:
            identity = registration._identity({"ownerUserId": record["ownerUserId"]}, conn, record["email"])
            try:
                table.put_item(
                    Item={"id": identity, "state": "confirmed", "customerId": customer["id"],
                          "confirmedAt": _iso_now()},
                    ConditionExpression="attribute_not_exists(id) OR customerId = :customer OR externalId = :external",
                    ExpressionAttributeValues={":customer": customer["id"], ":external": record.get("externalId", "")},
                )
            except registration.ClientError as error:
                if not _conditional(error):
                    raise
                raise registration.RegistrationError("KivlyReconciliationRequired", 409) from None
    else:
        raise registration.RegistrationError("KivlyReconciliationRequired", 409)
    return 0


def process(dynamo, identifier):
    table = registration._table(dynamo)
    record = _acquire(table, identifier, int(time.time()))
    if record is None:
        return
    delay = 0
    try:
        _, conn = _current(dynamo, record)
        delay = _reserve(table, record)
        if not delay:
            delay = _step(dynamo, table, record, conn)
    except (registration.RegistrationError, registration.KivlyError) as error:
        record.update(state="failed", error=error.code, failedAt=_iso_now())
        log("kivly_identity_resolution_failed", {"requestId": identifier, "code": error.code})
    if record["state"] in TERMINAL and (not record.get("createStarted") or record["state"] == "confirmed"):
        lock_id = "kivly-create:" + _digest([record["kivlyOwnerId"], record["locationId"]])
        try:
            table.update_item(
                Key={"id": lock_id}, UpdateExpression="SET leaseUntil = :zero REMOVE uncertain",
                ConditionExpression="requestId = :request",
                ExpressionAttributeValues={":zero": 0, ":request": identifier},
            )
        except registration.ClientError as error:
            if not _conditional(error):
                raise
    _save(table, record)
    if record["state"] not in TERMINAL:
        enqueue(identifier, delay)


def recover(dynamo, cursor=None):
    """Scheduled outbox repair; paginate through registrations, not Kivly data."""
    now = int(time.time())
    kwargs = {
        "FilterExpression": "#kind = :kind AND #state = :state AND queuedAt <= :stale",
        "ExpressionAttributeNames": {"#kind": "kind", "#state": "state"},
        "ExpressionAttributeValues": {":kind": "registration", ":state": "pending", ":stale": now - 60},
        "Limit": 100,
    }
    if cursor:
        kwargs["ExclusiveStartKey"] = cursor
    result = registration._table(dynamo).scan(**kwargs)
    for record in result.get("Items", []):
        if int(record.get("leaseUntil", 0)) <= now:
            enqueue(record["id"])
    if result.get("LastEvaluatedKey"):
        enqueue(None, kind="recovery", cursor=result["LastEvaluatedKey"])
