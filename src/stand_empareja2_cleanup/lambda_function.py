"""Delete unreferenced contribution images after their retry window."""
import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from stand_common.empareja2_contributions import META_ID, CatalogError, acquire_lock, release_lock
from stand_common.utils import log

_KEY = re.compile(r"^contributions/([^/]+)/([A-Z0-9]{6})/([0-9a-f]{32})/[0-9a-f]{64}\.png$")
_AGE = timedelta(days=7)


def cleanup(s3, catalog, bucket, now):
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix="contributions/"):
        for item in page.get("Contents", []):
            match = _KEY.fullmatch(item["Key"])
            if not match or item["LastModified"] > now - _AGE:
                continue
            owner, game_id, operation = match.groups()
            lock_key = {"catalogId": META_ID, "pairId": f"LOCK#{owner}#{operation}"}
            token = uuid.uuid4().hex
            try:
                acquire_lock(catalog, lock_key, token, lease_seconds=1800)
            except CatalogError as exc:
                if exc.code == "ContributionBusy":
                    continue
                raise
            try:
                marker = catalog.get_item(
                    Key={"catalogId": META_ID, "pairId": f"OP#{owner}#{operation}"},
                    ConsistentRead=True,
                ).get("Item")
                if marker:
                    continue
                s3.delete_object(Bucket=bucket, Key=item["Key"])
                deleted += 1
            finally:
                release_lock(catalog, lock_key, token)
    return deleted


def lambda_handler(event, context):
    bucket = os.environ["CHAR_BUCKET"]
    catalog = boto3.resource("dynamodb").Table(os.environ["CATALOG_TABLE"])
    deleted = cleanup(boto3.client("s3"), catalog, bucket, datetime.now(timezone.utc))
    log("empareja2_orphan_cleanup", {"deleted": deleted})
    return {"deleted": deleted}
