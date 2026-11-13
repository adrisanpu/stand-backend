import os
import random
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

from stand_common.utils import log
from stand_common.empareja2_catalog import group_id as _pair_group_id, group_sort_key

# Catalog
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "stand-prod-catalog-table")
EMPAREJA2_CATALOG_ID = os.environ.get("EMPAREJA2_CATALOG_ID", "EMPAREJA2#CHARACTERS#v1")

# Optional character images
CHAR_BUCKET = os.environ.get("CHAR_BUCKET", "")
CHAR_REGION = os.environ.get("CHAR_REGION", "us-east-1")
CHAR_URL_EXPIRES = int(os.environ.get("CHAR_URL_EXPIRES", "7200"))

_s3 = boto3.client("s3", region_name=CHAR_REGION)


def _as_int(x):
    if isinstance(x, Decimal):
        return int(x)
    try:
        return int(x)
    except Exception:
        return None


def _query_empareja2_catalog(dynamo_r):
    catalog = dynamo_r.Table(CATALOG_TABLE)
    items = []

    kwargs = {
        "KeyConditionExpression": Key("catalogId").eq(EMPAREJA2_CATALOG_ID),
        "ProjectionExpression": "pairId, pairGroupId, pairNumericId, characterId, characterName, imageKey",
        "ConsistentRead": True,
    }

    while True:
        page = catalog.query(**kwargs)
        items.extend(page.get("Items") or [])
        lek = page.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek

    if not items:
        raise RuntimeError(f"No items found in catalog: {EMPAREJA2_CATALOG_ID}")

    return items


def _pick_random_character(items):
    choice = random.choice(items)
    cid = _as_int(choice.get("characterId"))
    if cid is None:
        raise RuntimeError("characterId must be numeric in catalog")
    cname = choice.get("characterName") or "Personaje"
    pair_group_id = _pair_group_id(choice)
    if pair_group_id is None:
        raise RuntimeError("pairGroupId or pairNumericId required in catalog")
    return cid, cname, pair_group_id, choice.get("imageKey")


def _partner_details(items, pair_group_id: str, assigned_cid: int):
    for it in items:
        if _pair_group_id(it) == pair_group_id and _as_int(it.get("characterId")) != assigned_cid:
            return it.get("characterName") or "tu pareja", it.get("imageKey")
    return "tu pareja", None


def _character_image_url(character_name: str, image_key: str | None = None) -> str | None:
    if not CHAR_BUCKET:
        return None

    key = image_key or f"{character_name}.png"
    try:
        return _s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": CHAR_BUCKET, "Key": key},
            ExpiresIn=CHAR_URL_EXPIRES,
        )
    except Exception:
        return None


def assign_empareja2(ctx: dict):
    dynamo_r = ctx["dynamo_r"]

    catalog_items = _query_empareja2_catalog(dynamo_r)
    valid_items = [
        it for it in catalog_items
        if _as_int(it.get("characterId")) is not None and _pair_group_id(it) is not None
    ]
    if not valid_items:
        raise RuntimeError(
            f"No catalog items with numeric characterId for {EMPAREJA2_CATALOG_ID} - check catalog data"
        )
    if len(valid_items) < len(catalog_items):
        skipped = len(catalog_items) - len(valid_items)
        log("assign_empareja2_catalog_skipped_invalid", {
            "catalogId": EMPAREJA2_CATALOG_ID,
            "skipped": skipped,
            "total": len(catalog_items),
        })

    game_meta = ctx.get("gameMeta") or {}
    empareja2_blob = (game_meta.get("type") or {}).get("EMPAREJA2") or {}
    num_pairs = empareja2_blob.get("numPairs")
    if num_pairs is not None:
        if (
            isinstance(num_pairs, bool)
            or not isinstance(num_pairs, (int, Decimal))
            or (isinstance(num_pairs, Decimal) and not num_pairs.is_finite())
            or num_pairs < 1
            or num_pairs != int(num_pairs)
        ):
            raise RuntimeError("EMPAREJA2 numPairs must be a positive integer")
        group_ids = sorted(
            {_pair_group_id(it) for it in valid_items},
            key=group_sort_key,
        )
        if num_pairs > len(group_ids):
            raise RuntimeError("EMPAREJA2 numPairs exceeds the available catalog pair groups")
        allowed_groups = set(group_ids[:int(num_pairs)])
        valid_items = [it for it in valid_items if _pair_group_id(it) in allowed_groups]

    cid, cname, pair_group_id, image_key = _pick_random_character(valid_items)
    partner, partner_image_key = _partner_details(valid_items, pair_group_id, cid)

    patch = {
        "type": {
            "EMPAREJA2": {
                "characterId": cid,
                "characterName": cname,
                "partnerCharacterName": partner,
                "pairGroupId": pair_group_id,
                "raffleEligible": True,
                "quizAnswers": {},
            }
        }
    }
    if image_key:
        patch["type"]["EMPAREJA2"]["characterImageKey"] = image_key
    if partner_image_key:
        patch["type"]["EMPAREJA2"]["partnerCharacterImageKey"] = partner_image_key

    return patch
