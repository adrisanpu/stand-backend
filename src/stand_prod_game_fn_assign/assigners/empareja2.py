import os
import random
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

# Catalog
CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "stand-prod-catalog-table")
EMPAREJA2_CATALOG_ID = os.environ.get("EMPAREJA2_CATALOG_ID", "EMPAREJA2#CHARACTERS#v1")

# Optional character images
CHAR_BUCKET = os.environ.get("CHAR_BUCKET", "")
CHAR_REGION = os.environ.get("CHAR_REGION", "us-east-1")
CHAR_URL_EXPIRES = int(os.environ.get("CHAR_URL_EXPIRES", "7200"))

_CATALOG_CACHE = {}
_s3 = boto3.client("s3", region_name=CHAR_REGION)


def _as_int(x):
    if isinstance(x, Decimal):
        return int(x)
    try:
        return int(x)
    except Exception:
        return None


def _query_empareja2_catalog(dynamo_r):
    cached = _CATALOG_CACHE.get(EMPAREJA2_CATALOG_ID)
    if cached is not None:
        return cached

    catalog = dynamo_r.Table(CATALOG_TABLE)
    items = []

    kwargs = {
        "KeyConditionExpression": Key("catalogId").eq(EMPAREJA2_CATALOG_ID),
        "ProjectionExpression": "pairId, characterId, characterName",
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

    _CATALOG_CACHE[EMPAREJA2_CATALOG_ID] = items
    return items


def _pick_random_character(items):
    choice = random.choice(items)
    cid = _as_int(choice.get("characterId"))
    if cid is None:
        raise RuntimeError("characterId must be numeric in catalog")
    cname = choice.get("characterName") or "Personaje"
    pair_id = str(choice.get("pairId") or "")
    return cid, cname, pair_id


def _partner_name(items, pair_id: str, assigned_cid: int) -> str:
    for it in items:
        if str(it.get("pairId") or "") == pair_id and _as_int(it.get("characterId")) != assigned_cid:
            return it.get("characterName") or "tu pareja"
    return "tu pareja"


def _character_image_url(character_name: str) -> str | None:
    if not CHAR_BUCKET:
        return None

    key = f"{character_name}.png"
    try:
        return _s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": CHAR_BUCKET, "Key": key},
            ExpiresIn=CHAR_URL_EXPIRES,
        )
    except Exception:
        return None


def assign_empareja2(ctx: dict):
    """
    New contract:
      returns (patch, welcome_header, extra_messages)

    - welcome_header: ONLY custom intro (no quiz mention, no code)
    - extra_messages: images etc (sent before welcome by assign.py)
    """
    psid = ctx["psid"]
    username_at = ctx["username_at"]
    dynamo_r = ctx["dynamo_r"]

    catalog_items = _query_empareja2_catalog(dynamo_r)
    cid, cname, pair_id = _pick_random_character(catalog_items)
    partner = _partner_name(catalog_items, pair_id, cid)

    patch = {
        "type": {
            "EMPAREJA2": {
                "characterId": cid,
                "characterName": cname,
                "pairId": pair_id,
                "partnerName": partner,      # helpful later (quiz completion message / UI)
                "raffleEligible": True,
                "quizAnswers": {},
            }
        }
    }

    extra_messages = []
    img = _character_image_url(cname)
    if img:
        extra_messages.append({"psid": psid, "image_url": img})

    # IMPORTANT: Instagram DMs don't support Markdown like **bold**
    welcome_header = (
        f"👋 ¡Hola {username_at}!\n\n"
        f"Te toca: {cname}.\n"
        f"Tu misión es encontrar a: {partner}.\n\n"
    )

    return (patch, welcome_header, extra_messages)
