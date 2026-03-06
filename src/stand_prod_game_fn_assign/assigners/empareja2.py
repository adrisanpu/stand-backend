import os
import random
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

from stand_common.utils import log

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
        "ProjectionExpression": "pairId, pairGroupId, pairNumericId, characterId, characterName",
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


def _pair_group_id(item) -> str | None:
    """Same value for both characters in a pair; used to find partner and store in Gameplayer."""
    g = item.get("pairGroupId") or item.get("pairNumericId")
    if g is None:
        return None
    if isinstance(g, Decimal):
        return str(int(g))
    if isinstance(g, int):
        return str(g)
    return str(g).strip() or None


def _pick_random_character(items):
    choice = random.choice(items)
    cid = _as_int(choice.get("characterId"))
    if cid is None:
        raise RuntimeError("characterId must be numeric in catalog")
    cname = choice.get("characterName") or "Personaje"
    pair_group_id = _pair_group_id(choice)
    if pair_group_id is None:
        raise RuntimeError("pairGroupId or pairNumericId required in catalog")
    return cid, cname, pair_group_id


def _partner_name(items, pair_group_id: str, assigned_cid: int) -> str:
    for it in items:
        if _pair_group_id(it) == pair_group_id and _as_int(it.get("characterId")) != assigned_cid:
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

    - welcome_header: bienvenida + te toca/busca (sin código ni mención al quiz); lambda añade aviso quiz o código.
    - extra_messages: imagen del personaje (lambda envía primero).
    """
    psid = ctx["psid"]
    username_at = ctx["username_at"]
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
    cid, cname, pair_group_id = _pick_random_character(valid_items)
    partner = _partner_name(valid_items, pair_group_id, cid)

    patch = {
        "type": {
            "EMPAREJA2": {
                "characterId": cid,
                "characterName": cname,
                "pairGroupId": pair_group_id,
                "raffleEligible": True,
                "quizAnswers": {},
            }
        }
    }

    extra_messages = []
    img = _character_image_url(cname)
    if img:
        extra_messages.append({"psid": psid, "image_url": img})

    # Lambda sends: image first, then welcome_header + tail (quiz notice or code)
    welcome_header = (
        f"¡Hola {username_at}! 👋\n\n"
        f"Te toca: {cname}.\n"
        f"Tu misión es encontrar a: {partner}."
    )

    return (patch, welcome_header, extra_messages)
