"""Partial edits of existing reward conditions, never of the reward catalog."""
import hashlib
import json

from stand_common import stack_prizes as prizes
from stand_common.utils import _json_sanitize


def revision(value):
    return hashlib.sha256(json.dumps(_json_sanitize(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def native_update(dynamo, meta, body):
    catalog = meta.get("prizeCatalog") or {}
    if (set(body) != {"expectedRevision", "rankingTopX"}
            or meta.get("gameType") != "T1MER" or not catalog.get("ranking")
            or type(body.get("rankingTopX")) is not int or not 1 <= body["rankingTopX"] <= 100):
        raise prizes.PrizeError("InvalidRewardConditions", 400)
    if body["expectedRevision"] != revision(catalog):
        raise prizes.PrizeError("RewardConditionsChanged")
    updated = {**catalog, "ranking": {**catalog["ranking"], "topX": body["rankingTopX"]}}
    active_condition = "isActive = :active" if "isActive" in meta else "attribute_not_exists(isActive)"
    prizes.transact(dynamo, [prizes.game_update(
        meta, ", prizeCatalog = :catalog", {":catalog": updated, ":previousCatalog": catalog,
                                           **({":active": True} if "isActive" in meta else {})},
        extra=" AND " + active_condition + " AND attribute_not_exists(archivedAt) AND prizeCatalog = :previousCatalog",
    )])
    return updated
