"""Validation and compatibility defaults for the editable game Home canvas."""
import hashlib
import json
import re
import uuid

from stand_common.utils import _json_sanitize


VERSION = 1
LANDSCAPE = (32, 18)
PORTRAIT = (18, 32)
MAX_ITEMS = 40
MAX_TEXT_LENGTH = 1000
MIN_SIZE = {
    "qr": (4, 5),
    "logo": (3, 2),
    "players": (4, 2),
    "ranking": (4, 5),
    "raffle": (4, 4),
    "prizes": (4, 2),
    "text": (2, 1),
}
ITEM_TYPES = set(MIN_SIZE)
_ID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_COLOR = re.compile(r"#[0-9a-fA-F]{6}\Z")


class HomeCanvasError(ValueError):
    pass


def empty():
    return {
        "version": VERSION,
        "landscape": {"background": None, "items": []},
        "portrait": {"background": None, "items": []},
    }


def _seed_item(item_type, x, y, w, h):
    return {"id": f"legacy-{item_type}", "type": item_type, "x": x, "y": y, "w": w, "h": h}


def initial(meta):
    """Return the deterministic editable layout used for games created before canvases."""
    game_type = (meta.get("gameType") or "").upper()
    include_ranking = game_type in {"T1MER", "STACK"}
    include_raffle = meta.get("showRaffleOnHome", True) is not False
    landscape = [
        _seed_item("logo", 0, 0, 8, 5),
        _seed_item("players", 0, 5, 8, 4),
        *([_seed_item("raffle", 0, 9, 8, 9)] if include_raffle else []),
        _seed_item("qr", 8, 0, 14, 15),
        _seed_item("prizes", 8, 15, 14, 3),
        *([_seed_item("ranking", 22, 0, 10, 18)] if include_ranking else []),
    ]
    portrait = [
        _seed_item("logo", 0, 0, 18, 5),
        _seed_item("qr", 0, 5, 18, 10),
        _seed_item("prizes", 0, 15, 18, 2),
        _seed_item("players", 0, 17, 18, 3),
        *([_seed_item("raffle", 0, 20, 18, 5)] if include_raffle else []),
        *([_seed_item("ranking", 0, 25, 18, 7)] if include_ranking else []),
    ]
    return {
        "version": VERSION,
        "landscape": {"background": None, "items": landscape},
        "portrait": {"background": None, "items": portrait},
    }


def validate(value, owner_user_id=None):
    if not isinstance(value, dict) or set(value) != {"version", "landscape", "portrait"}:
        raise HomeCanvasError("InvalidHomeCanvas")
    if type(value["version"]) is not int or value["version"] != VERSION:
        raise HomeCanvasError("InvalidHomeCanvasVersion")

    normalized = {"version": VERSION}
    for orientation, (columns, rows) in (("landscape", LANDSCAPE), ("portrait", PORTRAIT)):
        composition = value[orientation]
        if not isinstance(composition, dict) or set(composition) != {"background", "items"}:
            raise HomeCanvasError("InvalidHomeCanvasComposition")
        background = composition["background"]
        if background is not None:
            if not isinstance(background, dict) or set(background) != {"key", "type"}:
                raise HomeCanvasError("InvalidHomeCanvasBackground")
            key, media_type = background["key"], background["type"]
            if (not isinstance(key, str) or len(key) > 1024 or ".." in key
                    or not isinstance(media_type, str) or media_type not in {"image", "video"}):
                raise HomeCanvasError("InvalidHomeCanvasBackground")
            if owner_user_id is not None and not key.startswith(f"users/{owner_user_id}/home-backgrounds/"):
                raise HomeCanvasError("InvalidHomeCanvasBackground")
            background = {"key": key, "type": media_type}

        items = composition["items"]
        if not isinstance(items, list) or len(items) > MAX_ITEMS:
            raise HomeCanvasError("InvalidHomeCanvasItems")
        normalized_items, ids, singleton_types = [], set(), set()
        for item in items:
            if not isinstance(item, dict):
                raise HomeCanvasError("InvalidHomeCanvasItem")
            allowed = {"id", "type", "x", "y", "w", "h", "backgroundColor"}
            if item.get("type") == "text":
                allowed |= {"text", "fontSize", "color", "align"}
            if set(item) - allowed or not {"id", "type", "x", "y", "w", "h"} <= set(item):
                raise HomeCanvasError("InvalidHomeCanvasItem")
            item_id, item_type = item["id"], item["type"]
            if not isinstance(item_id, str) or not _ID.fullmatch(item_id) or item_id in ids:
                raise HomeCanvasError("InvalidHomeCanvasItemId")
            if not isinstance(item_type, str) or item_type not in ITEM_TYPES:
                raise HomeCanvasError("InvalidHomeCanvasItemType")
            if item_type != "text":
                if item_type in singleton_types:
                    raise HomeCanvasError("DuplicateHomeCanvasItem")
                singleton_types.add(item_type)
            coords = [item[field] for field in ("x", "y", "w", "h")]
            if any(type(number) is not int for number in coords):
                raise HomeCanvasError("InvalidHomeCanvasBounds")
            x, y, w, h = coords
            min_w, min_h = MIN_SIZE[item_type]
            if x < 0 or y < 0 or w < min_w or h < min_h or x + w > columns or y + h > rows:
                raise HomeCanvasError("InvalidHomeCanvasBounds")
            if "backgroundColor" in item:
                background_color = item["backgroundColor"]
                if (not isinstance(background_color, str) or (
                        background_color != "transparent" and not _COLOR.fullmatch(background_color))):
                    raise HomeCanvasError("InvalidHomeCanvasBackgroundColor")
            if item_type == "text":
                text = item.get("text", "")
                if not isinstance(text, str) or len(text) > MAX_TEXT_LENGTH:
                    raise HomeCanvasError("InvalidHomeCanvasText")
                font_size = item.get("fontSize")
                if font_size is not None and (type(font_size) is not int or not 16 <= font_size <= 160):
                    raise HomeCanvasError("InvalidHomeCanvasFontSize")
                color = item.get("color")
                if color is not None and (not isinstance(color, str) or not _COLOR.fullmatch(color)):
                    raise HomeCanvasError("InvalidHomeCanvasColor")
                align = item.get("align")
                if align is not None and (
                    not isinstance(align, str) or align not in {"left", "center", "right"}
                ):
                    raise HomeCanvasError("InvalidHomeCanvasAlign")
            ids.add(item_id)
            normalized_items.append(dict(item))

        for index, first in enumerate(normalized_items):
            for second in normalized_items[index + 1:]:
                if (first["x"] < second["x"] + second["w"]
                        and second["x"] < first["x"] + first["w"]
                        and first["y"] < second["y"] + second["h"]
                        and second["y"] < first["y"] + first["h"]):
                    raise HomeCanvasError("HomeCanvasOverlap")
        normalized[orientation] = {"background": background, "items": normalized_items}
    return normalized


def resolved(meta, owner_user_id=None):
    canvas = meta.get("homeCanvas")
    return validate(_json_sanitize(canvas) if canvas is not None else initial(meta), owner_user_id)


def validate_availability(canvas, meta):
    allowed_ranking = (meta.get("gameType") or "").upper() in {"T1MER", "STACK"}
    raffle_enabled = meta.get("showRaffleOnHome", True) is not False
    for orientation in ("landscape", "portrait"):
        for item in canvas[orientation]["items"]:
            if item["type"] == "ranking" and not allowed_ranking:
                raise HomeCanvasError("HomeCanvasRankingUnavailable")
            if item["type"] == "raffle" and not raffle_enabled:
                raise HomeCanvasError("HomeCanvasRaffleUnavailable")


def revision(canvas):
    canonical = json.dumps(canvas, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def next_revision(canvas):
    canonical = json.dumps(canvas, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(f"{canonical}:{uuid.uuid4()}".encode("utf-8")).hexdigest()


def background_urls(canvas, owner_user_id, bucket, s3):
    result = {"landscape": None, "portrait": None}
    for orientation in result:
        background = canvas[orientation]["background"]
        if not background:
            continue
        key = background["key"]
        if not key.startswith(f"users/{owner_user_id}/home-backgrounds/") or not bucket:
            continue
        try:
            result[orientation] = s3.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600,
            )
        except Exception:
            result[orientation] = None
    return result
