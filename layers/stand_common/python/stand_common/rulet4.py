"""Canonical RULET4 panel configuration; counts are probability weights, not stock."""
from decimal import Decimal
import re


MAX_PANELS = 32
MAX_TOTAL_WEIGHT = 360
DEFAULT_CONFIG = {
    "panels": [
        {"id": "1", "label": "Rojo", "color": "#DC2626", "count": 4},
        {"id": "2", "label": "Negro", "color": "#000000", "count": 4},
        {"id": "3", "label": "Verde", "color": "#22C55E", "count": 1},
    ]
}


def _validate(config):
    if not isinstance(config, dict) or set(config) != {"panels"}:
        raise ValueError("RULET4 configuration must contain only panels.")
    panels = config["panels"]
    if not isinstance(panels, list) or not 1 <= len(panels) <= MAX_PANELS:
        raise ValueError(f"RULET4 must contain between 1 and {MAX_PANELS} panels.")
    result, ids, total = [], set(), 0
    for panel in panels:
        if not isinstance(panel, dict) or set(panel) - {"id", "label", "color", "count"}:
            raise ValueError("RULET4 panels only support id, label, color and count.")
        panel_id = panel.get("id")
        if not isinstance(panel_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", panel_id) or panel_id in ids:
            raise ValueError("RULET4 panel ids must be unique, 1–64 letters, digits, underscores or hyphens.")
        label = panel.get("label")
        if not isinstance(label, str) or not 1 <= len(label.strip()) <= 80:
            raise ValueError("RULET4 panel labels must contain 1–80 characters.")
        color = panel.get("color", "#DC2626")
        if not isinstance(color, str) or not re.fullmatch(r"#[0-9A-Fa-f]{6}", color):
            raise ValueError("RULET4 panel colors must be #RRGGBB.")
        count = panel.get("count")
        if isinstance(count, bool) or not isinstance(count, (int, Decimal)):
            raise ValueError("RULET4 panel counts must be positive integer weights.")
        if isinstance(count, Decimal) and not count.is_finite():
            raise ValueError("RULET4 panel counts must be finite integer weights.")
        if not 1 <= count <= MAX_TOTAL_WEIGHT or count != int(count):
            raise ValueError(f"RULET4 panel counts must be integer weights between 1 and {MAX_TOTAL_WEIGHT}.")
        total += int(count)
        ids.add(panel_id)
        result.append({"id": panel_id, "label": label.strip(), "color": color.upper(), "count": int(count)})
    if total > MAX_TOTAL_WEIGHT:
        raise ValueError(f"RULET4 total panel weight must not exceed {MAX_TOTAL_WEIGHT}.")
    return {"panels": result}


def validate_config(config=None, *, base=None):
    """Default absent creation config; merge edits only with an explicit validated base."""
    if config is None:
        return _validate(DEFAULT_CONFIG if base is None else base)
    if not isinstance(config, dict) or set(config) - {"panels"}:
        raise ValueError("settings.rulet4 must be an object containing only panels.")
    if base is None:
        return _validate(config)
    return _validate({**_validate(base), **config})


def game_config(meta):
    """Return fresh validated defaults only when canonical stored config is absent."""
    if not isinstance(meta, dict):
        raise ValueError("RULET4 game metadata must be an object.")
    if "type" not in meta:
        return _validate(DEFAULT_CONFIG)
    types = meta["type"]
    if not isinstance(types, dict):
        raise ValueError("RULET4 game type must be an object.")
    if "RULET4" not in types:
        return _validate(DEFAULT_CONFIG)
    return _validate(types["RULET4"])
