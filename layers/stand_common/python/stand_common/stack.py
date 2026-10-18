"""Shared STACK configuration, exact geometry replay and ranking."""
import hashlib
import json
import math
from decimal import Decimal, InvalidOperation

from boto3.dynamodb.conditions import Key
from stand_common.utils import get_game_type_blob, _parse_iso, log, mask_email


CONFIG_VERSION = "1"
DEFAULT_CONFIG = {
    "initialSpeed": Decimal("2"),
    "speedIncrement": Decimal("0.08"),
    "maxSpeed": Decimal("5"),
    "perfectTolerance": Decimal("0.06"),
    "initialWidth": Decimal("3"),
    "initialDepth": Decimal("2"),
    "layerHeight": Decimal("0.24"),
    "movementRange": Decimal("4"),
    "configVersion": CONFIG_VERSION,
    "skin": "blocks",
}
SKINS = frozenset({"blocks", "pizza", "baklava"})
EDITABLE_FIELDS = frozenset({"initialSpeed", "speedIncrement", "maxSpeed", "perfectTolerance", "skin"})
_RANGES = {
    "initialSpeed": (Decimal("0.1"), Decimal("10")),
    "speedIncrement": (Decimal("0"), Decimal("1")),
    "maxSpeed": (Decimal("0.1"), Decimal("10")),
    "perfectTolerance": (Decimal("0"), Decimal("0.25")),
    "initialWidth": (Decimal("0.5"), Decimal("10")),
    "initialDepth": (Decimal("0.5"), Decimal("10")),
    "layerHeight": (Decimal("0.05"), Decimal("1")),
    "movementRange": (Decimal("0.5"), Decimal("20")),
}


def validate_config(settings: dict | None = None, *, base: dict | None = None) -> dict:
    """Return a full, DynamoDB-safe configuration; partial edits merge with base."""
    if settings is not None and not isinstance(settings, dict):
        raise ValueError("settings.stack must be an object.")
    if set(settings or {}) - EDITABLE_FIELDS:
        raise ValueError("settings.stack only supports initialSpeed, speedIncrement, maxSpeed, perfectTolerance and skin.")
    return _validate_complete_config({**(DEFAULT_CONFIG if base is None else base), **(settings or {})})


def _validate_complete_config(raw_config: dict) -> dict:
    if not isinstance(raw_config, dict):
        raise ValueError("STACK configuration must contain every required field, with no unknown fields.")
    # Games and attempt snapshots stored before skins existed default to blocks.
    config = {"skin": DEFAULT_CONFIG["skin"], **raw_config}
    if set(config) != set(DEFAULT_CONFIG):
        raise ValueError("STACK configuration must contain every required field, with no unknown fields.")
    if config["configVersion"] != CONFIG_VERSION:
        raise ValueError("STACK configVersion must be '1'.")
    if not isinstance(config["skin"], str) or config["skin"] not in SKINS:
        raise ValueError("settings.stack.skin must be one of: baklava, blocks, pizza.")
    for field, (low, high) in _RANGES.items():
        raw = config[field]
        if isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal)):
            raise ValueError(f"settings.stack.{field} must be a finite number.")
        try:
            value = Decimal(str(raw))
        except InvalidOperation:
            raise ValueError(f"settings.stack.{field} must be a finite number.") from None
        if not value.is_finite() or not low <= value <= high:
            raise ValueError(f"settings.stack.{field} must be between {low} and {high}.")
        if field not in EDITABLE_FIELDS and value != DEFAULT_CONFIG[field]:
            raise ValueError(f"STACK {field} is fixed at {DEFAULT_CONFIG[field]}.")
        config[field] = value
    if config["initialSpeed"] > config["maxSpeed"]:
        raise ValueError("settings.stack.initialSpeed must not exceed maxSpeed.")
    if config["movementRange"] <= max(config["initialWidth"], config["initialDepth"]):
        raise ValueError("settings.stack.movementRange must exceed initialWidth and initialDepth.")
    if config["perfectTolerance"] >= min(config["initialWidth"], config["initialDepth"]) / 2:
        raise ValueError("settings.stack.perfectTolerance must be below half the smallest side.")
    return config


def game_config(game: dict) -> dict:
    return _validate_complete_config(get_game_type_blob(game, "STACK"))


def display_name(player: dict) -> str:
    name = (player.get("username") or "").strip()
    if name:
        return name
    return f"Jugador {player.get('playerId', '')}"


def public_assignment(player: dict, gp_table=None, profile_pic_url_fn=None) -> dict:
    blob = get_game_type_blob(player, "STACK")
    attempt = blob.get("attempt")
    email = (player.get("email") or "").strip()
    display = (player.get("username") or "").strip() or (
        mask_email(email) if email else f"Jugador {player.get('playerId', '')}"
    )
    result = {
        "gameType": "STACK",
        "playerId": player.get("playerId"),
        "displayName": display,
        "attempt": None,
    }
    pic = profile_pic_url_fn(player) if profile_pic_url_fn else None
    if pic:
        result["profilePicUrl"] = pic
    if attempt:
        result["attempt"] = {
            key: attempt[key] for key in ("attemptId", "status", "startedAt", "configVersion")
        }
        if attempt.get("finishedAt"):
            result["attempt"]["finishedAt"] = attempt["finishedAt"]
    if blob.get("score") is not None:
        result["score"] = blob["score"]
        result["rank"] = player_rank(player["gameId"], player["playerId"], gp_table) if gp_table else None
        if blob.get("scoreUpdatedAt"):
            result["scoreUpdatedAt"] = blob["scoreUpdatedAt"]
    return result


def replay(placements: list, snapshot: dict) -> tuple[int, float, str]:
    """Replay JS binary64 rectangle operations, returning score, minimum seconds, hash."""
    config = {key: float(value) for key, value in _validate_complete_config(snapshot).items() if key in _RANGES}
    if not isinstance(placements, list) or not placements:
        raise ValueError("InvalidPlacements")
    support = {"x": 0.0, "z": 0.0, "width": config["initialWidth"], "depth": config["initialDepth"]}
    score, minimum = 0, 0.0
    digest = hashlib.sha256()
    failed = False
    for placement in placements:
        if failed or not isinstance(placement, dict) or set(placement) != {"axis", "offset", "perfect"}:
            raise ValueError("InvalidPlacements")
        axis, raw_offset, perfect = placement["axis"], placement["offset"], placement["perfect"]
        if axis != ("x" if score % 2 == 0 else "z"):
            raise ValueError("InvalidPlacements")
        if type(raw_offset) not in (int, float) or type(perfect) is not bool:
            raise ValueError("InvalidPlacements")
        try:
            offset = float(raw_offset)
        except (ValueError, OverflowError):
            raise ValueError("InvalidPlacements") from None
        if not math.isfinite(offset) or abs(offset) > config["movementRange"] + 1e-9:
            raise ValueError("InvalidPlacements")
        moving = {**support, axis: support[axis] + offset}
        left = max(moving["x"] - moving["width"] / 2, support["x"] - support["width"] / 2)
        right = min(moving["x"] + moving["width"] / 2, support["x"] + support["width"] / 2)
        back = max(moving["z"] - moving["depth"] / 2, support["z"] - support["depth"] / 2)
        front = min(moving["z"] + moving["depth"] / 2, support["z"] + support["depth"] / 2)
        overlap = right > left and front > back
        if perfect != (overlap and abs(offset) <= config["perfectTolerance"]):
            raise ValueError("InvalidPlacements")
        speed = min(config["maxSpeed"], config["initialSpeed"] + score * config["speedIncrement"])
        minimum += max(0.0, config["movementRange"] + offset) / speed
        # Canonical binary64 values make JSON number spelling/key order irrelevant.
        digest.update(json.dumps([axis, (offset if offset else 0.0).hex(), perfect],
                                 separators=(",", ":")).encode("ascii"))
        if not overlap:
            failed = True
        else:
            if not perfect:
                support = {"x": left + (right - left) / 2, "z": back + (front - back) / 2,
                           "width": right - left, "depth": front - back}
            score += 1
            minimum += 0.22
    if not failed:
        raise ValueError("MissingFinalFailure")
    return score, minimum, digest.hexdigest()


def get_ranking(game_id: str, gp_table, limit: int | None = 10,
                profile_pic_url_fn=None) -> dict:
    """Read every page before sorting; never limit the candidate set by player ID."""
    entries, last = [], None
    while True:
        kwargs = {"KeyConditionExpression": Key("gameId").eq(game_id), "ConsistentRead": True}
        if last:
            kwargs["ExclusiveStartKey"] = last
        page = gp_table.query(**kwargs)
        for player in page.get("Items") or []:
            blob = get_game_type_blob(player, "STACK")
            attempt = blob.get("attempt") or {}
            score, pid = blob.get("score"), player.get("playerId")
            if (isinstance(score, bool) or not isinstance(score, (int, Decimal)) or
                    not math.isfinite(score) or score < 0 or score != int(score) or
                    isinstance(pid, bool) or not isinstance(pid, (int, Decimal)) or
                    pid <= 0 or pid != int(pid) or attempt.get("status") != "finished"):
                continue
            finished = _parse_iso(attempt.get("finishedAt"))
            if finished is None or finished.tzinfo is None:
                continue
            entries.append((finished, {
                "playerId": int(pid), "username": display_name(player),
                "profilePicUrl": profile_pic_url_fn(player) if profile_pic_url_fn else None,
                "score": int(score), "finishedAt": attempt["finishedAt"],
            }))
        last = page.get("LastEvaluatedKey")
        if not last:
            break
    entries.sort(key=lambda entry: (-entry[1]["score"], entry[0], entry[1]["playerId"]))
    results = [dict(entry, rank=index) for index, (_, entry) in enumerate(entries, 1)]
    if limit is not None:
        results = results[:min(100, limit if limit > 0 else 10)]
    return {"ok": True, "results": results}


def player_rank(game_id: str, player_id: int, gp_table) -> int | None:
    try:
        return next((entry["rank"] for entry in get_ranking(game_id, gp_table, None)["results"]
                     if entry["playerId"] == player_id), None)
    except Exception as exc:
        # Ranking is a separate multi-item read, not part of the atomic finish.
        log("stack_rank_unavailable", {"error": repr(exc)})
        return None


def response_status(result: dict) -> int:
    if result.get("ok"):
        return 200
    return {
        "AttemptAlreadyStarted": 409,
        "AttemptConflict": 409,
        "AttemptNotStarted": 409,
        "PlayerNotFound": 404,
        "GameInactive": 403,
        "GameNotFound": 404,
        "internal_error": 500,
        "BadScoreResponse": 502,
        "InvalidStackConfig": 400,
    }.get(result.get("error"), 400)
