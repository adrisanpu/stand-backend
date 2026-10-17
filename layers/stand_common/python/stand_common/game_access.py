"""Per-game play mode / join channel capabilities.

Mirror of stand-frontend core/settings/gameAccess.ts (keep both matrices in sync;
tests/test_game_access.py asserts the backend matrix).

- playMode: where the game is played/validated after access ("master" screen or "mobile" clients).
- joinChannel: how participants register/access ("instagram" DM or "web" form); decides the QR target.
"""

PLAY_MODES = ("master", "mobile")
JOIN_CHANNELS = ("instagram", "web")

GAME_ACCESS_CAPABILITIES = {
    "EMPAREJA2": {"playModes": ("master", "mobile"), "joinChannels": ("instagram", "web")},
    "T1MER": {"playModes": ("master", "mobile"), "joinChannels": ("instagram", "web")},
    "STACK": {"playModes": ("mobile",), "joinChannels": ("instagram", "web")},
    "L3TRAS": {"playModes": ("master",), "joinChannels": ("instagram",)},
    "RULET4": {"playModes": ("master",), "joinChannels": ("instagram",)},
    "CUPIDO": {"playModes": ("master",), "joinChannels": ("instagram",)},
    "SEMAFORO": {"playModes": ("master",), "joinChannels": ("instagram",)},
    "INFOCARDS": {"playModes": ("master",), "joinChannels": ("instagram",)},
    "IMPOSTOR": {"playModes": ("master",), "joinChannels": ("instagram",)},
}
_DEFAULT_CAPABILITIES = {"playModes": ("master",), "joinChannels": ("instagram",)}


class GameAccessError(ValueError):
    pass


def capabilities(game_type: str) -> dict:
    return GAME_ACCESS_CAPABILITIES.get((game_type or "").upper(), _DEFAULT_CAPABILITIES)


def _norm(value):
    return value.strip().lower() if isinstance(value, str) else value


def _legacy_channel(game_type: str) -> str:
    return "web" if "web" in capabilities(game_type)["joinChannels"] else "instagram"


def _legacy_mode(game_type: str, join_channel: str) -> str:
    gt = (game_type or "").upper()
    if gt == "STACK":
        return "mobile"
    if gt in ("EMPAREJA2", "T1MER"):
        return "mobile" if join_channel == "web" else "master"
    return "master"


def resolve_game_access(item: dict | None) -> dict:
    """Resolve missing legacy fields without hiding an invalid explicit configuration."""
    item = item or {}
    gt = (item.get("gameType") or "").upper()
    join_channel = _legacy_channel(gt) if item.get("joinChannel") is None else _norm(item["joinChannel"])
    play_mode = _legacy_mode(gt, join_channel) if item.get("playMode") is None else _norm(item["playMode"])
    return validate_game_access(gt, play_mode, join_channel)


def validate_game_access(game_type: str, play_mode, join_channel) -> dict:
    """Strict check used before any write."""
    pm, jc = _norm(play_mode), _norm(join_channel)
    if pm not in PLAY_MODES:
        raise GameAccessError("playMode must be 'master' or 'mobile'.")
    if jc not in JOIN_CHANNELS:
        raise GameAccessError("joinChannel must be 'web' or 'instagram'.")
    caps = capabilities(game_type)
    if pm not in caps["playModes"] or jc not in caps["joinChannels"]:
        raise GameAccessError(
            f"{(game_type or '').upper()} does not support playMode '{pm}' with joinChannel '{jc}'."
        )
    return {"playMode": pm, "joinChannel": jc}


def access_for_create(game_type: str, play_mode=None, join_channel=None) -> dict:
    jc = _legacy_channel(game_type) if join_channel is None else join_channel
    pm = _legacy_mode(game_type, _norm(jc)) if play_mode is None else play_mode
    return validate_game_access(game_type, pm, jc)


def access_for_update(item: dict, play_mode=None, join_channel=None) -> dict:
    """Merge a partial update with the effective access; both fields are always returned."""
    gt = (item.get("gameType") or "").upper()
    current = resolve_game_access(item)
    jc = current["joinChannel"] if join_channel is None else join_channel
    if play_mode is not None:
        pm = play_mode
    elif _norm(item.get("playMode")) in PLAY_MODES:
        pm = current["playMode"]
    else:
        # Legacy games coupled mode to channel; keep that behaviour until playMode is saved.
        pm = _legacy_mode(gt, _norm(jc))
    return validate_game_access(gt, pm, jc)