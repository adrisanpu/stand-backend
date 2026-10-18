"""Supported games use mobile gameplay and web enrollment exclusively."""

PLAY_MODES = ("mobile",)
JOIN_CHANNELS = ("web",)

_MOBILE_WEB = {"playModes": PLAY_MODES, "joinChannels": JOIN_CHANNELS}
GAME_ACCESS_CAPABILITIES = {
    game_type: _MOBILE_WEB
    for game_type in (
        "EMPAREJA2", "T1MER", "STACK", "L3TRAS", "RULET4", "CUPIDO",
        "SEMAFORO", "INFOCARDS", "IMPOSTOR",
    )
}
_DEFAULT_CAPABILITIES = _MOBILE_WEB


class GameAccessError(ValueError):
    pass


def capabilities(game_type: str) -> dict:
    return GAME_ACCESS_CAPABILITIES.get((game_type or "").upper(), _DEFAULT_CAPABILITIES)


def _norm(value):
    return value.strip().lower() if isinstance(value, str) else value


def _legacy_channel(game_type: str) -> str:
    return "web"


def _legacy_mode(game_type: str, join_channel: str) -> str:
    return "mobile"


def resolve_game_access(item: dict | None) -> dict:
    """Return the approved policy, ignoring stale or malformed stored legacy values."""
    return {"playMode": "mobile", "joinChannel": "web"}


def validate_game_access(game_type: str, play_mode, join_channel) -> dict:
    """Strict check used before any write."""
    pm, jc = _norm(play_mode), _norm(join_channel)
    if pm not in PLAY_MODES:
        raise GameAccessError("playMode must be 'mobile'.")
    if jc not in JOIN_CHANNELS:
        raise GameAccessError("joinChannel must be 'web'.")
    caps = capabilities(game_type)
    if pm not in caps["playModes"] or jc not in caps["joinChannels"]:
        raise GameAccessError(
            f"{(game_type or '').upper()} does not support playMode '{pm}' with joinChannel '{jc}'."
        )
    return {"playMode": pm, "joinChannel": jc}


def access_for_create(game_type: str, play_mode=None, join_channel=None) -> dict:
    jc = "web" if join_channel is None else join_channel
    pm = "mobile" if play_mode is None else play_mode
    return validate_game_access(game_type, pm, jc)


def access_for_update(item: dict, play_mode=None, join_channel=None) -> dict:
    """Merge a partial update with the effective access; both fields are always returned."""
    gt = (item.get("gameType") or "").upper()
    current = resolve_game_access(item)
    jc = current["joinChannel"] if join_channel is None else join_channel
    pm = current["playMode"] if play_mode is None else play_mode
    return validate_game_access(gt, pm, jc)