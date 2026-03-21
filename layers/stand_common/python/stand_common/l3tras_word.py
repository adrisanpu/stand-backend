"""Shared L3TRAS helpers (game meta type.L3TRAS). Used by assign + validate lambdas."""

DEFAULT_L3TRAS_WORD = "STAND"


def l3tras_word_from_game_meta(meta: dict) -> str:
    """Objective word from games_table item (type.L3TRAS), always A–Z uppercase."""
    blob = (meta.get("type") or {}).get("L3TRAS") or {}
    raw = str(blob.get("objectiveWord") or DEFAULT_L3TRAS_WORD).strip()
    letters_only = "".join(c for c in raw if c.isalpha())
    if not letters_only:
        letters_only = DEFAULT_L3TRAS_WORD
    return letters_only.upper()


def l3tras_letters_from_game_meta(meta: dict) -> list[str]:
    return list(l3tras_word_from_game_meta(meta))
