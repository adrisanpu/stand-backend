from assigners.generic import assign_generic
from stand_common.l3tras_word import l3tras_letters_from_game_meta


def assign_l3tras(ctx: dict):
    patch, welcome_header, extra_messages = assign_generic(ctx)
    player_id = int(ctx.get("playerId") or 0)
    meta = ctx.get("gameMeta") or {}
    letters = l3tras_letters_from_game_meta(meta)
    letter = letters[(player_id - 1) % len(letters)]
    game_type = "L3TRAS"
    t = patch["type"][game_type]
    t["letter"] = letter
    return (patch, welcome_header, extra_messages)
