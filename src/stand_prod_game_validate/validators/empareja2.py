from decimal import Decimal
from datetime import datetime, timezone

def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_char_id(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return int(x)
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _pair_group_id_from_player(t: dict) -> str:
    """pairGroupId stored in Gameplayer; fallback to pairId for legacy data."""
    return str(t.get("pairGroupId") or t.get("pairId") or "").strip()


def _is_valid_pair(t1: dict, t2: dict) -> bool:
    """True if both have same non-empty pairGroupId and different characterId."""
    g1 = _pair_group_id_from_player(t1)
    g2 = _pair_group_id_from_player(t2)
    if not g1 or not g2 or g1 != g2:
        return False
    # Compare characterIds: support numeric (int/Decimal) and string (e.g. "C1", "C2")
    c1 = t1.get("characterId")
    c2 = t2.get("characterId")
    n1, n2 = _norm_char_id(c1), _norm_char_id(c2)
    if n1 is not None and n2 is not None:
        if n1 == n2:
            return False
    else:
        if str(c1 or "") == str(c2 or ""):
            return False
    return True


def validate_empareja2(ctx: dict):
    game_id = ctx["gameId"]
    codes = ctx["codes"]

    to_int = ctx["to_int_code"]
    query_by_code = ctx["query_players_by_code"]
    set_validated = ctx["set_validated"]
    presign = ctx.get("presign_character_png")
    inc_validated_count = ctx.get("inc_validated_count")

    if len(codes) != 2:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "invalid_code_count",
            "message": f"Empareja2 necesita exactamente 2 códigos (has enviado {len(codes)})."
        }

    c1 = to_int(codes[0])
    c2 = to_int(codes[1])

    if c1 is None or c2 is None:
        return {"valid": False, "gameId": game_id, "reason": "invalid_code_format", "message": "Códigos inválidos."}
    if c1 == c2:
        return {"valid": False, "gameId": game_id, "reason": "same_code", "message": "No puedes validar contigo mismo 😉"}

    p1s = query_by_code(game_id, c1)
    p2s = query_by_code(game_id, c2)
    p1 = p1s[0] if p1s else None
    p2 = p2s[0] if p2s else None

    if not p1 or not p2:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "code_not_found",
            "message": "Uno (o los dos) códigos no existe en esta partida.",
            "found": {"code_1": bool(p1), "code_2": bool(p2)}
        }

    if bool(p1.get("validated")) or bool(p2.get("validated")):
        return {"valid": False, "gameId": game_id, "reason": "already_validated", "message": "Alguno ya fue validado."}

    t1 = (p1.get("type") or {}).get("EMPAREJA2") or {}
    t2 = (p2.get("type") or {}).get("EMPAREJA2") or {}

    pair_group_1 = _pair_group_id_from_player(t1)
    pair_group_2 = _pair_group_id_from_player(t2)

    if not pair_group_1 or not pair_group_2:
        return {"valid": False, "gameId": game_id, "reason": "missing_pair_data", "message": "Faltan datos de emparejamiento."}

    if str(t1.get("characterId")) == str(t2.get("characterId")):
        return {"valid": False, "gameId": game_id, "reason": "same_character", "message": "No puedes validar contigo mismo 😉"}

    if not _is_valid_pair(t1, t2):
        return {"valid": False, "gameId": game_id, "reason": "different_pair", "message": "No sois la pareja correcta 💘"}

    pid1 = int(p1["playerId"]) if isinstance(p1["playerId"], (int, Decimal)) else int(p1["playerId"])
    pid2 = int(p2["playerId"]) if isinstance(p2["playerId"], (int, Decimal)) else int(p2["playerId"])

    changed1 = set_validated(game_id, pid1)
    changed2 = set_validated(game_id, pid2)

    # solo incrementa por los que realmente cambiaron
    inc = (1 if changed1 else 0) + (1 if changed2 else 0)
    if inc and inc_validated_count:
        try:
            inc_validated_count(game_id, inc)
        except Exception as e:
            ctx["log"]("validatedCount_update_failed", {"error": repr(e), "gameId": game_id})

    c1_name = t1.get("characterName")
    c2_name = t2.get("characterName")

    return {
        "valid": True,
        "gameId": game_id,
        "pairId": pair_group_1,
        "message": "",
        "players": [
            {
                "playerId": pid1,
                "instagramUsername": p1.get("instagramUsername"),
                "characterId": t1.get("characterId"),
                "characterName": c1_name,
                "characterImageUrl": presign(c1_name) if presign else None,
            },
            {
                "playerId": pid2,
                "instagramUsername": p2.get("instagramUsername"),
                "characterId": t2.get("characterId"),
                "characterName": c2_name,
                "characterImageUrl": presign(c2_name) if presign else None,
            },
        ]
    }
