import json
from decimal import Decimal
from datetime import datetime, timezone

# Tipos que usan validación "un código". Etiqueta para el mensaje de éxito.
GAME_LABELS = {
    "T1MER": "T1mer",
    "RULET4": "Rulet4",
}

SINGLE_CODE_TYPES = frozenset(GAME_LABELS)


def _log(msg, data=None):
    if data is not None:
        print(json.dumps({"msg": msg, "data": data}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))


def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _validate_single_code(ctx: dict, game_label: str):
    game_id = ctx["gameId"]
    game_type = ctx["gameType"]
    codes = ctx["codes"]

    to_int = ctx["to_int_code"]
    query_by_code = ctx["query_players_by_code"]
    set_validated = ctx["set_validated"]
    quiz_required = ctx["is_quiz_required"]
    quiz_completed = ctx["is_quiz_completed"]
    inc_validated_count = ctx.get("inc_validated_count")

    numeric = [to_int(c) for c in codes]
    numeric = [c for c in numeric if c is not None]
    if not numeric:
        return {"valid": False, "gameId": game_id, "reason": "invalid_codes", "message": "Códigos inválidos."}

    found_any = False
    found_used = False
    found_quiz_missing = False

    for code in numeric:
        items = query_by_code(game_id, code)
        if items:
            found_any = True

        for it in items:
            if bool(it.get("validated", False)):
                found_used = True
                continue

            if quiz_required(it, game_type) and not quiz_completed(it, game_type):
                found_quiz_missing = True
                continue

            pid = int(it["playerId"]) if isinstance(it["playerId"], (int, Decimal)) else int(it["playerId"])

            changed = set_validated(game_id, pid)
            if not changed:
                found_used = True
                continue

            if inc_validated_count:
                try:
                    inc_validated_count(game_id, 1)
                except Exception as e:
                    _log("validatedCount_update_failed", {"error": repr(e), "gameId": game_id})

            return {
                "valid": True,
                "gameId": game_id,
                "playerId": pid,
                "username": it.get("instagramUsername", ""),
                "validatedAt": _iso_now(),
                "message": f"✅ Validación correcta ({game_label})."
            }

    if not found_any:
        return {"valid": False, "gameId": game_id, "reason": "no_code_match", "message": "Nadie tiene ese código."}
    if found_quiz_missing and not found_used:
        return {"valid": False, "gameId": game_id, "reason": "quiz_not_completed", "message": "Ese jugador aún no ha completado el quiz."}
    if found_used:
        return {"valid": False, "gameId": game_id, "reason": "already_validated", "message": "Ese código ya fue usado."}

    return {"valid": False, "gameId": game_id, "reason": "no_match", "message": "No hay jugador elegible con ese código."}


def validate_generic(ctx: dict):
    game_type = (ctx.get("gameType") or "UNKNOWN").upper()
    if game_type in SINGLE_CODE_TYPES:
        game_label = GAME_LABELS.get(game_type, game_type)
        return _validate_single_code(ctx, game_label)
    return {
        "valid": False,
        "gameId": ctx["gameId"],
        "reason": "unknown_game",
        "message": f"Juego desconocido: {ctx['gameType']}"
    }
