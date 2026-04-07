from decimal import Decimal


def validate_l3tras(ctx: dict):
    game_id = ctx["gameId"]
    codes = ctx["codes"]
    game_type = (ctx.get("gameType") or "L3TRAS").upper()

    to_int = ctx["to_int_code"]
    query_by_code = ctx["query_players_by_code"]
    quiz_required = ctx["is_quiz_required"]
    quiz_completed = ctx["is_quiz_completed"]

    if len(codes) != 1:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "invalid_code_count",
            "message": "Envía un único código.",
        }

    raw_code = codes[0]
    code_int = to_int(raw_code)
    if code_int is None:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "invalid_code_format",
            "message": "Código inválido.",
        }

    items = query_by_code(game_id, code_int)
    if not items:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "no_code_match",
            "message": "Ese código no existe en esta partida.",
        }

    it = items[0]
    if quiz_required(it, game_type) and not quiz_completed(it, game_type):
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "quiz_not_completed",
            "message": "Un jugador aún no ha completado el quiz.",
        }

    blob = (it.get("type") or {}).get(game_type) or {}
    letter = blob.get("letter")
    if not letter or not isinstance(letter, str) or len(letter) != 1:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "missing_letter",
            "message": "No se encontró la letra asignada para este jugador.",
        }

    pid = it["playerId"]
    if isinstance(pid, Decimal):
        pid = int(pid)
    else:
        pid = int(pid)

    return {
        "valid": True,
        "gameId": game_id,
        "players": [
            {
                "playerId": pid,
                "instagramUsername": it.get("instagramUsername", ""),
                "letter": letter,
            }
        ],
        "results": [{"code": str(code_int), "letter": letter}],
        "message": "✅ Código correcto.",
    }
