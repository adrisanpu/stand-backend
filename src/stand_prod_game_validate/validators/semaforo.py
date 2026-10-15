from decimal import Decimal

def _as_int(x):
    try:
        if isinstance(x, Decimal):
            return int(x)
        return int(x)
    except Exception:
        return None

def _game_has_quiz(game_meta: dict) -> bool:
    order = (game_meta or {}).get("quizOrder") or []
    return isinstance(order, list) and len(order) > 0

def _quiz_completed_for_game(player_item: dict, game_type_upper: str) -> bool:
    t = (player_item or {}).get("type") or {}
    g = t.get((game_type_upper or "").upper()) or {}
    return bool(g.get("quizCompleted", False))

def validate_semaforo(ctx: dict):
    game_id = ctx["gameId"]
    codes = ctx["codes"]
    game_type = (ctx.get("gameType") or "SEMAFORO").upper()
    meta = ctx.get("gameMeta") or {}

    to_int = ctx["to_int_code"]
    query_by_code = ctx["query_players_by_code"]
    set_validated = ctx["set_validated"]
    inc_validated_count = ctx.get("inc_validated_count")

    # ---- must be 2 codes ----
    if len(codes) != 2:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "invalid_code_count",
            "message": f"SEMAFORO necesita exactamente 2 códigos (has enviado {len(codes)}).",
        }

    c1 = to_int(codes[0])
    c2 = to_int(codes[1])

    if c1 is None or c2 is None:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "invalid_code_format",
            "message": "Códigos inválidos.",
        }

    if c1 == c2:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "same_code",
            "message": "No puedes validar contigo mismo 😉",
        }

    # ---- find players ----
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
            "found": {"code_1": bool(p1), "code_2": bool(p2)},
        }

    # ---- already validated ----
    if bool(p1.get("validated")) or bool(p2.get("validated")):
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "already_validated",
            "message": "Alguno de los dos códigos ya fue validado.",
        }

    # ---- quiz gate: if game has quiz => both must have completed ----
    if _game_has_quiz(meta):
        done1 = _quiz_completed_for_game(p1, game_type)
        done2 = _quiz_completed_for_game(p2, game_type)

        if not done1 or not done2:
            return {
                "valid": False,
                "gameId": game_id,
                "reason": "quiz_not_completed",
                "message": "Alguno aún no ha completado el quiz.",
                "quiz": {"p1Completed": done1, "p2Completed": done2},
            }

    # ---- set validated ----
    pid1 = _as_int(p1.get("playerId"))
    pid2 = _as_int(p2.get("playerId"))

    if pid1 is None or pid2 is None:
        return {
            "valid": False,
            "gameId": game_id,
            "reason": "bad_playerId",
            "message": "playerId inválido en uno de los registros.",
        }

    changed1 = set_validated(game_id, pid1)
    changed2 = set_validated(game_id, pid2)

    inc = (1 if changed1 else 0) + (1 if changed2 else 0)
    if inc and inc_validated_count:
        try:
            inc_validated_count(game_id, inc)
        except Exception as e:
            ctx["log"]("validatedCount_update_failed", {"error": repr(e), "gameId": game_id})

    safe_questions = {}
    for question_id, question in (meta.get("quizQuestions") or {}).items():
        safe_questions[question_id] = {
            "text": question.get("text"),
            "options": [
                {"title": option.get("title"), "payload": option.get("payload")}
                for option in question.get("options") or []
            ],
        }

    return {
        "valid": True,
        "gameId": game_id,
        "reason": "ok",
        "message": "✅ Validación correcta.",
        "game": {
            "gameId": meta.get("gameId"),
            "quizOrder": meta.get("quizOrder") or [],
            "quizQuestions": safe_questions,
        },
        "players": [
            {
                "playerId": pid1,
                "instagramUsername": p1.get("instagramUsername", ""),
                "type": {
                    "SEMAFORO": {
                        "quizAnswers": (((p1.get("type") or {}).get("SEMAFORO") or {}).get("quizAnswers") or {}),
                    },
                },
            },
            {
                "playerId": pid2,
                "instagramUsername": p2.get("instagramUsername", ""),
                "type": {
                    "SEMAFORO": {
                        "quizAnswers": (((p2.get("type") or {}).get("SEMAFORO") or {}).get("quizAnswers") or {}),
                    },
                },
            },
        ],
    }
