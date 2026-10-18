# Base común para type[gameType]: solo estos campos (sin campos extra por tipo).
_COMMON_TYPE_FIELDS = {
    "raffleEligible": True,
    "quizRequired": False,
    "quizCompleted": False,
    "quizCurrentQuestion": None,
    "quizAnswers": {},
}

def assign_generic(ctx: dict):
    """
    Default assigner for any future gameType.
    Returns the type-specific patch merged into the player record before writing.
    """
    game_type = (ctx.get("gameType") or "UNKNOWN").upper()
    return {
        "type": {
            game_type: dict(_COMMON_TYPE_FIELDS),
        }
    }
