# Base común para type[gameType]: solo estos campos (sin campos extra por tipo).
_COMMON_TYPE_FIELDS = {
    "raffleEligible": True,
    "quizRequired": False,
    "quizCompleted": False,
    "quizCurrentQuestion": None,
    "quizAnswers": {},
}

# Plantillas de bienvenida por gameType. Claves en mayúsculas. Usan {username_at} y opcionalmente {player_id}.
# Si no hay plantilla, se usa el mensaje por defecto con player_id.
WELCOME_TEMPLATES = {
    "T1MER": "⏱️ ¡Bienvenid@ a T1mer, {username_at}!\n\n",
    "RULET4": "🎡 ¡Bienvenid@ a Rulet4, {username_at}!\n\n",
    "SEMAFORO": "🚦 ¡Bienvenid@ a SEMÁFORO, {username_at}!\n\n",
    "INFOCARDS": "📇 ¡Bienvenid@ a Infocards, {username_at}!\n\n",
}


def assign_generic(ctx: dict):
    """
    Default assigner for any future gameType.
    Contract: returns (patch, welcome_header, extra_messages)
      - patch: dict merged into the player record before writing
      - welcome_header: str sent as the main welcome DM, or None to skip
      - extra_messages: list of additional messages sent before the welcome
    """
    player_id = ctx.get("playerId")
    game_type = (ctx.get("gameType") or "UNKNOWN").upper()
    patch = {
        "type": {
            game_type: dict(_COMMON_TYPE_FIELDS),
        }
    }
    template = WELCOME_TEMPLATES.get(game_type)
    if template:
        username_at = ctx.get("username_at") or ""
        welcome_header = template.format(username_at=username_at, player_id=player_id)
    else:
        welcome_header = f"¡Te has unido al juego! ✅\nTu número de jugador es: {player_id}"
    return (patch, welcome_header, [])
