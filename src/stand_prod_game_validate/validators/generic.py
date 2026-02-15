def validate_generic(ctx: dict):
    return {
        "valid": False,
        "gameId": ctx["gameId"],
        "reason": "unknown_game",
        "message": f"Juego desconocido: {ctx['gameType']}"
    }
