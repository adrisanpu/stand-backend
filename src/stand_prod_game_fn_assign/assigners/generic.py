def assign_generic(ctx: dict):
    """
    Default assigner for any future gameType.
    Only customizes messaging.
    """
    psid = ctx["psid"]
    username_at = ctx["username_at"]
    player_id = ctx["playerId"]

    messages = [{
        "psid": psid,
        "text": f"¡Te has unido al juego! ✅\nTu número de jugador es: {player_id}"
    }]

    return ({}, messages)
