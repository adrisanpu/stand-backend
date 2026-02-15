def assign_rulet4(ctx: dict):
    username_at = ctx["username_at"]

    patch = {
        "type": {
            "RULET4": {
                "lastSpin": None,
                "spinUsed": False,
                "quizAnswers": {},
            }
        }
    }

    welcome_header = f"🎡 ¡Bienvenid@ a Rulet4, {username_at}!\n\n"
    extra_messages = []  # none

    return (patch, welcome_header, extra_messages)

