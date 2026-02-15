def assign_t1mer(ctx: dict):
    username_at = ctx["username_at"]

    patch = {
        "type": {
            "T1MER": {
                "score": 0,
                "quizAnswers": {},
            }
        }
    }

    welcome_header = f"⏱️ ¡Bienvenid@ a T1mer, {username_at}!\n\n"
    extra_messages = []  # none

    return (patch, welcome_header, extra_messages)