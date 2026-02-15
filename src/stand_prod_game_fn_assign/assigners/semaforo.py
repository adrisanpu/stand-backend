def assign_semaforo(ctx: dict):
    """
    SEMÁFORO assignment (nuevo flujo común):

    El código se revelará al completar todo el onboarding
    en la lambda específica de SEMÁFORO.
    """
    psid = ctx["psid"]
    username_at = ctx["username_at"]

    patch = {
        "type": {
            "SEMAFORO": {
                "onboarding": {
                    "stepIndex": 0,      # 0 = color, 1..5 = preguntas
                    "completed": False,
                },
                "color": None,          # ROJO | AMARILLO | VERDE
                "quizAnswers": {},
            }
        }
    }

    welcome_header = f"🚦 ¡Bienvenid@ a SEMÁFORO, {username_at}!\n\n"
    extra_messages = []  # none

    return (patch, welcome_header, extra_messages)
