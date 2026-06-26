import sys
from pathlib import Path

import importlib.util


_LAMBDA_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "stand_prod_game_fn_quiz"
    / "lambda_function.py"
)

sys.path.insert(0, str(_LAMBDA_PATH.parent))

spec = importlib.util.spec_from_file_location("quiz_lambda", _LAMBDA_PATH)
quiz_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quiz_mod)


def test_send_validation_code_then_raffle_with_follows():
    captured = []

    def fake_sender(msgs):
        captured.extend(msgs)

    quiz_mod._invoke_instagram_sender = fake_sender
    meta = {"raffleRequiredFollows": ["account1"]}
    quiz_mod._send_validation_code_then_raffle("psid123", 4321, meta)

    assert len(captured) == 3
    assert captured[0] == {"psid": "psid123", "text": "🎟️ Tu código es: 4321"}
    assert "sorteo" in captured[1]["text"]
    assert captured[2] == {
        "psid": "psid123",
        "template": "follow_accounts",
        "handles": ["account1"],
    }


def test_send_validation_code_then_raffle_without_follows():
    captured = []

    def fake_sender(msgs):
        captured.extend(msgs)

    quiz_mod._invoke_instagram_sender = fake_sender
    quiz_mod._send_validation_code_then_raffle("psid123", 4321, {})

    assert captured == [{"psid": "psid123", "text": "🎟️ Tu código es: 4321"}]
