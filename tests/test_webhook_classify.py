"""Tests for _looks_like_game_id and _classify_message in the Instagram webhook."""
import sys
import importlib.util
from pathlib import Path

# Load the lambda_function module by path to avoid name collisions
_LAMBDA_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "stand_prod_webhook_fn_instagram"
    / "lambda_function.py"
)

spec = importlib.util.spec_from_file_location("webhook_lambda", _LAMBDA_PATH)
webhook = importlib.util.module_from_spec(spec)
spec.loader.exec_module(webhook)

_looks_like_game_id = webhook._looks_like_game_id
_classify_message = webhook._classify_message


# ---------------------------------------------------------------------------
# _looks_like_game_id
# ---------------------------------------------------------------------------

class TestLooksLikeGameId:
    def test_six_digits(self):
        assert _looks_like_game_id("123456") is True

    def test_leading_zeros(self):
        assert _looks_like_game_id("001234") is True

    def test_too_short(self):
        assert _looks_like_game_id("12345") is False

    def test_too_long(self):
        assert _looks_like_game_id("1234567") is False

    def test_letters(self):
        assert _looks_like_game_id("12345a") is False

    def test_empty(self):
        assert _looks_like_game_id("") is False

    def test_whitespace_stripped(self):
        assert _looks_like_game_id("  123456  ") is True


# ---------------------------------------------------------------------------
# _classify_message
# ---------------------------------------------------------------------------

def _msg(text="", quick_reply=None):
    m = {"message": {"text": text, "mid": "mid123"}}
    if quick_reply is not None:
        m["message"]["quick_reply"] = {"payload": quick_reply}
    return m


class TestClassifyMessage:
    def test_game_id_message(self):
        kind, data = _classify_message(_msg("123456"))
        assert kind == "game_id"
        assert data == "123456"

    def test_game_id_uppercased(self):
        # digits only so upper() has no effect, but ensure it returns uppercased
        kind, data = _classify_message(_msg(" 000001 "))
        assert kind == "game_id"
        assert data == "000001"

    def test_other_text(self):
        kind, data = _classify_message(_msg("Hola, ¿qué tal?"))
        assert kind == "other"
        assert data is None

    def test_quiz_answer_quick_reply(self):
        kind, data = _classify_message(_msg("", quick_reply='{"questionIndex":0,"answer":"A"}'))
        assert kind == "quiz_answer"
        assert data is not None
