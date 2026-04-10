"""Tests for join code validation and _classify_message in the Instagram webhook."""
import sys
import importlib.util
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))

from stand_common.utils import is_valid_join_code  # noqa: E402

_LAMBDA_PATH = ROOT / "src" / "stand_prod_webhook_fn_instagram" / "lambda_function.py"

spec = importlib.util.spec_from_file_location("webhook_lambda", _LAMBDA_PATH)
webhook = importlib.util.module_from_spec(spec)
spec.loader.exec_module(webhook)

_classify_message = webhook._classify_message


class TestIsValidJoinCode:
    def test_valid(self):
        assert is_valid_join_code("K9R4QM") is True
        assert is_valid_join_code("012345") is True

    def test_wrong_length(self):
        assert is_valid_join_code("12345") is False
        assert is_valid_join_code("1234567") is False

    def test_invalid_chars(self):
        assert is_valid_join_code("I23456") is False  # I excluded from alphabet
        assert is_valid_join_code("12345 ") is False

    def test_empty(self):
        assert is_valid_join_code("") is False


def _msg(text="", quick_reply=None):
    m = {"message": {"text": text, "mid": "mid123"}}
    if quick_reply is not None:
        m["message"]["quick_reply"] = {"payload": quick_reply}
    return m


class TestClassifyMessage:
    def test_join_code_text_is_other_here(self):
        # Plain join code is handled in _handle_and_dispatch, not _classify_message
        kind, data = _classify_message(_msg("K9R4QM"))
        assert kind == "other"
        assert data is None

    def test_other_text(self):
        kind, data = _classify_message(_msg("Hola, ¿qué tal?"))
        assert kind == "other"
        assert data is None

    def test_quiz_answer_quick_reply(self):
        kind, data = _classify_message(_msg("", quick_reply='{"questionIndex":0,"answer":"A"}'))
        assert kind == "quiz_answer"
        assert data is not None

    def test_play_payload_other(self):
        kind, data = _classify_message(_msg("", quick_reply="PLAY"))
        assert kind == "other"
        assert data is None
