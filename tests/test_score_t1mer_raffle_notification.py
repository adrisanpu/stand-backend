import sys
from pathlib import Path

import pytest


_LAMBDA_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "stand_prod_game_score"
    / "lambda_function.py"
)

sys.path.insert(0, str(_LAMBDA_PATH.parent))


import importlib.util  # noqa: E402


spec = importlib.util.spec_from_file_location("score_lambda", _LAMBDA_PATH)
score_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(score_mod)


def test_send_raffle_follow_after_score_no_psid(monkeypatch):
    calls = []

    class FakeTable:
        def get_item(self, Key):
            return {"Item": {"playerId": Key["playerId"], "instagramPSID": ""}}

    monkeypatch.setattr(score_mod, "gp_table", FakeTable())

    def fake_send_bulk(msgs):
        calls.append(msgs)

    monkeypatch.setattr(score_mod, "_score_send_bulk", fake_send_bulk)

    meta = {"raffleRequiredFollows": ["account1"]}

    # Should not raise and not send messages if there is no PSID
    score_mod._send_raffle_follow_after_score("GAME1", 1, meta)
    assert calls == []


def test_send_raffle_follow_after_score_with_psid(monkeypatch):
    captured = []

    class FakeTable:
        def get_item(self, Key):
            return {
                "Item": {
                    "playerId": Key["playerId"],
                    "instagramPSID": "psid123",
                }
            }

    monkeypatch.setattr(score_mod, "gp_table", FakeTable())

    def fake_send_bulk(msgs):
        captured.extend(msgs)

    monkeypatch.setattr(score_mod, "_score_send_bulk", fake_send_bulk)

    meta = {"raffleRequiredFollows": ["account1"]}

    score_mod._send_raffle_follow_after_score("GAME1", 1, meta)

    # We expect two messages: intro text + follow_accounts template
    assert len(captured) == 2
    assert captured[0]["psid"] == "psid123"
    assert "sigue estas cuentas" in captured[0]["text"]
    assert captured[1]["psid"] == "psid123"
    assert captured[1]["template"] == "follow_accounts"
    assert "account1" in captured[1]["handles"]

