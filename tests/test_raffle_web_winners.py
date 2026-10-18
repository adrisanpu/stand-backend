"""Tests for Web-only raffle winner selection in stand_prod_game_fn_raffle."""
import importlib.util
import json
from pathlib import Path

_LAMBDA_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "stand_prod_game_fn_raffle"
    / "lambda_function.py"
)

spec = importlib.util.spec_from_file_location("raffle_lambda", _LAMBDA_PATH)
raffle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(raffle)


def _run(monkeypatch, players, only_validated=False):
    saved = []
    marked = []
    monkeypatch.setattr(
        raffle, "_get_game", lambda _gid: {"gameId": "G1", "gameType": "T1MER", "isActive": True},
    )
    monkeypatch.setattr(raffle, "_query_all_players", lambda _gid: players)
    monkeypatch.setattr(raffle, "_save_raffle_winners_to_game", lambda *a, **k: saved.append(a))
    monkeypatch.setattr(raffle, "_mark_winner", lambda *a, **k: marked.append(a))
    resp = raffle.lambda_handler(
        {"gameId": "G1", "numberOfWinners": 1, "applicableOnlyValidated": only_validated}, None,
    )
    return json.loads(resp["body"]), saved, marked


def test_web_players_without_instagram_can_win(monkeypatch):
    monkeypatch.setattr(raffle.random, "randrange", lambda _a, _b: 0)
    players = [{"playerId": 1, "username": "Ana", "type": {"T1MER": {"raffleEligible": True}}}]

    body, saved, marked = _run(monkeypatch, players)

    assert body["ok"] is True
    assert body["winners"] == ["Ana"]
    assert "notifiedPlayers" not in body
    assert "raffleRequiredFollows" not in body
    assert saved[0][1][0]["username"] == "Ana"
    assert marked[0][1] == 1


def test_winner_ignores_legacy_instagram_identity(monkeypatch):
    monkeypatch.setattr(raffle.random, "randrange", lambda _a, _b: 0)
    players = [{"playerId": 4, "instagramUsername": "@legacy", "instagramPSID": "psid4"}]

    body, _, _ = _run(monkeypatch, players)

    assert body["winners"] == ["Jugador 4"]


def test_ineligible_players_are_skipped(monkeypatch):
    players = [
        {"playerId": 1, "username": "Ana", "type": {"T1MER": {"raffleEligible": False}}},
        {"playerId": 2, "username": "Bea", "validated": False},
    ]

    body, saved, _ = _run(monkeypatch, players, only_validated=True)

    assert body["winners"] == []
    assert saved == []


def test_save_winners_persists_web_identity_only(monkeypatch):
    calls = []

    class FakeTable:
        def get_item(self, Key):
            return {"Item": {"raffleWinners": []}}

        def update_item(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(raffle, "games_table", FakeTable())
    raffle._save_raffle_winners_to_game(
        "G1", [{"playerId": 3, "username": "Eva", "validationCode": 1234}], False, "2026-01-01T00:00:00Z",
    )

    row = calls[0]["ExpressionAttributeValues"][":w"][0]
    assert row == {"playerId": 3, "username": "Eva", "wonAt": "2026-01-01T00:00:00Z", "validationCode": 1234}
