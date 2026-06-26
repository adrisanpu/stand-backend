"""Tests for winner-only DM notifications in stand_prod_game_fn_raffle."""
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


def test_build_winner_message_personalized():
    msg = raffle._build_winner_message("T1MER", "@winner")
    assert "T1MER" in msg
    assert "@winner" in msg
    assert "Has ganado el sorteo" in msg
    assert "Personas ganadoras" not in msg


def test_raffle_notifies_only_winners(monkeypatch):
    captured = []

    monkeypatch.setattr(
        raffle,
        "_get_game",
        lambda _gid: {
            "gameId": "G1",
            "gameType": "T1MER",
            "isActive": True,
        },
    )

    all_players = [
        {
            "playerId": 1,
            "instagramPSID": "psid1",
            "instagramUsername": "@p1",
            "type": {"T1MER": {"raffleEligible": True}},
        },
        {
            "playerId": 2,
            "instagramPSID": "psid2",
            "instagramUsername": "@p2",
            "type": {"T1MER": {"raffleEligible": True}},
        },
        {
            "playerId": 3,
            "instagramPSID": "psid3",
            "instagramUsername": "@p3",
            "type": {"T1MER": {"raffleEligible": True}},
        },
    ]
    monkeypatch.setattr(raffle, "_query_all_players", lambda _gid: all_players)
    monkeypatch.setattr(raffle, "_is_user_following_business", lambda _psid: True)
    monkeypatch.setattr(raffle, "_save_raffle_winners_to_game", lambda *a, **k: None)
    monkeypatch.setattr(raffle, "_mark_winner", lambda *a, **k: None)
    monkeypatch.setattr(raffle.random, "randrange", lambda _a, _b: 0)

    def fake_send(messages):
        captured.extend(messages)

    monkeypatch.setattr(raffle, "_send_bulk_dms", fake_send)

    event = {
        "gameId": "G1",
        "numberOfWinners": 1,
        "applicableOnlyValidated": False,
    }
    resp = raffle.lambda_handler(event, None)
    body = json.loads(resp["body"])

    assert body["ok"] is True
    assert body["notifiedPlayers"] == 1
    assert len(captured) == 1
    assert captured[0]["psid"] == "psid1"
    assert all(m["psid"] != "psid2" for m in captured)
    assert all(m["psid"] != "psid3" for m in captured)
