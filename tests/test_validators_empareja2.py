"""Tests for stand_prod_game_validate/validators/empareja2.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "stand_prod_game_validate"))

from validators.empareja2 import validate_empareja2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _player(pid, code, pair_id, char_id, char_name="CharA", validated=False):
    return {
        "playerId": pid,
        "validationCode": code,
        "instagramUsername": f"user{pid}",
        "instagramPSID": f"psid{pid}",
        "validated": validated,
        "type": {
            "EMPAREJA2": {
                "pairId": pair_id,
                "characterId": char_id,
                "characterName": char_name,
            }
        },
    }


def _ctx(players, codes):
    db = {p["validationCode"]: [p] for p in players}

    def to_int(c):
        try:
            return int(c)
        except Exception:
            return None

    def query(game_id, code):
        return db.get(code, [])

    validated_set = set()

    def set_val(game_id, pid):
        if pid in validated_set:
            return False
        validated_set.add(pid)
        return True

    sent = []
    quizzed = []

    return {
        "gameId": "GAME01",
        "codes": codes,
        "to_int_code": to_int,
        "query_players_by_code": query,
        "set_validated": set_val,
        "send_bulk": lambda msgs: sent.extend(msgs),
        "invoke_quiz": lambda gid, psids: quizzed.extend(psids),
        "presign_character_png": lambda name: f"https://cdn.example.com/{name}.png",
        "inc_validated_count": None,
        "log": lambda msg, data=None: None,
        "_sent": sent,
        "_quizzed": quizzed,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_wrong_code_count():
    ctx = _ctx([], ["1111"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "invalid_code_count"


def test_same_code():
    p = _player(1, 1111, "P1", "C1")
    ctx = _ctx([p], ["1111", "1111"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "same_code"


def test_code_not_found():
    p = _player(1, 1111, "P1", "C1")
    ctx = _ctx([p], ["1111", "9999"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "code_not_found"


def test_already_validated():
    p1 = _player(1, 1111, "P1", "C1", validated=True)
    p2 = _player(2, 2222, "P1", "C2")
    ctx = _ctx([p1, p2], ["1111", "2222"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "already_validated"


def test_different_pair():
    p1 = _player(1, 1111, "P1", "C1")
    p2 = _player(2, 2222, "P2", "C2")
    ctx = _ctx([p1, p2], ["1111", "2222"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "different_pair"


def test_same_character():
    p1 = _player(1, 1111, "P1", "C1")
    p2 = _player(2, 2222, "P1", "C1")  # same characterId
    ctx = _ctx([p1, p2], ["1111", "2222"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "same_character"


def test_happy_path():
    p1 = _player(1, 1111, "P1", "C1", char_name="Romeo")
    p2 = _player(2, 2222, "P1", "C2", char_name="Juliet")
    ctx = _ctx([p1, p2], ["1111", "2222"])
    result = validate_empareja2(ctx)
    assert result["valid"] is True
    assert result["pairId"] == "P1"
    pids = {pl["playerId"] for pl in result["players"]}
    assert pids == {1, 2}
    # send_bulk and invoke_quiz should have been called
    assert len(ctx["_sent"]) == 2
    assert len(ctx["_quizzed"]) == 2


def test_presign_url_in_response():
    p1 = _player(1, 1111, "P1", "C1", char_name="Romeo")
    p2 = _player(2, 2222, "P1", "C2", char_name="Juliet")
    ctx = _ctx([p1, p2], ["1111", "2222"])
    result = validate_empareja2(ctx)
    urls = [pl["characterImageUrl"] for pl in result["players"]]
    assert all(url and url.startswith("https://") for url in urls)


def test_missing_pair_data():
    p1 = _player(1, 1111, "", "C1")   # empty pairId
    p2 = _player(2, 2222, "P1", "C2")
    ctx = _ctx([p1, p2], ["1111", "2222"])
    result = validate_empareja2(ctx)
    assert result["valid"] is False
    assert result["reason"] == "missing_pair_data"
