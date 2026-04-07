"""Tests for stand_prod_game_validate/validators/l3tras.py (single code per request)."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))
sys.path.insert(0, str(ROOT / "src" / "stand_prod_game_validate"))

from validators.l3tras import validate_l3tras


def _player(pid, code, letter, game_type="L3TRAS", quiz_required=False, quiz_completed=False):
    return {
        "playerId": pid,
        "validationCode": code,
        "instagramUsername": f"user{pid}",
        "instagramPSID": f"psid{pid}",
        "validated": False,
        "type": {
            game_type: {
                "letter": letter,
                "quizRequired": quiz_required,
                "quizCompleted": quiz_completed,
            }
        },
    }


def _ctx(players, codes, game_meta=None, game_type="L3TRAS"):
    db = {p["validationCode"]: [p] for p in players}

    def to_int(c):
        try:
            return int(c)
        except Exception:
            return None

    def query(game_id, code):
        return db.get(code, [])

    def quiz_req(item, gt):
        return bool((item.get("type") or {}).get(gt, {}).get("quizRequired"))

    def quiz_done(item, gt):
        return bool((item.get("type") or {}).get(gt, {}).get("quizCompleted"))

    return {
        "gameId": "GAME01",
        "gameType": game_type,
        "codes": codes,
        "gameMeta": game_meta or {},
        "to_int_code": to_int,
        "query_players_by_code": query,
        "set_validated": None,
        "is_quiz_required": quiz_req,
        "is_quiz_completed": quiz_done,
        "inc_validated_count": None,
    }


def test_l3tras_single_code_ok():
    players = [_player(1, 1001, "S")]
    ctx = _ctx(players, ["1001"])
    r = validate_l3tras(ctx)
    assert r["valid"] is True
    assert len(r["players"]) == 1
    assert r["players"][0]["letter"] == "S"
    assert r["results"][0]["letter"] == "S"


def test_l3tras_rejects_zero_or_many_codes():
    players = [_player(1, 1001, "S")]
    assert validate_l3tras(_ctx(players, []))["reason"] == "invalid_code_count"
    assert validate_l3tras(_ctx(players, ["1001", "1002"]))["reason"] == "invalid_code_count"


def test_l3tras_unknown_code():
    players = [_player(1, 111, "A")]
    r = validate_l3tras(_ctx(players, ["999"]))
    assert r["valid"] is False
    assert r["reason"] == "no_code_match"


def test_l3tras_quiz_not_completed():
    players = [_player(1, 111, "A", quiz_required=True, quiz_completed=False)]
    r = validate_l3tras(_ctx(players, ["111"]))
    assert r["valid"] is False
    assert r["reason"] == "quiz_not_completed"
