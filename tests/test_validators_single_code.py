"""Tests for stand_prod_game_validate/validators/generic.py (single-code: T1MER, RULET4)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "stand_prod_game_validate"))

from validators.generic import validate_generic


# ---------------------------------------------------------------------------
# Helpers to build a minimal ctx
# ---------------------------------------------------------------------------

def _player(pid, code, game_type="T1MER", validated=False, quiz_required=False, quiz_completed=False):
    return {
        "playerId": pid,
        "validationCode": code,
        "instagramUsername": f"user{pid}",
        "instagramPSID": f"psid{pid}",
        "validated": validated,
        "type": {
            game_type: {
                "quizRequired": quiz_required,
                "quizCompleted": quiz_completed,
            }
        },
    }


def _ctx(players, codes, game_type="T1MER"):
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

    def quiz_req(item, gt):
        return bool((item.get("type") or {}).get(gt, {}).get("quizRequired"))

    def quiz_done(item, gt):
        return bool((item.get("type") or {}).get(gt, {}).get("quizCompleted"))

    return {
        "gameId": "GAME01",
        "gameType": game_type,
        "codes": codes,
        "to_int_code": to_int,
        "query_players_by_code": query,
        "set_validated": set_val,
        "is_quiz_required": quiz_req,
        "is_quiz_completed": quiz_done,
        "inc_validated_count": None,
    }


# ---------------------------------------------------------------------------
# T1MER tests
# ---------------------------------------------------------------------------

def test_t1mer_happy_path():
    p = _player(1, 1234, game_type="T1MER")
    ctx = _ctx([p], ["1234"], game_type="T1MER")
    result = validate_generic(ctx)
    assert result["valid"] is True
    assert result["playerId"] == 1
    assert result["username"] == "user1"
    assert "T1mer" in result["message"]


def test_t1mer_invalid_code_format():
    ctx = _ctx([], ["abc"], game_type="T1MER")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "invalid_codes"


def test_t1mer_no_match():
    ctx = _ctx([], ["9999"], game_type="T1MER")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "no_code_match"


def test_t1mer_already_validated():
    p = _player(1, 1234, game_type="T1MER", validated=True)
    ctx = _ctx([p], ["1234"], game_type="T1MER")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "already_validated"


def test_t1mer_quiz_not_completed():
    p = _player(1, 1234, game_type="T1MER", quiz_required=True, quiz_completed=False)
    ctx = _ctx([p], ["1234"], game_type="T1MER")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "quiz_not_completed"


def test_t1mer_quiz_completed_allows_validation():
    p = _player(1, 1234, game_type="T1MER", quiz_required=True, quiz_completed=True)
    ctx = _ctx([p], ["1234"], game_type="T1MER")
    result = validate_generic(ctx)
    assert result["valid"] is True


def test_t1mer_double_validation_blocked():
    p = _player(1, 1234, game_type="T1MER")
    ctx = _ctx([p], ["1234"], game_type="T1MER")
    validate_generic(ctx)
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "already_validated"


def test_t1mer_inc_validated_count_called():
    calls = []
    p = _player(1, 1234, game_type="T1MER")
    ctx = _ctx([p], ["1234"], game_type="T1MER")
    ctx["inc_validated_count"] = lambda gid, n: calls.append(n)
    validate_generic(ctx)
    assert calls == [1]


# ---------------------------------------------------------------------------
# RULET4 tests
# ---------------------------------------------------------------------------

def test_rulet4_happy_path():
    p = _player(1, 5678, game_type="RULET4")
    ctx = _ctx([p], ["5678"], game_type="RULET4")
    result = validate_generic(ctx)
    assert result["valid"] is True
    assert result["playerId"] == 1
    assert result["username"] == "user1"
    assert "Rulet4" in result["message"]


def test_rulet4_no_match():
    ctx = _ctx([], ["9999"], game_type="RULET4")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "no_code_match"


def test_rulet4_already_validated():
    p = _player(1, 5678, game_type="RULET4", validated=True)
    ctx = _ctx([p], ["5678"], game_type="RULET4")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "already_validated"


# ---------------------------------------------------------------------------
# Unknown game type
# ---------------------------------------------------------------------------

def test_unknown_game_returns_invalid():
    ctx = _ctx([], ["1234"], game_type="UNKNOWN")
    result = validate_generic(ctx)
    assert result["valid"] is False
    assert result["reason"] == "unknown_game"
    assert "desconocido" in result["message"].lower() or "unknown" in result["message"].lower()
