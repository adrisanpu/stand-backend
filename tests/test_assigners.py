"""Tests for assigner modules (pure ctx-injection, no AWS). Contract: (patch, welcome_header, extra_messages)."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))
sys.path.insert(0, str(ROOT / "src" / "stand_prod_game_fn_assign"))

from assigners.generic import assign_generic
from assigners.l3tras import assign_l3tras

# Common fields expected in type[gameType] (no extra fields per type).
COMMON_FIELDS = (
    "raffleEligible",
    "quizRequired",
    "quizCompleted",
    "quizCurrentQuestion",
    "quizAnswers",
)


def _ctx(game_type="T1MER", player_id=42, username_at="@tester"):
    return {
        "gameId": "GAME01",
        "gameType": game_type,
        "playerId": player_id,
        "username_at": username_at,
        "instagramUsername": username_at,
        "instagramPSID": "psid42",
    }


# ---------------------------------------------------------------------------
# Generic assigner – T1MER, RULET4, SEMAFORO (solo base común + welcome)
# ---------------------------------------------------------------------------

class TestAssignGenericT1mer:
    def test_patch_has_only_common_fields(self):
        ctx = _ctx("T1MER")
        patch, welcome_header, extra_messages = assign_generic(ctx)
        t = patch["type"]["T1MER"]
        for field in COMMON_FIELDS:
            assert field in t, f"Missing field: {field}"
        assert len(t) == len(COMMON_FIELDS), "Patch should have only common fields, no extras"
        assert extra_messages == []

    def test_welcome_uses_template(self):
        ctx = _ctx("T1MER", username_at="@jugador")
        _, welcome_header, _ = assign_generic(ctx)
        assert "@jugador" in welcome_header
        assert "T1mer" in welcome_header


class TestAssignGenericRulet4:
    def test_patch_has_only_common_fields(self):
        ctx = _ctx("RULET4")
        patch, welcome_header, extra_messages = assign_generic(ctx)
        t = patch["type"]["RULET4"]
        for field in COMMON_FIELDS:
            assert field in t, f"Missing field: {field}"
        assert len(t) == len(COMMON_FIELDS)
        assert extra_messages == []

    def test_welcome_uses_template(self):
        ctx = _ctx("RULET4", username_at="@ruleta")
        _, welcome_header, _ = assign_generic(ctx)
        assert "@ruleta" in welcome_header
        assert "Rulet4" in welcome_header


class TestAssignGenericSemaforo:
    def test_patch_has_only_common_fields(self):
        ctx = _ctx("SEMAFORO")
        patch, welcome_header, extra_messages = assign_generic(ctx)
        t = patch["type"]["SEMAFORO"]
        for field in COMMON_FIELDS:
            assert field in t, f"Missing field: {field}"
        assert len(t) == len(COMMON_FIELDS)
        assert extra_messages == []

    def test_welcome_uses_template(self):
        ctx = _ctx("SEMAFORO", username_at="@semaforo")
        _, welcome_header, _ = assign_generic(ctx)
        assert "@semaforo" in welcome_header
        assert "SEMÁFORO" in welcome_header


# ---------------------------------------------------------------------------
# Generic assigner – tipo no configurado (fallback con player_id)
# ---------------------------------------------------------------------------

class TestAssignGenericUnknownType:
    def test_type_key_matches_game_type(self):
        ctx = _ctx("CUPIDO")
        ctx["gameType"] = "CUPIDO"
        patch, welcome_header, extra_messages = assign_generic(ctx)
        assert "CUPIDO" in patch["type"]
        for field in COMMON_FIELDS:
            assert field in patch["type"]["CUPIDO"], f"Missing field: {field}"

    def test_welcome_default_uses_player_id(self):
        ctx = _ctx("CUPIDO")
        ctx["gameType"] = "CUPIDO"
        ctx["playerId"] = 99
        _, welcome_header, _ = assign_generic(ctx)
        assert "99" in welcome_header
        assert "jugador" in welcome_header.lower() or "unido" in welcome_header.lower()

    def test_raffle_eligible_true(self):
        ctx = _ctx("CUPIDO")
        ctx["gameType"] = "CUPIDO"
        patch, _, _ = assign_generic(ctx)
        assert patch["type"]["CUPIDO"]["raffleEligible"] is True


# ---------------------------------------------------------------------------
# L3TRAS assigner
# ---------------------------------------------------------------------------


def _l3tras_meta(objective_word="STAND", normalize="UPPER"):
    return {"type": {"L3TRAS": {"objectiveWord": objective_word, "normalize": normalize}}}


class TestAssignL3tras:
    def test_letter_by_player_index(self):
        ctx = _ctx("L3TRAS", player_id=1)
        ctx["gameType"] = "L3TRAS"
        ctx["gameMeta"] = _l3tras_meta("AB")
        patch, welcome, _ = assign_l3tras(ctx)
        assert patch["type"]["L3TRAS"]["letter"] == "A"
        assert patch["type"]["L3TRAS"]["raffleEligible"] is True
        assert "L3tras" in welcome

    def test_letter_wraps(self):
        ctx = _ctx("L3TRAS", player_id=3)
        ctx["gameType"] = "L3TRAS"
        ctx["gameMeta"] = _l3tras_meta("AB")
        patch, _, _ = assign_l3tras(ctx)
        assert patch["type"]["L3TRAS"]["letter"] == "A"

    def test_default_word_when_missing_meta(self):
        ctx = _ctx("L3TRAS", player_id=1)
        ctx["gameType"] = "L3TRAS"
        ctx["gameMeta"] = {}
        patch, _, _ = assign_l3tras(ctx)
        assert patch["type"]["L3TRAS"]["letter"] == "S"
