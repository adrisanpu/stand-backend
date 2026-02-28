"""Tests for assigner modules (pure ctx-injection, no AWS). Contract: (patch, welcome_header, extra_messages)."""
import sys
from pathlib import Path

ASSIGNER_SRC = Path(__file__).parent.parent / "src" / "stand_prod_game_fn_assign"
sys.path.insert(0, str(ASSIGNER_SRC))

from assigners.generic import assign_generic

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
        ctx = _ctx("L3TRAS")
        ctx["gameType"] = "L3TRAS"
        ctx["playerId"] = 99
        _, welcome_header, _ = assign_generic(ctx)
        assert "99" in welcome_header
        assert "jugador" in welcome_header.lower() or "unido" in welcome_header.lower()

    def test_raffle_eligible_true(self):
        ctx = _ctx("L3TRAS")
        ctx["gameType"] = "L3TRAS"
        patch, _, _ = assign_generic(ctx)
        assert patch["type"]["L3TRAS"]["raffleEligible"] is True
