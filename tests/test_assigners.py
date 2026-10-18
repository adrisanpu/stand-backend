"""Tests for assigner modules (pure context injection, no AWS)."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))
sys.path.insert(0, str(ROOT / "src" / "stand_prod_game_fn_assign"))

from assigners.generic import assign_generic
from assigners import empareja2
from assigners.l3tras import assign_l3tras

# Common fields expected in type[gameType] (no extra fields per type).
COMMON_FIELDS = (
    "raffleEligible",
    "quizRequired",
    "quizCompleted",
    "quizCurrentQuestion",
    "quizAnswers",
)


def _ctx(game_type="T1MER", player_id=42):
    return {
        "gameId": "GAME01",
        "gameType": game_type,
        "playerId": player_id,
    }


# ---------------------------------------------------------------------------
# Generic assigner – T1MER, RULET4, SEMAFORO
# ---------------------------------------------------------------------------

class TestAssignGenericT1mer:
    def test_patch_has_only_common_fields(self):
        ctx = _ctx("T1MER")
        patch = assign_generic(ctx)
        t = patch["type"]["T1MER"]
        for field in COMMON_FIELDS:
            assert field in t, f"Missing field: {field}"
        assert len(t) == len(COMMON_FIELDS), "Patch should have only common fields, no extras"


class TestAssignGenericRulet4:
    def test_patch_has_only_common_fields(self):
        ctx = _ctx("RULET4")
        patch = assign_generic(ctx)
        t = patch["type"]["RULET4"]
        for field in COMMON_FIELDS:
            assert field in t, f"Missing field: {field}"
        assert len(t) == len(COMMON_FIELDS)


class TestAssignGenericSemaforo:
    def test_patch_has_only_common_fields(self):
        ctx = _ctx("SEMAFORO")
        patch = assign_generic(ctx)
        t = patch["type"]["SEMAFORO"]
        for field in COMMON_FIELDS:
            assert field in t, f"Missing field: {field}"
        assert len(t) == len(COMMON_FIELDS)


# ---------------------------------------------------------------------------
# Generic assigner – tipo no configurado
# ---------------------------------------------------------------------------

class TestAssignGenericUnknownType:
    def test_type_key_matches_game_type(self):
        ctx = _ctx("CUPIDO")
        ctx["gameType"] = "CUPIDO"
        patch = assign_generic(ctx)
        assert "CUPIDO" in patch["type"]
        for field in COMMON_FIELDS:
            assert field in patch["type"]["CUPIDO"], f"Missing field: {field}"

    def test_raffle_eligible_true(self):
        ctx = _ctx("CUPIDO")
        ctx["gameType"] = "CUPIDO"
        patch = assign_generic(ctx)
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
        patch = assign_l3tras(ctx)
        assert patch["type"]["L3TRAS"]["letter"] == "A"
        assert patch["type"]["L3TRAS"]["raffleEligible"] is True

    def test_letter_wraps(self):
        ctx = _ctx("L3TRAS", player_id=3)
        ctx["gameType"] = "L3TRAS"
        ctx["gameMeta"] = _l3tras_meta("AB")
        patch = assign_l3tras(ctx)
        assert patch["type"]["L3TRAS"]["letter"] == "A"

    def test_default_word_when_missing_meta(self):
        ctx = _ctx("L3TRAS", player_id=1)
        ctx["gameType"] = "L3TRAS"
        ctx["gameMeta"] = {}
        patch = assign_l3tras(ctx)
        assert patch["type"]["L3TRAS"]["letter"] == "S"


def test_empareja2_assigner_returns_player_patch_without_dm_payloads(monkeypatch):
    catalog = [
        {"characterId": 1, "characterName": "Alice", "pairGroupId": "1"},
        {"characterId": 2, "characterName": "Bob", "pairGroupId": "1"},
    ]
    monkeypatch.setattr(empareja2, "_query_empareja2_catalog", lambda _dynamo: catalog)
    monkeypatch.setattr(empareja2.random, "choice", lambda items: items[0])

    patch = empareja2.assign_empareja2({"dynamo_r": object(), "playerId": 1})

    assert patch["type"]["EMPAREJA2"]["characterName"] == "Alice"
    assert patch["type"]["EMPAREJA2"]["partnerCharacterName"] == "Bob"
    assert set(patch) == {"type"}
