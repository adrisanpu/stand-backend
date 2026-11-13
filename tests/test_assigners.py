"""Tests for assigner modules (pure context injection, no AWS)."""
import sys
from decimal import Decimal
from pathlib import Path

import pytest

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


def test_empareja2_assignment_retains_both_image_keys(monkeypatch):
    catalog = [
        {"characterId": 1, "characterName": "Alice", "pairGroupId": "1", "imageKey": "custom/a.png"},
        {"characterId": 2, "characterName": "Bob", "pairGroupId": "1", "imageKey": "custom/b.png"},
    ]
    monkeypatch.setattr(empareja2, "_query_empareja2_catalog", lambda _: catalog)
    monkeypatch.setattr(empareja2.random, "choice", lambda items: items[0])
    blob = empareja2.assign_empareja2({"dynamo_r": object()})["type"]["EMPAREJA2"]
    assert blob["characterImageKey"] == "custom/a.png"
    assert blob["partnerCharacterImageKey"] == "custom/b.png"


@pytest.mark.parametrize(
    "limit,expected",
    [(Decimal("1"), {"1"}), (Decimal("2"), {"1", "2"}), (3, {"1", "2", "10"}),
     (None, {"1", "2", "10"})],
)
def test_empareja2_assignment_pool_respects_saved_limit(monkeypatch, limit, expected):
    catalog = [
        {"characterId": index * 2 + offset, "characterName": str(index * 2 + offset), "pairGroupId": group}
        for index, group in enumerate(["10", "2", "1"])
        for offset in (1, 2)
    ]
    monkeypatch.setattr(empareja2, "_query_empareja2_catalog", lambda _: catalog)
    candidates = []
    def choose(items):
        candidates.extend(items)
        return items[-1]
    monkeypatch.setattr(empareja2.random, "choice", choose)
    settings = {} if limit is None else {"numPairs": limit}

    patch = empareja2.assign_empareja2({
        "dynamo_r": object(), "gameMeta": {"type": {"EMPAREJA2": settings}},
    })

    assert {item["pairGroupId"] for item in candidates} == expected
    assert len(candidates) == 2 * len(expected)
    assert patch["type"]["EMPAREJA2"]["pairGroupId"] in expected


@pytest.mark.parametrize("limit", [0, -1, True, "2", Decimal("1.5"), Decimal("NaN"), 4])
def test_empareja2_rejects_invalid_or_unavailable_limit(monkeypatch, limit):
    catalog = [
        {"characterId": index, "pairGroupId": str(index)}
        for index in range(1, 4)
    ]
    monkeypatch.setattr(empareja2, "_query_empareja2_catalog", lambda _: catalog)
    with pytest.raises(RuntimeError, match="EMPAREJA2 numPairs"):
        empareja2.assign_empareja2({
            "dynamo_r": object(), "gameMeta": {"type": {"EMPAREJA2": {"numPairs": limit}}},
        })


def test_empareja2_sorts_mixed_numeric_and_text_group_ids(monkeypatch):
    catalog = [
        {"characterId": index, "pairGroupId": group}
        for index, group in enumerate(["A", "10", "2"], 1)
    ]
    monkeypatch.setattr(empareja2, "_query_empareja2_catalog", lambda _: catalog)
    monkeypatch.setattr(empareja2.random, "choice", lambda items: items[0])

    patch = empareja2.assign_empareja2({
        "dynamo_r": object(), "gameMeta": {"type": {"EMPAREJA2": {"numPairs": Decimal(1)}}},
    })

    assert patch["type"]["EMPAREJA2"]["pairGroupId"] == "2"


def test_contributed_groups_follow_legacy_groups(monkeypatch):
    catalog = [
        {"characterId": index, "pairGroupId": group}
        for index, group in enumerate(["CONTRIB#0002", "Z", "CONTRIB#0001", "2"], 1)
    ]
    monkeypatch.setattr(empareja2, "_query_empareja2_catalog", lambda _: catalog)
    seen = []
    def choose(items):
        seen.extend(items)
        return items[0]
    monkeypatch.setattr(empareja2.random, "choice", choose)
    empareja2.assign_empareja2({
        "dynamo_r": object(), "gameMeta": {"type": {"EMPAREJA2": {"numPairs": Decimal(3)}}},
    })
    assert {item["pairGroupId"] for item in seen} == {"2", "Z", "CONTRIB#0001"}


def test_empareja2_catalog_reads_updated_data_on_each_assignment():
    from unittest.mock import MagicMock

    table = MagicMock()
    table.query.side_effect = [
        {"Items": [{"characterId": 1, "pairGroupId": "1"}]},
        {"Items": [{"characterId": 1, "pairGroupId": "1"}, {"characterId": 2, "pairGroupId": "2"}]},
    ]
    dynamo = MagicMock()
    dynamo.Table.return_value = table

    assert len(empareja2._query_empareja2_catalog(dynamo)) == 1
    assert len(empareja2._query_empareja2_catalog(dynamo)) == 2
    assert all(call.kwargs["ConsistentRead"] is True for call in table.query.call_args_list)
