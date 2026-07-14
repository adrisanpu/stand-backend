"""Tests for participant tokens and public web join helpers."""
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))

from stand_common.utils import (  # noqa: E402
    issue_player_token,
    verify_player_token,
    is_valid_email,
    normalize_email,
    mask_email,
    default_participant_fields,
)

ASSIGN_DIR = ROOT / "src" / "stand_prod_game_fn_assign"
sys.path.insert(0, str(ASSIGN_DIR))
ASSIGN_PATH = ASSIGN_DIR / "lambda_function.py"
_spec = importlib.util.spec_from_file_location("assign_lambda", ASSIGN_PATH)
assign_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(assign_mod)

SECRET = "test-secret"


def test_player_token_roundtrip():
    token = issue_player_token("ABC123", 7, SECRET, ttl_seconds=3600)
    claims = verify_player_token(token, SECRET)
    assert claims == {"gameId": "ABC123", "playerId": 7}


def test_player_token_rejects_bad_sig():
    token = issue_player_token("ABC123", 1, SECRET)
    bad = token[:-1] + ("a" if token[-1] != "a" else "b")
    assert verify_player_token(bad, SECRET) is None


def test_email_helpers():
    assert is_valid_email("ana@example.com")
    assert not is_valid_email("not-an-email")
    assert normalize_email(" Ana@Example.COM ") == "ana@example.com"
    assert mask_email("ana@example.com").startswith("a")


def test_default_participant_fields_empareja2():
    pf = default_participant_fields("EMPAREJA2")
    assert pf["email"]["required"] is True
    assert pf["instagram"]["required"] is False


def test_web_assign_requires_email():
    out = assign_mod._handle_web_assign({"source": "web", "game_id": "ABC123", "email": ""})
    assert out["ok"] is False
    assert out["reason"] == "missing_email"


@patch.object(assign_mod, "_get_game_meta", return_value=None)
def test_web_assign_game_not_found(mock_meta):
    out = assign_mod._handle_web_assign({"source": "web", "game_id": "ABC123", "email": "a@b.com"})
    assert out["reason"] == "game_not_found"


@patch.object(assign_mod, "_find_existing_player_by_email", return_value=None)
@patch.object(assign_mod, "_empareja2_web_assignment", return_value={"characterName": "Alice", "validationCode": 1234, "validated": False})
@patch.object(assign_mod, "_put_player")
@patch.object(assign_mod, "_reserve_player_slot", return_value=True)
@patch.object(assign_mod, "_ensure_game_counters")
@patch.object(assign_mod, "_get_last_player_id", return_value=0)
@patch.object(assign_mod, "ASSIGNERS")
@patch.object(assign_mod, "_get_game_meta")
def test_web_assign_creates_player(mock_meta, mock_assigners, *_mocks):
    mock_meta.return_value = {
        "gameId": "ABC123",
        "gameType": "EMPAREJA2",
        "isActive": True,
        "maxPlayers": 25,
        "participantFields": default_participant_fields("EMPAREJA2"),
    }
    mock_assigner = MagicMock(
        return_value=(
            {"type": {"EMPAREJA2": {"characterName": "Alice", "partnerCharacterName": "Bob", "pairGroupId": "1", "characterId": 1}}},
            "",
            [],
        )
    )
    mock_assigners.get.return_value = mock_assigner

    out = assign_mod._handle_web_assign({
        "source": "web",
        "game_id": "ABC123",
        "email": "player@example.com",
    })
    assert out["ok"] is True
    assert out["created_new"] is True
    assert out["playerToken"]
    assert out["assignment"]["characterName"] == "Alice"


@patch.object(assign_mod, "_empareja2_web_assignment", return_value={"characterName": "X", "validationCode": 1234, "validated": False})
@patch.object(assign_mod, "_find_existing_player_by_email")
@patch.object(assign_mod, "_get_game_meta")
def test_web_assign_dedup_email(mock_meta, mock_find, _mock_assignment):
    mock_meta.return_value = {
        "gameId": "ABC123",
        "gameType": "EMPAREJA2",
        "isActive": True,
        "participantFields": default_participant_fields("EMPAREJA2"),
    }
    mock_find.return_value = {
        "playerId": 3,
        "validationCode": 1234,
        "validated": False,
        "type": {"EMPAREJA2": {"characterName": "X", "partnerCharacterName": "Y", "characterId": 1}},
    }

    out = assign_mod._handle_web_assign({
            "source": "web",
            "game_id": "ABC123",
            "email": "player@example.com",
        })
    assert out["ok"] is True
    assert out["already_joined"] is True