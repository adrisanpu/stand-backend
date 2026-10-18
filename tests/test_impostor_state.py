"""Impostor state exposes Web participant identity only."""
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

_LAMBDA_PATH = Path(__file__).parent.parent / "src" / "stand_prod_game_fn_impostor" / "lambda_function.py"
spec = importlib.util.spec_from_file_location("impostor_lambda", _LAMBDA_PATH)
impostor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(impostor)


def test_state_uses_web_username_and_participant_photo(monkeypatch):
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "https://signed/photo"
    monkeypatch.setattr(impostor, "s3", s3)
    monkeypatch.setattr(impostor, "USER_MEDIA_BUCKET", "media")
    monkeypatch.setattr(impostor, "_get_game", lambda _gid: {"gameId": "G1", "type": {"IMPOSTOR": {}}})
    monkeypatch.setattr(impostor, "_query_players", lambda _gid: [
        {"playerId": 1, "validated": True, "username": "Ana", "participantPhotoKey": "players/G1/1.png"},
        {"playerId": 2, "validated": True, "instagramUsername": "@legacy",
         "instagramProfilePicUrl": "https://legacy/p.png"},
        {"playerId": 3, "validated": False, "username": "Pending"},
    ])

    players = impostor._get_state_payload("G1")["players"]

    assert players == [
        {"playerId": 1, "username": "Ana", "profilePicUrl": "https://signed/photo"},
        {"playerId": 2, "username": None, "profilePicUrl": None},
    ]
