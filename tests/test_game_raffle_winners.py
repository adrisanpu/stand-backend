"""Operator raffle winner list exposes Web identity and legacy display names without IG ids."""
import importlib.util
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).parent.parent
spec = importlib.util.spec_from_file_location(
    "game_raffle_winners_lambda", ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py",
)
game = importlib.util.module_from_spec(spec)
with patch("boto3.resource"), patch("boto3.client"):
    spec.loader.exec_module(game)


def test_raffle_winners_keep_web_and_legacy_names_without_psid():
    item = {"raffleWinners": [
        {"playerId": 1, "username": "Ana", "wonAt": "t1", "validationCode": 1234},
        {"playerId": 2, "instagramUsername": "legacy_user", "instagramPSID": "psid-2", "wonAt": "t0"},
        {"playerId": 3, "wonAt": "t2"},
        "corrupt",
    ]}

    assert game._raffle_winners_from_game(item) == [
        {"playerId": 1, "username": "Ana", "wonAt": "t1", "validationCode": 1234},
        {"playerId": 2, "username": "legacy_user", "wonAt": "t0"},
        {"playerId": 3, "username": None, "wonAt": "t2"},
    ]


def test_raffle_winners_ignore_non_list_values():
    assert game._raffle_winners_from_game({"raffleWinners": {"playerId": 1}}) == []
