"""T1MER store_score one-attempt guard."""
import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).parent.parent
SCORE_DIR = ROOT / "src" / "stand_prod_game_score"
sys.path.insert(0, str(SCORE_DIR))

_spec = importlib.util.spec_from_file_location("t1mer_score", SCORE_DIR / "games" / "t1mer.py")
t1mer = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(t1mer)


def test_store_score_rejects_already_scored():
    gp = MagicMock()
    gp.get_item.return_value = {
        "Item": {
            "gameId": "ABCDEF",
            "playerId": 1,
            "instagramUsername": "@ana",
            "type": {"T1MER": {"timer": Decimal("10.1"), "score": Decimal("100")}},
        }
    }
    out = t1mer.store_score("ABCDEF", "T1MER", gp, {"playerId": 1, "timer": "9.9", "score": 50}, print)
    assert out["ok"] is False
    assert out["error"] == "already_scored"
    gp.update_item.assert_not_called()


def test_store_score_writes_when_no_prior_score():
    gp = MagicMock()
    gp.get_item.return_value = {
        "Item": {
            "gameId": "ABCDEF",
            "playerId": 1,
            "instagramUsername": "@ana",
            "type": {"T1MER": {}},
        }
    }
    # ranking query returns the player after write
    gp.query.return_value = {
        "Items": [
            {
                "gameId": "ABCDEF",
                "playerId": 1,
                "instagramUsername": "@ana",
                "type": {"T1MER": {"timer": Decimal("10.05"), "score": Decimal("50")}},
            }
        ]
    }
    out = t1mer.store_score("ABCDEF", "T1MER", gp, {"playerId": 1, "timer": "10.050", "score": 50}, print)
    assert out["ok"] is True
    assert out["rank"] == 1
    gp.update_item.assert_called_once()
