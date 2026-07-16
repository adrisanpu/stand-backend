import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).parent.parent
LAMBDA_PATH = ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py"

spec = importlib.util.spec_from_file_location("game_lambda", LAMBDA_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mod)


def test_is_user_pro_dev_stage_bypasses_free_plan():
    os.environ["STAGE"] = "dev"
    assert mod._is_user_pro({"plan": "FREE"}) is True
    assert mod._is_user_pro(None) is True


def test_is_user_pro_prod_enforces_free_plan():
    os.environ["STAGE"] = "prod"
    assert mod._is_user_pro({"plan": "FREE"}) is False
    assert mod._is_user_pro(None) is False
