import sys
from pathlib import Path

import importlib.util


_LAMBDA_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "stand_prod_game_fn_assign"
    / "lambda_function.py"
)

sys.path.insert(0, str(_LAMBDA_PATH.parent))

spec = importlib.util.spec_from_file_location("assign_lambda", _LAMBDA_PATH)
assign_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assign_mod)


def test_send_code_then_raffle_with_follows():
    captured = []

    def fake_bulk(msgs):
        captured.append(msgs)

    assign_mod._send_bulk_messages = fake_bulk
    meta = {"raffleRequiredFollows": ["marca1"]}
    assign_mod._send_code_then_raffle("psid1", "code msg", meta)

    assert len(captured) == 1
    msgs = captured[0]
    assert len(msgs) == 3
    assert msgs[0] == {"psid": "psid1", "text": "code msg"}
    assert msgs[1]["psid"] == "psid1"
    assert "sorteo" in msgs[1]["text"]
    assert msgs[2] == {"psid": "psid1", "template": "follow_accounts", "handles": ["marca1"]}


def test_send_code_then_raffle_without_follows():
    captured = []

    def fake_bulk(msgs):
        captured.append(msgs)

    assign_mod._send_bulk_messages = fake_bulk
    assign_mod._send_code_then_raffle("psid1", "code msg", {})

    assert len(captured) == 1
    assert captured[0] == [{"psid": "psid1", "text": "code msg"}]
