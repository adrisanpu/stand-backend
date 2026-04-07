"""Tests for _parse_event in stand_prod_game_fn_raffle/lambda_function.py."""
import sys
import json
import base64
import importlib.util
from pathlib import Path

_LAMBDA_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "stand_prod_game_fn_raffle"
    / "lambda_function.py"
)

spec = importlib.util.spec_from_file_location("raffle_lambda", _LAMBDA_PATH)
raffle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(raffle)

_parse_event = raffle._parse_event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _http_event(method, body_dict=None, qs=None, base64_encoded=False):
    body = json.dumps(body_dict or {})
    if base64_encoded:
        body = base64.b64encode(body.encode()).decode()
    return {
        "httpMethod": method.upper(),
        "body": body,
        "isBase64Encoded": base64_encoded,
        "queryStringParameters": qs or {},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestParseEvent:
    def test_options_returns_early(self):
        event = _http_event("OPTIONS")
        method, game_id, n_winners, only_val = _parse_event(event)
        assert method == "OPTIONS"
        assert game_id is None

    def test_post_basic(self):
        event = _http_event("POST", {"gameId": "abc123", "numberOfWinners": 3, "applicableOnlyValidated": True})
        method, game_id, n_winners, only_val = _parse_event(event)
        assert method == "POST"
        assert game_id == "ABC123"
        assert n_winners == 3
        assert only_val is True

    def test_game_id_uppercased(self):
        event = _http_event("POST", {"gameId": "game01"})
        _, game_id, _, _ = _parse_event(event)
        assert game_id == "GAME01"

    def test_n_winners_defaults_to_none_on_bad_value(self):
        event = _http_event("POST", {"gameId": "G1", "numberOfWinners": "bad"})
        _, _, n_winners, _ = _parse_event(event)
        assert n_winners is None

    def test_only_validated_string_true(self):
        event = _http_event("POST", {"gameId": "G1", "applicableOnlyValidated": "true"})
        _, _, _, only_val = _parse_event(event)
        assert only_val is True

    def test_only_validated_string_false(self):
        event = _http_event("POST", {"gameId": "G1", "applicableOnlyValidated": "false"})
        _, _, _, only_val = _parse_event(event)
        assert only_val is False

    def test_base64_body_decoded(self):
        event = _http_event("POST", {"gameId": "B64GAME", "numberOfWinners": 2}, base64_encoded=True)
        _, game_id, n_winners, _ = _parse_event(event)
        assert game_id == "B64GAME"
        assert n_winners == 2

    def test_direct_invoke(self):
        event = {"gameId": "DIRECT", "numberOfWinners": 5, "applicableOnlyValidated": False}
        method, game_id, n_winners, only_val = _parse_event(event)
        assert method == "INVOKE"
        assert game_id == "DIRECT"
        assert n_winners == 5
        assert only_val is False

    def test_game_id_from_querystring(self):
        event = _http_event("POST", {}, qs={"gameId": "fromqs"})
        _, game_id, _, _ = _parse_event(event)
        assert game_id == "FROMQS"

    def test_http_api_v2_method(self):
        event = {
            "requestContext": {"http": {"method": "POST"}},
            "body": json.dumps({"gameId": "V2GAME"}),
            "isBase64Encoded": False,
            "queryStringParameters": {},
        }
        method, game_id, _, _ = _parse_event(event)
        assert method == "POST"
        assert game_id == "V2GAME"
