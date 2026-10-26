"""
Shared pytest fixtures and path/mock setup for backend tests.

All Lambda modules import boto3 and stand_common at module level, so we stub
those before any test module is imported.
"""
import sys
import types
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
import pytest

# ---------------------------------------------------------------------------
# 1. Stub boto3 so Lambda files can be imported without the real SDK
# ---------------------------------------------------------------------------
boto3_stub = MagicMock()
sys.modules.setdefault("boto3", boto3_stub)
sys.modules.setdefault("botocore", MagicMock())
sys.modules.setdefault("botocore.exceptions", MagicMock())
sys.modules.setdefault("boto3.dynamodb", MagicMock())
sys.modules.setdefault("boto3.dynamodb.conditions", MagicMock())

dynamodb_types = types.ModuleType("boto3.dynamodb.types")


class TypeSerializer:
    """Serialize values used by mocked low-level DynamoDB transaction requests."""
    def serialize(self, value):
        if isinstance(value, bool):
            return {"BOOL": value}
        if value is None:
            return {"NULL": True}
        if isinstance(value, (int, Decimal)):
            return {"N": str(value)}
        if isinstance(value, str):
            return {"S": value}
        if isinstance(value, list):
            return {"L": [self.serialize(item) for item in value]}
        if isinstance(value, dict):
            return {"M": {key: self.serialize(item) for key, item in value.items()}}
        raise TypeError(f"Unsupported DynamoDB test value: {type(value).__name__}")


dynamodb_types.TypeSerializer = TypeSerializer
sys.modules.setdefault("boto3.dynamodb.types", dynamodb_types)

# ---------------------------------------------------------------------------
# 2. Add the Lambda Layer (stand_common) to sys.path so imports resolve
# ---------------------------------------------------------------------------
BACKEND_ROOT = Path(__file__).parent.parent
LAYER_PATH = BACKEND_ROOT / "layers" / "stand_common" / "python"
if LAYER_PATH.exists() and str(LAYER_PATH) not in sys.path:
    sys.path.insert(0, str(LAYER_PATH))

# Fallback: if the layer tree is missing, provide a minimal stub (no submodules)
if not LAYER_PATH.exists() and "stand_common" not in sys.modules:
    stand_common = types.ModuleType("stand_common")
    utils = types.ModuleType("stand_common.utils")

    def _stub_log(msg, data=None):
        pass

    def _stub_resp(status, body):
        import json
        return {"statusCode": status, "body": json.dumps(body)}

    def _stub_iso_now():
        return "2024-01-01T00:00:00Z"

    def _stub_json_sanitize(obj):
        return obj

    def _stub_as_int(v):
        try:
            return int(v)
        except Exception:
            return None

    utils.log = _stub_log
    utils._resp = _stub_resp
    utils._iso_now = _stub_iso_now
    utils._json_sanitize = _stub_json_sanitize
    utils._as_int = _stub_as_int

    stand_common.utils = utils
    sys.modules["stand_common"] = stand_common
    sys.modules["stand_common.utils"] = utils


@pytest.fixture(autouse=True)
def configure_kivly_error_types(monkeypatch):
    """New handlers need real exception types instead of the SDK MagicMock."""
    from stand_common import kivly_registration, kivly_settings
    from tests.email_fake import FakeDynamoError

    monkeypatch.setattr(kivly_registration, "ClientError", FakeDynamoError)
    monkeypatch.setattr(kivly_registration, "BotoCoreError", FakeDynamoError)
    monkeypatch.setattr(kivly_settings, "ClientError", FakeDynamoError)
    # Unrelated offline tests start without an account integration.
    monkeypatch.setattr(kivly_registration, "connection", lambda *_: None)
    monkeypatch.setattr(kivly_registration, "ERRORS", (
        kivly_registration.RegistrationError, kivly_registration.KivlyError, FakeDynamoError,
    ))

@pytest.fixture
def verified_web_assignment(monkeypatch):
    """Assignment unit tests start after verification; real gates have separate integration coverage."""
    from tests.test_public_web import assign_mod

    table, record = MagicMock(), {"verifiedAt": 1700000000}
    monkeypatch.setattr(assign_mod, "_verified_email", lambda *_: (table, record))
    monkeypatch.setattr(
        assign_mod, "_find_verified_player",
        lambda _table, game_id, email: assign_mod._find_existing_player_by_email(game_id, email),
    )
    def put(_table, _record, player, _max, _now):
        assign_mod._put_player(player)
        return "created"
    monkeypatch.setattr(assign_mod, "_put_verified_player", put)
    monkeypatch.setattr(assign_mod, "_complete_verified_player", MagicMock())
    monkeypatch.setattr(assign_mod, "complete_registration", MagicMock())
