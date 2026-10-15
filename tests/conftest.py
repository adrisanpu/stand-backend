"""
Shared pytest fixtures and path/mock setup for backend tests.

All Lambda modules import boto3 and stand_common at module level, so we stub
those before any test module is imported.
"""
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# 1. Stub boto3 so Lambda files can be imported without the real SDK
# ---------------------------------------------------------------------------
boto3_stub = MagicMock()
sys.modules.setdefault("boto3", boto3_stub)
sys.modules.setdefault("stripe", MagicMock())
sys.modules.setdefault("botocore", MagicMock())
botocore_exceptions = types.ModuleType("botocore.exceptions")


class ClientError(Exception):
    def __init__(self, response, operation_name=""):
        super().__init__(response.get("Error", {}).get("Message", "AWS client error"))
        self.response = response
        self.operation_name = operation_name


botocore_exceptions.ClientError = ClientError
sys.modules.setdefault("botocore.exceptions", botocore_exceptions)

boto3_dynamodb = types.ModuleType("boto3.dynamodb")
boto3_dynamodb.__path__ = []
sys.modules.setdefault("boto3.dynamodb", boto3_dynamodb)
sys.modules.setdefault("boto3.dynamodb.conditions", MagicMock())
boto3_dynamodb_types = types.ModuleType("boto3.dynamodb.types")


class TypeSerializer:
    def serialize(self, value):
        if isinstance(value, bool):
            return {"BOOL": value}
        if isinstance(value, str):
            return {"S": value}
        if isinstance(value, (int, float)):
            return {"N": str(value)}
        if value is None:
            return {"NULL": True}
        if isinstance(value, dict):
            return {"M": {key: self.serialize(item) for key, item in value.items()}}
        if isinstance(value, list):
            return {"L": [self.serialize(item) for item in value]}
        raise TypeError(f"Unsupported DynamoDB test value: {type(value).__name__}")


boto3_dynamodb_types.TypeSerializer = TypeSerializer
sys.modules.setdefault("boto3.dynamodb.types", boto3_dynamodb_types)

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
