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
sys.modules.setdefault("botocore", MagicMock())
sys.modules.setdefault("botocore.exceptions", MagicMock())
sys.modules.setdefault("boto3.dynamodb", MagicMock())
sys.modules.setdefault("boto3.dynamodb.conditions", MagicMock())

# ---------------------------------------------------------------------------
# 2. Add the Lambda Layer (stand_common) to sys.path so imports resolve
# ---------------------------------------------------------------------------
BACKEND_ROOT = Path(__file__).parent.parent
LAYER_PATH = BACKEND_ROOT / "src" / "stand_common" / "python"
if LAYER_PATH.exists() and str(LAYER_PATH) not in sys.path:
    sys.path.insert(0, str(LAYER_PATH))

# Fallback: if the layer hasn't been built yet, provide a minimal stub
if "stand_common" not in sys.modules:
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
