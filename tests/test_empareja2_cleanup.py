import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

from stand_common.empareja2_contributions import CatalogError

path = Path(__file__).parent.parent / "src" / "stand_empareja2_cleanup" / "lambda_function.py"
spec = importlib.util.spec_from_file_location("empareja2_cleanup", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_cleanup_only_deletes_old_uncommitted_images(monkeypatch):
    now = datetime.now(timezone.utc)
    owner = "owner"
    prefix = f"contributions/{owner}/ABC123/"
    old = now - timedelta(days=8)
    s3, catalog = MagicMock(), MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [{"Contents": [
        {"Key": prefix + "a" * 32 + "/" + "f" * 64 + ".png", "LastModified": old},
        {"Key": prefix + "b" * 32 + "/" + "f" * 64 + ".png", "LastModified": old},
        {"Key": prefix + "c" * 32 + "/" + "f" * 64 + ".png", "LastModified": now},
        {"Key": "unrelated.png", "LastModified": old},
    ]}]
    catalog.get_item.side_effect = [{}, {"Item": {"gameId": "ABC123"}}]
    acquire = MagicMock()
    release = MagicMock()
    monkeypatch.setattr(module, "acquire_lock", acquire)
    monkeypatch.setattr(module, "release_lock", release)

    assert module.cleanup(s3, catalog, "bucket", now) == 1
    s3.delete_object.assert_called_once_with(Bucket="bucket", Key=prefix + "a" * 32 + "/" + "f" * 64 + ".png")
    assert acquire.call_count == release.call_count == 2
    assert all(call.kwargs["lease_seconds"] == 1800 for call in acquire.call_args_list)


def test_cleanup_skips_busy_publication(monkeypatch):
    now = datetime.now(timezone.utc)
    key = "contributions/owner/ABC123/" + "a" * 32 + "/" + "f" * 64 + ".png"
    s3, catalog = MagicMock(), MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": key, "LastModified": now - timedelta(days=8)}]}]
    monkeypatch.setattr(module, "acquire_lock", MagicMock(side_effect=CatalogError("ContributionBusy", 409)))
    assert module.cleanup(s3, catalog, "bucket", now) == 0
    s3.delete_object.assert_not_called()
    catalog.get_item.assert_not_called()
