from pathlib import Path

import pytest

from scripts.check_common_layer import check_layer


FILES = (
    "python/stand_common/__init__.py",
    "python/stand_common/utils.py",
    "python/cryptography/__init__.py",
    "python/cffi/__init__.py",
    "python/cryptography/hazmat/bindings/_rust.abi3.so",
    "python/_cffi_backend.cpython-313-x86_64-linux-gnu.so",
)


@pytest.fixture
def artifact(tmp_path: Path) -> Path:
    for relative in FILES:
        file = tmp_path.joinpath(*relative.split("/"))
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()
    return tmp_path


def test_accepts_lambda_layer_layout(artifact):
    check_layer(artifact)


@pytest.mark.parametrize("relative", FILES)
def test_rejects_missing_code_or_dependency(artifact, relative):
    artifact.joinpath(*relative.split("/")).unlink()
    with pytest.raises(ValueError, match="Missing"):
        check_layer(artifact)


def test_rejects_nested_python_directory(artifact):
    (artifact / "python" / "python" / "stand_common").mkdir(parents=True)
    with pytest.raises(ValueError, match="python/python"):
        check_layer(artifact)


def test_rejects_missing_artifact(tmp_path):
    with pytest.raises(ValueError, match="Missing layer file"):
        check_layer(tmp_path / "missing")


def test_rejects_windows_native_dependencies(artifact):
    for file in artifact.rglob("*.so"):
        file.rename(file.with_suffix(".pyd"))
    with pytest.raises(ValueError, match="Missing Linux native dependency"):
        check_layer(artifact)
