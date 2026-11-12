"""Check the SAM layer artifact, not the source tree, before deploying."""
import argparse
from pathlib import Path


def check_layer(artifact: Path) -> None:
    python = artifact / "python"
    if (python / "python" / "stand_common").exists():
        raise ValueError("stand_common is nested under python/python; check ContentUri.")
    for relative in (
        "stand_common/__init__.py",
        "stand_common/utils.py",
        "cryptography/__init__.py",
        "cffi/__init__.py",
    ):
        if not python.joinpath(*relative.split("/")).is_file():
            raise ValueError(f"Missing layer file: python/{relative}")
    for pattern in (
        "cryptography/hazmat/bindings/_rust*.so",
        "_cffi_backend*.so",
    ):
        if not list(python.glob(pattern)):
            raise ValueError(f"Missing Linux native dependency: python/{pattern}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    try:
        check_layer(args.artifact)
    except ValueError as error:
        parser.exit(1, f"Invalid stand_common layer: {error}\n")
    print("stand_common layer layout and Linux dependencies are present.")


if __name__ == "__main__":
    main()
