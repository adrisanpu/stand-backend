"""Interactive local check of Kivly token and context only; no AWS access."""
import argparse
import getpass
import json
from pathlib import Path
import sys
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "layers" / "stand_common" / "python"))

from stand_common.kivly import KivlyClient, KivlyError


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ca-bundle", help="Trusted PEM bundle; TLS verification remains enabled.")
    args = parser.parse_args(argv)
    if not sys.stdin.isatty():
        print("InteractiveTerminalRequired: run locally; do not pipe credentials.", file=sys.stderr)
        return 2
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            client_id = getpass.getpass("Kivly client_id (hidden): ")
            client_secret = getpass.getpass("Kivly client_secret (hidden): ")
        client = KivlyClient(client_id, client_secret, ca_bundle=args.ca_bundle)
        del client_id, client_secret
        context = client.get_context()
    except (getpass.GetPassWarning, EOFError, KeyboardInterrupt):
        print("SecureInputCancelledOrUnavailable", file=sys.stderr)
        return 2
    except KivlyError as exc:
        print(json.dumps({
            "ok": False, "code": exc.code, "http_status": exc.http_status,
            "retry_after_seconds": exc.retry_after,
        }), file=sys.stderr)
        return 1
    print(json.dumps({
        "ok": True, "environment": "production", "business_writes": False,
        "scope_matches_token": True,
        "token_expires_in_max": context.token_expires_in_max,
        "requests_per_minute": context.requests_per_minute,
        "requests_per_hour": context.requests_per_hour,
        "resource_count": len(context.resources),
    }, indent=2))
    print("Token/context only: expected business and write permissions are not approved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
