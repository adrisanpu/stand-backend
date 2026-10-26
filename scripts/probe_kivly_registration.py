"""Controlled customer creation in a test business; never enables Stand registration."""
import argparse
import getpass
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlencode
from urllib.request import Request
import uuid
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "layers" / "stand_common" / "python"))

from stand_common.kivly import BASE_URL, KivlyClient, KivlyError

ATTEMPT_FILE = Path(__file__).resolve().parents[1] / ".kivly-registration-probe.lock"
CONFIRMATION = "CREAR EN NEGOCIO DE PRUEBAS"
TEST_NAME = "Stand integration test"
PAGE_LIMIT = 100
MAX_PAGES = 5


class ProbeClient(KivlyClient):
    """Business operations exist only in this explicitly invoked local script."""

    def __init__(self, *args, expected_location, **kwargs):
        super().__init__(*args, **kwargs)
        self.expected_location = expected_location
        self.expected_owner = None

    def verify_business(self):
        context = self.get_context()
        if context.location_id != self.expected_location:
            raise KivlyError("KivlyUnexpectedTestBusiness")
        self.expected_owner = context.owner_id

    def _customers(self, *, query=None, customer=None):
        token = self._authenticate()
        if (
            self.expected_owner is None or token.location_id != self.expected_location
            or token.owner_id != self.expected_owner
        ):
            raise KivlyError("KivlyUnexpectedTestBusiness")
        headers = {
            "Accept": "application/json", "User-Agent": "Stand-Kivly/1.0",
            "Authorization": f"Bearer {token.value}",
        }
        url = BASE_URL + "/api/v1/customers/"
        data = None
        if customer is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(customer).encode("utf-8")
        else:
            url += "?" + urlencode(query)
        return self._send_request(
            Request(url, method="POST" if customer is not None else "GET",
                    headers=headers, data=data),
            expected_status=201 if customer is not None else 200,
        )

    def find_exact_email(self, email):
        matches = {}
        seen = set()
        total = None
        for page in range(1, MAX_PAGES + 1):
            result = self._customers(query={
                "search": email, "page": page, "limit": PAGE_LIMIT,
                "include_balance": "false",
            })
            rows = result.get("data")
            if (
                not isinstance(rows, list)
                or type(result.get("total")) is not int or result["total"] < 0
                or type(result.get("page")) is not int or result["page"] != page
                or type(result.get("limit")) is not int or result["limit"] != PAGE_LIMIT
                or type(result.get("has_next")) is not bool
                or type(result.get("has_previous")) is not bool
                or result["has_previous"] != (page > 1) or len(rows) > PAGE_LIMIT
            ):
                raise KivlyError("KivlySearchResponseInvalid")
            if total is None:
                total = result["total"]
            if total != result["total"]:
                raise KivlyError("KivlySearchChanged")
            for row in rows:
                if (
                    not isinstance(row, dict)
                    or not isinstance(row.get("id"), str) or not row["id"].strip()
                    or row["id"] in seen
                    or (row.get("email") is not None and not isinstance(row["email"], str))
                ):
                    raise KivlyError("KivlySearchResponseInvalid")
                seen.add(row["id"])
                if (row.get("email") or "").strip().casefold() == email.casefold():
                    matches[row["id"]] = row
            if not result["has_next"]:
                if len(seen) != total:
                    raise KivlyError("KivlySearchIncomplete")
                return list(matches.values())
            if not rows or len(seen) >= total:
                raise KivlyError("KivlySearchIncomplete")
        raise KivlyError("KivlySearchIncomplete")

    def create_test_customer(self, email, external_id):
        result = self._customers(customer={
            "name": TEST_NAME, "email": email,
            "source": "stand-controlled-test", "external_id": external_id,
        })
        if (
            not isinstance(result.get("id"), str) or not result["id"].strip()
            or not isinstance(result.get("email"), str)
            or result["email"].strip().casefold() != email.casefold()
        ):
            raise KivlyError("KivlyCreatedCustomerInvalid")
        return result


def _hidden(prompt):
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        return getpass.getpass(prompt)


def _summary(customer, *, created):
    print(json.dumps({
        "customer_created_this_run": created,
        "customer_found_by_exact_test_email": True,
        "google_wallet_link_present": bool(customer.get("pass_url_google")),
        "club_membership_verified": False,
        "email_delivery_verified": False,
        "stand_registration_enabled": False,
    }, indent=2))
    print("Check the customer in the test business panel. API success does not prove club membership.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ca-bundle", help="Trusted PEM bundle; TLS remains enabled.")
    parser.add_argument("--lookup-only", action="store_true", help="Never create a customer.")
    args = parser.parse_args(argv)
    if not sys.stdin.isatty():
        print("InteractiveTerminalRequired: never pipe credentials.", file=sys.stderr)
        return 2
    attempted = False
    created = False
    try:
        location = _hidden("Expected TEST location_id from the Kivly panel (hidden): ").strip()
        email = _hidden("Controlled test email you own (hidden; no real customer): ").strip()
        if not location or not re.fullmatch(r"[^@\s\x00-\x1f\x7f-\x9f]+@[^@\s\x00-\x1f\x7f-\x9f]+\.[^@\s\x00-\x1f\x7f-\x9f]+", email) or len(email) > 254:
            print("InvalidTestLocationOrEmail", file=sys.stderr)
            return 2
        client_id = _hidden("Kivly TEST client_id (hidden): ")
        client_secret = _hidden("Kivly TEST client_secret (hidden): ")
        api = ProbeClient(
            client_id, client_secret, expected_location=location, ca_bundle=args.ca_bundle,
        )
        del client_id, client_secret
        api.verify_business()
        matches = api.find_exact_email(email)
        if len(matches) > 1:
            raise KivlyError("KivlyDuplicateCustomers")
        if matches:
            _summary(matches[0], created=False)
            return 0
        if args.lookup_only or ATTEMPT_FILE.exists():
            print("CustomerNotFound: no creation. If an earlier attempt exists, reconcile in the panel; do not remove the attempt file to retry.", file=sys.stderr)
            return 1
        print("This will create ONE customer in the verified location on the production API host.")
        print("It may trigger the test business's automations. No automatic deletion or POST retry.")
        if input(f"Confirm this is a TEST business: type {CONFIRMATION}: ") != CONFIRMATION:
            print("CreationCancelled", file=sys.stderr)
            return 2
        external_id = "stand-probe-" + uuid.uuid4().hex
        # Persist intent before sending: even a timeout or crash must not enable another POST.
        with ATTEMPT_FILE.open("x", encoding="ascii") as marker:
            marker.write(external_id + "\n")
            marker.flush()
            os.fsync(marker.fileno())
        attempted = True
        customer = api.create_test_customer(email, external_id)
        created = True
        matches = api.find_exact_email(email)
        if len(matches) != 1 or matches[0]["id"] != customer["id"]:
            raise KivlyError("KivlyCreatedCustomerNotReconciled")
        _summary(matches[0], created=True)
        return 0
    except (getpass.GetPassWarning, EOFError, KeyboardInterrupt):
        print(json.dumps({
            "code": "SecureInputCancelledOrUnavailable",
            "creation_may_have_occurred": attempted, "creation_confirmed": created,
        }), file=sys.stderr)
        return 2
    except KivlyError as exc:
        print(json.dumps({
            "code": exc.code, "http_status": exc.http_status,
            "retry_after_seconds": exc.retry_after,
            "creation_may_have_occurred": attempted, "creation_confirmed": created,
        }), file=sys.stderr)
        return 1
    except OSError:
        print("LocalAttemptStateUnavailable: no new creation; preserve the attempt file and check the panel if a previous attempt exists.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
