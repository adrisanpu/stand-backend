# Kivly connection implementation

See [KIVLY_INTEGRATION.md](KIVLY_INTEGRATION.md) for the full registration contract
and dev-only release scope. Account connection is shared, but club membership is
required only for STACK. Connection success is not evidence of a customer
registration or any reward/card delivery.

## External operations in this release

`stand_common.kivly.KivlyClient` uses only the fixed HTTPS host
`https://api.getkivly.com`:

| Operation | Purpose |
| --- | --- |
| `POST /api/v1/auth/token` | Exchange server-only client credentials |
| `GET /api/v1/integration/context` | Verify authorized business and owner scope |
| `GET /api/v1/customers/` | Read-only access check or bounded complete email lookup |
| `POST /api/v1/customers/` | Create a consented, email-verified STACK participant only |

Customer POST is never part of connect/status. It carries only `name`, `email`,
`external_id`, `source: "stand"` and optional enabled `phone`. There are no
reward, catalog, redemption or Wallet methods/routes in this release.
The transport validates TLS, rejects redirects, bounds response size to 1 MiB,
uses an eight-second per-request timeout, and never retries a request internally.
Large businesses or slow upstream responses can fail closed within the Lambda
time budget; lack of a complete lookup is never treated as customer absence.

## Credentials and isolation

Each connection/reconnection creates a new immutable secret:
`stand/{env}/kivly/{sha256(organizer-id)}/{random-version}`. The user record stores
only its private ARN, authorized location/owner, version and shared club settings.
Conditional version checks prevent concurrent updates from silently overwriting
one another. Replaced/disconnected secrets are scheduled for deletion with seven
days' recovery; cleanup failure is explicitly reported.

An uncertain account write keeps its new secret for reconciliation rather than
deleting a secret that a successful-but-unacknowledged write might reference.
Investigate `KivlyConnectionReconciliationRequired`; do not silently discard it.

The account-management role can create/read/delete only this environment's
Kivly secrets. The assignment role can read those secrets and get/put/update the
registration table. The public role does not gain Kivly secret or registration
table access. Non-STACK public flows skip integration lookup entirely.

Tokens stay in process memory, not public responses or DynamoDB. Warm assignment
invocations cache clients by secret ARN and connection version; reconnecting or
changing club details cannot reuse a different owner's token. Connection reads
are consistent, malformed saved configuration fails closed, and API errors log
only safe codes. Never log credentials, tokens, complete participant email,
verification codes or request bodies.

## Evidence boundary

`tests/test_kivly_client.py`, `tests/test_kivly_account.py` and
`tests/test_kivly_registration.py` are offline mocked contract tests. They are not
live probes. No credentials, `VARS.txt`, external response dumps or live-writing
probe scripts are packaged. Live testing requires a designated dev organizer,
authorized scoped credentials and separate approval for customer writes.
