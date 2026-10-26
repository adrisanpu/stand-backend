# Kivly connection implementation

See [KIVLY_INTEGRATION.md](KIVLY_INTEGRATION.md) for the full registration contract
and dev-only release scope. Account connection is shared, but club membership is
required only for STACK. Connection success is not evidence of a customer
registration or any reward/card delivery.

## External operations

`stand_common.kivly.KivlyClient` uses only the fixed HTTPS host
`https://api.getkivly.com`:

| Operation | Purpose |
| --- | --- |
| `POST /api/v1/auth/token` | Exchange server-only client credentials |
| `GET /api/v1/integration/context` | Verify authorized business and owner scope |
| `GET /api/v1/customers/` | Read-only access check or bounded complete email lookup |
| `POST /api/v1/customers/` | Create a consented, email-verified STACK participant only |
| `GET /api/v1/rewards/` | Read the active catalog for owner selection |
| `GET /api/v1/redemptions/customer/{customer_id}` | Read a customer's reward assignments and returned Wallet links |
| `POST /api/v1/redemptions/rewards/earn` | Assign the selected reward after a saved STACK finish |

Customer POST is never part of connect/status. It carries only `name`, `email`,
`external_id`, `source: "stand"` and optional enabled `phone`. There are no
reward creation, redemption, mark-used or Wallet-generation methods/routes. The
completion flow uses `earn` only; an uncertain write is not automatically retried.
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
Kivly secrets. The assignment and public roles can read those secrets and
get/put/update the registration table for their respective registration and
completion-reward flows. Non-STACK public flows skip integration lookup entirely.

Tokens stay in process memory, not public responses or DynamoDB. Warm assignment
invocations cache clients by secret ARN and connection version; reconnecting or
changing club details cannot reuse a different owner's token. Connection reads
are consistent, malformed saved configuration fails closed, and API errors log
only safe codes. Never log credentials, tokens, complete participant email,
verification codes or request bodies.

## Evidence boundary

`tests/test_kivly_client.py`, `tests/test_kivly_account.py`,
`tests/test_kivly_registration.py` and `tests/test_kivly_rewards.py` are offline
mocked contract tests, not live probes. The repository includes local connection
and controlled registration probe scripts; neither is packaged into a Lambda.
Never run a customer-creating probe without separate approval and designated test
data. Live reward/Wallet behavior is not proven by these tests.
