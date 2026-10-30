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
| `POST /api/v1/rewards/` | Create an owner-requested reward from STACK prize settings |
| `GET /api/v1/redemptions/customer/{customer_id}` | Read a customer's reward assignments and returned Wallet links |
| `POST /api/v1/redemptions/rewards/earn` | Assign an earned STACK unit from the server-side outbox |

Customer POST is never part of connect/status. It carries only `name`, `email`,
`external_id`, `source: "stand"` and the required STACK `phone`. There are no
redemption, mark-used or Wallet-generation methods/routes. Reward creation is
available only from the authenticated STACK owner prize panel and uses the
connected credential's authorized location; it does not accept a location from
the browser. The award flow uses `earn` only; uncertain writes are not
automatically retried.
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
The STACK delivery worker reads the same scoped connection and secrets, consumes
FIFO jobs and updates durable awards. Its scheduled dispatcher recovers pending
work independently of player sessions. Pre-earn errors can retry safely;
`sending` work interrupted after reservation requires reconciliation, not a
second earn. The owner prize panel exposes delivery states, safe error codes,
operation stages and upstream HTTP statuses. Registration and delivery logs use
the same stage/status fields without participant identifiers.
`KIVLY_EXTERNAL_WRITES_ENABLED=false` blocks customer creation, reward
creation and reward assignment while preserving read-only diagnostics. Check
the deployed parameter before using the feature; never make a live reward
creation request without explicit authorization and a designated test account.

Reward creation uses the bundled Public API `POST /api/v1/rewards/` contract and
requires HTTP 201 plus a valid `RewardResponse`. Kivly does not provide generic
idempotency for this operation. A timeout, server error or malformed success
response is reported as `KivlyRewardCreationUncertain`; refresh the active
catalog and inspect it before any explicit retry. The client never retries a
reward-creation POST automatically.

The business storefront origin is configured once in account `clubSettings.storefrontUrl`,
not inferred from a customer or redemption response. Until that key is saved,
each STACK program retains its own legacy URL; no game URL is promoted to the account.
An explicitly saved empty string disables the link for all STACK games. Only HTTPS `*.kivly.store`
origins are accepted. Public wallet links use the official `/add-to-wallet`
page and verified customer/location membership; STAND never constructs Apple
pass downloads or implements device push. No storefront URL should be saved
with a real customer ID in its query.

STACK can instead deliver a reward notification by email, exclusively per
game/attempt. The QR contains `CustomerResponse.phone`, per the operator's
provided scanner contract. Before sending, the worker reads the customer with
the existing scoped customer lookup and checks its ID against the award.
It never substitutes the STAND form phone, updates existing customers or calls
a Kivly redemption endpoint. Missing phone is an actionable notification error,
not an assignment failure. Kivly remains responsible for identification and
one-time consumption of each reward. QR acceptance outside wallet still needs
an authorized live test; phone QR is not a secret or single-use credential.

Tokens stay in process memory, not public responses or DynamoDB. Warm assignment
invocations cache clients by secret ARN and connection version; reconnecting or
changing club details cannot reuse a different owner's token. Connection reads
are consistent, malformed saved configuration fails closed, and API errors log
only safe codes. Never log credentials, tokens, complete participant email,
verification codes or request bodies.

## Evidence boundary

`tests/test_kivly_client.py`, `tests/test_kivly_account.py`,
`tests/test_kivly_registration.py` and `tests/test_kivly_rewards.py` are offline
mocked contract tests, not live probes. Application-specific manual connection
and registration probes have been retired. The bundled integration skill tools
remain available, but no live requests are part of this change.
`tests/test_stack_prize_rules.py` additionally covers idempotent stock
reservation, AND/OR selection, partial awards, native SDK serialization,
closure, individual redemption and uncertain-delivery handling. Live
reward/Wallet behavior is not proven by these tests.
