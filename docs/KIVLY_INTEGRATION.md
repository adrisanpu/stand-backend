# Kivly: account connection and STACK registration

## Release scope

The organizer connects Kivly once for the entire account. **Only STACK** uses
that connection to require club registration in this release. EMPAREJA2, T1MER,
RULET4 and other non-STACK games do not request club consent or call Kivly in
their public registration, session or gameplay flows, even when old game data
contains a Kivly flag.

The general form policy is independent of Kivly: full name (`username`),
mandatory verified email, and optional phone when `phone.enabled` is true.
The existing public join support remains EMPAREJA2, T1MER, STACK and RULET4;
this feature does not introduce new game-specific join implementations.

STACK can select one existing Kivly reward for every participant who finishes,
including a zero score. After the server confirms the saved result, the public
participant can check the reward status and use only Wallet URLs returned by
Kivly. This extension does not create rewards, redeem them, mark them used, or
generate passes. The reward/Wallet extension is local and pending review; its
presence here is not evidence that it has been deployed or externally verified.

## Organizer API (Cognito authenticated)

The organizer connection route is `/v1/kivly`, handled by the game-management Lambda.
Use the organizer's existing authenticated API client. A game ID is not needed.

### Read connection: `GET /v1/kivly`

Disconnected is a successful state, not an error:

```json
{
  "ok": true,
  "connection": {
    "connected": false,
    "locationId": null,
    "locations": [],
    "clubSettings": null
  }
}
```

A connected response contains only the location authorized by the credential:

```json
{
  "ok": true,
  "connection": {
    "connected": true,
    "locationId": "location-id",
    "locations": [{"id": "location-id", "name": "Club name"}],
    "clubSettings": {
      "clubName": "Club name",
      "privacyUrl": "https://club.test/privacy",
      "termsUrl": "https://club.test/terms"
    }
  }
}
```

GET verifies the saved credentials using token/context and a read-only customer
listing. A connection failure returns an error, never a misleading disconnected
success. `clubSettings` can be null on older connections; those do not activate
STACK club registration. No secret ARN, credentials, owner ID or token is returned.
Generic `/v1/user` responses also omit the private connection.

### Connect/reconnect: `PUT /v1/kivly`

```json
{"clientId": "entered-in-owner-ui", "clientSecret": "entered-in-owner-ui"}
```

This authenticates and verifies scoped read access, creates a private Secrets
Manager entry, and atomically saves its reference on the organizer. It does not
create or modify any Kivly customer. New connections initialize club name from
the authorized location and legal URLs to
`https://playstand.app/pages/privacy-es.html` and
`https://playstand.app/pages/legal.html`. Reconnecting the same location retains
the organizer's saved club details.

Success: `{"ok":true}`. Old-secret cleanup failure is an additive success warning:
`{"ok":true,"warning":"KivlySecretCleanupRequired"}`.

### Save shared club details: `PUT /v1/kivly`

```json
{
  "clubSettings": {
    "clubName": "Club name",
    "privacyUrl": "https://club.test/privacy",
    "termsUrl": "https://club.test/terms"
  }
}
```

All three keys are required; names are limited to 200 characters and each URL to
2048. Enabled registration requires nonempty values and HTTP(S) legal URLs
without embedded credentials. `example.com` or its subdomains are demo settings
and leave registration disabled; they must not be used as real legal details.
Saving this object does not replace credentials. Success is `{"ok":true}`.
No connection returns HTTP 409 `KivlyConnectionRequired`. There is no per-game
enable switch. The scope remains STACK even though details are account-wide.

### Disconnect: `DELETE /v1/kivly`

Returns `{"ok":true}` (idempotent if already disconnected), optionally with the
cleanup warning above. It removes the account reference and schedules secret
deletion with a seven-day recovery window. It does not delete Kivly customers,
STAND players, consent records or creation intents. STACK stops requiring club
membership once the disconnect is read. Concurrent account writes fail with
HTTP 409 `KivlyConnectionChanged`; reload before retrying.

## Public registration contract

`GET /v1/public/game?gameId=ABC123` keeps its existing response. Only an active
STACK integration adds:

```json
{
  "loyalty": {
    "clubName": "Club name",
    "privacyUrl": "https://club.test/privacy",
    "termsUrl": "https://club.test/terms",
    "consentVersion": "opaque-server-generated-version"
  }
}
```

Missing connection, missing club details, demo-disabled settings and all
non-STACK games omit `loyalty`; do not show club consent in those cases.
Public field metadata always resolves email to `{"required":true}` and phone to
`{"required":false,"enabled":true|false}`, including legacy game configuration.
There is no public customer ID, location ID or credential in `loyalty`.

### Request email challenge

`POST /v1/public/email/request`:

```json
{
  "gameId": "ABC123",
  "username": "Full Name",
  "email": "participant@club.test",
  "phone": "+34 612345678",
  "consent": true,
  "language": "es",
  "loyaltyConsentVersion": "version-returned-in-loyalty"
}
```

`username` is trimmed, nonempty and at most 100 Unicode characters; control
characters are rejected. Email is mandatory, validated and normalized. Phone is
optional, trimmed and limited to 64 characters; it is ignored when disabled.
`consent: true` is the general STAND consent. For enabled STACK club registration,
the participant must additionally agree to the displayed club legal details;
only then send their exact `loyaltyConsentVersion`. Missing/stale club consent
returns HTTP 400 `KivlyConsentRequired` before email or customer creation.
Omit the version for all other games or disabled club registration.

Success:

```json
{
  "ok": true,
  "challengeId": "opaque-challenge-id",
  "emailMasked": "p***@club.test",
  "expiresAt": "2026-10-26T13:10:00Z",
  "resendAvailableAt": "2026-10-26T13:01:00Z"
}
```

The form values and club version are captured server-side. This request only
sends email; it never creates a customer.

### Confirm and register

`POST /v1/public/join`:

```json
{"gameId":"ABC123","challengeId":"opaque-challenge-id","code":"123456"}
```

Do not resend editable form values. Confirmation verifies the challenge before
any Kivly creation and passes a server-only signed receipt to assignment.
Changed legal details invalidate an old challenge; request another one with fresh
consent. Invalid/expired codes, missing consent and failed Kivly membership do
not issue a player session.

The successful response remains the existing PlayerResponse:
`{ok:true,gameId,gameType,playerToken,assignment,prizes}`. An initial STACK
assignment has no started attempt (`assignment.attempt: null`). Persist the
returned player token, not email/code/credentials. Repeating a valid confirmation
recovers the same player without consuming another slot or renaming them.
Optional phone is passed to Kivly only on new customer creation; an existing
customer is reused without modifying its name, email, phone or other data.

STACK session/access recovery and gameplay check membership against the current
location/consent version. HTTP 403 `KivlyMembershipRequired` means discard the
saved session and restart registration. These checks never contact Kivly for
non-STACK games. A failed organizer connection-status read does not affect
unrelated game-rule saves.

## Durable creation and failure handling

The dedicated `stand-{env}-kivly-registration-table` uses a private HMAC identity
scoped to organizer, authorized location and normalized email. It is retained
with point-in-time recovery and deliberately has **no TTL**.

1. Look up a previously confirmed identity.
2. Otherwise perform a complete, bounded customer listing (at most five pages
   of 100) and match the verified email case-insensitively.
3. Reuse exactly one active match. Never update an existing customer. Duplicate,
   malformed, incomplete or oversized listings fail closed.
4. If absent, conditionally persist `creating` plus a random `externalId`
   **before** issuing a single customer POST.
5. Confirm the active returned customer and exact attempt identity. Persist
   confirmation before granting membership, and recheck the account connection.

An uncertain POST result leaves its intent permanently recorded. Retrying may
confirm an existing customer only if its `external_id` exactly matches that
intent. It must never automatically repeat POST. Operators must investigate
`KivlyReconciliationRequired` using the durable intent and external business
record; do not delete the intent or blindly retry customer creation.
This prioritizes avoiding duplicate external records over automatic recovery.
There is no distributed transaction between Kivly and DynamoDB: a club customer
may exist even if subsequent STAND player persistence fails or capacity changes.
Known-full games are rejected before external creation.

Errors use `{"ok":false,"error":"SafeCode"}`; a propagated assignment failure may
also include `reason` and `status`. HTTP 409 includes busy/changed connection,
uncertain creation, or invalid customer; HTTP 429 includes rate limiting; HTTP
503 indicates unavailable/unverified configuration or upstream access. A safe
numeric `retryAfter` may be included. Do not automatically replay writes on
network failures. Existing email rate limits, expiry and attempt caps still apply.

## STACK completion rewards and Wallet

The owner-only `GET /v1/game/kivly/rewards?gameId=...` lists the connected
location's active rewards. `GET /v1/game/kivly?gameId=...` reads the selected
reward, and `PUT /v1/game/kivly` accepts `{gameId, rewardId}`; `rewardId: null`
clears it. These endpoints reject non-STACK games and rewards outside the
connected Kivly location.

After the STACK result is durably finished, `POST /v1/public/kivly/reward`
accepts `{gameId, playerToken}`. A durable per-player reservation prevents a
second `earn` after an uncertain write. Responses have status `none`, `pending`
or `confirmed`; confirmed results may include only Kivly-returned Apple/Google
Wallet URLs from approved HTTPS hosts. The integration uses `earn`, never
`redeem` or `mark-used`. It does not generate Wallet passes. Do not repeat an
uncertain assignment; it requires operator reconciliation.

## Validation, deployment and live smoke

Automated tests use stateful DynamoDB fakes, fake SES/Lambda invocation and mocked
Kivly HTTP transport. They cover verified registration, consent binding, optional
phone, existing-customer reuse, concurrent claims, uncertain POST recovery,
connection changes, and non-STACK isolation. They do **not** prove current live
Kivly response schemas, business permissions, customer creation or card delivery.
No live write or credential-based probe is part of packaging.

Deploy backend/template and compatible frontend together to **dev only** after
approval. Existing SES identity/sender and a private `PlayerTokenSecret` of at
least 32 characters are required. The template supplies the retained registration
table and narrowly scoped secret/table permissions; no new committed credential
or production Python dependency is needed. The test requirements include boto3
for offline validation of DynamoDB transaction serialization.

Safe initial dev checks: authenticated disconnected GET; load existing RULET4,
T1MER and EMPAREJA2 games and ensure no club prompt; confirm game rules and
native player deletion still work. Connecting/GET status uses external read-only
token/context/customer requests with organizer-provided credentials.
An actual STACK registration writes a Kivly customer and must be a separately
authorized test in a designated business with an approved test email, verified
email access and agreed retention/cleanup. Do not perform it implicitly as a
deployment smoke test. Reward/Wallet delivery has not been live-verified and
requires its own approved test before being relied on.
