# Kivly: account connection and STACK registration

## Release scope

The organizer connects Kivly once for the entire account. **Only STACK** uses
that connection to require club registration in this release. EMPAREJA2, T1MER,
RULET4 and other non-STACK games do not request club consent or call Kivly in
their public registration, session or gameplay flows, even when old game data
contains a Kivly flag.

The general form policy is independent of Kivly: full name (`username`) and
mandatory verified email. STACK also requires phone for both wallet and email;
other games retain optional phone when `phone.enabled` is true.
The existing public join support remains EMPAREJA2, T1MER, STACK and RULET4;
this feature does not introduce new game-specific join implementations.

STACK supports textual rules combining existing Kivly rewards and game-local
STAND prizes. Earned STAND units have individual collection QRs; Kivly units are
assigned server-side, including awards decided after the participant leaves.
The club card is added through the business's official Kivly storefront.
STACK owners can create Kivly rewards from the prize panel when connected.
Playstand does not redeem Kivly rewards, mark them used, generate passes or push
updates to devices. This extension's presence here is not evidence that it has
been deployed or that installed-wallet propagation is verified.

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

`clubSettings` also accepts optional `storefrontUrl: string`. Save only an HTTPS
`*.kivly.store` origin (maximum 300 characters), without credentials, path, query,
fragment or nonstandard port; a trailing slash is normalized away.
GET returns the saved key inside `connection.clubSettings`. It is shared by all
STACK games and overrides their legacy program URLs. Omitting it preserves an
already saved shared value; `""` explicitly disables all wallet links rather than
restoring game fallbacks. Until the key has ever been saved, use the individual
game's legacy URL. No old URL is automatically selected for the account.
Changing only the storefront does not change `consentVersion`, invalidate an
email challenge, or invalidate registered membership. Legal name/URLs and location
remain the inputs to consent versioning.

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
`{"required":true,"enabled":true}` for STACK, including legacy configuration.
Other games retain `{"required":false,"enabled":true|false}`.
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
characters are rejected. Email is mandatory, validated and normalized. STACK
phone is mandatory, trimmed, at most 64 characters, with at least three ASCII
digits and only an optional leading `+`, digits, spaces, parentheses or hyphens.
STACK settings cannot disable phone or make it optional. Other games keep
optional phone, ignored when disabled. Existing valid sessions are not revoked
solely because they predate required phone.
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
Phone is passed to Kivly only on new customer creation; an existing
customer is reused without modifying its name, email, phone or other data.
The private membership stores its valid Kivly phone when returned, never a
substitute from the form. Missing Kivly phone is logged by safe code only;
registration remains possible but email delivery reports `KivlyPhoneRequired`.

STACK session/access recovery and gameplay check membership against the current
location/consent version. HTTP 403 `KivlyMembershipRequired` means discard the
saved session and restart registration. These checks never contact Kivly for
non-STACK games. A failed organizer connection-status read does not affect
unrelated game-rule saves.

## Durable creation and failure handling

The dedicated `stand-{env}-kivly-registration-table` uses a private HMAC identity
scoped to organizer, authorized location and normalized email. It is retained
with point-in-time recovery and deliberately has **no TTL**.

1. Read the saved identity, including any unresolved creation intent.
2. Search using the normalized verified email in `GET /api/v1/customers/?search=...`.
   Read the complete filtered listing (at most five pages of 100 search results,
   not all business customers) and match the email exactly, case-insensitively,
   even for confirmed identities. Search can also return name/phone or partial
   email matches; those do not establish membership.
3. Reuse exactly one active match. Never update an existing customer. Duplicate,
   malformed, incomplete or oversized listings fail closed.
   Filtered totals must agree with the returned unique rows and pagination.
   Inconsistent totals still produce `KivlySearchIncomplete`; they are never
   ignored to authorize a new customer POST.
4. If absent, conditionally persist `creating` plus a random `externalId`
   **before** issuing a single customer POST.
5. Confirm the active returned customer and exact attempt identity. Persist
   confirmation before granting membership, and recheck the account connection.

Deleting a STAND player removes that event's participant (including its stored
membership) and verified-email mapping. The same email can rejoin with a fresh
verification and consent, under a new player ID; old player tokens cannot recover
the replacement. It never deletes Kivly customers, rewards or other events'
participants. Shared registration records remain to protect unresolved writes.
On registration, an active customer is reused without modification. If a
previously confirmed customer was deleted externally, a complete lookup proving
absence permits a conditional replacement of that confirmed record with one new
creation intent. Concurrent joins cannot issue a second POST; an uncertain
creation intent is never reset by deleting a participant.

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

## STACK prize rules, delivery and Wallet

The owner-only `GET /v1/game/kivly/rewards?gameId=...` lists the connected
location's active rewards. `POST /v1/game/kivly/rewards` creates a Kivly reward
for an owned STACK game through the organizer's saved connection; the browser
never supplies credentials or a location:

```json
{
  "gameId": "ABC123",
  "title": "Coffee",
  "description": "One coffee",
  "rewardType": "free_item",
  "value": 1,
  "pointsRequired": 100
}
```

The required fields follow Kivly's bundled `RewardCreateRequest`. Playstand
currently exposes `discount_percentage`, `discount_fixed` and `free_item`;
`value` remains a generic number because the contract does not define its unit.
The create API still accepts optional `image` (HTTPS URL), `termsAndConditions`,
`validFrom`, `validUntil`, `maxRedemptions` and `maxRedemptionsPerUser`.
The operator UI uses local PNG uploads instead of a URL field. These images
belong to the game's prize inventory and do not mutate Kivly reward artwork.
`pointType` is omitted so Kivly applies its documented `POINT` default. A
successful upstream HTTP 201 is validated and returned as
`{"ok":true,"reward":{"id":"...","name":"..."}}` with HTTP 201.

The upstream create operation is not assumed idempotent. Playstand never
automatically retries it. A timeout, 5xx or malformed success response returns
`KivlyRewardCreationUncertain`; refresh the catalog and inspect a matching
reward before explicitly trying again. `KIVLY_EXTERNAL_WRITES_ENABLED=false`
blocks creation and returns `KivlyWritesDisabled` while leaving catalog reads
available.

The other owner endpoint is
`/v1/game/stack/prizes`: GET takes `gameId` in the query; PUT accepts
`{gameId, program}`; POST accepts `{gameId, excludePlayerIds: []}` to request or
progress definitive closure. All reject other owners and non-STACK games.

GET and successful PUT return normalized `program` with `version: 3`,
`storefrontUrl`, `deliveryChannel`, `catalog` and `rules`. The retained program URL is a
read-compatible effective value; saving a game does not update the account.
Catalog entries have a local `id`, `source` (`stand` or `kivly`), `name` and
`stockTotal` (Y, 1-1,000); both sources accept an optional verified PNG `imageKey`,
while Kivly also binds a real `rewardId` and the connected `locationId`. Rules
contain a stable `id`, `condition`, `groups: {allocations: {prizeId, quantity}[]}[]`
and, for ranking conditions, `topX` (1-100). Version 3 has no `mode` or flat
`allocations`. Limits are 32 catalog entries, eight rules, eight groups per rule,
eight unique prize allocations per group and quantities 1-10. The same prize can
appear in different groups. At most one group (80 units maximum) is reserved in
one rule transaction, unchanged from the previous AND allocation bound.
Both old input formats remain accepted: version 1 becomes one group with X=1 and
Y=1,000; version-2 `all` becomes one group and `random_one` becomes singleton
groups, preserving quantities. Normalization preserves catalog/rule IDs and
compares normalized old/new rules before identity checks, so a format-only save
neither changes identity nor causes previously earned prizes to be awarded again.

```json
{
  "version": 3,
  "storefrontUrl": "",
  "catalog": [{"id":"pizza","source":"stand","name":"Pizza","stockTotal":20}],
  "rules": [{
    "id":"completion","condition":"completed",
    "groups":[{"allocations":[{"prizeId":"pizza","quantity":2}]}]
  }]
}
```

| Condition | Award decision |
| --- | --- |
| `completed` | A valid result is saved, including zero points |
| `top_finish` | Rank when that participant finishes; later scores never revoke the award |
| `top_final` | Final ranking after definitive closure |
| `raffle` | Selected raffle winners |

OR chooses uniformly among groups with some rank-eligible remaining stock,
without weighting by the number of prizes or remaining stock. A single eligible
group needs no random draw; entirely unavailable groups are excluded.
AND grants every eligible allocation inside the selected group, partially:
`min(quantity, remaining stock)` for each prize. No eligible stock means no award.
Y is one shared event budget per catalog item, consumed
across all rules; it is a STAND delivery budget for Kivly rewards, not a claim
about Kivly's own inventory. Stock reservations and per-rule allocation records
are atomic and idempotent. Attempts snapshot the program at start; finishing
atomically saves the result and a durable grant job, which then allocates units.
Changing a rule requires a new rule ID; existing awards keep their original
prize snapshot.

Unit allocation accepts integral DynamoDB `Decimal` quantities as well as Python
integers, and rejects fractional, non-finite or out-of-range quantities before
creating awards. This also applies to previously queued version-2 grant jobs;
they keep their saved program and idempotent award identities.

Closing blocks new starts but accepts valid finishes for active attempts, even
if the game is paused. A scheduled worker finalizes automatically once no
attempts remain. Explicitly excluding pending attempts gives them status
`excluded`, without score or prizes; late finishes are rejected. Final ranking
is immutable, and player deletion is blocked while closing/closed.
The existing unified close POST also works before any program has been saved,
initializing a normalized legacy program in the same transaction that begins
closing. `isActive` is not set false at the start of closing: admitted pending
attempts can finish. It becomes false atomically with `stackPrizeState: "closed"`
and the final-award outbox job after all pending attempts finish or are explicitly
excluded. Final delivery is asynchronous and remains recoverable after closure.
Generic `PUT /v1/game {isActive:false}` returns `409 StackCloseRequired` for STACK;
use the close POST instead. Reactivation during/after closure returns
`409 StackClosing`. Other game types retain their existing active-flag behavior.
Owner game reads and public result/collection reads remain available after
definitive STACK closure; new registrations/starts do not.

The retained prize table stores unit awards, allocation records and a durable
outbox. After the finish transaction commits, the score handler immediately
enqueues its grant job in FIFO SQS. Committed Kivly unit awards are likewise
enqueued immediately after allocation, without waiting for the scheduler.
Enqueue failures are logged and leave the durable outbox pending; a one-minute
schedule recovers due work into the same queue. Both producers need
`STACK_PRIZES_QUEUE` and `sqs:SendMessage` permission. Duplicate messages remain
safe through allocation ledgers and conditional delivery transitions; grant jobs
process winners in chunks. Owner views show remaining stock, allocation
outcomes and safe Kivly failure stage/HTTP status.
Kivly awards move from `queued` to `sending` before `earn`, then to
`confirmed` when the earn endpoint returns its explicit `success: true`
acknowledgement. The customer-redemptions endpoint is not a reliable receipt
for this operation: Kivly can acknowledge an earned Wallet reward while that
list remains empty, so absence of a new redemption does not negate success.
Safe failures before `earn` remain queued with delayed retry and owner-visible
`lastError`, stage and HTTP status. A rejected, malformed or interrupted earn
becomes `reconciliation_required`, never an automatic second earn. Missing
participant club binding becomes `connection_required`. Historical
`reconciliation_required` awards from before acknowledgement-based
confirmation stay unchanged and require explicit reconciliation; do not replay
their earn requests.

Public player responses add `stackPrizes`: individual awards, lifecycle state,
whether final awards are pending, and a `walletUrl` when verified membership
matches the current connection. STAND tokens bind game, player and award;
atomic redemption consumes only that unit. Kivly awards never get a STAND redemption token.
The validator can supply its game ID to reject a QR from a different game.

Configure shared `clubSettings.storefrontUrl` as only the HTTPS business origin, for example
`https://business.kivly.store`, without customer identifiers, path or query.
The server builds the official `/add-to-wallet` link from verified membership;
Kivly offers Apple/Google choices there. No undocumented Apple pass URL is
constructed. Missing card access is shown explicitly when a club card is
expected. Installing or refreshing a pass remains Kivly/provider behavior.

### Exclusive wallet or email delivery

The organizer selects `program.deliveryChannel: "wallet" | "email"` per STACK
game. Omitted values default to wallet; `"both"` and unknown values are rejected.
The program is snapshotted at attempt start. Changes only affect new attempts,
including the delivery channel used later for final-ranking and raffle awards;
existing awards are not resent. Legacy unmanaged attempts keep wallet.
STAND prizes retain their existing unit QRs in either mode.

Email requires `StackRewardEmailEnabled=true` and an enabled account Kivly
connection. The parameter defaults to false until SES and an authorized
real-world QR acceptance test are complete. Owner prize GET returns
`emailAvailable`; the UI disables email selection when false.

After explicit successful Kivly `earn`, the durable award remains `confirmed`
and the outbox sends a separate bilingual transactional email through SES.
The email includes the participant name, prize name, available prize image,
instructions and a locally generated inline PNG QR. Its payload is exactly
the phone in Kivly's `CustomerResponse.phone`, following the operator's supplied
QR specification; it is not a per-award token or proof of identity.
The worker re-reads the customer by verified email, checks the same customer ID,
active state and location, and never uses the STAND form phone as a fallback.
No additional Kivly calls are used by email delivery.

Kivly staff identify the customer with that QR and handle available rewards and
redemption in Kivly. The email is not a live balance or proof that a prize
remains unredeemed. A copy of the QR can identify the same person: staff must
follow their usual identity procedure. No SMS verification is added.

Public and owner awards expose `deliveryChannel`, `emailState` and a safe
`emailError`. States are `pending`, `sending`, `accepted`, `failed` and
`reconciliation_required`, independently of the Kivly assignment state.
`accepted` means SES accepted the message, not inbox delivery.
Email-mode player responses have no wallet link or wallet expectation; results
remain visible without opening/installing a wallet. No phone or QR payload is
returned publicly.

Throttled sends and failed pre-send Kivly reads retry at most three times.
An interrupted send, network failure or SES 5xx requires reconciliation, never
automatic resending. The organizer can POST
`{"gameId":"ABC123","action":"retry_email","awardId":"award_..."}`
to `/v1/game/stack/prizes` for a `confirmed` email award with `emailState: failed`,
even after closure. Fix a missing phone in Kivly first. This resets only the
notification and never calls `earn` again. Accepted or uncertain sends cannot
use that retry action; investigate provider evidence before any manual recovery.

Public game metadata and the owner `GET /v1/game` response (by game ID or name)
expose the same top-level `stackPrizeRules` promotional list:
`{condition, topX?, names: string[], prizes: {id, name, imageUrl: string|null}[]}[]`.
Each rule flattens all groups, deduplicating by prize ID; `names` is retained for
old clients. Zero-limit disabled prizes stay excluded; remaining stock does not
hide configured promotions. Uploaded image URLs are fresh one-hour signed URLs for
committed PNG keys belonging to that owner and game, never client-supplied URLs.
For Kivly, an explicit `imageKey` overrides external artwork; null explicitly
clears the image. Omitting it retains the existing external-image behavior.
The documented upstream reward `image` string is optionally mapped to `imageUrl`
only when it is a safe HTTPS URL. Missing/unsafe images remain absent/null; no
additional Kivly operation or invented image field is used.
The owner response also exposes top-level `stackPrizeProgram` in normalized
version 3 with the shared effective storefront and resolved catalog images, and
`stackPrizeState`. No additional widget API request is necessary.

### Migration and legacy participants

Opening the editor adapts the old ranking/raffle catalog and completion reward;
saving enables the new program. Migration is blocked while legacy attempts are
active. Already finished legacy participants retain their existing claims;
final jobs do not retroactively award them. Legacy prize stickers are not used
as promotions for a new program.

The previous `/v1/game/kivly` selection and
`POST /v1/public/kivly/reward` remain for legacy data. The latter returns `none`
for managed attempts: viewing/reloading a result must never trigger another
Kivly earn. New awards are delivered without a browser request.

## Validation, deployment and live smoke

Automated tests use stateful DynamoDB fakes, fake SES/Lambda invocation and mocked
Kivly HTTP transport. They cover verified registration, consent binding, required
STACK phone and optional phone elsewhere, existing-customer reuse, concurrent claims, uncertain POST recovery,
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
requires its own approved test before being relied on: install the official
card, leave the game, trigger final-ranking/raffle awards, confirm one Kivly
assignment per unit and check both installed wallet types. Also exercise two
STAND QRs for the same prize, repeat scans, closing with an active attempt,
exclusion and a rejected late finish.
