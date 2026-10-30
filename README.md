# Stand Backend

Backend for the Stand project: AWS Serverless Application Model (SAM) app with Python 3.13 Lambda functions.

## Stack

- **Runtime:** Python 3.13
- **Infrastructure:** AWS SAM (API Gateway HTTP APIs, Lambda)
- **Features:** Stripe webhooks, game logic, user and billing flows

## Prerequisites

- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- Python 3.13
- AWS CLI configured (for deploy)

## Development

```bash
# Build
sam build

# Local invoke (example)
sam local invoke StandProdUserFnUserSam --event events/user.json

# Deploy prod
sam deploy --config-env prod

# Deploy dev (after one-time setup — see docs/DEV_ENVIRONMENT.md)
sam deploy --config-env dev
```

Deploy config is in `samconfig.toml`:

| Config env | Stack | URL path (Game API) | Resource prefix |
|------------|-------|---------------------|-----------------|
| `prod` | `stand-backend-prod` | `/prod` | `stand-prod-*` |
| `dev` | `stand-backend-dev` | `/prod` (different API host) | `stand-dev-*` |

Region: `us-east-1`. Full dev setup: **[docs/DEV_ENVIRONMENT.md](docs/DEV_ENVIRONMENT.md)**.

## Structure

- `template.yaml` – SAM template (APIs, Lambdas, roles)
- `src/` – Lambda source code (one folder per function)
- `samconfig.toml` – SAM deploy configuration
- `layers/stand_common/` – Shared Python utilities (log, _resp, get_game_type_blob, set_game_type_blob, etc.)

## Data model: game-table `type` column

The game table (`stand-prod-game-table` in prod, `stand-dev-game-table` in dev) uses a generic **`type`** attribute (DynamoDB Map) for game-type-specific configuration, mirroring the pattern used in **stand-prod-gameplayer-table**. This avoids adding a new top-level column for each new game type.

- **Structure:** `type.<GAME_TYPE>` where `GAME_TYPE` is the same value as `gameType` in uppercase (e.g. `INFOCARDS`, `EMPAREJA2`).
- **Example:** For an INFOCARDS game, cards are stored under `type.INFOCARDS.cards`; the blob can include other keys (e.g. `sourceUploadKey`, `generatedQuizGameId`) as needed.
- **Usage:** Use the helpers from `stand_common.utils`: `get_game_type_blob(game_item, "INFOCARDS")` to read and `set_game_type_blob(games_table, game_id, "INFOCARDS", {"cards": [...]}, updated_at=...)` to write. New game types that need type-specific blobs should follow the same pattern and reuse the `type` column instead of defining new top-level attributes.

## Adding a new backend game (lean checklist)

### Participant gameplay and access

Every game uses `playMode: "mobile"` and `joinChannel: "web"`: gameplay and
validation happen on participant phones, reached through the Web registration
form and its QR. Game create/update APIs reject other mode/channel values and
persist both fields together. Existing records are exposed as mobile/Web without
a table migration; configured participant and plan limits are unchanged.

Instagram is not a participant registration field or game access channel.
Participant registration uses a required full name (`username`), mandatory verified
email, and optional phone when enabled. Existing Instagram values remain in stored records but are
not requested or used by the public registration contract.

`POST /v1/public/player` accepts `{ "gameId": "...", "playerToken": "..." }`
for participant session recovery without query-string credentials.

EMPAREJA2 assignments returned by web registration, session recovery and personal
access exchange include nullable `characterImageUrl` and `partnerCharacterImageUrl`.
Both use signed URLs for the corresponding character's PNG in `CHAR_BUCKET`;
the partner field is additive and may be absent in older responses. The participant
screen uses it for the mission thumbnail. Deploy the backend before the frontend
that displays this thumbnail.

EMPAREJA2 public game metadata also returns nullable `clientLogoUrl`: a signed URL
for the game owner's selected client logo, without exposing media history or owner
details. If no logo is selected, the participant uses the game logo. The public
Lambda reads the owner's media selection from `USERS_TABLE` and signs only keys
under that owner's `users/{owner}/logos/` prefix in `USER_MEDIA_BUCKET`; its role
has read-only object access to logo prefixes. Deploy the updated SAM template
alongside the Lambda before the frontend.

`PUT /v1/user/media` also accepts `{ "kind": "appearance", "appearance":
{ "colors": { "primary": "#112233", "accent": "#445566" }, "themeMode": "dark" } }`.
Colors must be six-digit hex; use `colors: null` for the game's default palette,
and `themeMode` is `dark` or `light`. The authenticated owner's media document
stores the selection; GET media and EMPAREJA2/T1MER/STACK public game metadata return it as
`appearance`. Old events without a saved appearance return default dark/game
colors. Existing browser-only palettes need one new save to publish them.

Participant and prize token secrets remain private deployment configuration;
do not commit secrets to `samconfig.toml`. Instagram registration, assignment,
webhook, DM sender, raffle follow requirements and quiz/impostor/raffle DMs are
removed. Validation, score, raffle and impostor responses use only the Web
`username` and signed `participantPhotoKey` photo. Raffle winners are stored as
`{playerId, username, wonAt, validationCode?}`; the operator game API shows a
legacy winner's stored historical name when it has no Web `username`, without
exposing Instagram ids. Impostor participants read their own role, word (crew
only), turn and result from `POST /v1/public/player` with their player token.
The legacy `gsi-instagramPSID` index and stored Instagram attributes are kept
for history but are not used for gameplay.

### Configurable web registration fields (`/play`)

Game POST/PUT accepts `participantFields`. Public registration for EMPAREJA2,
T1MER, STACK and RULET4 resolves it to this policy:

```json
{
  "email": { "required": true },
  "phone": { "required": false, "enabled": false }
}
```

Email is always required and verified, including legacy games stored with
`email.required: false`. Phone is never mandatory: `phone.enabled: true` requests
an optional value; `false` ignores incoming phone values and does not store them.
Legacy `phone.required: true` enables optional phone unless an explicit `enabled`
overrides it. Omitted phone settings disable it. The public endpoint exposes only
email and phone; the game configuration Lambda tolerates the historical
`instagram` field, but the public endpoint hides and ignores it. PUT replaces the
field configuration, filling omitted keys from defaults. Malformed configuration
returns 400 before writes.

Web registration always requires the full name in `username`. This name is
trimmed, limited to 100 Unicode characters and cannot contain control characters
(U+0000–001F, U+007F–009F). Missing, null, empty or whitespace-only names return
`missing_username`; malformed names return `invalid_username` (HTTP 400).
`POST /v1/public/email/request` captures the normalized name in the server-side
challenge and signed registration binding. Verification submits only the challenge
ID and code, never a replacement name. New players store it as top-level `username`;
assignments, player recovery and rankings use it before existing fallbacks
(STACK rankings still never use email). Repeat email registrations
recover the existing player without renaming it, including legacy players without
names. Existing session/access recovery does not require a new name and retains
legacy fallbacks. Usernames are not unique. Photo upload remains optional in every
game and separate from email verification.

`POST /v1/public/email/request` accepts `gameId`, `username`, `consent: true`,
`email`, optional enabled `phone`, and optional `language: "es" | "en"`.
Email is normalized to lowercase; phone is trimmed, preserving `+`, spacing and
punctuation (no country-specific format restriction). Disabled incoming phone
values are ignored, even malformed ones. Errors include `missing_email`,
`invalid_email`, `missing_username`, `invalid_username`, and `invalid_phone`
(HTTP 400). Existing stored contact data is not retroactively removed.
The final `/v1/public/join` submits only the challenge identity and code; names,
phone and consent cannot be replaced at confirmation time.

### Email verification for web registration

Always first call `POST /v1/public/email/request` with the
registration fields (`gameId`, `username`, `consent: true`, enabled contact fields), plus
`language: "es" | "en"`. Its response is:

```json
{
  "ok": true,
  "challengeId": "...",
  "emailMasked": "p***@example.com",
  "expiresAt": "2026-10-17T12:00:00Z",
  "resendAvailableAt": "2026-10-17T11:51:00Z"
}
```

Both timestamps are ISO 8601. Submit the registration to `/v1/public/join` with
the returned `challengeId` and the emailed `code`. All joins, including
duplicate-registration recovery, require verification before a participant
session can be issued; the successful join response is unchanged. Resending uses
the request endpoint again after `resendAvailableAt`. Do not log codes, full
email addresses, participant tokens or request bodies.

Verification uses SES in `us-east-1`, from `contact@playstand.app` in dev
and `no-reply@playstand.app` in prod, and
`stand-{prod|dev}-email-verification-table` (string PK `id`, on-demand billing).
Pending challenges and rate counters use numeric epoch-seconds `expiresAt` TTL;
TTL cleanup is asynchronous, so expiry must also be enforced by application code.
Permanent HMAC-keyed verified-email-to-player mappings intentionally omit
`expiresAt`. Assignment atomically creates the player and identity mapping and
increments the game's player count, preventing concurrent duplicate registrations.
Repeating the same correctly verified challenge before its original expiry
returns the same participant without consuming another slot or starting an
attempt; it cannot authorize a different registration.
Public/assign use `EMAIL_VERIFICATION_TABLE`; the game-management Lambda also
uses it to clean up a deleted player's identity mapping. The public Lambda alone
can send email, restricted to the configured identity and From address. Both
dedicated roles retain the existing game managed policy to preserve public
session, assignment, media and game behavior. Only public has SES permissions.

Public limits default to `EMAIL_MAX_PER_HOUR=5` per email,
`EMAIL_IP_MAX_PER_HOUR=3000` per source IP, and
`EMAIL_GLOBAL_MAX_PER_HOUR=20000` per environment. These abuse controls do not
replace SES account quotas (shared by dev/prod). Existing valid sessions do not
require another email challenge unless an enabled STACK club requires fresh consent.
The existing private `PlayerTokenSecret` also derives a
purpose-separated verification key; no new committed secret is needed.

**Release gate:** set up and verify the shared sender using
[docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md) before enabling email registration.
The identity is managed once by `infra/email-sender.yaml`, never once per
dev/prod stack. Deploy the compatible backend before the new UI, but coordinate
the breaking verified-email join contract: old forms cannot submit any new joins
without a code. Use a maintenance window/controlled event and refresh cached
clients rather than leaving old email forms live. No automatic production
deployment or SES provisioning is performed by this change.

### Kivly connection and STACK club registration

Authenticated `GET/PUT/DELETE /v1/kivly` manages an organizer-wide connection and
shared club legal details. In this first release **only STACK** requires club
registration when the connection has valid club settings. Other games neither
show club consent nor contact Kivly during registration/gameplay.
STACK public metadata includes `loyalty`; email-code requests must bind its
`loyaltyConsentVersion` to the verified form. Customer creation uses a durable,
non-expiring intent to prevent duplicate POSTs after uncertain results.

See [docs/KIVLY_INTEGRATION.md](docs/KIVLY_INTEGRATION.md) for the exact API
contract, failure/reconciliation policy, and the distinction between offline
tests and live verification; [docs/KIVLY_CONNECTION.md](docs/KIVLY_CONNECTION.md)
describes secret storage, reward routes and connection checks. STACK owners can
create rewards and select them alongside existing Kivly rewards; participants
can see their status and returned Wallet links after a saved finish. Playstand
does not redeem rewards or create Wallet passes. This extension is pending
review and live reward delivery has not been verified. Existing STAND prize
catalogs and gameplay remain unchanged.

### Approved prize PNG catalog

Authenticated `PUT /v1/game` accepts a `prizeCatalog` replacement:

```json
{
  "gameId": "ABC123",
  "prizeCatalog": {
    "validation": { "name": "Gift", "limit": 10, "imageKey": "games/..." },
    "ranking": { "name": "Top prize", "limit": null, "topX": 3, "imageKey": null },
    "raffle": { "name": "Raffle prize", "limit": 5, "imageKey": null }
  }
}
```

Catalog slots are optional and omitted slots are not configured. Validation is
available for EMPAREJA2, CUPIDO, SEMAFORO and L3TRAS; ranking for T1MER and
STACK; raffle prizes retain the existing paid-plan availability. `name` is a
non-empty string (up to 200 characters), `limit` is a non-negative integer or
`null`, and ranking `topX` is an integer from 1 to 100. `imageKey` may be
omitted to preserve that slot's existing image, or set to `null` to remove it.
Client-supplied URLs and
unknown catalog fields are rejected. Saving `{}` explicitly stores an empty
catalog; records without a catalog remain unchanged for legacy compatibility.

`POST /v1/game/prizes?action=upload-url` accepts
`{ "gameId": "...", "slot": "validation|ranking|raffle", "fileName": "...",
"contentType": "image/png", "sizeBytes": N }` and returns
`{ "ok": true, "key": "...", "uploadUrl": "..." }`. The owner and slot/plan
are checked before issuing a one-hour S3 PUT URL. Uploads are bound to a
server-generated owner/game/slot key and tagged `staged=true`. Before catalog
association, the backend checks the stored object's actual size (maximum 5 MiB),
PNG signature, chunk CRCs, image structure, bounded decompression and scanlines.
Only verified objects for the same owner, game and slot can be associated.
Catalog and `EMPAREJA2` `type.EMPAREJA2.maxValidations` (when a validation
prize is included) are saved in one conditional DynamoDB update. The cap update
targets only that nested setting (initializing only the required map when absent),
so unrelated `type` configuration is not rewritten from a read snapshot. A
`null` validation limit removes the legacy cap in the same update. Invalid images,
foreign keys and failed writes do not replace the existing catalog. Upload URLs
write only under `games/staged/`; at save, the backend reads and validates the
bounded bytes then writes those exact bytes under a new server-generated
`games/{owner}/{game}/{slot}/...` key. The catalog refers only to that committed
key, which is never exposed through a client PUT URL. After the catalog commit,
the staging tag is removed from the source; a seven-day lifecycle rule expires
only staged-prefix uploads
that were abandoned before association. If tag removal fails after a successful
catalog commit, the response includes a `PrizeImageCleanupPending` warning and
the backend logs the affected key for cleanup.

A user whose plan no longer enables raffles may still save other catalog changes
when the request includes the unchanged existing raffle entry. Editing, replacing,
or omitting that raffle entry is rejected until the plan allows raffle edits;
this prevents an unrelated save from silently deleting previously configured
raffle data.

Authenticated `GET /v1/game?gameId=...` and `?gameName=...`, public
`GET /v1/public/game?gameId=...`, and public player responses include the
optional top-level `prizeCatalog` only after it has been explicitly saved. Each
configured prize contains its stored fields and an `imageUrl` (a fresh signed
read URL, or `null` when no image is associated); URLs are never stored as
catalog data. The player response continues to include all already-awarded
prizes, including raffle wins. For existing validation/raffle prizes, display
configuration does not change award eligibility or delivery semantics. STACK
ranking claims use the configured ranking prize as described below.

### RULET4 server-side configuration and spins

Owner game POST/PUT accepts `settings.rulet4.panels`, persisted as `type.RULET4`.
Each panel has a unique `id`, a `label`, a hex `color` and a positive integer
`count` (probability weight, not stock). There are at most 32 panels and 360 total
weight. Missing stored configuration uses defaults; malformed configuration
is rejected rather than silently replaced.

`GET /v1/public/game` exposes `rulet4Config`. RULET4 supports the existing web
registration flow. `POST /v1/public/rulet4/spin` accepts `gameId` and `playerToken`;
the owner route `POST /v1/game/score` accepts `gameId` and `playerId`.
Both select and persist one server-side result per player, including its
configuration snapshot. Retries and session recovery return that same result.
The paired frontend uses these endpoints for operator and mobile play.
This increment does not change the general email/phone registration contract.

### Operator player management

The game owner can list the current roster with authenticated
`GET /v1/game/players?gameId=...`. The response includes only each player's
`playerId` and display `username`, and never includes contact details.
Authenticated `DELETE /v1/game/players` accepts
`{"gameId":"...","playerId":123}` and atomically removes that player's record,
associated raffle wins, verified-email identity mapping and cached
player/validation counts. Scores and quiz answers are stored on the player
record, so removing it also removes those results from rankings. The game
retains its player-ID high-water mark so later joins cannot reuse a removed
player's ID.

The removal transaction uses native Python values with the DynamoDB resource
client, which serializes them once. Regression tests check the actual SDK
request payload without making network calls.

### STACK: mobile `/play` validated finish and ranking (Phase 3)

STACK is registered for operator CRUD and public web join using the existing
game/player tables, Lambdas and endpoints. This phase supports one explicit
start, validated atomic finish, finished assignment recovery and descending
rankings. There is no automatic start or gameplay resume on reload. This backend
change does not add frontend changes, TV controls, infrastructure or deployment.

#### Ranking prize collection

For STACK, `POST /v1/public/player` returns a `kind: "ranking"` prize claim only
after a finished, server-scored attempt ranks within `prizeCatalog.ranking.topX`
and the ranking prize's `limit` (when non-null). The prize must have a name or
image; zero limit, no Top X, no prize or no confirmed rank grants no claim.
Ranking tokens use the distinct signed kind `k`, never a player/session token.

The mobile result generates the QR locally, without sending the claim to an
external QR service. `POST /v1/public/redeem` accepts its usual
`{"prizeToken":"..."}` payload, rechecks the active game's current ranking and
prize configuration, then conditionally sets
`type.STACK.rankingPrizeDelivered = true`. Duplicate/concurrent scans return
409 `AlreadyDelivered`; ineligible claims return 400 `NotEligible`. A claim
does not reserve a future rank: eligibility is checked again at collection.

Successful redemption responses also include `prize`: the redeemed catalog entry
with a fresh signed `imageUrl`, or `null` for legacy prizes without catalog data.
The QR validator shows its image and name above the delivery confirmation.
Player responses retain delivered claims even if the rank later changes.

#### Configuration

`POST /v1/game` accepts `gameType: "STACK"` and optional `settings.stack`.
`PUT /v1/game` accepts partial `settings.stack` edits, merged with current
configuration; operator GET by gameId/gameName returns `game.type.STACK`
(not `settings.STACK` or `settings.stack`).
The complete configuration is stored directly in game `type.STACK`, not a
new table. `playMode` is always `mobile` and `joinChannel` is always `web`.
Web registration defaults to required email and a required display username;
phone is optional and Instagram is not collected as participant data.

`GET /v1/public/game?gameId=...` returns **`stackConfig`**:

```json
{
  "initialSpeed": 2,
  "speedIncrement": 0.08,
  "maxSpeed": 5,
  "perfectTolerance": 0.06,
  "initialWidth": 3,
  "initialDepth": 2,
  "layerHeight": 0.24,
  "movementRange": 4,
  "configVersion": "1",
  "skin": "blocks"
}
```

Only initialSpeed, speedIncrement, maxSpeed, perfectTolerance and skin are editable
through `settings.stack`. `skin` is purely visual (not used by score replay) and
must be `blocks`, `pizza` or `baklava`; stored configs without it read as `blocks`.
Numeric fields accept only finite JSON numbers (not booleans,
strings or null). Dimensions, movementRange and configVersion are fixed server
values, not accepted as settings edits. Unknown fields are rejected. Bounds:

| Field | Inclusive range |
|---|---|
| initialSpeed, maxSpeed | 0.1–10 |
| speedIncrement | 0–1 |
| perfectTolerance | 0–0.25 |
| initialWidth, initialDepth | fixed: 3, 2 |
| layerHeight | fixed: 0.24 |
| movementRange | fixed: 4 |

Additionally initialSpeed <= maxSpeed, movementRange must exceed both initial
sides, and perfectTolerance must be less than half the smaller side.
At start the score dispatcher reads server configuration and stores the full
authoritative `configSnapshot` and `configVersion: "1"` inside the attempt.
Configuration edits do not change an already consumed attempt.
Public `stackConfig` and operator `game.type.STACK` always contain the complete
object, including `configVersion: "1"`. Missing/invalid persisted fields are
rejected, never silently replaced by defaults. Creation initializes all fields.

#### Public assignment and start

The `gameType: "STACK"` discriminator selects this assignment shape in both
public join and `GET /v1/public/player`:

```typescript
type StackAssignment = {
  gameType: "STACK";
  playerId: number;
  displayName: string;
  profilePicUrl?: string;
  attempt: null | {
    attemptId: string;
    status: "started" | "finished";
    startedAt: string; // server UTC ISO timestamp
    finishedAt?: string;
    configVersion: "1";
  };
  score?: number; // present after finalization, including zero
  scoreUpdatedAt?: string;
  rank?: number | null; // current rank, null if unavailable
};
```

Email-based display names are masked. Neither token nor assignment contains
gameplay state to resume. Reading metadata/assignment or joining consumes no
attempt. Rejoining an existing email recovers the same participant; it does not
prove unique human identity or prevent registering a different email.
The assignment-level gameType discriminator is added only for STACK; T1MER is
unchanged. An absent profile picture is omitted rather than returned as null.
Existing assignment identity/display fields are preserved. No frontend changes
are included in this backend phase.

An explicit user start consumes the single attempt:

```json
POST /v1/public/score
{"gameId":"ABCDEF","playerToken":"<signed token>","action":"start","attemptId":"<client-generated unique id>"}
```

`attemptId` is the idempotency key (no separate `clientRequestId` field):
1–128 ASCII letters/digits, `_` or `-`, e.g. a UUID. The public Lambda validates
the signed token/game, active game and existing participant, and forwards only
the verified player identity to the existing score dispatcher. Start rejects
extra fields, including supplied score, timer, playerId or configuration.

Success (HTTP 200), including retry of the **same** attemptId:

```json
{"ok":true,"gameId":"ABCDEF","playerId":7,"attemptId":"<same id>","status":"started","configVersion":"1","startedAt":"<server ISO timestamp>","configSnapshot":{"configVersion":"1","initialSpeed":2,"speedIncrement":0.08,"maxSpeed":5,"perfectTolerance":0.06,"initialWidth":3,"initialDepth":2,"layerHeight":0.24,"movementRange":4,"skin":"blocks"}}
```

A single DynamoDB conditional update stores `type.STACK.attempt` only if the
participant still exists and neither attempt nor score exists. Concurrent
different IDs cannot both win. A different ID after consumption returns HTTP
409 `AttemptAlreadyStarted`; the original ID remains idempotent, including
after a lost response or a configuration change. Strongly consistent player
reads recover the persisted `started` state, **not a new or resumed match**.
The start response includes the persisted timestamp and complete configuration
snapshot, including on idempotent retries after an operator changes the game's
settings. The mobile engine must use this snapshot, not potentially stale public
metadata. Public getGame advertises `stackStartSnapshot: true` and
`stackFinishSupported: true` for client capability checks.
Abandonment/reload does not restore the attempt or invent a zero/result.
Invalid/expired/cross-game tokens and inactive games return HTTP 403;
Missing game/participant returns 404; invalid action/id/configuration returns 400.

#### Finish contract, validation and limitations

```json
POST /v1/public/score
{"gameId":"ABCDEF","playerToken":"<signed token>","action":"finish","attemptId":"<same id>","placements":[{"axis":"x","offset":0,"perfect":true},{"axis":"z","offset":-4,"perfect":false}]}
```

No declared score, timer, configuration or extra fields are accepted. The public
layer derives playerId from the token and forwards the history to the existing
`POST /v1/game/score` dispatcher (operator envelope replaces playerToken with
playerId). Each placement must contain exactly axis (`x`/`z`), a finite JSON
number offset (not bool/string/null), and a boolean perfect. Axes alternate
starting with x. Offset is in ±movementRange (1e-9 floating tolerance).

Validation uses only the attempt's stored configSnapshot, even if the operator
has since changed or invalidated current settings. Replay uses JS-equivalent
binary64 rectangle centres/intersections, not subtraction of absolute offsets;
touching edges fail, perfect requires overlap and abs(offset) <= tolerance and
snaps the entire inherited support. Nonperfect successes retain the actual
intersection. Exactly one final failed placement is required, with nothing
after it; score is the number of successes, including valid zero-score games.

The elapsed server time since startedAt must cover, for every layer including
failure, `(movementRange + offset) / min(maxSpeed, initialSpeed + score * speedIncrement)`,
plus 0.22 seconds for every successful layer before spawning the next.
Only a small floating allowance (1 microsecond plus 1e-12 relative) is applied.
This is **not strong anticheat**: a client can still fabricate any mathematically
and temporally plausible history. Tokens bind a participant, not a unique human.

Success (HTTP 200):

```json
{"ok":true,"gameId":"ABCDEF","playerId":7,"attemptId":"<same id>","status":"finished","configVersion":"1","score":1,"scoreUpdatedAt":"<server ISO timestamp>","finishedAt":"<same timestamp>","rank":1}
```

One conditional DynamoDB update writes finished attempt, score and timestamp
together, requiring the correct attemptId, status started, no existing score,
and an existing participant. It stores only the configuration snapshot, a
canonical SHA256 evidence hash (axis, binary64 offset, perfect), and result
metadata—not the placement history. JSON key order/number spelling/negative
zero do not change the hash. No arbitrary score/history-length cap is imposed;
existing API Gateway/Lambda request-size and runtime limits still apply.

Identical finish retries return the stored score/timestamps (rank can change).
Different valid evidence or another attemptId returns 409 AttemptConflict;
finish before start returns 409 AttemptNotStarted. Conditional-write races are
resolved by a strongly consistent read-back: same evidence succeeds, otherwise
conflict. Invalid history/time returns 400 without any write. Retrying start
with the original ID after finish returns the finished result, never another
attempt. A different start ID remains 409 AttemptAlreadyStarted.

`GET /v1/game/score?gameId=...&limit=10` returns `{ok:true,results:[...]}`;
public `GET /v1/public/game` embeds the same results in `ranking`. Both use
`stand_common.stack` and query every page of the existing participant table,
including only nonnegative integer scores with finished status and timestamp.
Ordering is score **DESC**, finished timestamp ASC, then numeric playerId ASC;
ranks are ordinal (1,2,3 even on tied scores). Rows expose playerId, username
(web username or `Jugador <id>`, never an email fallback), existing
profilePicUrl or null, score, finishedAt and rank. No PII identity fields are
copied. Rankings default to 10 with a maximum response limit of 100, but finish
and assignment calculate the participant rank across all finished players.
Ranking reads are strongly consistent per page, not a transactional snapshot
of all players. Finish and recovery still succeed with rank null if its
separate ranking read fails. Ranking work scales with all game participants
(no new index/table), and can become expensive for very large events.

Joining/recovering an existing player returns the finished assignment with
finishedAt, score, scoreUpdatedAt and current rank when available, without
consuming or resuming another attempt. Abandoned started attempts remain
consumed and have no fabricated result or automatic timeout finalization.
T1MER requests without action and EMPAREJA2 assignments retain their contracts.

Offline targeted tests (existing venv, no AWS network):

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_stack_score.py tests\test_stack_integration.py tests\test_public_web.py tests\test_t1mer_score.py tests\test_game_plan_dev.py tests\test_assigners.py tests\test_validators_empareja2.py -q
```

Tests use local table/SDK doubles, including simultaneous starts/finishes and the full
public → dispatcher → assignment recovery flow. They do not certify a live
DynamoDB deployment or a physical mobile browser. The existing Phase 2 frontend
still needs a separate integration to submit finishes and display persisted
results/rankings. Backend Phase 3 does not deploy or modify that frontend.

### General checklist

Follow this sequence to add backend support for a new game type without overengineering.

Frontend counterpart playbook:

- [stand-frontend/docs/new-game-playbook.md](../stand-frontend/docs/new-game-playbook.md)

### 1) Define canonical identifiers

- Frontend id: lowercase (example: `mygame1`)
- Backend game type: uppercase (example: `MYGAME1`)
- Keep naming aligned with frontend mapping (`mapGameTypeToId` in frontend)

### 2) Reuse the existing data model

- Store game-specific payload in `type.<GAME_TYPE>`
- Read with `get_game_type_blob(game_item, "<GAME_TYPE>")`
- Write with `set_game_type_blob(games_table, game_id, "<GAME_TYPE>", payload, updated_at=...)`
- Do not add top-level DynamoDB attributes for type-specific data

### 3) Implement handlers with standard utilities

In each Lambda handler, reuse the common flow from `stand_common.utils`:

- `_get_claims(event)` for auth identity
- `_read_json_body(event)` for request parsing
- `_resp(status, body)` for responses
- `_json_sanitize(data)` (or `_as_int`) before returning DynamoDB-derived values

Keep handlers focused: validate input, execute game logic, persist/retrieve blob, return sanitized response.

### 4) Add/extend API routes in SAM

- Add route/function entries in `template.yaml` only when needed
- Follow existing naming patterns (`StandProd...Fn...Sam`)
- Reuse existing IAM roles when permissions match
- Keep CORS/auth behavior consistent with current API setup

### 5) Keep contract minimal and explicit

For each new endpoint, define:

- expected request body/query params
- success response shape
- error cases and status codes

Prefer extending existing endpoint patterns over introducing a new endpoint family.

### 6) Test before deploy

- Unit test business logic where possible (`pytest tests/`)
- Smoke test Lambda locally (`sam local invoke ...`)
- Validate a real HTTP path in a non-production-safe flow before prod use

### 7) Common mistakes to avoid

- Mismatching frontend `id` and backend `GAME_TYPE`
- Creating custom serialization instead of `_json_sanitize`
- Adding extra tables/attributes for data that belongs in `type.<GAME_TYPE>`
- Creating new roles/routes when an existing one already fits

## Repo

- **Remote:** [stand-backend](https://github.com/adrisanpu/stand-backend)
- **Branches:** `main` (default), `dev` (integration / auto-deploy target)

### Branch flow and merge

| Target | Merge method | Deploy |
|--------|--------------|--------|
| PR → `dev` | **Squash merge** (only method enabled) | On merge: CI deploys `stand-backend-dev` via GitHub Actions |
| `main` | **Squash merge** (same repo setting) | **Manual** `sam deploy --config-env prod` |

CI: [`.github/workflows/ci.yml`](.github/workflows/ci.yml) — `pytest` on PRs; SAM deploy to dev on push to `dev` only. Full workflow (agents, cross-repo): [docs/DEVELOPMENT_WORKFLOW.md](docs/DEVELOPMENT_WORKFLOW.md). Setup: [docs/DEV_ENVIRONMENT.md](docs/DEV_ENVIRONMENT.md#cicd).

To use `main` as the default on GitHub: **Settings → General → Default branch** → switch to `main` → Save. Then you can delete the `master` branch on the remote if desired.
