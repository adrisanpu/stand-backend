# Stand Backend

Backend for the Stand project: AWS Serverless Application Model (SAM) app with Python 3.13 Lambda functions.

## Stack

- **Runtime:** Python 3.13
- **Infrastructure:** AWS SAM (API Gateway HTTP APIs, Lambda)
- **Features:** Webhooks (Instagram, Stripe), game logic, user and billing flows

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

### Independent gameplay and participant access

Game CRUD and public metadata expose `playMode: "master" | "mobile"` independently
of `joinChannel: "instagram" | "web"`. The former selects where gameplay/validation
happens; the latter selects the QR destination (Instagram DM or the web form).
Updates validate compatibility before writing and persist the two access fields
together in one conditional update. Other game settings can be saved without
overwriting either access field.

| Games | Allowed play modes | Allowed join channels |
| --- | --- | --- |
| EMPAREJA2, T1MER | master, mobile | instagram, web (all four combinations) |
| STACK | mobile | instagram, web |
| L3TRAS, RULET4, CUPIDO, SEMAFORO, INFOCARDS, IMPOSTOR | master | instagram |

Legacy records without `playMode` retain the previous behavior: EMPAREJA2/T1MER
infer mobile from web access and master from Instagram; missing channels default
to web for these games and STACK, and Instagram for the remaining games. No table
migration is necessary. Explicit saves persist both fields. Public mobile score
and validation actions reject master-mode campaigns; authenticated operator
endpoints remain available for master gameplay.

For Instagram/mobile campaigns, the existing assignment and optional quiz flow
sends a personal **Jugar ahora** button for the same PSID/player. Repeating the
game code reissues a link without registering a second player or resetting a
STACK attempt. The link has a 15-minute default lifetime (bounded to 1–60 minutes
through `ACCESS_LINK_TTL_SECONDS`) and contains a purpose-separated signed
credential in its URL fragment, never in a public QR. Only a random nonce hash
and issuance timestamp are retained on the existing player record.

`POST /v1/public/access` accepts `{ "gameId": "...", "accessToken": "..." }`.
It checks the signature, expiration, game/player, mobile configuration and PSID identity
and quiz completion, then conditionally consumes the nonce exactly once and
returns the existing `PlayerResponse` session/assignment/prizes shape. Superseded
or used links return 409, expired links 410, and invalid/mismatched credentials
403. `POST /v1/public/player` accepts `{ "gameId": "...", "playerToken": "..." }`
for session recovery without query-string credentials; the old GET remains for
compatibility. Opening or exchanging a link never starts an attempt.

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

**Deployment configuration:** provide a private `PlayerTokenSecret` of at least
32 characters through the authorized deployment configuration; it has no public
default in SAM and is shared by assignment, quiz and public API through Globals.
Personal access fails closed with 503 if the signing configuration is missing
or too short. Do not commit a secret to `samconfig.toml`. Reuse a sufficiently
strong existing secret where available; rotating a key invalidates existing
participant/prize sessions and requires coordinated rollout. `PublicAppOrigin`
must point to the matching frontend environment (`samconfig.toml` distinguishes
dev and prod); personal links require HTTPS except localhost development.

Publish the compatible backend before the frontend. Automated tests cover the
contracts and DM payloads offline; actual Instagram button delivery, fragment
preservation in its in-app browser and the quiz-to-mobile journey still require
an authorized test event before release. This change does not itself deploy
either application or modify any configured secrets.

### Configurable web registration fields (`/play`)

For EMPAREJA2, T1MER and STACK, game POST/PUT accepts `participantFields`:

```json
{
  "email": { "required": true },
  "instagram": { "required": false },
  "phone": { "required": false }
}
```

`required: true` means **request and require** the field; `false` means **do not
request or store** it. There is no optional-but-requested state. Missing
configuration/keys use the defaults shown above (including legacy records without
phone). PUT replaces the field configuration, filling omitted keys from defaults,
not from the previous configuration. Only these three field names and boolean
`required` values are accepted; malformed configuration returns 400 before writes.
Operator game responses and `GET /v1/public/game?gameId=...` expose the resolved
configuration, including phone.

When **Instagram is not requested**, web registration requires a `username`,
including email-only, phone-only, email+phone, and no-contact-field forms.
When Instagram is requested, it serves as the username; the form hides the
separate name and ignores alternate client usernames, even malformed ones.
Username is a display name, not an Instagram handle or identity key: it is
trimmed, limited to 100 Unicode characters and cannot contain control characters
(U+0000–001F, U+007F–009F). Missing, null, empty or whitespace-only names return
`missing_username`; malformed names return `invalid_username` (HTTP 400).
`POST /v1/public/email/request` captures the normalized name in the server-side
challenge and signed registration binding. Verification submits only the challenge
ID and code, never a replacement name. New players store it as top-level `username`;
assignments, player recovery and rankings use it after Instagram, before existing
fallbacks (STACK rankings still never use email). Repeat email registrations
recover the existing player without renaming it, including legacy players without
names. Existing session/access recovery does not require a new name and retains
legacy fallbacks. Usernames are not unique. Photo upload remains optional in every
mode and separate from email verification.

`POST /v1/public/join` accepts `gameId`, `consent: true`, and only enabled fields:
`email`, `instagram` (legacy alias `instagramUsername` accepted), and `phone`.
Required fields must be non-empty strings. Email retains its format validation and
is lowercased; Instagram is trimmed and stored with one leading `@`; phone is
trimmed and stored as the top-level player `phone` string, preserving `+`, spacing
and punctuation (no country-specific format restriction). Disabled incoming values
are ignored, including malformed values, and never persisted. Errors include
`missing_email`, `invalid_email`, `missing_instagram`, `invalid_instagram`,
`missing_phone` and `invalid_phone` (HTTP 400).

An enabled email retains existing duplicate-registration recovery after email
verification (see below). Without email,
joins create distinct players; Instagram/phone are not substitute deduplication
keys. Keep the returned `playerToken` for session recovery. All fields may be
disabled. Existing stored contact data is not retroactively removed by config
changes. Instagram DM registration remains PSID-based and does not require these
web form fields; Instagram-only campaigns still reject public web joins.

### Email verification for web registration

When email is requested, first call `POST /v1/public/email/request` with the same
registration fields (`gameId`, `consent: true`, enabled contact fields), plus
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
the returned `challengeId` and the emailed `code`. Email-enabled joins, including
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
Only public/assign receive `EMAIL_VERIFICATION_TABLE`. The public Lambda alone
can send email, restricted to the configured identity and From address. Both
dedicated roles retain the existing game managed policy to preserve public
session, assignment, media and game behavior; the shared game role receives no
new verification-table or SES permissions.

Public limits default to `EMAIL_MAX_PER_HOUR=5` per email,
`EMAIL_IP_MAX_PER_HOUR=3000` per source IP, and
`EMAIL_GLOBAL_MAX_PER_HOUR=20000` per environment. These abuse controls do not
replace SES account quotas (shared by dev/prod). Existing valid sessions,
Instagram registration and no-email web registration do not require an email
challenge. The existing private `PlayerTokenSecret` also derives a
purpose-separated verification key; no new committed secret is needed.

**Release gate:** set up and verify the shared sender using
[docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md) before enabling email registration.
The identity is managed once by `infra/email-sender.yaml`, never once per
dev/prod stack. Deploy the compatible backend before the new UI, but coordinate
the breaking email-enabled join contract: old forms cannot submit email joins
without a code. Use a maintenance window/controlled event and refresh cached
clients rather than leaving old email forms live. No automatic production
deployment or SES provisioning is performed by this change.

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
Player responses retain delivered claims even if the rank later changes.

#### Configuration

`POST /v1/game` accepts `gameType: "STACK"` and optional `settings.stack`.
`PUT /v1/game` accepts partial `settings.stack` edits, merged with current
configuration; operator GET by gameId/gameName returns `game.type.STACK`
(not `settings.STACK` or `settings.stack`).
The complete configuration is stored directly in game `type.STACK`, not a
new table. `playMode` is always `mobile`; `joinChannel` supports `web` and
`instagram`. Web registration defaults to required email with Instagram/phone disabled;
Instagram registration uses the existing PSID identity and personal access flow.

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
(Instagram username, web username, or `Jugador <id>`, never an email fallback), existing
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
