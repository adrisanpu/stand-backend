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

### STACK: mobile `/play` validated finish and ranking (Phase 3)

STACK is registered for operator CRUD and public web join using the existing
game/player tables, Lambdas and endpoints. This phase supports one explicit
start, validated atomic finish, finished assignment recovery and descending
rankings. There is no automatic start or gameplay resume on reload. This backend
change does not add frontend changes, TV controls, infrastructure or deployment.

#### Configuration

`POST /v1/game` accepts `gameType: "STACK"` and optional `settings.stack`.
`PUT /v1/game` accepts partial `settings.stack` edits, merged with current
configuration; operator GET by gameId/gameName returns `game.type.STACK`
(not `settings.STACK` or `settings.stack`).
The complete configuration is stored directly in game `type.STACK`, not a
new table. Only `joinChannel: "web"` is supported. Default participant fields
require email; Instagram is optional (same signed identity/token helpers).

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
  "configVersion": "1"
}
```

Only initialSpeed, speedIncrement, maxSpeed and perfectTolerance are editable
through `settings.stack`. Only finite JSON numbers are accepted (not booleans,
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
{"ok":true,"gameId":"ABCDEF","playerId":7,"attemptId":"<same id>","status":"started","configVersion":"1","startedAt":"<server ISO timestamp>","configSnapshot":{"configVersion":"1","initialSpeed":2,"speedIncrement":0.08,"maxSpeed":5,"perfectTolerance":0.06,"initialWidth":3,"initialDepth":2,"layerHeight":0.24,"movementRange":4}}
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
(Instagram username or `Jugador <id>`, never an email fallback), existing
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
