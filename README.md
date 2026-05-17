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
| PR → `dev` | **Squash merge** (ruleset) | On merge: CI deploys `stand-backend-dev` via GitHub Actions |
| `main` | Your choice | **Manual** `sam deploy --config-env prod` |

CI: [`.github/workflows/ci.yml`](.github/workflows/ci.yml) — `pytest` on PRs; SAM deploy to dev on push to `dev` only. Setup: [docs/DEV_ENVIRONMENT.md](docs/DEV_ENVIRONMENT.md#cicd).

To use `main` as the default on GitHub: **Settings → General → Default branch** → switch to `main` → Save. Then you can delete the `master` branch on the remote if desired.
