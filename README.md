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

# Deploy (prod)
sam deploy --config-env prod
```

Config for deploy is in `samconfig.toml` (stack: `stand-backend-prod`, region: `us-east-1`).

## Structure

- `template.yaml` – SAM template (APIs, Lambdas, roles)
- `src/` – Lambda source code (one folder per function)
- `samconfig.toml` – SAM deploy configuration
- `layers/stand_common/` – Shared Python utilities (log, _resp, get_game_type_blob, set_game_type_blob, etc.)

## Data model: game-table `type` column

The table **stand-prod-game-table** uses a generic **`type`** attribute (DynamoDB Map) for game-type-specific configuration, mirroring the pattern used in **stand-prod-gameplayer-table**. This avoids adding a new top-level column for each new game type.

- **Structure:** `type.<GAME_TYPE>` where `GAME_TYPE` is the same value as `gameType` in uppercase (e.g. `INFOCARDS`, `EMPAREJA2`).
- **Example:** For an INFOCARDS game, cards are stored under `type.INFOCARDS.cards`; the blob can include other keys (e.g. `sourceUploadKey`, `generatedQuizGameId`) as needed.
- **Usage:** Use the helpers from `stand_common.utils`: `get_game_type_blob(game_item, "INFOCARDS")` to read and `set_game_type_blob(games_table, game_id, "INFOCARDS", {"cards": [...]}, updated_at=...)` to write. New game types that need type-specific blobs should follow the same pattern and reuse the `type` column instead of defining new top-level attributes.

## Repo

- **Remote:** [stand-backend](https://github.com/adrisanpu/stand-backend)
- **Default branch:** `main`

To use `main` as the default on GitHub: **Settings → General → Default branch** → switch to `main` → Save. Then you can delete the `master` branch on the remote if desired.
