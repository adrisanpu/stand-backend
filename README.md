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

## Repo

- **Remote:** [stand-backend](https://github.com/adrisanpu/stand-backend)
- **Default branch:** `main`

To use `main` as the default on GitHub: **Settings → General → Default branch** → switch to `main` → Save. Then you can delete the `master` branch on the remote if desired.
