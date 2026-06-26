# Dev environment setup

This guide covers one-time AWS setup and deploy for the **dev** stack (`stand-backend-dev`). Production uses `sam deploy --config-env prod` unchanged.

## Prerequisites

- AWS CLI and SAM CLI configured for account `us-east-1`
- IAM user **`stand-deploy-cli`** with deploy permissions (CloudFormation, SAM, Cognito read, etc.)
- Access to Cognito User Pool `us-east-1_54eXBuNq1`

### AWS CLI profile (local dev)

Use profile **`stand-deploy-cli`** for all Stand work (`sam deploy`, `aws` scripts). Not the default `dj-os-bedrock-local` user.

One-time setup:

```powershell
aws configure --profile stand-deploy-cli
# Access key + secret for stand-deploy-cli, region us-east-1, output json
aws sts get-caller-identity --profile stand-deploy-cli
# Arn should end with :user/stand-deploy-cli
```

Per terminal session (or before `sam deploy`):

```powershell
cd stand-backend
.\scripts\use-stand-aws.ps1
```

`samconfig.toml` already sets `profile = "stand-deploy-cli"` for dev and prod deploys.

## 1. Secrets Manager

Create secrets (Stripe **test mode** keys recommended for dev):

| Secret name | Contents |
|-------------|----------|
| `stand/dev/stripe` | Stripe secret key (test) |
| `stand/dev/instagram` | Instagram page token (optional; only if testing webhooks/DMs) |

Same JSON shape as the prod secrets under `stand/prod/*`.

## 2. Cognito App Client (dev)

In the **same** User Pool as prod (`us-east-1_54eXBuNq1`):

1. Create app client **stand-dev** (or similar name).
2. **Do not** enable “Generate client secret” — the SPA uses PKCE without a secret; a confidential client causes `invalid_client_secret` on `/oauth2/token`.
3. Enable OAuth: Authorization code grant + PKCE.
4. Callback URLs:
   - `http://localhost:5173/auth/callback`
   - `https://dev.playstand.app/auth/callback`
5. Sign-out URLs:
   - `http://localhost:5173/logout`
   - `https://dev.playstand.app/logout`
6. Copy the **Client ID** into [`samconfig.toml`](../samconfig.toml) (`CognitoAppClientId` under `[dev.deploy.parameters]`), `.env.development`, and Amplify env vars for `dev.playstand.app`.

### Login: `invalid_client_secret`

Cognito returns this when the app client has a **client secret** but the frontend token exchange (PKCE, no secret) does not send one. Fix:

1. Cognito → User pool `us-east-1_54eXBuNq1` → App clients → open the dev client (`5qh9cbsgtmriggl76740o0f5qa` or whatever is in Amplify).
2. If **Client secret** is set, you cannot remove it — create a **new** public app client (no secret), same OAuth settings and callback URLs as above.
3. Update `CognitoAppClientId` in `samconfig.toml`, `VITE_COGNITO_CLIENT_ID` in Amplify (dev app) and `.env.development`, then `sam deploy --config-env dev` and redeploy Amplify.
4. Confirm Amplify dev uses the **dev** client ID, not the prod client (prod clients often have a secret).

## API stage name (`/prod`)

Both stacks use `StageName: prod` (literal) on HTTP APIs so CloudFormation keeps `GameApiprodStage` on prod updates. Dev is isolated by **API Gateway id** (`stand-dev-game-api`) and **DynamoDB** (`stand-dev-*`), not by a `/dev` URL path. Frontend `VITE_API_BASE_URL` must end with `/prod` for both environments.

## 3. Deploy backend dev stack

```bash
sam build
sam deploy --config-env dev
```

Note outputs:

- `GameApiUrl` — use as `VITE_API_BASE_URL` in frontend (ends with `/prod`; same stage name as prod stack, different API host).
- `WebhooksApiUrl` — use in Stripe test / Meta dev app if testing webhooks.

## 4. Frontend local dev

In `stand-frontend`, set `.env.development` (or copy from `.env.example`) with the dev `GameApiUrl` and dev Cognito client ID. Then:

```bash
npm run dev
```

## 5. Amplify (dev.playstand.app)

Connect the `stand-frontend` repo to a separate Amplify app with [`amplify.yml`](../../stand-frontend/amplify.yml). Set environment variables at build time (override localhost redirects):

```env
VITE_API_BASE_URL=https://<game-api-id>.execute-api.us-east-1.amazonaws.com/prod
VITE_COGNITO_DOMAIN=us-east-154exbunq1.auth.us-east-1.amazoncognito.com
VITE_COGNITO_CLIENT_ID=<DEV_CLIENT_ID>
VITE_COGNITO_REDIRECT_URI=https://dev.playstand.app/auth/callback
VITE_COGNITO_LOGOUT_URI=https://dev.playstand.app/logout
```

## 6. Optional: seed data and media

```bash
# Catalog (Empareja2)
python scripts/populate_empareja2_catalog.py --table stand-dev-catalog-table

# Default user media in dev bucket
python scripts/upload_user_media_defaults.py --bucket stand-dev-user-media-<AWS_ACCOUNT_ID>
```

## 7. Webhooks (optional)

- **Stripe (test):** point webhook to `{WebhooksApiUrl}/webhook/stripe`.
- **Instagram:** point to `{WebhooksApiUrl}/webhook/instagram` only if using `stand/dev/instagram`.

## Validation checklist

- [ ] `sam deploy --config-env dev` creates tables `stand-dev-*` and API (stage path `/prod` on dev API host)
- [ ] Login at `http://localhost:5173` hits dev API (check Network tab: dev API host, path `/prod`)
- [ ] Login at `https://dev.playstand.app` works
- [ ] Create a test game; prod tables unchanged
- [ ] `sam deploy --config-env prod` changeset has no resource replacements (only metadata or empty)

## Tear down dev stack

```bash
aws cloudformation delete-stack --stack-name stand-backend-dev --region us-east-1
```

Empty S3 buckets first if delete fails.

## CI/CD

Full cross-repo workflow (branches, CI jobs, prod, Cursor agents): **[DEVELOPMENT_WORKFLOW.md](DEVELOPMENT_WORKFLOW.md)**.

### Branch flow

| Step | Action |
|------|--------|
| Feature work | Open PR **into `dev`** → **squash merge** (repo allows only squash) |
| After merge to `dev` | GitHub Actions runs `pytest` (on PR) then **deploys** `stand-backend-dev` (on push to `dev`) |
| Production | Merge `dev` → `main` when ready; deploy prod **manually**: `sam deploy --config-env prod` |

Frontend/website: PRs to `dev` also use squash merge; `npm run build` runs on PRs. Hosted UI deploy stays on Amplify (branch/app per environment).

### One-time: GitHub Actions OIDC (backend deploy)

1. Deploy the IAM role (us-east-1):

   ```powershell
   cd stand-backend
   .\scripts\setup-github-oidc.ps1
   ```

   Or manually: `aws cloudformation deploy --template-file infra/github-actions-oidc.yaml --stack-name stand-github-actions-oidc --capabilities CAPABILITY_NAMED_IAM --region us-east-1`

2. Copy the stack output `RoleArn`.

3. In GitHub **adrisanpu/stand-backend** → Settings → Secrets → Actions, add:

   | Secret | Value |
   |--------|--------|
   | `AWS_ROLE_ARN_DEV` | Role ARN from step 2 |

4. Workflow [`.github/workflows/ci.yml`](../.github/workflows/ci.yml): `test` on PRs to `dev`/`main`; `deploy-dev` on push to `dev` only. Prod is never deployed from CI.

### Squash merge (all 3 repos, GitHub Free)

Private repos on the free plan cannot use branch rulesets. Use **squash-only** at repo level (applies to all branches, including `main`):

```powershell
cd stand-backend
.\scripts\apply-squash-merge-settings.ps1
```

Already applied if merge commit and rebase are disabled in **Settings → General → Pull requests**.

### SAM config note

`[dev.deploy.parameters]` uses `confirm_changeset = false` so CI can deploy non-interactively. Prod keeps `confirm_changeset = true` for local deploys.
