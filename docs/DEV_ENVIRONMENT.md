# Dev environment setup

This guide covers one-time AWS setup and deploy for the **dev** stack (`stand-backend-dev`). Production uses `sam deploy --config-env prod` unchanged.

## Prerequisites

- AWS CLI and SAM CLI configured for account `us-east-1`
- Access to Cognito User Pool `us-east-1_54eXBuNq1`

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
2. Enable OAuth: Authorization code grant + PKCE.
3. Callback URLs:
   - `http://localhost:5173/auth/callback`
   - `https://dev.playstand.app/auth/callback`
4. Sign-out URLs:
   - `http://localhost:5173/logout`
   - `https://dev.playstand.app/logout`
5. Copy the **Client ID** into [`samconfig.toml`](../samconfig.toml): replace `REPLACE_WITH_DEV_CLIENT_ID` under `[dev.deploy.parameters]`.

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

## CI/CD (futuro)

- GitHub Actions: `pytest` / `npm run build` en PRs; deploy opcional a dev en merge a `dev`.
- Amplify: rama o app por entorno (`dev` → dev.playstand.app, prod → playstand.app).
- Documentar el flujo en cada repo cuando se implemente.
