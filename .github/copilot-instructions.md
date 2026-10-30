# Kivly integration development

Before implementing or changing any Kivly integration behavior, read
[`kivly-integrations/SKILL.md`](kivly-integrations/SKILL.md) and the references
relevant to the task. Treat
[`kivly-integrations/references/openapi.json`](kivly-integrations/references/openapi.json)
as the source of truth for Kivly Public API operations and schemas. Use
`python .github/kivly-integrations/scripts/inspect_api.py search <term>` and
`python .github/kivly-integrations/scripts/inspect_api.py operation METHOD /api/v1/path`
to check the bundled contract; do not invent endpoints, fields, scopes, monetary
units, response envelopes, or retry and idempotency behavior.

For this application's connection, registration, and STACK rewards behavior,
also follow [`docs/KIVLY_CONNECTION.md`](../docs/KIVLY_CONNECTION.md),
[`docs/KIVLY_INTEGRATION.md`](../docs/KIVLY_INTEGRATION.md), and existing
`stand_common.kivly` patterns. Keep internal API behavior consistent with
those documents while validating upstream assumptions against the supplied
Kivly package.

Do not run live Kivly mutations or credential-based probes unless explicitly
authorized for the task and a designated test account. Never expose or log
credentials, tokens, or full participant data.
