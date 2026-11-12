# Stand Backend

Backend for the Stand project: AWS Serverless Application Model (SAM) app with Python 3.13 Lambda functions.

## Editable Home and mobile gameplay

The stand renders only a Home canvas; gameplay runs through participant-token
endpoints on phones. L3TRAS gathers codes on one phone; SEMAFORO and CUPIDO validate
from one participant's phone; SEMAFORO's cross quiz alternates turns there.
INFOCARDS serves participant-authorized swipeable cards. CUPIDO remains development-only.
`POST /public/player` with `action: "play"` uses revision/request IDs for retries;
the server owns timing, validation, results, rewards, and private-answer evaluation.

Operator canvas configuration uses `GET /v1/game?gameId=...&home=true` and
`PUT /v1/game` with `{gameId, homeCanvas, expectedHomeRevision}`. Both the minimal
and regular game reads expose `homeCanvas`, `homeRevision`, signed
`homeBackgroundUrls`, and read-only state. Canvas revisions are independent of
form/questionnaire revisions. New events persist empty landscape/portrait
compositions; legacy events without the field receive deterministic editable defaults.
Positions/sizes use integer grid units, with bounds, minimum sizes, unique widget
types (except text), and no overlaps enforced on the server.

The existing user-media API has a separate `home-background.items` library.
Upload and register with `kind: "home-background"`; this never selects a logo.
Images are limited to 5 MB; MP4/WebM videos to 100 MB. Registration checks the
owner prefix and S3 object type/size; canvas selection checks ownership and plan.
Backgrounds and custom colors require advanced customization, while layout/text
remain available to all plans. Persist media keys, not signed URLs.
Individual items can also store `backgroundColor` as `"transparent"` or a
six-digit hex color on every plan; omission preserves the existing widget style.

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
python scripts/check_common_layer.py .aws-sam/build/StandCommonLayer

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

## Participant schema and dev migration

Game participants use `verificationMethod` (currently `email-code`) and
`verifiedAt` (an ISO timestamp). These describe identity verification, not game
completion: `validated`, `validatedAt` and the four-digit `validationCode` retain
their game-specific meaning. The six-digit email OTP is separate, expires after
10 minutes, and is stored as a hash in the email verification table.

All new participants join through the web; `joinSource` is no longer stored or
required for authenticated photo uploads. Kivly participant data is grouped in
one DynamoDB Map:

```json
{
  "kivly": {
    "membership": {
      "customerId": "...",
      "locationId": "...",
      "consentVersion": "...",
      "consentedAt": "...",
      "phone": "...",
      "status": "confirmed"
    },
    "recovery": {
      "run": "...",
      "disposition": "...",
      "duplicateOf": 1
    }
  }
}
```

Only existing optional fields are retained; recovery is omitted when absent.
The contact phone and membership phone are intentionally separate. Membership
updates preserve recovery metadata and use optimistic concurrency. Reward email
requires an `email-code` verification method, timestamp and valid email address.

`scripts/migrate_gameplayer_schema.py` migrates only
`stand-dev-gameplayer-table` in account `781283585341`, region `us-east-1`.
It defaults to a paginated, read-only preflight:

```bash
python scripts/migrate_gameplayer_schema.py --profile stand-deploy-cli
python scripts/migrate_gameplayer_schema.py --profile stand-deploy-cli --apply
```

There is **no legacy runtime fallback**. Before `--apply`, create and verify an
on-demand DynamoDB backup and pause participant operations and asynchronous
Kivly/prize workers. Deploy the new dev backend and migrate while they remain
paused. Each item update atomically renames verification fields, merges Kivly
metadata, removes obsolete attributes and checks their previously read values.
Conflicting formats or values stop the migration; a partially applied run can be
rerun after resolving the cause. The final consistent scan compares all records
with their expected migrated values without logging participant data.

Resume operations only after deployment and migration verification succeed.
Keep the backup until the cutover is accepted. Rollback requires coordinating
old code with old-schema data, not merely redeploying old code. Production is
excluded: a future production deployment needs its own coordinated migration.

## Structure

- `template.yaml` – SAM template (APIs, Lambdas, roles)
- `src/` – Lambda source code (one folder per function)
- `samconfig.toml` – SAM deploy configuration
- `layers/stand_common/` – Shared Python utilities (log, _resp, get_game_type_blob, set_game_type_blob, etc.)

### Shared layer packaging

`StandCommonLayer` uses SAM's Python builder with
`ContentUri: layers/stand_common/python/`. Keep its dependencies in
`layers/stand_common/python/requirements.txt`, next to `stand_common/`.
SAM adds the outer `python/` directory: the built layer must contain
`python/stand_common/`, never `python/python/stand_common/`.
Build before packaging/deploying and run `scripts/check_common_layer.py`
against the built layer directory. CI runs this check before deployment.
Native dependencies must target Lambda Linux x86_64/Python 3.13, not Windows.

## Required initial configuration review

New games persist `game.setupReview` with `version: 1`, an opaque `revision`,
`confirmedSections` and `completed: false`. Owner list/get/configuration and
creation responses return it; legacy games without the marker are exempt.
The six section IDs follow UI order: `appearance` (Home), `configuration` (Game),
`form`, `questionnaire`, `conditions` (Prizes), `prizes` (Raffle).

Owner-authenticated `PUT /v1/game` accepts
`{gameId, setupReview: {section, confirmed, expectedRevision}}`.
Every mutation returns the new revision in `{ok, gameId, setupReview}`.
After all six confirmations, explicitly finish with
`{gameId, setupReview: {finish: true, expectedRevision}}`.
Stale revisions return 409 `SetupReviewChanged`; premature finish returns
409 `SetupReviewIncomplete`; legacy updates return 409 `SetupReviewNotRequired`.
Unconfirming a panel clears completion. Existing ownership and lifecycle checks
apply. This persists operator review only: it does not introduce a publication
state or block public participant access.

## Event forms and participant surveys

Owner-authenticated `GET /v1/game?gameId=...&form=true` reads the presentation,
questions, registration fields, edit lock and revision. `PUT /v1/game` accepts
`{gameId, form: {formConfig, participantFields, questions, expectedRevision}}`.
The server checks ownership, active lifecycle, plan, limits and optimistic
concurrency. `formConfig` is event-wide presentation plus a single `quizMoment`:
the event retains its shared logo, banner, colors and theme, which are
used by the event home, gameplay, registration and questionnaire. The normal
owner `GET /v1/game?gameId=...` also returns `formConfig`, `formLogoUrl` and
`formBannerUrl` alongside `game`, so the event home uses the same saved settings.
Media URLs are signed on reads; persisted settings continue to contain only keys.
`disabled`, `after_registration`, `after_game`, `prize` or `raffle`.
It stores owned image keys, never signed URLs. After-game placement is rejected
for INFOCARDS and IMPOSTOR until a suitable final participant event is supported.

Questions retain `quizOrder`/`quizQuestions`. A participant submits all answers
using `POST /v1/public/player` with
`{gameId, playerToken, action: "quiz", answers, revision}`. The transaction locks
the questionnaire on the game and writes `quizAnswers`, `quizCompleted` and
`quizCompletedAt` in `type.<GAME>` without replacing gameplay progress.
Question/moment changes are rejected after any answers exist; presentation
changes do not reset answers. Identical retries are idempotent.
The normal player response includes the questionnaire and completion state.
Draft-only AI generation uses `POST /v1/game/quiz/generate` with `draftOnly: true`;
legacy writers cannot replace questionnaires managed through the new editor.

Prize surveys preserve earned awards and inventory reservations but withhold
usable claim tokens, wallet passes, STAND collection, Kivly assignment and reward
email until completion. The existing durable outbox retains pending work; its
dispatcher resumes delivery after completion without requiring the browser to
stay open. Already uncertain external writes retain their reconciliation rules,
and already delivered prizes are not revoked. Raffle surveys filter both native
and STACK draws regardless of the operator's validated-only filter.

Owner-authenticated `GET /v1/game/quiz?gameId=...&analytics=1` returns the selected
event's questions and participant responses for the Respuestas panel. It follows
all participant query pages within the selected game partition, not a table
scan. The UI filters/paginates that complete dataset and exports the selected
results with CSV formula escaping. There are no visit/abandonment events,
cross-questionnaire insights, new tables or new external API operations.
Deploy the changed functions and shared layer together; no deployment or live
Kivly mutation is performed by local verification.

## Account profile

Authenticated `GET /v1/user/me` includes optional `name`, `profilePhotoKey`
and a one-hour signed `profilePicUrl`. `PUT /v1/user/me` accepts exactly
`{name, profilePhotoKey}`: a trimmed name of 1-100 characters and either an
owned uploaded photo key or `null` to remove the photo. The update preserves
email, plans, integrations and game branding; it does not edit Cognito attributes.

Request a photo upload through `POST /v1/user/media?action=upload-url` with
`files: [{kind: "profile-photo", fileName, contentType, sizeBytes}]`, then PUT
the binary to its signed URL and submit the returned `key` with the profile.
JPEG, PNG and WebP files must be nonempty and at most 5 MB. The profile update
checks object ownership, stored content type and size before saving. Photos
use `users/<sub>/profile-photos/`, separate from logos and legacy game avatars.
Removing a photo clears its account reference; it does not delete the S3 object.
Only the key is persisted, not the signed URL. Deploy the user/media functions
and SAM route/environment changes together.

## Data model: game-table `type` column

The game table (`stand-prod-game-table` in prod, `stand-dev-game-table` in dev) uses a generic **`type`** attribute (DynamoDB Map) for game-type-specific configuration, mirroring the pattern used in **stand-prod-gameplayer-table**. This avoids adding a new top-level column for each new game type.

- **Structure:** `type.<GAME_TYPE>` where `GAME_TYPE` is the same value as `gameType` in uppercase (e.g. `INFOCARDS`, `EMPAREJA2`).
- **Example:** For an INFOCARDS game, cards are stored under `type.INFOCARDS.cards`; the blob can include other keys (e.g. `sourceUploadKey`, `generatedQuizGameId`) as needed.
- **Usage:** Use the helpers from `stand_common.utils`: `get_game_type_blob(game_item, "INFOCARDS")` to read and `set_game_type_blob(games_table, game_id, "INFOCARDS", {"cards": [...]}, updated_at=...)` to write. New game types that need type-specific blobs should follow the same pattern and reuse the `type` column instead of defining new top-level attributes.

## Owner game listing

Authenticated `GET /v1/game` queries the current owner's index and reads all
DynamoDB pages before sorting and returning `{ok, ownerUserId, gameType, count,
games}`. Each listed game includes its existing `createdAt` when present and
`playerCount` from the persisted `playersCount` counter (zero when absent, as
in the existing game stats). No per-event roster queries are needed.
The list also includes `isActive` (legacy default true), `lifecycleState`
(`active`, `closing`, `inactive`), and `stackPrizeState` for STACK. Archived
events are excluded.
The optional `gameType` filter applies to the complete result. A failed
later page returns an error rather than a successful partial list. The response
contract, ownership checks and plan rules are unchanged; no new resources or
data migration are required.

The administration's **Mis partidas** view uses this existing endpoint.
Publish the updated game-management Lambda before relying on a complete
multi-page list in the frontend.

## Per-event administration settings (delivery 1B)

Owner-only `GET /v1/game?gameId=...&configuration=true` returns the usual owned
game metadata plus `configuration` (the existing type-specific blob) and an
opaque `rewardConditionsRevision`. It permits reading legacy inactive events,
not archives/deleted events or another owner's event. Effective STACK/RULET4
settings remain under `game.type`. No selection from browser storage is needed.
Existing mechanic PUTs reject inactive events; affected mechanic writes also
condition on activity/ownership at commit, so a concurrent close cannot silently
accept a configuration update.

`PUT /v1/game` with exactly `{gameId, rewardConditions: {rankingTopX,
expectedRevision}}` updates only an existing T1MER ranking reward's top positions
(integer 1–100). It cannot create a reward or edit its name, image or limit.
The revision hashes the stored catalog, not signed image URLs. Conflicts return
409; success includes the new `rewardConditionsRevision`.

STACK's existing `/v1/game/stack/prizes` GET includes `programRevision`.
Its PUT accepts `{gameId, action: "rules", expectedRevision, rules}`: validate
existing rules while retaining the current catalog, inventory consumption,
delivery channel and existing awards. The operation uses the usual optimistic
transaction and must target an active, open event. Missing prizes, invalid rules,
identity reuse and disabled plan features keep their existing validation.
Success returns the saved program and next revision. Revision mismatches return
`RewardConditionsChanged`; transactional races return `StackPrizeConflict`.
The new catalog client uses `{gameId, action: "catalog", program}`; the server
takes rules from its current snapshot instead of trusting stale client rules.
Native catalog saves with `preserveRewardConditions: true` similarly retain the
latest existing T1MER top, with a catalog comparison at write time.
The legacy whole-program API remains compatible; there is no new rule engine.

INFOCARDS persists optional `cardLayouts` (`top/left/right/bottom`, keyed by card
ID) alongside cards in `type.INFOCARDS`, instead of leaving these edits only in
one browser. Regeneration preserves other blob settings. Editing, media upload
requests and AI generation reject inactive events, with active write guards on
card/quiz commits. Reads remain available for the administration's read-only
view. Registration, catalogs and integrations are not part of mechanic saves.
No tables, dependencies, services or automatic deployment are added. Deploy the
game handler/shared layer and affected INFOCARDS/quiz functions together with
the compatible frontend.

## Prize delivery and Wallet (delivery 1C)

Prize catalogs and manual raffles are managed within each event's settings
overlay, not from the operational home. Native validation and manual-raffle
awards are persisted when their result is confirmed; ranking awards that
depend on the final leaderboard are persisted during event closure. Available
STAND awards enter the durable delivery outbox. Email failure never invalidates
the award or its individual QR claim.

The delivery worker emails the verified participant address automatically for
native STAND awards across all web-enabled game types. Emails include Apple
Wallet and Google Wallet links when their credentials are configured.
`StackRewardEmailEnabled` controls prize notification emails and defaults to
disabled. Participants can retry a failed email; the award and QR remain
available regardless of email or Wallet-provider failures. Kivly awards keep
their Kivly Wallet link and use the same notification format.

### Shared prize catalog

The authenticated owner API exposes `GET`, `PUT` and `POST /v1/prizes/catalog`.
The catalog is stored on the owner's user record, so definitions are shared
across that owner's games; it is not a cross-owner/global catalog. `GET`
returns `{ok, catalog, revision}` with fresh signed image URLs. `PUT` accepts
`{catalog, expectedRevision}` and uses optimistic revision checks; entries are
`{id, name, imageKey}`. Existing event prize names/images are migrated on first
access and merged only when both name and image key match exactly. Shared
definition edits propagate to linked event assignments while preserving each
event's limits, ranking positions and STACK stock. Deleting a referenced
definition returns `SharedPrizeInUse`.

`POST` accepts `{action:"upload-url", contentType:"image/png", sizeBytes:N}`
and returns an owner-scoped S3 PUT URL. Images are bounded to 5 MiB and checked
as PNG before association. Catalog saves validate new or changed image keys;
unchanged images on existing entries are not downloaded again when deleting
or renaming prizes. Owner-scoped key checks still apply to every image.
A failed propagation after a successful catalog
write returns `SharedPrizeCatalogSyncFailed`; retrying the catalog read
reapplies the idempotent synchronization. STACK STAND items retain their
shared-prize identity when legacy prize programs are upgraded. Deploy the game
handler, shared layer, routes and media-bucket permissions together; no
deployment is performed by these local changes.

### General prize inventory and activation

`PRIZE_INVENTORY_TABLE` points to `stand-${Environment}-prize-inventory`.
There is **one table per environment**, shared by all operators, partitioned by
authenticated `ownerId`; it is not one table per operator. PITR and retention
are enabled. `CONTROL`, `BAL#`, `MOVE#`, `REQUEST#` and `IMPORT#` sort-key
prefixes hold cutover state, balances, movements, idempotency receipts and
resumable import progress. Inventory/history survive event deletion.

The inventory applies to both STAND shared prize IDs and Kivly identities
derived from location/reward IDs. Available units are consumed atomically
**when assigned**, alongside the award and event cap. STAND redemption updates
the assignment and redeemed count once, without another stock debit. A Kivly
earn acknowledgement or a Wallet/email delivery is not evidence of collection:
Kivly `canjeados` is `null` ("Sin confirmar"). Failed/uncertain external writes
and failed emails never return units automatically.

Event caps are additional limits, not reservations. STACK `stockTotal` accepts
`null` (no event cap), `0` (disabled) or an integer up to 1000; native `limit`
keeps its existing nullable contract. Existing explicit caps are retained.
When the same general prize has several local assignments, game associations
are counted once and shown active if any assignment is enabled; the displayed
cap is the sum of those local caps (or unlimited if any is unlimited).
Uninitialized/exhausted general stock or an exhausted cap omits that prize,
preserves valid alternatives and does not stop gameplay. STACK allocation
records expose `inventoryReasons`; eligible native player responses expose
`prizeUnavailable: [{condition, reason}]`. Historical native award identities
are reused when shared definitions acquire an ID or change name/image.

Owner endpoints:

- `GET /v1/prizes/inventory`: `{ok, mode, catalog, unresolved, imports}`.
  Each catalog item contains `inventory` (available, initialized, assigned,
  redeemed, historicalAssigned, historicalRedeemed, revision), `canjeados`
  and game associations `{gameId, name, active, limit}`. Active associations
  exclude archived/closing events, disabled conditions and zero caps.
- `GET /v1/prizes/inventory?prizeId=ID&cursor=...`: paginated
  `{ok, items, cursor}` movements, including assignment-unit states and event
  snapshots. Cursors are bound to owner and prize.
- `POST /v1/prizes/inventory` with
  `{action: "initialize"|"replenish"|"adjust", prizeId, quantity, reason,
  requestId, expectedRevision}` records an audited change. Initialize uses an
  absolute available quantity; replenish adds positive units; adjust applies a
  signed delta and cannot make stock negative. Reuse the same request ID and
  body after an uncertain response; a changed payload with that ID conflicts.
  Revision conflicts require reloading, not silently replacing the balance.
- `POST` with `{action:"import", gameId, cursor?}` imports up to 20 award or
  legacy participant rows per page, returning `{ok, cursor, unresolved}`.
  Continue with the **opaque returned cursor** until `null`; skipped cursors
  are rejected. Retrying the last processed cursor after a lost response returns
  its stored page result without advancing or recounting. Repeating from the
  beginning is safe. The prize panel automatically walks every pending game's
  pages on opening, including archives and games without a current catalog;
  completed imports are skipped and errors remain visible. Confirmed old player
  flags and legacy Kivly reservations are projected without issuing external
  requests or debiting manually initialized stock. Old QR tokens resolve to
  the same awards; missing historical collection dates are not invented.
- `POST` with `{action:"activate"}` explicitly switches the operator to general
  consumption. It requires initialized associated prizes and completed,
  reconciled imports for owned games. A transactional mode check prevents an
  in-flight legacy concession from committing across cutover.

  The maximum supported grouped assignment (8 prizes x 10 units) stays within
  DynamoDB's 100-operation limit, including the legacy player snapshot guard.
  When inventory infrastructure is enabled, new legacy Kivly completions use
  the authoritative award/outbox rather than an additional duplicate reservation;
  existing reservations remain readable and are imported without replaying earn.

Rollout is coordinated and **not automatic**: deploy the table, IAM, routes,
shared layer and all award/claim/delivery writers together, let old invocations
drain, then publish the compatible frontend. Until explicit activation, linked
new awards are historical projections and existing consumption stays in legacy
mode. Open Administration > Premios, initialize **free units for new awards**
(exclude units already promised to outstanding awards; never sum old per-game
stocks), import every game including archives, resolve
unmatched historical definitions using exact identity evidence, then activate.
Unmatched rows block activation; do not merge on a similar name. Historical
assigned/redeemed counts remain separate from post-cutover counts. A later
new, uninitialized prize cannot be assigned until initialized.

Deleting shared definitions with initialized stock or history is rejected.
Permanent event deletion first projects any remaining award history and keeps
shared catalog images. Operator reads use the owner game index and scoped
inventory queries; movement history is paginated, with no global inventory
scan. These changes do not authorize deployment or frontend publication.

Wallet credentials are optional and must be stored outside the repository in
AWS Secrets Manager as `stand/${Environment}/wallet`. The secret is a JSON
object with `apple` and/or `google` properties:

```json
{
  "apple": {
    "certificateP12Base64": "<base64-encoded pass certificate>",
    "certificatePassword": "<optional certificate password>",
    "wwdrCertificatePem": "<Apple WWDR certificate PEM>",
    "passTypeIdentifier": "pass.example.stand",
    "teamIdentifier": "<Apple team identifier>",
    "organizationName": "STAND"
  },
  "google": {
    "issuerId": "<Google Wallet issuer ID>",
    "classId": "<pre-created generic pass class ID>",
    "serviceAccountEmail": "<service-account>@<project>.iam.gserviceaccount.com",
    "privateKeyPem": "<service-account private key PEM>"
  }
}
```

Only configure credential blocks for providers that are ready. Create and
validate provider-side pass identifiers/classes before enabling that provider.
The public pass endpoint reads this secret using `StandPublicRole`. Credentials
must never be put in source control or frontend configuration. Configure and
verify the SES sender identity separately.

Deploy the backend stack, including the shared layer, public API, validation
and raffle functions, and delivery worker, before publishing the matching
frontend. This delivery does not deploy automatically.

## Clients and prize history (delivery 1D)

The authenticated owner API exposes `GET /v1/clients?limit=25&cursor=...`
and `GET /v1/clients/{clientId}`. The first returns a paginated client list;
the second returns that client's participant records across all the owner's
games and persisted prize, email and Kivly-assignment states. Access is scoped
to the authenticated `ownerUserId`. Client IDs are HMAC-derived from the
normalized, verified email and owner; names never create or merge identities.
Pages default to 25 unique clients and never exceed 25, even when an older
frontend requests 50. Continuation cursors load the next page; participant and
prize history is fetched only when a client is opened, not for the entire list.
The Gmail-style directory uses `GET /v1/clients?page=1&search=...` and returns
`page`, `pageSize: 25` and the exact filtered `total`. Name/email/phone searches
are owner-scoped and case/accent-insensitive, with normalized phone matching.
It queries all profile pages of that owner's `owner-clients` index to compute
the exact count and search results, but returns only the requested 25 summaries.
It never scans participant tables or loads award history for this search.
This makes read cost linear in the owner's client directory; large directories
would need a dedicated search index/counter rather than filtering participants.
The cursor endpoint remains available for older clients.

`StandClientIndexTable` stores the derived profile and per-game participant
references. New verified web registrations maintain the index; participant
removal and permanent event deletion remove their references. Archived games
remain in history. Historical population is an explicit, resumable migration,
not part of deployment or API reads. From the backend root, first preview:

```powershell
python scripts/migrate_client_index.py --owner-id <owner-id> --checkpoint <path>
```

After reviewing the counts, run the same command with `--apply` to write the
derived index. The script requires the deployed table names and
`PLAYER_TOKEN_SECRET` in its environment, uses only verification markers or an
exact existing per-game email mapping, and skips unresolved duplicate or
unverified records. Re-running the same checkpoint is idempotent.

The API reports a STAND award separately from email status and STAND collection.
For Kivly it reports only the external-assignment state already stored in STAND;
it does not query Kivly or claim that an externally assigned reward was
physically redeemed. `accepted` email state means the provider accepted the
send request, not inbox delivery. The endpoints expose no claim tokens.

## Closing, reactivation and archival

Owner-only `PUT /v1/game` with exactly `{gameId, isActive}` is the common
lifecycle action for every game. `false` atomically stops admission and
starts a durable closure job in the existing prize outbox. The response
includes the persisted `game.isActive` and `game.lifecycleState`; a successful
request can still be `closing`. STACK accepts finishes for attempts already
started. Other mechanics settle confirmed validations, configured T1MER
rankings and already drawn raffles, not all registered players. No automatic
raffles or new game/reward rules are introduced.

The worker leaves the event `closing` until award calculations finish.
Native rewards are allocated, not marked collected. Kivly assignment, email
acceptance and actual collection remain separate, recoverable states.
Existing local prizes are snapshotted into the existing award table; supported
public participant flows expose `nativePrizes` with individually redeemable QRs.
This does not add web gameplay to unsupported game types.

`true` is allowed only after calculations finish. It preserves players,
scores, consumed attempts, previous awards and consumed stock. A later closure
can reward new winners without repeating a reward for the same player.
Explicit multi-unit allocations still issue one QR per unit. Unique closure
cycles and optimistic game revisions prevent stale jobs and concurrent
requests from reopening or settling a different cycle. Legacy Kivly completion
reservations are retained: confirmed or uncertain upstream writes are never
replayed by conversion to the outbox.

Owner-only `DELETE /v1/game` now **archives**, returning
`{gameId, message: "Game archived successfully"}`. It requires `inactive` and
no outstanding award-calculation jobs; active/closing events return 409.
Legacy inactive events first enter the closure queue and return 409: reload
and retry archival after settlement. `archivedAt` hides an event from the normal owner
listing and prevents reactivation/configuration changes, but retains metadata,
participants, awards, queued deliveries and valid prize-collection access.
Archival is not a personal-data purge and does not free its identifier or name.

`GET /v1/game?archived=true` lists only the authenticated owner's archived
events, including every query page. The default (or `archived=false`) lists
current events. `PUT /v1/game` with exactly `{gameId, archived:false}` restores
an archived event **inactive**, preserving players, attempts, results, stock
and awards. Activation remains a separate action.

Permanent deletion uses owner-only `DELETE /v1/game` with exactly
`{gameId, permanent:true, confirmation:<exact gameName>}`. It requires archival,
inactivity, no pending prize jobs, no uncollected local awards, confirmed
external assignments, accepted email deliveries where applicable, and no
uncertain event-specific registration/completion writes. This does not claim
to know whether an externally assigned Kivly reward has been physically redeemed.
External Kivly customers/rewards, shared customer idempotency records, account
settings/logos and global rate limits are never deleted.

After checks, `deletionStartedAt` permanently prevents restoration while the
handler deletes event-specific verification/registration records, email identity
mappings, players, awards/jobs and media under this event's exact photo/prize/staging
prefixes and `infocards/{gameId}/` in the INFOCARDS upload bucket. Shared content
is untouched. Metadata/configuration writers reject archived/deleted events
at write time, including delayed quiz/INFOCARDS generation and both steps of
the type-blob helper, so stale requests cannot restore deleted event content.
Auxiliary tables have no event index; this infrequent owner action
uses paginated filtered scans, with no new index or service. Cleanup is bounded
to 18 seconds/500 steps per request. A partial failure or `GameDeletionInProgress`
is not success: reload and explicitly repeat the same confirmed deletion.
Metadata is retained until completion; the final response is
`{gameId, message:"Game permanently deleted successfully"}`. Only a technical
`{gameId,deletedAt,isActive:false}` tombstone remains, without owner/event/player
data, reserving the join code so old signed links cannot target a future event.
Previously issued upload URLs can still write until their normal expiry;
staged-prize lifecycle retention remains in force. This is event removal, not
a regulated-erasure guarantee, and does not remove backups or AWS logs.

Deploy the shared layer, game/public/score/validate/raffle functions, worker
and its SAM permission update together before publishing the matching frontend.
The worker uses the existing queue and one-minute recovery schedule; closing
is asynchronous. No new table, dependency or external service is required.

## Adding a new backend game (lean checklist)

### Participant gameplay and access

Every game uses `playMode: "mobile"` and `joinChannel: "web"`: gameplay and
validation happen on participant phones, reached through the Web registration
form and its QR. Game create/update APIs reject other mode/channel values and
persist both fields together. Existing records are exposed as mobile/Web without
a table migration; configured participant and plan limits are unchanged.

Instagram is not a participant registration field or game access channel.
Participant registration uses a required full name (`username`), mandatory verified
email and mandatory phone in every game. Existing Instagram values remain in stored records but are
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
  "phone": { "required": true, "enabled": true },
  "address": { "enabled": false },
  "company": { "enabled": false },
  "jobTitle": { "enabled": false },
  "birthDate": { "enabled": false }
}
```

Email is always required and verified, including legacy games stored with
`email.required: false`. Phone is always enabled and mandatory, including legacy
disabled/optional configurations. Writes cannot disable email or phone. The four
optional profile fields are collected only when enabled and supplied, validated
and bound to the verification challenge; they do not modify Kivly customers.
Registration starts with email, then missing identity/details and consent, then
verification. Complete returning participants are not asked to fill optional fields.
Public metadata exposes field configuration, never profile values; authenticated
administrative responses/CSV include those values. The game configuration Lambda tolerates the historical
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
    "validation": { "name": "Gift", "limit": 10, "sharedPrizeId": "catalog-id" },
    "ranking": { "name": "Top prize", "limit": null, "topX": 3, "sharedPrizeId": "catalog-id" },
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
`sharedPrizeId` selects a definition from the authenticated owner's shared
catalog; the backend resolves its canonical name/image while retaining the
event-specific limit and ranking position. The legacy event-scoped image
upload flow remains supported for non-shared entries.
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
