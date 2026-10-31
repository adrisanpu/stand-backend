# Participant email sender setup (manual)

The participant email-code provider is Amazon SES in **us-east-1**, sending from
**contact@playstand.app** in dev and **no-reply@playstand.app** in prod.
This runbook requires an authorized AWS operator and the owner of
`playstand.app` DNS. Repository changes do not provision, verify or deploy
anything automatically. Production remains a manual, approved release.

## Returning participants

`POST /v1/public/email/request` accepts `mode: "resume"` with `gameId`, `email`
and optional `language`. A recoverable participant in that same game receives
the usual email challenge directly. Otherwise the response has
`detailsRequired: true`, `existingPlayer` and `fields` flags for `username`,
`phone`, `consent` and `loyaltyConsent`. No profile values or player credentials
are returned before verification. New participants complete the regular signup;
existing participants submit only the requested fields in resume mode.

Existing email-code verification is evidence of the original privacy acceptance.
Legacy records without that evidence require acceptance again. Club acceptance
must match the current version and location. Missing profile fields are written
only after successful code verification; existing names, photos and game progress
are preserved. Resending uses the same endpoint, cooldown and quotas. The
different next screens necessarily disclose whether an email has a recoverable
registration; they do not grant access without the code.

## 1. Establish one shared domain identity

Use AWS profile `stand-deploy-cli` in the same account as the backend. First
inspect SES identities in us-east-1. If `playstand.app` already exists, reuse
it and agree on its owning stack; do not attempt to create it again. Otherwise,
after approval, deploy **once**, independently of dev/prod:

```powershell
aws cloudformation deploy --template-file infra\email-sender.yaml --stack-name stand-email-sender --parameter-overrides EmailIdentityName=playstand.app --region us-east-1 --profile stand-deploy-cli
aws cloudformation describe-stacks --stack-name stand-email-sender --query "Stacks[0].Outputs" --region us-east-1 --profile stand-deploy-cli
```

`infra/email-sender.yaml` creates an `AWS::SES::EmailIdentity` with 2048-bit Easy
DKIM and outputs three `DkimDNSTokenNameN`/`DkimDNSTokenValueN` pairs (N=1..3).
Publish all three as **CNAME** records at the domain's DNS provider, with exactly
the names/values from the outputs. Account for providers that append the zone
name automatically; disable HTTP proxying on these records. Keep them published
for DKIM key rotation. CloudFormation completion alone does not mean DNS
verification succeeded.

Check the SES console or:

```powershell
aws sesv2 get-email-identity --email-identity playstand.app --region us-east-1 --profile stand-deploy-cli
```

Require `VerifiedForSendingStatus=true` and successful DKIM status before
testing. A verified domain covers both `contact@playstand.app` and
`no-reply@playstand.app`; no separate
per-environment or per-mailbox identity is necessary. The shared template retains
the identity on deletion/replacement; deleting its stack does not remove it.
Do not delete the identity or its DNS records during backend dev teardown.

## 2. Domain authentication and feedback ownership

- **DKIM:** the three SES CNAMEs establish domain ownership and sign outgoing
  mail with the aligned `playstand.app` domain.
- **SPF:** the default SES MAIL FROM uses an SES-owned domain with SES-managed
  SPF. Do not overwrite the site's existing SPF or add a second SPF TXT record.
  If aligned SPF is required, have the DNS/email owner configure a dedicated
  custom MAIL FROM subdomain in SES (for example `mail.playstand.app`), with
  SES's specified MX and `v=spf1 include:amazonses.com ~all` TXT records on that
  subdomain. This optional configuration is not provisioned by this template.
- **DMARC:** inspect the existing `_dmarc.playstand.app` TXT policy first.
  Publish/adjust a single policy with an owned aggregate-report mailbox, starting
  with monitoring (`p=none`) if appropriate, then progress to quarantine/reject
  only after reviewing all legitimate domain senders. Aligned DKIM supports
  DMARC even with the default SES MAIL FROM.
- **Feedback:** this template enables SES email feedback forwarding. Ensure
  feedback actually reaches a monitored mailbox (do not leave a non-receiving
  `no-reply` address as the only path). Configure identity-level SNS
  bounce/complaint notifications or an identity-default SES configuration set
  with event destinations, owned by operations. Subscribe/confirm recipients
  and test delivery. Do not disable forwarding until another feedback path
  works. Suppress hard-bounce/complaint recipients and investigate abuse.

Coordinate changes with the DNS/email owner; never replace unrelated MX, SPF,
DMARC or existing provider records.

## 3. Sandbox, quotas and controlled delivery

SES sandbox status and quotas are regional/account-wide, not specific to the
verified identity or backend environment. In us-east-1, request **production
access** for transactional verification emails and document consent, rate
limiting, bounce/complaint handling and expected volume. Sender verification
does not remove the sandbox. While sandboxed, use only verified controlled
recipient mailboxes or supported SES simulator addresses; simulator delivery
does not substitute for a real inbox end-to-end code test.

Inspect account sending status and limits:

```powershell
aws sesv2 get-account --region us-east-1 --profile stand-deploy-cli
```

Before opening public traffic, confirm production access, sending enabled,
daily quota and send rate appropriate to event bursts (shared by dev/prod).
The application defaults are 5/email/hour, 3000/IP/hour and
20000/environment/hour; those ceilings neither grant SES quota nor guarantee
delivery. Set CloudWatch SES reputation alarms for bounce and complaint rates
(alert before 5% and 0.1% respectively), and alerts for sending rejection,
throttling, daily-quota consumption and Lambda email-request failures.
Assign an on-call owner and notification destination, and verify alarm delivery.
Quota-consumption alerts may need a scheduled check/custom metric of SES quota
data. These operational alarms/destinations are not created by the templates.

## 4. Backend configuration and release gate

`samconfig.toml` supplies `EmailSenderAddress=contact@playstand.app` for dev,
`EmailSenderAddress=no-reply@playstand.app` for prod, and
`EmailIdentityName=playstand.app` for both. Deploying a sender change must update
both the public Lambda environment and its IAM `ses:FromAddress` condition;
changing only the environment leaves sending unauthorized. Changing the sender
does not remove sandbox recipient restrictions or establish the cause of an
existing `AccessDenied` error. The backend constructs the IAM
identity ARN for us-east-1 in its own AWS account; no cross-account sending
authorization is configured. Only the dedicated public Lambda role receives
`ses:SendEmail`, limited to that identity and `ses:FromAddress`. Assignment has
table permissions but cannot send mail. The existing game managed policy is
reused on these roles to preserve all prior game/session behavior.

### STACK reward emails

The STACK delivery worker separately receives `ses:SendRawEmail` on the same
identity, restricted to `EmailSenderAddress`. Its MIME message contains
bilingual text/HTML and a locally generated inline PNG QR; no QR/image service
receives participant phone numbers. The worker's pinned `qrcode`/`pypng`
requirements are installed by SAM build and by the test requirements.
Assignment Lambdas still cannot send email.

`StackRewardEmailEnabled` defaults to `"false"` in the template. The dev
`samconfig.toml` explicitly enables it for authorized testing; production does
not. For an approved dev rollout, first verify SES identity, quotas,
sandbox recipients, bounce/complaint handling and worker From-address permission.
Enable it only for a controlled test, then use an authorized test mailbox/customer
to confirm the emailed phone QR works with the actual Kivly scanner. A setting
or mocked test is not evidence that the Kivly reader accepts the QR.
Keep production disabled until the controlled flow is accepted.

Select Email in a STACK game's prize settings after enabling the environment.
New attempts use that channel; existing attempts/awards retain their previous
channel. Name, verified email and phone are required for new STACK registrations
in either mode. Publish backend and matching frontend together so older forms
without phone do not remain in circulation.

The worker sends only for confirmed Kivly assignments. Email acceptance is
tracked independently: `accepted` means SES returned a MessageId, not delivery
to the recipient. The existing verification-email rate counters do not apply
to reward notices; award stock, persisted allocation and bounded retries limit
reward sends. Account-wide SES quotas still apply and must cover both flows.
The operator prize panel lists safe notification status/errors and permits retry
only after definitive failure. Fix a missing phone in Kivly and retry the email;
this must never allocate another reward or consume additional stock.
Network/5xx errors and interrupted sends require provider-side investigation,
not automatic retry. SDK-level retries are disabled for the SES send operation.
An uncertain or already accepted email cannot be resent
through the retry API. Disabling `StackRewardEmailEnabled` pauses pending
notifications without revoking awards; it does not revert past emails.

`StandEmailVerificationTable` is per environment, on-demand, with string PK
`id` and numeric epoch-seconds `expiresAt` TTL for challenges/rate records.
Permanent HMAC-keyed identity-to-player mappings intentionally omit `expiresAt`.
Assignment transactions create the player and identity mapping and increment
the game's player count atomically, preventing concurrent duplicate registration.
Public transactions atomically update email/IP/global counters and issue the
challenge.
DynamoDB removes expired items asynchronously; application expiry checks remain
mandatory. Transactional IAM uses `PutItem`, `UpdateItem` and
`ConditionCheckItem`, not a nonexistent `TransactWriteItems` IAM action.
Keep `PlayerTokenSecret` private and stable; verification uses a
purpose-separated derived key rather than a second committed secret.

1. Validate locally without deploying:
   `sam validate --lint --template-file template.yaml` and
   `cfn-lint infra\email-sender.yaml`.
2. Provision/verify the shared sender only after operator approval. Arrange the
   feedback path, alarms and sandbox/production access above.
3. Deploy the backend to dev through the approved workflow. Using a controlled
   real mailbox, request a code with the full registration and `language` es/en;
   inspect From/DKIM/SPF/DMARC results. Verify join with `challengeId` + `code`,
   wrong/expired codes, resend timing, rate limits and duplicate recovery.
   Retrying the same correct challenge before its original expiry must return
   the same participant without another slot or attempt; it must not authorize
   another registration.
   Do not record OTPs, raw email addresses or participant tokens in logs.
4. Regression-check existing participant sessions, Web assignment and
   no-email forms. Check stack changesets for unexpected replacements and
   confirm prod tables are untouched by dev.
5. Coordinate a release window before deploying production backend **before**
   the matching UI. Email-enabled `/join` now requires a challenge/code, so old
   forms are not compatible: pause those campaigns, release/refresh the UI and
   cached clients, then reopen after a smoke test. Keep existing valid sessions
   and no-email flows available. Do not bypass verification to
   accommodate stale clients. A rollback also requires coordinated backend/UI
   versions, not an unreviewed reopening of unverified email recovery.

References: [SES CloudFormation identity and DKIM attributes](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ses-emailidentity.html),
[SES sandbox removal](https://docs.aws.amazon.com/ses/latest/dg/request-production-access.html),
[SES authentication](https://docs.aws.amazon.com/ses/latest/dg/email-authentication-methods.html).
