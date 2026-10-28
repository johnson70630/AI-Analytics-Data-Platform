# DataOps incident email delivery

V2.3C defines a provider-neutral delivery boundary and leaves the mock provider as the safe default.
The SES v2 adapter is available only through explicit dependency injection; it is not selected by
configuration automatically. This milestone does not create credentials, send mail, or schedule a
delivery worker.

## Runtime configuration

The provider-neutral routing configuration uses:

- `DATAOPS_EMAIL_PROVIDER`: provider adapter name; defaults to `mock`.
- `DATAOPS_EMAIL_FROM`: verified sender address required by a live provider.
- `DATAOPS_EMAIL_TO`: comma-separated operator recipients required by a live provider.
- `DATAOPS_EMAIL_REPLY_TO`: optional reply-to address.

Provider credentials are deliberately outside `EmailDeliveryConfig`. A live adapter must retrieve
credentials from an approved secret or workload-identity mechanism and must never place them in the
outbox payload, environment logs, email content, or repository. The current Databricks App
configuration does not bind any email credential.

The mock default uses reserved `.invalid` addresses and performs no network request. Selecting a
non-mock provider without sender and recipient routing values fails closed during configuration.

The SES adapter uses boto3's standard credential provider chain and accepts an injected SES v2
client for testing. A live SES sender therefore also requires an explicit AWS Region (`AWS_REGION`,
`AWS_DEFAULT_REGION`, shared AWS configuration, or an injected client) and an approved AWS identity.
No access-key environment variable is read by the alerting code directly.

## Recommended provider: Amazon SES

Amazon SES is the recommended production candidate for this AWS-hosted, low-volume operational
workload because it provides an API/SDK interface, IAM-scoped sending permissions, and usage-based
pricing. As of 2026-10-27, SES lists Essentials outbound mail at $0.16 per 1,000 messages for the
first 10 million messages, while its à-la-carte option lists outbound mail at $0.10 per 1,000 plus
$0.12 per GB of attachment data. New account/Region combinations can default to Essentials, so the
actual plan must be checked rather than assuming the à-la-carte rate.

Production prerequisites:

1. Select the AWS account and Region explicitly.
2. Verify the sending email address or, preferably, the sending domain in SES.
3. While SES remains in its sandbox, verify recipients too; sandbox limits also apply. Request
   production access before alerting arbitrary operator recipients.
4. Grant only `ses:SendEmail` (and `ses:SendRawEmail` only if a MIME-based adapter genuinely needs
   it) to a dedicated runtime identity. Do not grant SES administration or IAM management.
5. Use short-lived/federated AWS credentials where the deployment supports them. If a secret is
   unavoidable, bind it through an approved secret manager and rotate it; never add it to `.env`,
   `app.yaml`, source, Delta payloads, or logs.
6. Permit outbound TLS to the selected regional SES API endpoint. If SMTP is chosen instead, create
   SES-specific SMTP credentials and permit only the required TLS endpoint and port.
7. Verify bounce/suppression handling and sender-domain SPF, DKIM, and DMARC before production use.

For read-only preflight, the operator identity needs `ses:GetAccount`, `ses:ListEmailIdentities`, and
optionally `ses:GetEmailIdentity` for the intended sender. The eventual delivery identity needs only
the reviewed sending action and must not inherit configuration-administration permissions.

Official references:

- [Amazon SES pricing](https://aws.amazon.com/ses/pricing/)
- [SES verified identities and sandbox behavior](https://docs.aws.amazon.com/ses/latest/dg/verify-addresses-and-domains.html)
- [SES IAM access control](https://docs.aws.amazon.com/ses/latest/dg/control-user-access.html)
- [SES SMTP credentials](https://docs.aws.amazon.com/ses/latest/dg/smtp-credentials.html)

## Alternatives

| Provider | Credential and verification model | Network requirement | Current published entry price | Fit |
| --- | --- | --- | --- | --- |
| Amazon SES | AWS IAM/API or SES-specific SMTP credentials; verified sender/domain; sandbox recipients verified | Regional HTTPS API or TLS SMTP endpoint | Essentials $0.16/1,000 for the first 10M; à-la-carte $0.10/1,000 plus message-data charges | Recommended for low cost and least-privilege IAM |
| Postmark | Server API token; approved sender signature or domain | HTTPS API or TLS SMTP | Free developer tier: 100/month; Basic: $15/month for 10,000, then $1.80/1,000 | Simple transactional-email operations, higher fixed cost |
| Twilio SendGrid | Restricted API key; single-sender verification or domain authentication | HTTPS API or TLS SMTP | Published Essentials entry: $19.95/month for 50,000 | Mature alternative, but unnecessary fixed cost for expected alert volume |

Pricing changes over time and must be rechecked before procurement. Postmark pricing is documented
at <https://postmarkapp.com/pricing/>. SendGrid publishes its plan comparison at
<https://sendgrid.com/content/dam/sendgrid/global/en/other/sendgrid-pricing/twi121--sendgrid-pricing-pdf-st1.pdf>.

## Databricks network and authentication considerations

Databricks Apps run on the serverless compute plane. Outbound access can be restricted by workspace
network policies, and a network connectivity configuration can provide controlled/stable egress.
Before enabling any adapter, allow only the chosen provider endpoint, validate DNS/TLS from the
runtime, and inspect outbound-denial events if the connection is blocked. See
[Databricks Apps networking](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/networking).

The existing Agent Bricks authentication grants Databricks workspace access; it does not itself
grant AWS SES, Postmark, or SendGrid credentials. Email delivery credentials require a separate,
explicitly reviewed binding.

## Delivery guarantees and content boundary

The Delta outbox keeps its stable notification identity, commit fencing, claim token, lease, and
bounded-attempt behavior. Provider delivery remains at-least-once: a provider may accept a message
immediately before the worker loses its lease or crashes, so guaranteed exactly-once email delivery
is not claimed.

Email content is built from a bounded outbox payload. It includes observed checks, distinct
monitoring execution and reference timestamps, synthetic activity dates, up to three prioritized
recommendations, approval requirements, verification steps, and explicit root-cause/impact
assessments. It excludes complete report JSON, query result rows, complete Genie results, and
credentials.

SES `SendEmail` success is recorded as `accepted`, together with the sanitized provider name and
provider message identifier. Acceptance means that SES accepted the API request; it does not mean
the recipient mailbox delivered or displayed the message. Provider receipt fields require the
reviewed one-time Delta outbox upgrade before persistence-enabled delivery:

- `002_add_notification_delivery_receipt_dev.sql` for the isolated development registry.
- `003_add_notification_delivery_receipt.sql` for production, only after separate approval.

Neither migration is executed automatically by the application.
