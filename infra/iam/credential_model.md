# AWS workload credential model

The policy files in this directory use `${S3_BUCKET_NAME}` as a deployment-time
placeholder. Deployment tooling must replace it with the target bucket name
before attaching the JSON as an IAM policy.

## Component boundaries

The data-generator workload assumes a generator role. Daily generation reads
existing `raw/*` objects to reconstruct historical state and prevent ID
collisions. Other generator validation and Bronze-materialization modes also
read the generator-owned `bronze/*` and `quality/injection_manifest/*` outputs.
The component can list, read, and write only those three prefix families. S3
copy operations use source `GetObject` plus destination `PutObject`; they do not
require an S3 `CopyObject` IAM action or delete permission.

The Airflow PostgreSQL landing task assumes a separate ingestion role. It can
list only `bronze/*` prefixes and read only `bronze/*` objects. It cannot read
Raw or quality data and cannot write or delete S3 objects.

The standalone profiling utility is not part of the production Airflow path.
If deployed as a service, it should receive a separate read-only role scoped to
the Raw, Bronze, and injection-manifest inputs it profiles rather than either
component role above.

## Credential delivery

Local development can supply credentials through `.env`, the shared AWS
credentials/config files, or another standard boto3 provider. The existing
Docker Compose `env_file` passes environment credentials, including an optional
`AWS_SESSION_TOKEN`, into Airflow without application-level credential wiring.

Production workloads should receive temporary, automatically rotated
credentials by assuming their component role through the runtime platform
(for example, a container task role or web-identity provider). The Python code
is identical in both environments because boto3 resolves credentials through
its standard provider chain. No access key, secret key, session token, account
identity, or user-specific role assignment belongs in source control.
