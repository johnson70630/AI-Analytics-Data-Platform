# DataOps Portfolio Demo Script

Target length: 2–3 minutes. This script uses results recorded during the bounded V2.6 cloud
acceptance. It does not imply that the Databricks App is currently running or that the matching
browser footage has already been captured.

## 0:00–0:30 — Platform and problem

“This project is a Databricks-native analytics platform for a synthetic AI chatbot product. A
deterministic Python and Spark generator writes managed, append-only Delta Bronze data. dbt then
builds typed staging models, Silver dimensions and facts, Gold business marts, and four
observability datasets. Lakeflow orchestrates the daily workflow, while Unity Catalog separates
operational state, PII, analytics, and monitoring access.”

## 0:30–1:00 — Data engineering depth

“The platform preserves controlled defects in Bronze without allowing them to corrupt the clean
generator state. High-volume facts use incremental Delta merges with both event-time and physical
partition lookbacks, so late or malformed records remain accounted for. User history combines a
historical bootstrap with dbt snapshots for SCD Type 2 behavior, and reconciliation tests validate
the movement from staging through Silver and Gold.”

## 1:00–1:35 — Observability and Daily Health Review

“The Command Center exposes a lightweight Daily Health Review using the Jobs API and fixed,
read-only observability queries. During the final cloud acceptance, all 9 Lakeflow tasks succeeded,
but the separately retrieved monitoring snapshot contained 48 checks: 44 passed, four warned, and
none failed. The interface keeps those signals separate because pipeline success does not prove
that every data-health check passed. It also preserves monitoring execution time, monitoring
reference time, and synthetic activity dates as distinct fields.”

## 1:35–2:15 — One-click investigation

“Run Full Investigation calls a fixed LangGraph workflow directly rather than asking a model to
choose a tool. Jobs and observability evidence are collected in parallel. Supported anomalies can
trigger at most two bounded Genie questions, after which deterministic rules produce prioritized,
evidence-linked recommendations. The accepted schema version 1.1 report used two Genie
investigations and returned twelve recommendations. Root cause and business impact remained marked
unknown where the evidence did not support a conclusion, and proposed corrections remained
advisory with human-approval labels.”

## 2:15–2:40 — Security, recovery, and close

“The Agent Bricks application uses a dedicated identity with read-only access: Jobs view, Warehouse
use, Genie run, and select access to governed observability tables. Runtime Store supports
authenticated invocation recovery after a browser reload without submitting a second
investigation. The result is a portfolio platform that connects data engineering, governed
observability, and evidence-grounded AI operations without exposing arbitrary SQL, job control, or
automatic repair.”

## Optional final note

“One tracing limitation remains isolated from the product flow: MLflow trace metadata is recorded,
but span artifact upload is blocked at the workspace-storage network boundary. The dashboard API,
investigation workflow, managed sessions, and report API were cloud-validated independently; final
pixel-level browser acceptance and portfolio capture remain outstanding.”
