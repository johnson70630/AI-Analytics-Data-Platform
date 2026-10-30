# DataOps Agent deployment runbook

This runbook governs controlled deployment and redeployment of `agent-bricks-dataops-agent`.
Commands in this document require explicit deployment approval; source validation alone does not
authorize resource creation, permission grants, or live model and tool calls.

## Accepted baseline

V2.6 release `07377686d31d6d98b84c428c698d1abdc853f363` passed bounded cloud acceptance on the existing
one-Medium App and finished with App compute confirmed `STOPPED`.

- Lakeflow execution: 9 of 9 tasks passed.
- Monitoring: 48 checks, including 44 PASS, 4 WARN, and 0 FAIL.
- Composite investigation: one POST, two bounded Genie investigations, and twelve evidence-linked
  recommendations in a validated schema `1.1` report.
- Security: Job `CAN VIEW`, Warehouse `CAN USE`, Genie `CAN RUN`, and `SELECT` on the four
  observability tables; no job-control, repair, or production-write tool was exposed.

Execution success and monitored data health are separate acceptance signals. The successful job did
not override the four monitoring warnings. Pipeline-to-monitoring association remains unverified.

## Approval gate

Before running `agentbricks deploy`, confirm all of the following:

- The reviewed source is the approved release revision and `agentbricks doctor .` passes.
- Creation or reuse of the Databricks App, its dedicated service principal, Runtime Store, Session
  Store, MLflow experiment, and Unity Gateway model service is explicitly authorized for that
  release.
- The deployer can create or manage those declared resources and grant the app service principal
  access to them.
- The non-secret Job, Warehouse, and Genie identifiers in `app.yaml` are still the approved targets.
- The expected App, SQL Warehouse, model-inference, Lakebase, and tracing costs are approved.
- No email, alerting schedule, Lakeflow job control, incident-persistence write, or repair capability
  is enabled.

## Approved resource scope

The validated deployment uses one Medium App instance and these existing Agent Bricks-managed
resources. A future deployment must stop if it requests an unapproved replacement or additional
resource:

- Databricks App `agent-bricks-dataops-agent` and its dedicated service principal.
- The App's dedicated managed Runtime Store.
- Session Store `dataops-agent-gkxczy-sessions`.
- MLflow experiment `/Shared/agentbricks_traces/dataops-agent-gkxczy`.
- Unity Gateway model service `ai_analytics.default.dataops_agent_llm`, initially routed to
  `system.ai.gpt-oss-120b`.
- The deployment's workspace source synchronization directory.

A Memory Store is not declared for this release. The deployment must not create alerting tables,
jobs, schedules, email resources, or another Genie Agent or SQL Warehouse.

## Stage 1: deploy or reconcile without validation traffic

From the release worktree's `dataops-agent/` directory:

```bash
agentbricks --profile ai-analytics-dev deploy dataops-agent --source . --instances 1 --no-wait
agentbricks --profile ai-analytics-dev deployments wait agent-bricks-dataops-agent \
  --source . --timeout 100
agentbricks --profile ai-analytics-dev deployments get agent-bricks-dataops-agent
agentbricks --profile ai-analytics-dev deployments stop agent-bricks-dataops-agent --yes
```

Stopping the App after reconciliation prevents validation traffic while the dedicated app service
principal permissions are reviewed. Do not delete the deployment or its managed stores during
failure investigation.

## Stage 2: least-privilege grants

Resolve the dedicated service principal from the App Authorization page or deployment details. Use
that principal only in the grants below.

- App users: `CAN USE`; release operators only: `CAN MANAGE`.
- Lakeflow Job `807892462186478`: `CAN VIEW` only.
- SQL Warehouse `08401a2b34023edc`: `CAN USE` only; verify Databricks SQL access entitlement.
- Genie Agent `01f1d0cfc8691413ad422631b434bdbb`: `CAN RUN` only.
- Unity Gateway model service `ai_analytics.default.dataops_agent_llm`: `EXECUTE`, plus the required
  `USE CATALOG` and `USE SCHEMA`. Agent Bricks should reconcile these declared-model grants.
- Unity Catalog observability reads:

```sql
GRANT USE CATALOG ON CATALOG ai_analytics TO `<app-service-principal>`;
GRANT USE SCHEMA ON SCHEMA ai_analytics.observability TO `<app-service-principal>`;
GRANT SELECT ON TABLE ai_analytics.observability.dq_entity_freshness
  TO `<app-service-principal>`;
GRANT SELECT ON TABLE ai_analytics.observability.dq_row_count_daily
  TO `<app-service-principal>`;
GRANT SELECT ON TABLE ai_analytics.observability.dq_quality_metrics_daily
  TO `<app-service-principal>`;
GRANT SELECT ON TABLE ai_analytics.observability.dq_pipeline_reconciliation
  TO `<app-service-principal>`;
```

Do not grant Lakeflow run management, warehouse management, catalog writes, alerting-schema writes,
or permission to create or modify production data objects.

## Stage 3: bounded proof of life

Start the App only after the grants are reviewed:

```bash
agentbricks --profile ai-analytics-dev deployments start agent-bricks-dataops-agent
agentbricks --profile ai-analytics-dev deployments get agent-bricks-dataops-agent
```

Validate, in order: App health and Command Center loading; one model-only response; Jobs execution
health; the fixed latest-observability summary; one bounded Genie question; a two-turn Session Store
conversation; and the resulting MLflow trace. Memory validation is intentionally omitted because no
Memory Store is bound for this release. Confirm each of the four custom tools is selected exactly
once for its matching test request and that no write, email, alerting, or job-control tool appears.

For Daily Health Review releases, additionally verify that initial load and manual Refresh create no
LLM or Genie trace, submit at most the explicitly authorized number of composite investigations,
validate report schema `1.1`, and recover the same Runtime Store invocation ID after reload without a
second POST.

After the bounded validation, stop the App while deployment findings and costs are reviewed:

```bash
agentbricks --profile ai-analytics-dev deployments stop agent-bricks-dataops-agent --yes
agentbricks --profile ai-analytics-dev deployments get agent-bricks-dataops-agent
```

Record App instance-hours, SQL Warehouse DBUs, model-inference usage, managed database/Lakebase
usage, and MLflow trace storage from the deployment window. Reconcile the observed SKUs with
`system.billing.usage` and the effective prices in `system.billing.list_prices`. Confirm the App is
stopped and the existing SQL Warehouse returns to its configured stopped state after auto-stop.

Billing ingestion can lag the test window; report unavailable cost attribution rather than assigning
workspace-level usage to the App without evidence. MLflow trace metadata currently persists while
span artifact upload fails at the workspace-storage network boundary. Keep that networking defect
separate from functional acceptance and do not deploy the synchronous tracing diagnostic override.

## Failure containment and rollback

If startup, authorization, or grounded-response validation fails:

```bash
agentbricks --profile ai-analytics-dev deployments stop agent-bricks-dataops-agent --yes
agentbricks --profile ai-analytics-dev deployments get agent-bricks-dataops-agent
agentbricks --profile ai-analytics-dev deployments logs agent-bricks-dataops-agent
```

Keep the stopped App and managed resources for diagnosis. Do not delete stores, the model service,
or the App automatically. Correct only the reviewed configuration or permission, then redeploy the
same approved release. Redeploying an earlier tested revision or removing grants requires a separate
operator decision.
