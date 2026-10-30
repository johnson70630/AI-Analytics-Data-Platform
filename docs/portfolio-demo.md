# Portfolio Demo Outline

This 8–10 minute walkthrough demonstrates the platform as one system rather than a collection of
isolated tools.

## 1. Architecture and problem statement — 1 minute

- Introduce a synthetic AI-chatbot workload with realistic daily product, model, and finance events.
- Show the [Databricks platform diagram](../images/AI%20Analytics%20Data%20Platform%20Architecture.png)
  and the [DataOps agent architecture](dataops-agent-architecture.md).
- Explain the core boundary: controlled defects are retained in append-only Bronze, while clean
  generator state remains isolated so the next daily run stays deterministic.

## 2. Lakeflow and dbt transformation path — 2 minutes

- Open the Lakeflow Job and show the generator-to-dbt dependency chain.
- Use the accepted run as evidence: all **9 of 9 tasks succeeded**.
- Trace one entity through managed Delta Bronze, dbt staging, Silver facts/dimensions, Gold marts,
  and observability.
- Point out SCD2 user history, the dedicated PII schema, incremental Delta `MERGE` for high-volume
  facts, and Unity Catalog governance.

## 3. Observability is data — 1 minute

- Open the four observability models: freshness, row counts, quality metrics, and reconciliation.
- Show the accepted snapshot: **48 checks = 44 PASS, 4 WARN, 0 FAIL**.
- State the key operational lesson explicitly: the Lakeflow execution succeeded, but monitored data
  health still contained four warnings. Execution success is not equivalent to data-quality health.
- Keep monitoring execution time, monitoring reference time, and synthetic activity dates separate.

## 4. Daily Health Review — 1 minute

- Open the Command Center and select **Daily Health Review**.
- Show pipeline status, task counts, monitored checks, warnings, timestamps, and source availability.
- Use **Refresh** and explain that dashboard load/refresh performs no LLM or Genie call.
- Call out the `UNVERIFIED` run-to-monitoring association rather than implying causal linkage.

## 5. One-click LangGraph investigation — 2 minutes

- Select **Run Full Investigation** once.
- Explain the deterministic graph: parallel Jobs/observability collection, conditional bounded Genie
  routing, recommendations, then schema validation.
- Show the accepted report: schema `1.1`, four detected warnings, **two Genie investigations**, and
  **twelve evidence-linked recommendations**.
- Expand Genie SQL/result evidence and show that root cause and business impact remain explicitly
  unknown when evidence does not support a conclusion.
- Show diagnostic guidance before the four human-approval-required remediation proposals. Emphasize
  that no repair is executed.

## 6. Runtime, security, and recovery — 1 minute

- Explain Agent Bricks hosting, the dedicated app identity, the Runtime Store, and Session Store.
- Show that an active investigation can be recovered after reload by polling the same authenticated
  invocation ID; reload does not submit a second invocation.
- Summarize least privilege: Job `CAN VIEW`, Warehouse `CAN USE`, Genie `CAN RUN`, and `SELECT` on
  governed observability tables.

## 7. Honest limitations and next steps — 1 minute

- Pipeline-to-monitoring association remains unverified without an authoritative shared run key.
- MLflow trace metadata persists, but span artifact upload is blocked by workspace-storage network
  connectivity and remains a Databricks support item.
- Pixel-level real-browser visual acceptance is still outstanding; deployed HTML/CSS/API and
  responsive breakpoint checks passed.
- App, model, and warehouse cost attribution can lag in system billing tables, so the acceptance
  window does not yet have a reliable isolated USD cost.

End with the engineering value: deterministic generation, governed medallion modeling, measurable
data quality, and an evidence-grounded read-only operational assistant.
