# DataOps AI Agent Architecture

The DataOps AI Agent is a read-only operational plane over the Databricks analytics platform. It
does not sit in the data path and cannot change Lakeflow jobs, tables, permissions, schedules, or
monitoring results.

```mermaid
flowchart LR
    GEN[Deterministic synthetic generator] --> BRONZE[Managed Delta Bronze]
    BRONZE --> STAGING[dbt staging]
    STAGING --> SILVER[Silver dimensions and facts]
    SILVER --> GOLD[Gold analytics marts]
    GOLD --> BI[Databricks AI/BI]
    STAGING --> OBS[Observability models]
    SILVER --> OBS
    GOLD --> OBS

    LAKEFLOW[Databricks Lakeflow Job] -. orchestrates .-> GEN
    LAKEFLOW -. orchestrates .-> STAGING

    JOBS["Jobs API<br/>execution evidence"] --> GRAPH[LangGraph investigation]
    OBS --> SQL["Fixed read-only SQL<br/>latest health"]
    SQL --> GRAPH
    OBS --> GENIE["Databricks Genie<br/>bounded historical evidence"]
    GENIE --> GRAPH
    GRAPH --> RECS[Deterministic recommendations]
    RECS --> REPORT[InvestigationReport v1.1]

    REPORT --> UI[DataOps AI Command Center]
    SQL --> REVIEW[Daily Health Review]
    REVIEW --> UI

    AB[Agent Bricks runtime] --> UI
    AB --> RUNTIME["Runtime Store<br/>invocation recovery"]
    AB --> SESSION["Session Store<br/>conversation history"]
    AB --> MODEL["Unity Gateway model service<br/>chat routing"]

    UC[Unity Catalog] -. governs .-> BRONZE
    UC -. governs .-> STAGING
    UC -. governs .-> SILVER
    UC -. governs .-> GOLD
    UC -. governs .-> OBS
```

## Investigation contract

The Daily Health Review is lightweight: opening it or refreshing it calls the Jobs API and fixed
observability queries only. **Run Full Investigation** deterministically invokes the existing
LangGraph composite workflow; an LLM does not decide which tool to call. The graph collects Jobs
and observability evidence in parallel, conditionally asks Genie at most two bounded historical
questions, generates deterministic recommendations, and validates the result as report schema
`1.1`.

Pipeline execution health and monitored data health are separate signals. A successful Lakeflow run
does not turn monitoring `WARN` or `FAIL` results into `PASS`, and the application does not claim
that a monitoring snapshot belongs to a particular run unless authoritative evidence establishes
that association.

## Read-only boundary

- Jobs access is `CAN VIEW`; there is no job-control tool.
- The SQL Warehouse is used by fixed `SELECT` statements only.
- The app can read the four governed observability tables and run the existing Genie Agent.
- Recommendations are advisory. Proposed corrections carry human-approval labels but have no
  execution button or repair implementation.
- Incident alerting and email libraries are inactive and are not registered as agent tools.
