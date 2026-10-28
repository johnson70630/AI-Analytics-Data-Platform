# DataOps alerting persistence prerequisites

This migration is reviewed source only. V2.3B does not execute it or grant permissions.

Controlled integration validation uses only:

- catalog `ai_analytics`;
- schema `ai_analytics.dataops_alerting_dev`;
- four managed Delta tables: `incident_episodes`, `incident_observations`,
  `monitor_checkpoints`, and `notification_outbox`;
- the existing SQL warehouse configured by `DATABRICKS_SQL_WAREHOUSE_ID`;
- a dedicated validation principal (or the current development identity), never a production
  workload identity.

An administrator must separately approve and perform provisioning. The provisioning identity needs
`USE CATALOG` plus `CREATE SCHEMA` on `ai_analytics`, then ownership or `CREATE TABLE` on the isolated
development schema. Those creation privileges are not runtime privileges and should be removed after
provisioning.

The validation runtime identity needs only:

- `USE CATALOG` on `ai_analytics`;
- `USE SCHEMA` on `ai_analytics.dataops_alerting_dev`;
- `SELECT` and `MODIFY` on each of the four development tables;
- SQL Warehouse `CAN USE` on the configured development warehouse.

It does not need schema ownership, `MANAGE`, table creation, permissions on production schemas,
Lakeflow job control, secret management, or email credentials.

Before any future production rollout, an administrator must separately approve:

- creation of `ai_analytics.dataops_alerting`;
- an audit and retention policy before any observation or terminal outbox cleanup;
- confirmation that informational primary keys are not being treated as uniqueness enforcement.

The adapter defaults to the development schema and to writes disabled. A future scheduled workload
must explicitly select the production target and opt in to persistence, and must not reuse an
interactive user's credentials.
