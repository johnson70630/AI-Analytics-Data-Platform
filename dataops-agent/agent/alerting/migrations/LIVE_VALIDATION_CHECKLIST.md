# Controlled Delta validation checklist

This is a review plan, not an executable migration. Do not run it against production. Use only
mock email delivery and the isolated `ai_analytics.dataops_alerting_dev` schema.

## Approval and resources

1. Obtain approval to provision the development resources in
   `000_create_dataops_alerting_dev.sql` on a Unity Catalog-enabled Databricks SQL warehouse.
2. Use a dedicated validation principal with only the grants listed in
   `ACCESS_PREREQUISITES.md`.
3. Confirm the adapter and `DatabricksFixedStatementExecutor` both use
   `target="development"`, `write_enabled=False` for the initial read-only connection check, and
   a unique checkpoint name such as `dataops-alerting-validation-<run-id>`.
4. Verify `MockEmailSender` is the injected sender. No SMTP/API email sender is permitted.
5. Record the warehouse ID, validation principal, start time, and approved cleanup owner.

## Schema and SQL compatibility

1. Review each `CREATE TABLE` statement and then provision only the development migration.
2. Use `DESCRIBE TABLE EXTENDED` and `SHOW CREATE TABLE` to verify all four tables are managed
   Delta tables with the expected columns. Treat primary keys as informational only.
3. Confirm the warehouse supports named parameter markers, Unity Catalog constraints, `MERGE`,
   and SQL Warehouse Statement Execution.
4. Verify the runtime principal can `SELECT` and `MODIFY` the four development tables but cannot
   create tables, use the production `dataops_alerting` schema, control Lakeflow jobs, or access
   email credentials.

## Test data boundaries

- Use deterministic test fingerprints, observation IDs, episode IDs, and notification IDs that
  contain a recorded validation namespace in their evidence.
- Do not use current production checkpoint names or real incident records.
- Capture row counts and duplicate-key queries before and after every scenario.
- Do not use real monitoring delivery recipients. Outbox delivery remains mocked.

## Scenarios

### Crash replay

1. Persist a deterministic transition while injecting a failure after the observation MERGE and
   before checkpoint finalization.
2. Confirm the checkpoint version did not advance and no outbox row is claimable.
3. After the checkpoint lease expires, replay the identical bundle with the same expected version.
4. Confirm exactly one observation, one episode, and one logical notification exist and the
   checkpoint advances once.
5. Repeat with a failure after outbox MERGE and with a simulated lost response after checkpoint
   finalization.
6. Adversarially rebuild a different bundle from the same checkpoint version after lease expiry.
   Confirm the abandoned transition remains uncommitted, the winning bundle replaces its pending
   outbox payload, and only the checkpoint's matching `last_transition_id` is activated.

### Concurrent checkpoint/MERGE writers

1. Start two processors from the same checkpoint version using a synchronization barrier.
2. Confirm only one owns the checkpoint lease and finalizes the next version.
3. Confirm the losing processor reports a retryable concurrency conflict and cannot write after
   its lease token is replaced or expired.
4. Query every logical identifier for duplicates. Informational constraints are not evidence of
   uniqueness.

### Duplicate suppression

1. Submit an identical bundle twice, including intentional retry after a lost client response.
2. Confirm deterministic IDs cause insert-only MERGEs to retain one physical logical record.
3. Submit a bundle containing duplicate IDs and confirm application validation rejects it before
   any statement executes.

### Lease expiry and fencing

1. Claim a checkpoint with a short approved validation lease and let it expire.
2. Confirm MERGE source fencing and checkpoint finalization reject the stale token.
3. Claim an outbox row, let its lease expire, and confirm the old token cannot mark it `SENT` or
   `FAILED`.
4. Reclaim with a new token and confirm the stale token remains fenced.

### Outbox retry

1. Use `MockEmailSender(failures_before_success=1)`.
2. Confirm `PENDING -> CLAIMED -> RETRYABLE`, retry delay, bounded attempt increment, then
   `CLAIMED -> SENT` with a new token.
3. Repeat through `max_attempts` and confirm terminal `FAILED` cannot be claimed again.
4. Confirm an outbox row whose checkpoint version is not committed cannot be claimed.

## Acceptance checks

- Checkpoint versions advance monotonically by one and only after observations, episodes, and
  outbox rows are durable.
- `GROUP BY <logical_id> HAVING COUNT(*) > 1` returns no rows for all four tables.
- Exactly two distinct PASS observation IDs are required to resolve an incident.
- Monitoring errors or unavailable evidence never create PASS observations.
- Claim tokens and checkpoint leases reject stale processors.
- An outbox row whose checkpoint version has not advanced is ineligible for mock delivery.
- Divergent same-version replay activates only the outbox row matching the checkpoint's committed
  transition identity; checkpoint version alone is never treated as a commit identity.
- Repeated WARN observations remain one ongoing episode; a WARN after confirmed resolution creates
  the next recurring episode.
- No production schema, job, email, or external service is modified.

## Growth and retention gates

- Record checkpoint JSON byte size and processed-observation-ID count per run. The adapter rejects
  checkpoint JSON above 1,000,000 bytes rather than silently truncating idempotency state.
- Do not enable a recurring workload until processed-observation deduplication is normalized or a
  reviewed compaction policy is implemented; the current checkpoint list is intentionally lossless
  and therefore grows.
- Define and approve observation and terminal-outbox retention before deletion. Retention must be
  longer than the replay/cursor horizon and must not remove records needed for audit or idempotency.
- Incident episodes and active/retryable outbox rows are not retention-cleanup candidates.

## Cleanup

Cleanup is a separately approved destructive step. First export validation evidence, confirm the
validation namespace, and obtain explicit authorization before deleting validation rows or dropping
the isolated development schema.
