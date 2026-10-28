-- Reviewed development-validation migration only. V2.3B does not execute this file.
CREATE SCHEMA IF NOT EXISTS ai_analytics.dataops_alerting_dev
COMMENT 'Isolated development validation for the DataOps incident registry and outbox';

CREATE TABLE IF NOT EXISTS ai_analytics.dataops_alerting_dev.incident_episodes (
  episode_id STRING NOT NULL COMMENT 'SHA-256(fingerprint_digest:episode_number)',
  fingerprint_digest STRING NOT NULL,
  source STRING NOT NULL,
  category STRING NOT NULL,
  check_identity STRING NOT NULL,
  episode_number INT NOT NULL,
  lifecycle_status STRING NOT NULL COMMENT 'OPEN or RESOLVED',
  first_seen_at TIMESTAMP NOT NULL,
  episode_started_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL,
  resolved_at TIMESTAMP,
  last_status STRING NOT NULL,
  last_observation_id STRING NOT NULL,
  observation_count BIGINT NOT NULL,
  consecutive_passes INT NOT NULL,
  version BIGINT NOT NULL COMMENT 'Optimistic lifecycle snapshot version',
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT incident_episodes_dev_pk PRIMARY KEY (episode_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Development-only incident episodes';

CREATE TABLE IF NOT EXISTS ai_analytics.dataops_alerting_dev.incident_observations (
  observation_id STRING NOT NULL COMMENT 'Deterministic source observation identifier',
  fingerprint_digest STRING NOT NULL,
  episode_id STRING,
  source STRING NOT NULL,
  category STRING NOT NULL,
  check_identity STRING NOT NULL,
  observation_status STRING NOT NULL,
  observed_at TIMESTAMP NOT NULL,
  reference_time TIMESTAMP,
  synthetic_reference_date STRING,
  execution_run_id BIGINT,
  association_status STRING NOT NULL,
  association_reason STRING NOT NULL,
  decision STRING NOT NULL,
  would_investigate BOOLEAN NOT NULL,
  would_notify BOOLEAN NOT NULL,
  evidence_json STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  CONSTRAINT incident_observations_dev_pk PRIMARY KEY (observation_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Development-only monitoring observations';

CREATE TABLE IF NOT EXISTS ai_analytics.dataops_alerting_dev.monitor_checkpoints (
  checkpoint_name STRING NOT NULL,
  snapshot_version BIGINT NOT NULL,
  last_transition_id STRING,
  run_completed_at TIMESTAMP,
  run_id BIGINT,
  snapshot_json STRING NOT NULL,
  lease_owner STRING,
  lease_token STRING,
  lease_expires_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT monitor_checkpoints_dev_pk PRIMARY KEY (checkpoint_name) NOT ENFORCED
)
USING DELTA
COMMENT 'Development-only optimistic-concurrency checkpoints';

CREATE TABLE IF NOT EXISTS ai_analytics.dataops_alerting_dev.notification_outbox (
  notification_id STRING NOT NULL COMMENT 'SHA-256(episode_id:event_type)',
  episode_id STRING NOT NULL,
  checkpoint_name STRING NOT NULL,
  checkpoint_version BIGINT NOT NULL COMMENT 'Outbox rows are claimable only after this version commits',
  transition_id STRING NOT NULL COMMENT 'Deterministic identity of the complete persistence bundle',
  committed BOOLEAN NOT NULL COMMENT 'True only after the matching checkpoint transition commits',
  event_type STRING NOT NULL,
  status STRING NOT NULL COMMENT 'PENDING, CLAIMED, SENT, RETRYABLE, or FAILED',
  payload_json STRING NOT NULL,
  attempt_count INT NOT NULL,
  max_attempts INT NOT NULL,
  available_at TIMESTAMP NOT NULL,
  claimed_by STRING,
  claim_token STRING,
  lease_expires_at TIMESTAMP,
  last_error STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  sent_at TIMESTAMP,
  CONSTRAINT notification_outbox_dev_pk PRIMARY KEY (notification_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Development-only at-least-once notification outbox';

-- Primary keys are informational. The application enforces deterministic identity and fencing.
-- No retention or VACUUM operation is performed by this migration.
