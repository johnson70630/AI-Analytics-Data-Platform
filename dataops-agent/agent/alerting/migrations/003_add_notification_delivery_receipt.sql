-- Production upgrade template. Do not apply without a separate production migration approval.
ALTER TABLE ai_analytics.dataops_alerting.notification_outbox ADD COLUMNS (
  provider_name STRING COMMENT 'Sanitized provider adapter name',
  provider_message_id STRING COMMENT 'Provider acceptance identifier; not proof of delivery',
  provider_status STRING COMMENT 'Sanitized acceptance status, currently accepted'
);
