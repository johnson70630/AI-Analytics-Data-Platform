-- analytics_reader
GRANT USE CATALOG
ON CATALOG ai_analytics
TO `analytics_reader`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.silver
TO `analytics_reader`;

GRANT SELECT
ON SCHEMA ai_analytics.silver
TO `analytics_reader`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.gold
TO `analytics_reader`;

GRANT SELECT
ON SCHEMA ai_analytics.gold
TO `analytics_reader`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.observability
TO `analytics_reader`;

GRANT SELECT
ON SCHEMA ai_analytics.observability
TO `analytics_reader`;


-- pii_approved
GRANT USE CATALOG
ON CATALOG ai_analytics
TO `pii_approved`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.silver
TO `pii_approved`;

GRANT SELECT
ON SCHEMA ai_analytics.silver
TO `pii_approved`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.gold
TO `pii_approved`;

GRANT SELECT
ON SCHEMA ai_analytics.gold
TO `pii_approved`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.observability
TO `pii_approved`;

GRANT SELECT
ON SCHEMA ai_analytics.observability
TO `pii_approved`;

GRANT USE SCHEMA
ON SCHEMA ai_analytics.pii
TO `pii_approved`;

GRANT SELECT
ON SCHEMA ai_analytics.pii
TO `pii_approved`;