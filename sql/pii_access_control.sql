DO $roles$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'analytics_reader'
    ) THEN
        CREATE ROLE analytics_reader NOLOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'pii_approved'
    ) THEN
        CREATE ROLE pii_approved NOLOGIN;
    END IF;
END
$roles$;

ALTER ROLE analytics_reader
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE pii_approved
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

DO $database_grants$
BEGIN
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO analytics_reader, pii_approved',
        current_database()
    );
END
$database_grants$;

GRANT USAGE ON SCHEMA silver, gold TO analytics_reader;
GRANT USAGE ON SCHEMA silver TO pii_approved;

REVOKE ALL ON ALL TABLES IN SCHEMA silver
    FROM analytics_reader, pii_approved;
REVOKE ALL ON ALL TABLES IN SCHEMA gold
    FROM analytics_reader, pii_approved;

GRANT SELECT ON TABLE silver.dim_user
    TO analytics_reader, pii_approved;
GRANT SELECT ON ALL TABLES IN SCHEMA gold
    TO analytics_reader;

DO $sensitive_grants$
BEGIN
    IF to_regclass('silver.dim_user_sensitive') IS NOT NULL THEN
        REVOKE ALL ON TABLE silver.dim_user_sensitive
            FROM PUBLIC, analytics_reader;
        GRANT SELECT ON TABLE silver.dim_user_sensitive TO pii_approved;
    END IF;
END
$sensitive_grants$;
