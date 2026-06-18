-- ============================================================
-- config/control_table_schema.sql
-- Master control + audit table DDL
-- ============================================================

-- Source control table: one row per ingestion source
CREATE TABLE IF NOT EXISTS config.file_sources (
    source_id         STRING        NOT NULL   COMMENT 'Unique source identifier, e.g. gl_transactions',
    source_path       STRING        NOT NULL   COMMENT 'ADLS Gen2 path: abfss://container@storage.dfs...',
    file_format       STRING        NOT NULL   COMMENT 'csv | parquet | json | delta',
    delimiter         STRING                   COMMENT 'Field delimiter for CSV sources',
    header            BOOLEAN       DEFAULT true,
    target_table      STRING        NOT NULL   COMMENT 'Target Delta table: bronze.gl_transactions',
    load_type         STRING        NOT NULL   COMMENT 'full | incremental',
    watermark_column  STRING                   COMMENT 'Column for incremental load: transaction_date',
    last_loaded_at    TIMESTAMP                COMMENT 'Timestamp of last successful load',
    dq_rules          STRING                   COMMENT 'JSON: {"null_cols":["id"],"min_rows":100}',
    is_active         BOOLEAN       DEFAULT true,
    created_at        TIMESTAMP     DEFAULT current_timestamp(),
    updated_at        TIMESTAMP     DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Master control table — add a row here to onboard a new source';

-- Audit table: one row per pipeline run
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id            STRING        NOT NULL,
    source_id         STRING        NOT NULL,
    layer             STRING        NOT NULL   COMMENT 'bronze | silver | gold',
    status            STRING        NOT NULL   COMMENT 'running | success | failed',
    rows_processed    LONG,
    start_time        TIMESTAMP     DEFAULT current_timestamp(),
    end_time          TIMESTAMP,
    error_message     STRING,
    cluster_id        STRING,
    notebook_path     STRING
)
USING DELTA
COMMENT 'Audit log for all pipeline runs across all layers';

-- Sample source registrations
INSERT INTO config.file_sources VALUES
('gl_transactions',
 'abfss://raw@prodadlsstorage.dfs.core.windows.net/finance/gl/',
 'csv', ',', true,
 'bronze.gl_transactions', 'incremental', 'posting_date', null,
 '{"null_cols":["journal_id","account_code"],"min_rows":500,"value_ranges":{"amount":{"min":-9999999,"max":9999999}}}',
 true, current_timestamp(), current_timestamp()),

('ap_invoices',
 'abfss://raw@prodadlsstorage.dfs.core.windows.net/finance/ap_invoices/',
 'csv', ',', true,
 'bronze.ap_invoices', 'incremental', 'invoice_date', null,
 '{"null_cols":["invoice_id","vendor_id","amount"],"min_rows":50}',
 true, current_timestamp(), current_timestamp()),

('vendor_master',
 'abfss://raw@prodadlsstorage.dfs.core.windows.net/reference/vendors/',
 'parquet', null, null,
 'bronze.vendor_master', 'full', null, null,
 '{"null_cols":["vendor_id","vendor_name"],"min_rows":10}',
 true, current_timestamp(), current_timestamp());
