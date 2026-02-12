-- =============================================================
-- AIRFLOW AGENTIC AI - Schema Migration v3.0
-- Enterprise Upgrade: Adds columns for schema drift, retry,
-- SCD2, dead letter queue, data profiling
-- Run AFTER v2.0 migration
-- =============================================================

-- =============================================================
-- 1. Upgrade config.pipeline_config with enterprise columns
-- =============================================================
ALTER TABLE config.pipeline_config
    ADD COLUMN IF NOT EXISTS source_table VARCHAR(200),
    ADD COLUMN IF NOT EXISTS target_path TEXT,
    ADD COLUMN IF NOT EXISTS connection_id INTEGER,
    ADD COLUMN IF NOT EXISTS load_type VARCHAR(20) DEFAULT 'full',
    ADD COLUMN IF NOT EXISTS watermark_column VARCHAR(100),
    ADD COLUMN IF NOT EXISTS watermark_value TEXT,
    ADD COLUMN IF NOT EXISTS primary_key VARCHAR(100),
    ADD COLUMN IF NOT EXISTS partition_columns TEXT,
    ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 100,
    ADD COLUMN IF NOT EXISTS table_type VARCHAR(20) DEFAULT 'fact',
    ADD COLUMN IF NOT EXISTS scd_type INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 3,
    ADD COLUMN IF NOT EXISTS retry_delay_seconds INTEGER DEFAULT 30,
    ADD COLUMN IF NOT EXISTS batch_size INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS enable_schema_drift BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS enable_cdc BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS enable_profiling BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS enable_dlq BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS null_handling JSONB DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

-- Add constraint for load_type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_pipeline_load_type'
    ) THEN
        ALTER TABLE config.pipeline_config
            ADD CONSTRAINT chk_pipeline_load_type 
            CHECK (load_type IN ('full', 'incremental', 'cdc'));
    END IF;
END $$;

-- Add constraint for table_type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_pipeline_table_type'
    ) THEN
        ALTER TABLE config.pipeline_config
            ADD CONSTRAINT chk_pipeline_table_type 
            CHECK (table_type IN ('fact', 'dimension', 'bridge', 'aggregate'));
    END IF;
END $$;

-- Add constraint for scd_type
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_pipeline_scd_type'
    ) THEN
        ALTER TABLE config.pipeline_config
            ADD CONSTRAINT chk_pipeline_scd_type 
            CHECK (scd_type IN (0, 1, 2));
    END IF;
END $$;

-- =============================================================
-- 2. Add new transformation types for enterprise
-- =============================================================
ALTER TABLE config.transformation_rules
    DROP CONSTRAINT IF EXISTS transformation_rules_transformation_type_check;

ALTER TABLE config.transformation_rules
    ADD CONSTRAINT transformation_rules_transformation_type_check 
    CHECK (transformation_type IN (
        'rename', 'cast', 'derive', 'mask', 'hash',
        'trim', 'upper', 'lower', 'coalesce', 'default_value',
        'regex_replace', 'concat', 'split', 'lookup'
    ));

-- =============================================================
-- 3. Upgrade config.schema_registry for drift tracking
-- =============================================================
ALTER TABLE config.schema_registry
    ADD COLUMN IF NOT EXISTS previous_version INTEGER,
    ADD COLUMN IF NOT EXISTS drift_type VARCHAR(30),
    ADD COLUMN IF NOT EXISTS drift_details JSONB,
    ADD COLUMN IF NOT EXISTS auto_resolved BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS resolved_by VARCHAR(100),
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_schema_drift_type'
    ) THEN
        ALTER TABLE config.schema_registry
            ADD CONSTRAINT chk_schema_drift_type 
            CHECK (drift_type IN (
                'column_added', 'column_removed', 'type_changed',
                'nullable_changed', 'initial', 'no_change'
            ));
    END IF;
END $$;

-- =============================================================
-- 4. Create config.null_handling_rules
-- =============================================================
CREATE TABLE IF NOT EXISTS config.null_handling_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    column_name VARCHAR(100) NOT NULL,
    strategy VARCHAR(30) CHECK (strategy IN (
        'drop_row', 'default_value', 'coalesce', 'forward_fill', 'mean', 'median'
    )),
    default_value TEXT,
    coalesce_columns TEXT[],
    execution_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- 5. Create config.referential_integrity_rules
-- =============================================================
CREATE TABLE IF NOT EXISTS config.referential_integrity_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    fk_column VARCHAR(100) NOT NULL,
    reference_table VARCHAR(200) NOT NULL,
    reference_column VARCHAR(100) NOT NULL,
    reference_layer VARCHAR(20) CHECK (reference_layer IN ('bronze', 'silver', 'gold')),
    on_violation VARCHAR(30) CHECK (on_violation IN ('reject', 'quarantine', 'warn', 'set_null')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- 6. Create ai.data_profiles (auto-generated statistics)
-- =============================================================
CREATE TABLE IF NOT EXISTS ai.data_profiles (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER,
    table_name VARCHAR(200) NOT NULL,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver', 'gold')),
    column_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50),
    total_count BIGINT,
    null_count BIGINT,
    null_percent NUMERIC(5,2),
    distinct_count BIGINT,
    min_value TEXT,
    max_value TEXT,
    mean_value NUMERIC,
    std_dev NUMERIC,
    p25 NUMERIC,
    p50 NUMERIC,
    p75 NUMERIC,
    sample_values JSONB,
    profiled_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_profiles_table 
    ON ai.data_profiles(table_name, layer, profiled_at DESC);

-- =============================================================
-- 7. Create ai.schema_drift_events (drift audit log)
-- =============================================================
CREATE TABLE IF NOT EXISTS ai.schema_drift_events (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER,
    table_name VARCHAR(200) NOT NULL,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver', 'gold')),
    drift_type VARCHAR(30) NOT NULL,
    column_name VARCHAR(100),
    old_type VARCHAR(50),
    new_type VARCHAR(50),
    action_taken VARCHAR(30) CHECK (action_taken IN (
        'auto_added', 'auto_cast', 'quarantined', 'ignored', 'pending_review'
    )),
    is_resolved BOOLEAN DEFAULT FALSE,
    detected_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schema_drift_pipeline 
    ON ai.schema_drift_events(pipeline_id, detected_at DESC);

-- =============================================================
-- 8. Create ai.dead_letter_queue (quarantine tracking)
-- =============================================================
CREATE TABLE IF NOT EXISTS ai.dead_letter_queue (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER,
    table_name VARCHAR(200) NOT NULL,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver')),
    quarantine_path TEXT NOT NULL,
    reason VARCHAR(50) CHECK (reason IN (
        'dq_failure', 'schema_mismatch', 'type_error',
        'referential_integrity', 'null_violation', 'parse_error'
    )),
    row_count BIGINT,
    sample_data JSONB,
    is_reprocessed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    reprocessed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dlq_pipeline 
    ON ai.dead_letter_queue(pipeline_id, is_reprocessed, created_at DESC);

-- =============================================================
-- SEED DATA: Sample Enterprise Pipeline Config
-- =============================================================
-- Only insert if no pipelines exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM config.pipeline_config LIMIT 1) THEN
        
        -- Bronze layer pipelines
        INSERT INTO config.pipeline_config (
            name, layer, source_table, target_path, connection_id,
            load_type, watermark_column, primary_key, table_type, scd_type,
            enable_schema_drift, enable_dlq, is_active, priority
        ) VALUES 
        (
            'bronze_sales_order_header', 'bronze',
            'Sales.SalesOrderHeader', 's3a://bronze/sales/salesorderheader', 1,
            'incremental', 'ModifiedDate', 'SalesOrderID', 'fact', 1,
            TRUE, TRUE, TRUE, 10
        ),
        (
            'bronze_sales_order_detail', 'bronze',
            'Sales.SalesOrderDetail', 's3a://bronze/sales/salesorderdetail', 1,
            'incremental', 'ModifiedDate', 'SalesOrderDetailID', 'fact', 1,
            TRUE, TRUE, TRUE, 20
        ),
        (
            'bronze_person', 'bronze',
            'Person.Person', 's3a://bronze/person/person', 1,
            'full', NULL, 'BusinessEntityID', 'dimension', 2,
            TRUE, TRUE, TRUE, 30
        ),
        (
            'bronze_product', 'bronze',
            'Production.Product', 's3a://bronze/production/product', 1,
            'full', NULL, 'ProductID', 'dimension', 2,
            TRUE, TRUE, TRUE, 40
        ),
        (
            'bronze_customer', 'bronze',
            'Sales.Customer', 's3a://bronze/sales/customer', 1,
            'full', NULL, 'CustomerID', 'dimension', 2,
            TRUE, TRUE, TRUE, 50
        );

        -- Silver layer pipelines
        INSERT INTO config.pipeline_config (
            name, layer, source_table, target_path,
            load_type, primary_key, table_type, scd_type,
            enable_schema_drift, enable_dlq, enable_profiling, is_active, priority
        ) VALUES
        (
            'silver_sales_orders', 'silver',
            's3a://bronze/sales/salesorderheader', 's3a://silver/fact_sales_orders',
            'incremental', 'order_id', 'fact', 1,
            TRUE, TRUE, TRUE, TRUE, 10
        ),
        (
            'silver_sales_order_items', 'silver',
            's3a://bronze/sales/salesorderdetail', 's3a://silver/fact_order_items',
            'incremental', 'order_line_id', 'fact', 1,
            TRUE, TRUE, TRUE, TRUE, 20
        ),
        (
            'silver_customers', 'silver',
            's3a://bronze/sales/customer', 's3a://silver/dim_customer',
            'full', 'customer_id', 'dimension', 2,
            TRUE, TRUE, TRUE, TRUE, 30
        ),
        (
            'silver_products', 'silver',
            's3a://bronze/production/product', 's3a://silver/dim_product',
            'full', 'product_id', 'dimension', 2,
            TRUE, TRUE, TRUE, TRUE, 40
        );

    END IF;
END $$;

-- Done!
SELECT 'Migration v3.0 completed successfully!' AS status;
