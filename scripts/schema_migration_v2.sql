-- =============================================================
-- AIRFLOW AGENTIC AI - Schema Migration v2.0
-- Creates 18 tables across 2 schemas: config.* and ai.*
-- =============================================================

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================
-- SCHEMA 1: config (Data Engineering Metadata)
-- =============================================================
CREATE SCHEMA IF NOT EXISTS config;

-- Move existing tables to config schema
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'pipeline_config') THEN
        ALTER TABLE public.pipeline_config SET SCHEMA config;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'connections') THEN
        ALTER TABLE public.connections SET SCHEMA config;
    END IF;
END $$;

-- config.pipeline_dependencies
CREATE TABLE IF NOT EXISTS config.pipeline_dependencies (
    id SERIAL PRIMARY KEY,
    upstream_pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    downstream_pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    dependency_type VARCHAR(30) CHECK (dependency_type IN ('hard', 'soft', 'sensor')),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(upstream_pipeline_id, downstream_pipeline_id)
);

-- config.transformation_rules
CREATE TABLE IF NOT EXISTS config.transformation_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    source_column VARCHAR(100),
    target_column VARCHAR(100),
    transformation_type VARCHAR(50) CHECK (transformation_type IN ('rename', 'cast', 'derive', 'mask', 'hash')),
    transformation_expression TEXT,
    execution_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- config.data_quality_rules
CREATE TABLE IF NOT EXISTS config.data_quality_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) CHECK (rule_type IN ('not_null', 'unique', 'range', 'regex', 'custom_sql')),
    column_name VARCHAR(100),
    rule_expression TEXT,
    severity VARCHAR(20) CHECK (severity IN ('warning', 'error', 'critical')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- config.schema_registry
CREATE TABLE IF NOT EXISTS config.schema_registry (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver', 'gold')),
    version INTEGER NOT NULL,
    schema_definition JSONB NOT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(table_name, layer, version)
);

-- config.notebook_registry
CREATE TABLE IF NOT EXISTS config.notebook_registry (
    id SERIAL PRIMARY KEY,
    notebook_name VARCHAR(100) UNIQUE NOT NULL,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver', 'gold')),
    notebook_type VARCHAR(30) CHECK (notebook_type IN ('spark_sdp', 'pyspark', 'sql', 'dbt_model')),
    notebook_path TEXT,
    description TEXT,
    parameters JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- config.gold_metrics
CREATE TABLE IF NOT EXISTS config.gold_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) UNIQUE NOT NULL,
    notebook_id INTEGER REFERENCES config.notebook_registry(id),
    metric_type VARCHAR(30) CHECK (metric_type IN ('aggregation', 'ratio', 'count', 'time_series', 'snapshot')),
    calculation_logic TEXT,
    grain VARCHAR(50),
    dimensions JSONB,
    business_owner VARCHAR(100),
    refresh_frequency VARCHAR(30),
    created_at TIMESTAMP DEFAULT NOW()
);

-- config.model_lineage
CREATE TABLE IF NOT EXISTS config.model_lineage (
    id SERIAL PRIMARY KEY,
    notebook_id INTEGER REFERENCES config.notebook_registry(id),
    input_table VARCHAR(100) NOT NULL,
    input_layer VARCHAR(20) CHECK (input_layer IN ('bronze', 'silver', 'gold', 'external')),
    output_table VARCHAR(100) NOT NULL,
    output_layer VARCHAR(20) CHECK (output_layer IN ('bronze', 'silver', 'gold')),
    relationship_type VARCHAR(30) CHECK (relationship_type IN ('read', 'write', 'append', 'merge')),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(notebook_id, input_table, output_table)
);

-- config.sla_definitions
CREATE TABLE IF NOT EXISTS config.sla_definitions (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES config.pipeline_config(id),
    sla_type VARCHAR(30) CHECK (sla_type IN ('freshness', 'completeness', 'latency')),
    threshold_value INTEGER,
    threshold_unit VARCHAR(20) CHECK (threshold_unit IN ('minutes', 'hours', 'rows', 'percent')),
    alert_channels JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- config.feature_flags
CREATE TABLE IF NOT EXISTS config.feature_flags (
    id SERIAL PRIMARY KEY,
    flag_name VARCHAR(100) UNIQUE NOT NULL,
    flag_type VARCHAR(30) CHECK (flag_type IN ('boolean', 'percentage', 'user_list')),
    flag_value JSONB,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- SCHEMA 2: ai (Agent Control Plane)
-- =============================================================
CREATE SCHEMA IF NOT EXISTS ai;

-- Move existing audit_logs to ai schema
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'audit_logs') THEN
        ALTER TABLE public.audit_logs SET SCHEMA ai;
    END IF;
END $$;

-- ai.memory (Vector semantic memory)
CREATE TABLE IF NOT EXISTS ai.memory (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content TEXT NOT NULL,
    metadata JSONB,
    embedding vector(384),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_memory_embedding ON ai.memory 
USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- ai.conversation_history
CREATE TABLE IF NOT EXISTS ai.conversation_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL,
    trace_id UUID,
    role VARCHAR(20) CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT,
    tool_calls JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_conv_session ON ai.conversation_history(session_id);
CREATE INDEX IF NOT EXISTS idx_ai_conv_trace ON ai.conversation_history(trace_id);

-- ai.traces (Distributed tracing)
CREATE TABLE IF NOT EXISTS ai.traces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id UUID NOT NULL,
    parent_span_id UUID,
    span_name VARCHAR(100) NOT NULL,
    span_type VARCHAR(30) CHECK (span_type IN ('llm_call', 'tool_execution', 'planning', 'memory_recall')),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_ms INTEGER,
    status VARCHAR(20) CHECK (status IN ('started', 'success', 'error')),
    attributes JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_traces_trace_id ON ai.traces(trace_id);
CREATE INDEX IF NOT EXISTS idx_ai_traces_parent ON ai.traces(parent_span_id);

-- ai.execution_metrics
CREATE TABLE IF NOT EXISTS ai.execution_metrics (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER,
    run_id VARCHAR(100) NOT NULL,
    trace_id UUID,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    rows_read BIGINT,
    rows_written BIGINT,
    bytes_processed BIGINT,
    error_message TEXT,
    spark_app_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_metrics_trace ON ai.execution_metrics(trace_id);
CREATE INDEX IF NOT EXISTS idx_ai_metrics_pipeline ON ai.execution_metrics(pipeline_id);

-- ai.approval_policies (HITL governance rules)
CREATE TABLE IF NOT EXISTS ai.approval_policies (
    id SERIAL PRIMARY KEY,
    policy_name VARCHAR(100) UNIQUE NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    target_pattern VARCHAR(255),
    risk_level VARCHAR(20) CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    requires_approval BOOLEAN DEFAULT TRUE,
    approvers JSONB,
    auto_approve_conditions JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_policies_action ON ai.approval_policies(action_type);

-- ai.approval_requests (HITL approval queue)
CREATE TABLE IF NOT EXISTS ai.approval_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id UUID NOT NULL,
    policy_id INTEGER REFERENCES ai.approval_policies(id),
    action_type VARCHAR(50) NOT NULL,
    action_payload JSONB NOT NULL,
    requested_by VARCHAR(100),
    status VARCHAR(20) CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'auto_approved')),
    reviewed_by VARCHAR(100),
    review_comment TEXT,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    reviewed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_approvals_status ON ai.approval_requests(status);
CREATE INDEX IF NOT EXISTS idx_ai_approvals_trace ON ai.approval_requests(trace_id);
CREATE INDEX IF NOT EXISTS idx_ai_approvals_pending ON ai.approval_requests(status, expires_at) WHERE status = 'pending';

-- Add indexes to existing ai.audit_logs if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'ai' AND tablename = 'audit_logs') THEN
        CREATE INDEX IF NOT EXISTS idx_ai_audit_trace ON ai.audit_logs(trace_id);
        CREATE INDEX IF NOT EXISTS idx_ai_audit_time ON ai.audit_logs(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ai_audit_entity ON ai.audit_logs(entity_type, entity_id);
    END IF;
END $$;

-- =============================================================
-- SEED DATA: Default approval policies
-- =============================================================
INSERT INTO ai.approval_policies (policy_name, action_type, target_pattern, risk_level, requires_approval)
VALUES 
    ('run_gold_pipeline', 'run_pipeline', 'gold.*', 'high', true),
    ('delete_table', 'delete_table', '*', 'critical', true),
    ('add_table', 'add_table', '*', 'high', true),
    ('run_notebook', 'run_notebook', '*', 'high', true)
ON CONFLICT (policy_name) DO NOTHING;

-- Done!
SELECT 'Migration completed successfully!' AS status;
