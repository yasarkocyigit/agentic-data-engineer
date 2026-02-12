-- =============================================================
-- AIRFLOW AGENTIC AI - Schema Migration v4.0
-- Config-as-Code: Drop config.* schema (moved to YAML)
-- Keep: ai.* schema (runtime data for AI Agent)
-- Run AFTER v3.0 migration
-- =============================================================

-- =============================================================
-- 1. Drop all config.* tables (now in config/pipelines.yml)
-- =============================================================

-- Drop tables with FK dependencies first
DROP TABLE IF EXISTS config.referential_integrity_rules CASCADE;
DROP TABLE IF EXISTS config.null_handling_rules CASCADE;
DROP TABLE IF EXISTS config.pipeline_dependencies CASCADE;
DROP TABLE IF EXISTS config.transformation_rules CASCADE;
DROP TABLE IF EXISTS config.data_quality_rules CASCADE;
DROP TABLE IF EXISTS config.sla_definitions CASCADE;
DROP TABLE IF EXISTS config.gold_metrics CASCADE;
DROP TABLE IF EXISTS config.model_lineage CASCADE;
DROP TABLE IF EXISTS config.notebook_registry CASCADE;
DROP TABLE IF EXISTS config.schema_registry CASCADE;
DROP TABLE IF EXISTS config.feature_flags CASCADE;
DROP TABLE IF EXISTS config.connections CASCADE;
DROP TABLE IF EXISTS config.pipeline_config CASCADE;

-- Drop the config schema entirely
DROP SCHEMA IF EXISTS config CASCADE;

-- =============================================================
-- 2. Verify ai.* schema is intact
-- =============================================================
-- These tables remain for AI Agent runtime data:
--   ai.audit_logs
--   ai.memory
--   ai.conversation_history
--   ai.traces
--   ai.execution_metrics
--   ai.approval_policies
--   ai.approval_requests
--   ai.data_profiles
--   ai.schema_drift_events
--   ai.dead_letter_queue

DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM pg_tables WHERE schemaname = 'ai';
    
    RAISE NOTICE 'ai.* schema has % tables (expected 10)', table_count;
END $$;

-- Done!
SELECT 'Migration v4.0 completed: config.* dropped, ai.* retained' AS status;
