-- =====================================================
-- Land Temperature Change API - Database Setup
-- Script 01: Create Schemas
-- =====================================================

-- Staging schema
DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;
COMMENT ON SCHEMA staging IS 'Raw data layer after initial extraction';
GRANT USAGE ON SCHEMA staging TO PUBLIC;

-- Core schema
DROP SCHEMA IF EXISTS core CASCADE;
CREATE SCHEMA core;
COMMENT ON SCHEMA core IS 'Dimensional model layer (star schema)';
GRANT USAGE ON SCHEMA core TO PUBLIC;

-- Analytics schema
DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA analytics;
COMMENT ON SCHEMA analytics IS 'Analytical views and aggregations';
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
