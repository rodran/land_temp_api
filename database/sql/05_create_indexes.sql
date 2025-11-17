-- =====================================================
-- Land Temperature Change API - Database Setup
-- Script 05: Create Indexes for Fact Table
-- =====================================================

-- =====================================================
-- Basic Single-Column Indexes
-- =====================================================
CREATE INDEX idx_fact_area ON core.fact_temperature(area_key);
CREATE INDEX idx_fact_period ON core.fact_temperature(period_key);
CREATE INDEX idx_fact_metric ON core.fact_temperature(metric_key);
CREATE INDEX idx_fact_year ON core.fact_temperature(year);

-- =====================================================
-- Composite Indexes (Common Query Patterns)
-- =====================================================

-- Time series queries by area
CREATE INDEX idx_fact_area_year ON core.fact_temperature(area_key, year);

-- Filtered time series by area and period type
CREATE INDEX idx_fact_area_period_year ON core.fact_temperature(area_key, period_key, year);

-- Analytics queries with year range
CREATE INDEX idx_fact_year_area ON core.fact_temperature(year, area_key);

-- Metric-specific queries
CREATE INDEX idx_fact_metric_area ON core.fact_temperature(metric_key, area_key);

-- =====================================================
-- Partial Indexes (Optional - for specific use cases)
-- =====================================================

-- Index for non-null values only (faster analytics)
CREATE INDEX idx_fact_value_not_null ON core.fact_temperature(area_key, year, value) 
WHERE value IS NOT NULL;

-- Comments
COMMENT ON INDEX core.idx_fact_area_year IS 'Optimizes time series queries for specific areas';
COMMENT ON INDEX core.idx_fact_area_period_year IS 'Optimizes filtered time series (for example: monthly, annual)';
COMMENT ON INDEX core.idx_fact_value_not_null IS 'Optimizes analytics queries ignoring missing data';
