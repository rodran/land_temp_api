-- =====================================================
-- Land Temperature Change API - Database Setup
-- Script 04: Create Fact Table
-- =====================================================

-- =====================================================
-- Fact Table: Temperature Measurements
-- =====================================================
CREATE TABLE core.fact_temperature (
    fact_id BIGSERIAL PRIMARY KEY,
    area_key INTEGER NOT NULL REFERENCES core.dim_area(area_key),
    period_key INTEGER NOT NULL REFERENCES core.dim_time_period(period_key),
    metric_key INTEGER NOT NULL REFERENCES core.dim_metric(metric_key),
    year INTEGER NOT NULL CHECK (year BETWEEN 1880 AND 2200),
    value NUMERIC(10, 4),
    
    -- Business key constraint (one measurement per combination)
    CONSTRAINT uk_temp_measurement UNIQUE (area_key, period_key, metric_key, year)
);

-- Comments
COMMENT ON TABLE core.fact_temperature IS 'Temperature measurements fact table (~616k records estimated)';
COMMENT ON COLUMN core.fact_temperature.value IS 'Temperature change or standard deviation value (nullable for missing data)';
COMMENT ON COLUMN core.fact_temperature.year IS 'Measurement year (1880-2200)';
COMMENT ON CONSTRAINT uk_temp_measurement ON core.fact_temperature IS 'Ensures no duplicate measurements';
