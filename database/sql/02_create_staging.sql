-- =====================================================
-- Land Temperature Change API - Database Setup
-- Script 02: Create Staging Layer
-- =====================================================

-- Staging table: receives raw data after unpivot transformation
CREATE TABLE staging.raw_temperature (
    id SERIAL PRIMARY KEY,
    area_code INTEGER NOT NULL,
    m49_code VARCHAR(10) NOT NULL,
    area_name VARCHAR(200) NOT NULL,
    months_code INTEGER NOT NULL,
    period_name VARCHAR(50) NOT NULL,
    element_code INTEGER NOT NULL,
    element_name VARCHAR(100) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL CHECK (year BETWEEN 1880 AND 2200),
    value NUMERIC(10, 4),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Basic indexes for staging queries
CREATE INDEX idx_staging_area_code ON staging.raw_temperature(area_code);
CREATE INDEX idx_staging_year ON staging.raw_temperature(year);
CREATE INDEX idx_staging_loaded_at ON staging.raw_temperature(loaded_at);

-- Comments
COMMENT ON TABLE staging.raw_temperature IS 'Raw temperature data after unpivoting year columns';
COMMENT ON COLUMN staging.raw_temperature.m49_code IS 'UN M49 standard country/area code';
COMMENT ON COLUMN staging.raw_temperature.value IS 'Temperature change value or standard deviation';
