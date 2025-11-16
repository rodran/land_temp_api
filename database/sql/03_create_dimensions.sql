-- =====================================================
-- Land Temperature Change API - Database Setup
-- Script 03: Create Dimension Tables
-- =====================================================

-- =====================================================
-- Dimension 1: Area (Hierarchical)
-- =====================================================
CREATE TABLE core.dim_area (
    area_key SERIAL PRIMARY KEY,
    area_code INTEGER NOT NULL,
    m49_code VARCHAR(10) NOT NULL UNIQUE,
    area_name VARCHAR(200) NOT NULL,
    area_type VARCHAR(20) NOT NULL CHECK (area_type IN ('country', 'subregion', 'continent', 'world')),
    parent_area_key INTEGER REFERENCES core.dim_area(area_key),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_area_type ON core.dim_area(area_type);
CREATE INDEX idx_area_parent ON core.dim_area(parent_area_key);
CREATE INDEX idx_area_m49 ON core.dim_area(m49_code);
CREATE INDEX idx_area_name ON core.dim_area(area_name);

-- Comments
COMMENT ON TABLE core.dim_area IS 'Geographic hierarchy: country -> subregion -> continent -> world';
COMMENT ON COLUMN core.dim_area.parent_area_key IS 'Self-referencing FK for hierarchy (NULL for world level)';
COMMENT ON COLUMN core.dim_area.area_type IS 'Level in geographic hierarchy';

-- =====================================================
-- Dimension 2: Time Period (Hierarchical)
-- =====================================================
CREATE TABLE core.dim_time_period (
    period_key SERIAL PRIMARY KEY,
    period_code INTEGER NOT NULL UNIQUE,
    period_name VARCHAR(50) NOT NULL UNIQUE,
    period_type VARCHAR(20) NOT NULL CHECK (period_type IN ('month', 'season', 'annual')),
    month_number INTEGER CHECK (month_number BETWEEN 1 AND 12),
    quarter INTEGER CHECK (quarter BETWEEN 1 AND 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes 
CREATE INDEX idx_period_type ON core.dim_time_period(period_type);
CREATE INDEX idx_period_month ON core.dim_time_period(month_number);

-- Comments
COMMENT ON TABLE core.dim_time_period IS 'Temporal hierarchy: month -> season -> annual (17 fixed records)';
COMMENT ON COLUMN core.dim_time_period.period_type IS 'Granularity level: month, season, or annual';
COMMENT ON COLUMN core.dim_time_period.month_number IS 'Month number (1-12) for monthly periods, NULL otherwise';
COMMENT ON COLUMN core.dim_time_period.quarter IS 'Quarter number (1-4) for seasonal periods, NULL otherwise';

-- =====================================================
-- Dimension 3: Metric
-- =====================================================
CREATE TABLE core.dim_metric (
    metric_key SERIAL PRIMARY KEY,
    metric_code INTEGER NOT NULL UNIQUE,
    metric_name VARCHAR(100) NOT NULL UNIQUE,
    unit VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comments
COMMENT ON TABLE core.dim_metric IS 'Measurement types: Temperature change, Standard Deviation, etc.';
COMMENT ON COLUMN core.dim_metric.metric_code IS 'Element code from source data (FAOSTAT standard)';
COMMENT ON COLUMN core.dim_metric.unit IS 'Measurement unit (e.g., °C)';
