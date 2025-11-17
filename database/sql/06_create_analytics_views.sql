-- =====================================================
-- Land Temperature Change API - Database Setup
-- Script 06: Create Analytics Views
-- =====================================================

-- =====================================================
-- View 1: Annual Temperature by Country
-- =====================================================
CREATE MATERIALIZED VIEW analytics.vw_annual_temp_by_country AS
SELECT 
    a.m49_code,
    a.area_name,
    f.year,
    MAX(CASE WHEN m.metric_name = 'Temperature change' THEN f.value END) AS temperature_change,
    MAX(CASE WHEN m.metric_name = 'Standard Deviation' THEN f.value END) AS std_dev
FROM core.fact_temperature f
INNER JOIN core.dim_area a ON f.area_key = a.area_key
INNER JOIN core.dim_time_period p ON f.period_key = p.period_key
INNER JOIN core.dim_metric m ON f.metric_key = m.metric_key
WHERE a.area_type = 'country'
  AND p.period_type = 'annual'
GROUP BY a.m49_code, a.area_name, f.year
ORDER BY a.area_name, f.year;

-- Indexes 
CREATE INDEX idx_mv_annual_country_m49 ON analytics.vw_annual_temp_by_country(m49_code);
CREATE INDEX idx_mv_annual_country_year ON analytics.vw_annual_temp_by_country(year);
CREATE INDEX idx_mv_annual_country_m49_year ON analytics.vw_annual_temp_by_country(m49_code, year);

COMMENT ON MATERIALIZED VIEW analytics.vw_annual_temp_by_country IS 
'Pre-aggregated annual temperature data by country with pivoted metrics';

-- =====================================================
-- View 2: Seasonal Patterns
-- =====================================================
CREATE MATERIALIZED VIEW analytics.vw_seasonal_patterns AS
SELECT 
    a.m49_code,
    a.area_name,
    a.area_type,
    p.period_name AS season,
    f.year,
    m.metric_name,
    AVG(f.value) AS avg_value,
    COUNT(*) AS record_count
FROM core.fact_temperature f
INNER JOIN core.dim_area a ON f.area_key = a.area_key
INNER JOIN core.dim_time_period p ON f.period_key = p.period_key
INNER JOIN core.dim_metric m ON f.metric_key = m.metric_key
WHERE p.period_type = 'season'
  AND f.value IS NOT NULL
GROUP BY a.m49_code, a.area_name, a.area_type, p.period_name, f.year, m.metric_name
ORDER BY a.area_name, f.year, p.period_name;

-- Indexes
CREATE INDEX idx_mv_seasonal_m49 ON analytics.vw_seasonal_patterns(m49_code);
CREATE INDEX idx_mv_seasonal_year ON analytics.vw_seasonal_patterns(year);
CREATE INDEX idx_mv_seasonal_type ON analytics.vw_seasonal_patterns(area_type);

COMMENT ON MATERIALIZED VIEW analytics.vw_seasonal_patterns IS 
'Seasonal temperature patterns for all areas (quarters/seasons)';

-- =====================================================
-- View 3: Warming by Continent
-- =====================================================
CREATE MATERIALIZED VIEW analytics.vw_warming_by_continent AS
SELECT 
    a.area_name AS continent,
    f.year,
    AVG(f.value) AS avg_temp_change,
    MIN(f.value) AS min_temp_change,
    MAX(f.value) AS max_temp_change,
    COUNT(*) AS measurement_count,
    STDDEV(f.value) AS temp_variability
FROM core.fact_temperature f
INNER JOIN core.dim_area a ON f.area_key = a.area_key
INNER JOIN core.dim_time_period p ON f.period_key = p.period_key
INNER JOIN core.dim_metric m ON f.metric_key = m.metric_key
WHERE a.area_type = 'continent'
  AND p.period_type = 'annual'
  AND m.metric_name = 'Temperature change'
  AND f.value IS NOT NULL
GROUP BY a.area_name, f.year
ORDER BY a.area_name, f.year;

-- Indexes
CREATE INDEX idx_mv_continent_year ON analytics.vw_warming_by_continent(year);
CREATE INDEX idx_mv_continent_name ON analytics.vw_warming_by_continent(continent);

COMMENT ON MATERIALIZED VIEW analytics.vw_warming_by_continent IS 
'Continental warming trends with statistical aggregations';

-- =====================================================
-- Refresh Function (for convenience)
-- =====================================================
CREATE OR REPLACE FUNCTION analytics.refresh_all_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW analytics.vw_annual_temp_by_country;
    REFRESH MATERIALIZED VIEW analytics.vw_seasonal_patterns;
    REFRESH MATERIALIZED VIEW analytics.vw_warming_by_continent;
    
    RAISE NOTICE 'All analytics views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION analytics.refresh_all_views IS 
'Convenience function to refresh all materialized views at once';
