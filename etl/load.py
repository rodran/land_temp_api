"""
Load Module
Handles loading transformed data into PostgreSQL data warehouse.
"""
import logging
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, text, Engine

from . import config
from .utils.area_classifier import get_parent_area

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """
    Create SQLAlchemy engine for database connection.
    
    Returns:
        SQLAlchemy Engine instance
    """
    return create_engine(config.DATABASE_URL)


def test_connection(engine: Engine) -> None:
    """
    Test database connection.
    
    Args:
        engine: SQLAlchemy engine
        
    Raises:
        ConnectionError: If database connection fails
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            logger.info(f"Database connection successful: {version}")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise ConnectionError(f"Cannot connect to database: {e}")


def truncate_staging(engine: Engine) -> None:
    """
    Truncate staging table before loading new data.
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Truncating staging.raw_temperature...")
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.raw_temperature RESTART IDENTITY CASCADE"))
        conn.commit()
    
    logger.info("Staging table truncated")


def load_to_staging(df: pd.DataFrame, engine: Engine) -> None:
    """
    Load transformed data to staging table in batches.
    
    Args:
        df: Transformed dataframe with all required columns
        engine: SQLAlchemy engine
    """
    logger.info("Loading data to staging.raw_temperature...")
    
    # Select only columns needed for staging table
    staging_cols = [
        "area_code", "m49_code", "area_name",
        "months_code", "period_name",
        "element_code", "element_name", "unit",
        "year", "value"
    ]
    
    df_staging = df[staging_cols].copy()
    
    # Load in batches to avoid memory issues
    total_rows = len(df_staging)
    batch_size = config.BATCH_SIZE
    
    for i in range(0, total_rows, batch_size):
        batch = df_staging.iloc[i:i+batch_size]
        batch.to_sql(
            "raw_temperature",
            engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi"
        )
        
        batch_num = i // batch_size + 1
        logger.info(f"Loaded batch {batch_num}: {i+len(batch):,}/{total_rows:,} rows")
    
    logger.info(f"Successfully loaded {total_rows:,} rows to staging")


def load_dim_area(df: pd.DataFrame, engine: Engine) -> None:
    """
    Load dim_area dimension table.
    
    Extracts unique areas from dataframe and loads them.
    Then updates parent relationships for hierarchy.
    
    Args:
        df: Transformed dataframe with area classifications
        engine: SQLAlchemy engine
    """
    logger.info("Loading dim_area...")
    
    # Get unique areas with their classification
    areas = df[["area_code", "m49_code", "area_name", "area_type"]].drop_duplicates()
    
    # Truncate dimension table
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE core.dim_area RESTART IDENTITY CASCADE"))
        conn.commit()
    
    # Load areas
    areas.to_sql(
        "dim_area",
        engine,
        schema="core",
        if_exists="append",
        index=False,
        method="multi"
    )
    
    logger.info(f"Loaded {len(areas):,} areas to dim_area")
    
    # Update parent relationships
    update_area_hierarchy(engine)


def update_area_hierarchy(engine: Engine) -> None:
    """
    Update parent_area_key relationships in dim_area table.
    
    Sets up hierarchical relationships:
    - Continents point to World
    - Subregions point to their parent continent
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Updating area hierarchy (parent relationships)...")
    
    with engine.connect() as conn:
        # Update continents to point to World
        conn.execute(text("""
            UPDATE core.dim_area
            SET parent_area_key = (
                SELECT area_key 
                FROM core.dim_area 
                WHERE area_type = 'world'
            )
            WHERE area_type = 'continent'
        """))
        
        # Update subregions to point to their continents
        result = conn.execute(text("""
            SELECT area_key, area_name 
            FROM core.dim_area 
            WHERE area_type = 'subregion'
        """))
        
        subregions = result.fetchall()
        
        for area_key, area_name in subregions:
            # Get parent continent name using classifier
            parent_name = get_parent_area(area_name, "subregion")
            
            if parent_name:
                conn.execute(text("""
                    UPDATE core.dim_area
                    SET parent_area_key = (
                        SELECT area_key 
                        FROM core.dim_area 
                        WHERE area_name = :parent_name AND area_type = 'continent'
                    )
                    WHERE area_key = :area_key
                """), {"parent_name": parent_name, "area_key": area_key})
        
        conn.commit()
    
    logger.info("Area hierarchy updated")


def load_dim_time_period(df: pd.DataFrame, engine: Engine) -> None:
    """
    Load dim_time_period dimension table.
    
    Args:
        df: Transformed dataframe with period classifications
        engine: SQLAlchemy engine
    """
    logger.info("Loading dim_time_period...")
    
    # Get unique periods with their attributes
    periods = df[[
        "months_code", "period_name", "period_type", 
        "month_number", "quarter"
    ]].drop_duplicates()
    
    # Rename to match dimension table schema
    periods = periods.rename(columns={"months_code": "period_code"})
    
    # Truncate dimension table
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE core.dim_time_period RESTART IDENTITY CASCADE"))
        conn.commit()
    
    # Load periods
    periods.to_sql(
        "dim_time_period",
        engine,
        schema="core",
        if_exists="append",
        index=False,
        method="multi"
    )
    
    logger.info(f"Loaded {len(periods):,} time periods to dim_time_period")


def load_dim_metric(df: pd.DataFrame, engine: Engine) -> None:
    """
    Load dim_metric dimension table.
    
    Args:
        df: Transformed dataframe with metric information
        engine: SQLAlchemy engine
    """
    logger.info("Loading dim_metric...")
    
    # Get unique metrics
    metrics = df[["element_code", "element_name", "unit"]].drop_duplicates()
    
    # Rename to match dimension table schema
    metrics = metrics.rename(columns={
        "element_code": "metric_code",
        "element_name": "metric_name"
    })
    
    # Truncate dimension table
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE core.dim_metric RESTART IDENTITY CASCADE"))
        conn.commit()
    
    # Load metrics
    metrics.to_sql(
        "dim_metric",
        engine,
        schema="core",
        if_exists="append",
        index=False,
        method="multi"
    )
    
    logger.info(f"Loaded {len(metrics):,} metrics to dim_metric")


def load_dimensions(df: pd.DataFrame, engine: Engine) -> None:
    """
    Load all dimension tables.
    
    Args:
        df: Transformed dataframe with all classifications
        engine: SQLAlchemy engine
    """
    logger.info("Loading dimension tables...")
    
    load_dim_area(df, engine)
    load_dim_time_period(df, engine)
    load_dim_metric(df, engine)
    
    logger.info("Dimension tables loaded successfully")


def load_facts(engine: Engine) -> None:
    """
    Load fact_temperature table from staging and dimensions.
    
    Uses SQL joins to map foreign keys from staging to dimensions.
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Loading fact_temperature...")
    
    # Truncate fact table
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE core.fact_temperature RESTART IDENTITY"))
        conn.commit()
    
    # Insert facts with foreign key lookups via SQL joins
    insert_sql = """
        INSERT INTO core.fact_temperature (area_key, period_key, metric_key, year, value)
        SELECT 
            a.area_key,
            p.period_key,
            m.metric_key,
            s.year,
            s.value
        FROM staging.raw_temperature s
        INNER JOIN core.dim_area a 
            ON s.m49_code = a.m49_code
        INNER JOIN core.dim_time_period p 
            ON s.months_code = p.period_code
        INNER JOIN core.dim_metric m 
            ON s.element_code = m.metric_code
    """
    
    logger.info("Executing fact table insert (this may take a few minutes)...")
    
    with engine.connect() as conn:
        result = conn.execute(text(insert_sql))
        conn.commit()
        rows_inserted = result.rowcount
    
    logger.info(f"Successfully loaded {rows_inserted:,} facts to fact_temperature")


def refresh_analytics_views(engine: Engine) -> None:
    """
    Refresh all materialized views in analytics schema.
    
    Args:
        engine: SQLAlchemy engine
    """
    logger.info("Refreshing analytics materialized views...")
    
    with engine.connect() as conn:
        conn.execute(text("SELECT analytics.refresh_all_views()"))
        conn.commit()
    
    logger.info("Analytics views refreshed successfully")


def get_load_statistics(engine: Engine) -> Dict[str, int]:
    """
    Get row counts from all loaded tables.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        Dictionary with table labels and row counts
    """
    logger.info("Gathering load statistics...")
    
    stats = {}
    
    tables = [
        ("staging.raw_temperature", "Staging"),
        ("core.dim_area", "Areas"),
        ("core.dim_time_period", "Time Periods"),
        ("core.dim_metric", "Metrics"),
        ("core.fact_temperature", "Facts")
    ]
    
    with engine.connect() as conn:
        for table, label in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            stats[label] = count
            logger.info(f"  {label}: {count:,} rows")
    
    return stats


def load_data(df: pd.DataFrame, engine: Engine) -> Dict[str, int]:
    """
    Execute complete loading pipeline.
    
    Pipeline steps:
    1. Test database connection
    2. Truncate and load staging table
    3. Load dimension tables
    4. Load fact table
    5. Refresh analytics views
    6. Return statistics
    
    Args:
        df: Transformed dataframe ready for loading
        engine: SQLAlchemy engine
        
    Returns:
        Dictionary with final row counts for all tables
        
    Raises:
        ConnectionError: If database connection fails
    """
    logger.info("Starting load pipeline...")
    
    # Test connection
    test_connection(engine)
    
    # Load staging
    truncate_staging(engine)
    load_to_staging(df, engine)
    
    # Load dimensions
    load_dimensions(df, engine)
    
    # Load facts
    load_facts(engine)
    
    # Refresh analytics
    refresh_analytics_views(engine)
    
    # Get statistics
    stats = get_load_statistics(engine)
    
    logger.info("Load pipeline completed successfully!")
    
    return stats
