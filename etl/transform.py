"""
Transform Module
Handles data transformation: unpivot, classification, validation, and enrichment.
"""
import logging
import pandas as pd

from . import config
from .utils.area_classifier import classify_area
from .utils.period_classifier import get_period_attributes

logger = logging.getLogger(__name__)


def unpivot_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot year columns (Y1961-Y2024) into rows.
    
    Transforms wide format with year columns into long format with
    year and value as separate columns.
    
    Args:
        df: Raw dataframe with year columns (Y1961, Y1962, ..., Y2024)
        
    Returns:
        Unpivoted dataframe with normalized column names      
    """
    logger.info("Starting unpivot transformation...")
    
    # Identify year columns
    year_cols = [col for col in df.columns if col.startswith("Y")]
    logger.info(f"Found {len(year_cols)} year columns: {year_cols[0]} to {year_cols[-1]}")
    
    # Columns to keep as identifiers
    id_cols = [
        "Area Code", "Area Code (M49)", "Area",
        "Months Code", "Months",
        "Element Code", "Element", "Unit"
    ]
    
    # Unpivot using pandas melt
    df_unpivoted = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=year_cols,
        var_name="year_col",
        value_name="value"
    )
    
    # Extract year from column name (Y1961 -> 1961)
    df_unpivoted["year"] = df_unpivoted["year_col"].str.replace("Y", "").astype(int)
    df_unpivoted = df_unpivoted.drop(columns=["year_col"])
    
    # Rename columns to match staging schema (snake_case)
    df_unpivoted = df_unpivoted.rename(columns={
        "Area Code": "area_code",
        "Area Code (M49)": "m49_code",
        "Area": "area_name",
        "Months Code": "months_code",
        "Months": "period_name",
        "Element Code": "element_code",
        "Element": "element_name",
        "Unit": "unit"
    })
    
    logger.info(f"Unpivoted to {len(df_unpivoted):,} rows")
    
    return df_unpivoted


def add_area_classification(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add area_type classification column to dataframe.
    
    Classifies each area as: world, continent, subregion, or country.
    
    Args:
        df: DataFrame with area_name column
        
    Returns:
        DataFrame with area_type column added
    """
    logger.info("Classifying area types...")
    
    df = df.copy()
    df["area_type"] = df["area_name"].apply(classify_area)
    
    # Log distribution for monitoring
    type_counts = df["area_type"].value_counts()
    logger.info(f"Area type distribution:\n{type_counts}")
    
    return df


def add_period_classification(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add period classification columns to dataframe.
    
    Adds: period_type, month_number, quarter
    
    Args:
        df: DataFrame with period_name column
        
    Returns:
        DataFrame with period attributes added
    """
    logger.info("Classifying period types...")
    
    df = df.copy()
    
    # Apply period classification
    period_attrs = df["period_name"].apply(get_period_attributes)
    
    df["period_type"] = period_attrs.apply(lambda x: x["period_type"])
    df["month_number"] = period_attrs.apply(lambda x: x["month_number"])
    df["quarter"] = period_attrs.apply(lambda x: x["quarter"])
    
    # Log distribution for monitoring
    type_counts = df["period_type"].value_counts()
    logger.info(f"Period type distribution:\n{type_counts}")
    
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data quality and filter invalid records.
    
    Performs validation checks:
    - Year range validation (1880-2200)
    - Temperature value range checks (warning only)
    - Null value logging
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Validated dataframe with invalid records removed
    """
    logger.info("Validating data quality...")
    
    initial_count = len(df)
    
    # Log null values (keep them - missing measurements are valid)
    null_count = df["value"].isna().sum()
    logger.info(
        f"Records with null values: {null_count:,} "
        f"({null_count/initial_count*100:.1f}%)"
    )
    
    # Validate year range
    invalid_years = df[
        (df["year"] < config.MIN_YEAR) | 
        (df["year"] > config.MAX_YEAR)
    ]
    
    if len(invalid_years) > 0:
        logger.warning(f"Removing {len(invalid_years):,} records with invalid years")
        df = df[
            (df["year"] >= config.MIN_YEAR) & 
            (df["year"] <= config.MAX_YEAR)
        ]
    
    # Check for extreme temperature values (warning only, keep data)
    df_with_values = df[df["value"].notna()]
    temp_change_mask = df_with_values["element_name"] == "Temperature change"
    
    extreme_temps = df_with_values[
        temp_change_mask &
        (
            (df_with_values["value"] < config.MIN_TEMP_CHANGE) |
            (df_with_values["value"] > config.MAX_TEMP_CHANGE)
        )
    ]
    
    if len(extreme_temps) > 0:
        logger.warning(
            f"Found {len(extreme_temps):,} temperature change records "
            f"outside reasonable range ({config.MIN_TEMP_CHANGE} to "
            f"{config.MAX_TEMP_CHANGE}°C)"
        )
        logger.warning(
            f"Extreme values sample:\n"
            f"{extreme_temps[['area_name', 'year', 'value']].head()}"
        )
        # Keep them - might be legitimate extreme climate events
    
    final_count = len(df)
    logger.info(f"Validation complete: {final_count:,} records retained")
    
    return df


def transform_data(main_data: pd.DataFrame) -> pd.DataFrame:
    """
    Execute complete transformation pipeline.
    
    Pipeline steps:
    1. Unpivot year columns into rows
    2. Classify areas into hierarchical types
    3. Classify periods and extract temporal attributes
    4. Validate data quality
    
    Args:
        main_data: Raw temperature data from CSV
        
    Returns:
        Transformed dataframe ready for database loading
    """
    logger.info("Starting transformation pipeline...")
    
    # Step 1: Unpivot years
    df = unpivot_years(main_data)
    
    # Step 2: Classify areas
    df = add_area_classification(df)
    
    # Step 3: Classify periods
    df = add_period_classification(df)
    
    # Step 4: Validate data
    df = validate_data(df)
    
    logger.info("Transformation pipeline completed successfully")
    logger.info(f"Final dataset shape: {df.shape}")
    
    return df
