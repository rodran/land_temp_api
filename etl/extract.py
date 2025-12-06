"""
Extract Module
Reads CSV files and validates their existence and format.
"""
import logging
import pandas as pd
from typing import Tuple

from . import config

logger = logging.getLogger(__name__)


def validate_files() -> None:
    """
    Validate that all required CSV files exist.
    
    Raises:
        FileNotFoundError: If any required file is missing
    """
    files_to_check = [
        ("Main CSV", config.MAIN_CSV),
        ("Area Codes", config.AREA_CODES_CSV),
        ("Elements", config.ELEMENTS_CSV)
    ]
    
    missing_files = []
    
    for name, filepath in files_to_check:
        if not filepath.exists():
            logger.error(f"{name} not found at: {filepath}")
            missing_files.append(name)
        else:
            logger.info(f"{name} found: {filepath}")
    
    if missing_files:
        raise FileNotFoundError(
            f"Required CSV files are missing: {', '.join(missing_files)}"
        )


def extract_main_data() -> pd.DataFrame:
    """
    Extract main temperature data from CSV.
    
    Returns:
        pd.DataFrame: Raw temperature data with year columns (Y1961-Y2024)
    
    Raises:
        Exception: If file cannot be read
    """
    logger.info(f"Reading main CSV: {config.MAIN_CSV}")
    
    try:
        df = pd.read_csv(config.MAIN_CSV, encoding="utf-8")
        logger.info(f"Successfully loaded {len(df):,} rows")
        logger.info(f"Columns: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to read main CSV: {e}")
        raise


def extract_area_codes() -> pd.DataFrame:
    """
    Extract area codes reference data.
    
    Returns:
        pd.DataFrame: Area codes mapping with columns cleaned
    
    Raises:
        Exception: If file cannot be read
    """
    logger.info(f"Reading area codes CSV: {config.AREA_CODES_CSV}")
    
    try:
        df = pd.read_csv(config.AREA_CODES_CSV, encoding="utf-8")
        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        logger.info(f"Loaded {len(df):,} area codes")
        return df
        
    except Exception as e:
        logger.error(f"Failed to read area codes CSV: {e}")
        raise


def extract_elements() -> pd.DataFrame:
    """
    Extract elements (metrics) reference data.
    
    Returns:
        pd.DataFrame: Elements mapping with columns cleaned
    
    Raises:
        Exception: If file cannot be read
    """
    logger.info(f"Reading elements CSV: {config.ELEMENTS_CSV}")
    
    try:
        df = pd.read_csv(config.ELEMENTS_CSV, encoding="utf-8")
        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        logger.info(f"Loaded {len(df):,} elements")
        return df
        
    except Exception as e:
        logger.error(f"Failed to read elements CSV: {e}")
        raise


def extract_all() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extract all data sources with validation.
    
    Returns:
        Tuple of (main_data, area_codes, elements) DataFrames
    
    Raises:
        FileNotFoundError: If required files are missing
        Exception: If any file cannot be read
    """
    logger.info("Starting data extraction...")
    
    validate_files()
    
    main_data = extract_main_data()
    area_codes = extract_area_codes()
    elements = extract_elements()
    
    logger.info("Data extraction completed successfully")
    
    return main_data, area_codes, elements
