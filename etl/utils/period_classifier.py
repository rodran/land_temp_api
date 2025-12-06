"""
Period Classification Utilities
Functions to classify time periods and extract temporal attributes.
"""
from typing import Dict, Optional

from .. import config


def classify_period(period_name: str) -> str:
    """
    Classify time period into hierarchy level.
    
    Args:
        period_name: Name of the time period
        
    Returns:
        Period type: 'month', 'season', or 'annual'
        
    Raises:
        ValueError: If period name is not recognized
    """
    period_name = period_name.strip()
    
    # Check against known mappings
    for period_type, names in config.PERIOD_TYPE_MAPPING.items():
        if period_name in names:
            return period_type
    
    raise ValueError(f"Unknown period name: {period_name}")


def get_month_number(period_name: str) -> Optional[int]:
    """
    Get month number (1-12) for monthly periods.
    
    Args:
        period_name: Name of the period
        
    Returns:
        Month number (1-12) or None if not a monthly period
    """
    return config.MONTH_TO_NUMBER.get(period_name)


def get_quarter(period_name: str) -> Optional[int]:
    """
    Get quarter number (1-4) for seasonal periods.
    
    Args:
        period_name: Name of the period
        
    Returns:
        Quarter number (1-4) or None if not a seasonal period
    """
    return config.SEASON_TO_QUARTER.get(period_name)


def get_period_attributes(period_name: str) -> Dict[str, Optional[int] | str]:
    """
    Get all temporal attributes for a period.
    
    Combines period type classification with month/quarter extraction.
    
    Args:
        period_name: Name of the period
        
    Returns:
        Dictionary with:
            - period_type: 'month', 'season', or 'annual'
            - month_number: 1-12 for months, None otherwise
            - quarter: 1-4 for seasons, None otherwise
            
    Raises:
        ValueError: If period name is not recognized
    """
    return {
        "period_type": classify_period(period_name),
        "month_number": get_month_number(period_name),
        "quarter": get_quarter(period_name)
    }
