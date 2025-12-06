"""
Area Classification Utilities
Functions to classify geographic areas into hierarchical types.
"""
from typing import Optional

from .. import config


def classify_area(area_name: str) -> str:
    """
    Classify geographic area into hierarchy level.
    
    Args:
        area_name: Name of the geographic area
        
    Returns:
        Area type: 'world', 'continent', 'subregion', or 'country'
    """
    area_name = area_name.strip()
    
    # Check against known mappings
    for area_type, names in config.AREA_TYPE_MAPPING.items():
        if area_name in names:
            return area_type
    
    # Default to country for any unrecognized area
    return "country"


def get_parent_area(area_name: str, area_type: str) -> Optional[str]:
    """
    Determine parent area in geographic hierarchy.
    
    Hierarchy: country -> subregion -> continent -> world
    
    Args:
        area_name: Name of the area
        area_type: Type of the area (world/continent/subregion/country)
        
    Returns:
        Parent area name or None if no parent (World is top level)
    """
    if area_type == "world":
        return None
    
    if area_type == "continent":
        return "World"
    
    if area_type == "subregion":
        return config.SUBREGION_TO_CONTINENT.get(area_name)
    
    # Countries: parent assignment handled in load phase
    # via M49 code relationships from the actual data
    return None
