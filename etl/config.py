"""
ETL Pipeline Configuration
Centralized settings for data extraction, transformation, and loading.
"""
import os
from pathlib import Path


# ===== Database Configuration ===== 
DATABASE_USER = os.getenv("POSTGRES_USER")
DATABASE_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DATABASE_HOST = os.getenv("POSTGRES_HOST")
DATABASE_PORT = os.getenv("POSTGRES_PORT")
DATABASE_INSTANCE = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_INSTANCE}"
BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE"))


# ===== File Paths ===== 
# Source CSV files
MAIN_CSV = Path(os.getenv("CSV_INPUT_PATH"))
AREA_CODES_CSV = Path(os.getenv("CSV_AREAS_PATH"))
ELEMENTS_CSV = Path(os.getenv("CSV_ELEMENTS_PATH"))


# ===== Data Validation Ranges ===== 
MIN_YEAR = 1880
MAX_YEAR = 2200
MIN_TEMP_CHANGE = -20.0  # Reasonable lower bound for temperature change (°C)
MAX_TEMP_CHANGE = 20.0   # Reasonable upper bound for temperature change (°C)


# ===== Logging Configuration ===== 
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL")


# ===== Hierarchical Classifications ===== 
AREA_TYPE_MAPPING = {
    "world": ["World"],
    "continent": [
        "Africa", "Americas", "Asia", "Europe", "Oceania"
    ],
    "subregion": [
        # Africa subregions
        "Eastern Africa", "Middle Africa", "Northern Africa", 
        "Southern Africa", "Western Africa",
        # Americas subregions
        "Caribbean", "Central America", "South America", "Northern America",
        # Asia subregions
        "Central Asia", "Eastern Asia", "South-eastern Asia", 
        "Southern Asia", "Western Asia",
        # Europe subregions
        "Eastern Europe", "Northern Europe", "Southern Europe", "Western Europe",
        # Oceania subregions
        "Australia and New Zealand", "Melanesia", "Micronesia", "Polynesia"
    ]
}

PERIOD_TYPE_MAPPING = {
    "month": [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ],
    "season": [
        # Support both regular hyphens and en-dashes from FAO data
        "December-January-February", "March-April-May",
        "June-July-August", "September-October-November",
        "Dec–Jan–Feb", "Mar–Apr–May",
        "Jun–Jul–Aug", "Sep–Oct–Nov"
    ],
    "annual": ["Meteorological year"]
}

# Month to number mapping
MONTH_TO_NUMBER = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}

# Season to quarter mapping
SEASON_TO_QUARTER = {
    # Standard format with hyphens
    "December-January-February": 1,
    "March-April-May": 2,
    "June-July-August": 3,
    "September-October-November": 4,
    # FAO format with en-dashes (U+2013)
    "Dec–Jan–Feb": 1,
    "Mar–Apr–May": 2,
    "Jun–Jul–Aug": 3,
    "Sep–Oct–Nov": 4
}


# ===== Subregion to Continent Mapping ===== 
# Used for building area hierarchy relationships

SUBREGION_TO_CONTINENT = {
    # Africa
    "Eastern Africa": "Africa",
    "Middle Africa": "Africa",
    "Northern Africa": "Africa",
    "Southern Africa": "Africa",
    "Western Africa": "Africa",
    # Americas
    "Caribbean": "Americas",
    "Central America": "Americas",
    "South America": "Americas",
    "Northern America": "Americas",
    # Asia
    "Central Asia": "Asia",
    "Eastern Asia": "Asia",
    "South-eastern Asia": "Asia",
    "Southern Asia": "Asia",
    "Western Asia": "Asia",
    # Europe
    "Eastern Europe": "Europe",
    "Northern Europe": "Europe",
    "Southern Europe": "Europe",
    "Western Europe": "Europe",
    # Oceania
    "Australia and New Zealand": "Oceania",
    "Melanesia": "Oceania",
    "Micronesia": "Oceania",
    "Polynesia": "Oceania"
}
