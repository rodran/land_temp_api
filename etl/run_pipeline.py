"""
ETL Pipeline Runner
Main entry point for executing the complete ETL process.
"""
import logging
import sys
from datetime import datetime

from . import config
from .extract import extract_all
from .transform import transform_data
from .load import load_data, get_engine


def setup_logging() -> None:
    """
    Configure logging for the ETL pipeline.
    
    Sets up console and file logging with appropriate formatting.
    """
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('etl_pipeline.log')
        ]
    )


def run_pipeline() -> int:
    """
    Execute the complete ETL pipeline.
    
    Pipeline phases:
    1. Extract: Read CSV files
    2. Transform: Unpivot, classify, validate
    3. Load: Insert into PostgreSQL data warehouse
    
    Returns:
        Exit code: 0 for success, 1 for failure
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 70)
    logger.info("LAND TEMPERATURE CHANGE ETL PIPELINE")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    logger.info(f"Pipeline started at: {start_time}")
    
    try:
        # ===== PHASE 1: EXTRACT =====
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 1: EXTRACT")
        logger.info("=" * 70)
        
        main_data, area_codes, elements = extract_all()
        
        logger.info(f"Extracted {len(main_data):,} rows from main CSV")
        logger.info(f"Extracted {len(area_codes):,} area codes")
        logger.info(f"Extracted {len(elements):,} elements")
        
        # ===== PHASE 2: TRANSFORM =====
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 2: TRANSFORM")
        logger.info("=" * 70)
        
        transformed_data = transform_data(main_data)
        
        logger.info(f"Transformation complete: {len(transformed_data):,} rows")
        logger.info(f"Columns: {list(transformed_data.columns)}")
        
        # ===== PHASE 3: LOAD =====
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 3: LOAD")
        logger.info("=" * 70)
        
        engine = get_engine()
        stats = load_data(transformed_data, engine)
        
        # ===== SUMMARY =====
        logger.info("\n" + "=" * 70)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 70)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"Pipeline completed at: {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info("\nFinal Record Counts:")
        for table, count in stats.items():
            logger.info(f"  {table}: {count:,} rows")
        
        logger.info("\n" + "=" * 70)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 70)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        logger.error("Please ensure all CSV files are in the correct location")
        return 1
        
    except ConnectionError as e:
        logger.error(f"Database connection error: {e}")
        logger.error("Please ensure PostgreSQL is running and accessible")
        return 1
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(run_pipeline())
