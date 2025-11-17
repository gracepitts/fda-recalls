"""
Prefect workflow orchestration for FDA recall data pipeline
"""
import logging
from pathlib import Path

from prefect import flow, task

import sys
sys.path.append(str(Path(__file__).parent.parent))

import config
from scripts import ingest_fda, process_fda, visualize_fda

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "pipeline_prefect.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@task(name="ingest_fda_data", retries=2, retry_delay_seconds=60)
def ingest_data_task():
    """Task to ingest FDA recall data"""
    logger.info("Running ingest data task")
    ingest_fda.main()
    return "Ingestion completed"


@task(name="process_fda_data", retries=1)
def process_data_task():
    """Task to process FDA recall data"""
    logger.info("Running process data task")
    process_fda.main()
    return "Processing completed"


@task(name="visualize_fda_data", retries=1)
def visualize_data_task():
    """Task to visualize FDA recall data"""
    logger.info("Running visualize data task")
    visualize_fda.main()
    return "Visualization completed"


@flow(name="fda_recall_pipeline", log_prints=True)
def fda_recall_pipeline():
    """
    Main Prefect flow for FDA recall data pipeline
    
    This flow orchestrates:
    1. Data ingestion from FDA API
    2. Data processing and loading to DuckDB
    3. Data visualization
    """
    logger.info("Starting FDA recall pipeline")
    
    # Step 1: Ingest data
    ingest_result = ingest_data_task()
    logger.info(f"Ingest result: {ingest_result}")
    
    # Step 2: Process data
    process_result = process_data_task()
    logger.info(f"Process result: {process_result}")
    
    # Step 3: Visualize data
    visualize_result = visualize_data_task()
    logger.info(f"Visualize result: {visualize_result}")
    
    logger.info("FDA recall pipeline completed successfully")
    return {
        "ingest": ingest_result,
        "process": process_result,
        "visualize": visualize_result
    }


if __name__ == "__main__":
    # Run the pipeline
    result = fda_recall_pipeline()
    print(f"Pipeline result: {result}")
