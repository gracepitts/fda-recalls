"""Prefect pipeline for FDA recalls data processing."""

import logging
import sys
from pathlib import Path

from prefect import flow, task

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from ingest_fda import ingest_all_recalls
from process_fda import process_and_store
from visualize_fda import generate_all_visualizations

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "pipeline_prefect.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@task(name="ingest-fda-recalls", retries=3, retry_delay_seconds=60)
def ingest_task(save_raw: bool = True, limit: int = 100):
    """
    Prefect task to ingest FDA recall data.

    Args:
        save_raw: Whether to save raw JSON files
        limit: Number of records to fetch per type

    Returns:
        Dictionary with recall data by type
    """
    logger.info("Starting ingest task")
    recalls = ingest_all_recalls(save_raw=save_raw, limit=limit)
    logger.info(f"Ingest task complete. Total recalls: {sum(len(v) for v in recalls.values())}")
    return recalls


@task(name="process-fda-recalls", retries=2)
def process_task(recalls):
    """
    Prefect task to process and store FDA recall data.

    Args:
        recalls: Dictionary with recall data by type
    """
    logger.info("Starting process task")
    process_and_store(recalls)
    logger.info("Process task complete")


@task(name="visualize-fda-recalls", retries=2)
def visualize_task(save_plots: bool = True):
    """
    Prefect task to generate visualizations.

    Args:
        save_plots: Whether to save plots to files
    """
    logger.info("Starting visualize task")
    generate_all_visualizations(save_plots=save_plots)
    logger.info("Visualize task complete")


@flow(name="fda-recalls-pipeline", log_prints=True)
def fda_recalls_pipeline(
    save_raw: bool = True, limit: int = 100, save_plots: bool = True
):
    """
    Main Prefect flow for FDA recalls data pipeline.

    Args:
        save_raw: Whether to save raw JSON files
        limit: Number of records to fetch per recall type
        save_plots: Whether to save visualization plots
    """
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting FDA recalls pipeline")

    # Ingest data from FDA API
    recalls = ingest_task(save_raw=save_raw, limit=limit)

    # Process and store in DuckDB
    process_task(recalls)

    # Generate visualizations
    visualize_task(save_plots=save_plots)

    logger.info("FDA recalls pipeline complete")


if __name__ == "__main__":
    # Run the pipeline
    fda_recalls_pipeline(save_raw=True, limit=100, save_plots=True)
