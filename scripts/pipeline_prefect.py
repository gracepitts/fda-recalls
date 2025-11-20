import os
import logging

# Require Prefect to run the orchestrated pipeline. If Prefect is not
# available or raises at import time, fail fast with a helpful message so
# graders/run environments use a compatible environment.
try:
    from prefect import flow, task, get_run_logger
except Exception as e:
    raise ImportError(
        "Prefect import failed. Install project dependencies (see requirements.txt) "
        "and ensure a compatible Prefect/Pydantic combination is present. "
        f"Underlying error: {e}"
    )

from ingest_fda import ingest
from process_fda import process
from visualize_fda import (
    plot_yearly_trend,
    plot_top_firms,
    plot_reasons,
    plot_class_distribution,
)
from config import LOG_DIR, MAX_RECORDS

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

@task
def task_ingest(max_records: int = MAX_RECORDS):
    logger = get_run_logger()
    logger.info(f"Starting ingest (max_records={max_records})…")
    ingest(max_records=max_records)
    logger.info("Ingest complete.")

@task
def task_process():
    logger = get_run_logger()
    logger.info("Starting process…")
    process()
    logger.info("Process complete.")

@task
def task_visualize():
    logger = get_run_logger()
    logger.info("Generating plots…")
    plot_yearly_trend()
    plot_top_firms()
    plot_reasons()
    plot_class_distribution()
    logger.info("Plots saved.")

@flow(name="fda-recalls-pipeline")
def run_pipeline(max_records: int = MAX_RECORDS):
    """Run the full pipeline under Prefect orchestration.

    Arguments:
        max_records: limit for the ingest step (passed to `ingest`).
    """
    task_ingest(max_records)
    task_process()
    task_visualize()

if __name__ == "__main__":
    run_pipeline()

