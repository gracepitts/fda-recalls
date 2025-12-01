#!/usr/bin/env python3
"""CLI runner for the FDA recalls pipeline.

Usage examples:

# Run with Prefect orchestration (default mode)
python scripts/run_pipeline.py --max-records 1000

# Run sequentially without Prefect (useful when Prefect is not installed or fails)
python scripts/run_pipeline.py --max-records 100 --no-prefect
"""

import argparse
# Argument parsing enables CLI flags for user control (records, Prefect mode)

import sys
import logging
import os

# Add the project root to Python path so imports remain valid
# even when script is run from outside the repository root.
# This ensures imports like `from config import ...` work properly.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import MAX_RECORDS  # Default max ingest value

# Allow overriding the default max-records via an environment variable so a user
# can run the script without typing the flag each time. Set `PIPELINE_MAX_RECORDS`
# to a positive integer to override the value from `config.py`.
try:
    DEFAULT_MAX_RECORDS = int(os.environ.get("PIPELINE_MAX_RECORDS", MAX_RECORDS))
except Exception:
    DEFAULT_MAX_RECORDS = MAX_RECORDS

# Basic logging for console visibility (file logs handled elsewhere)
logging.basicConfig(level=logging.INFO)

# Define command-line arguments supported by this runner
parser = argparse.ArgumentParser(description="Run the FDA recalls pipeline")
parser.add_argument(
    "--max-records",
    type=int,
    default=DEFAULT_MAX_RECORDS,
    help="Max records to ingest"
)
parser.add_argument(
    "--no-prefect",
    action="store_true",
    help="Run sequentially without Prefect"
)
args = parser.parse_args()

if args.no_prefect:
    # Run pipeline steps one-by-one without Prefect
    # This mode guarantees the pipeline runs even if Prefect dependencies break.
    sys.path.insert(0, "scripts")
    from ingest_fda import ingest
    from process_fda import process
    from visualize_fda import (
        plot_yearly_trend,
        plot_top_firms,
        plot_reasons,
        plot_class_distribution
    )

    logging.info(f"Running sequential pipeline with max_records={args.max_records}")

    # Execute each stage in correct order
    ingest(max_records=args.max_records)
    process()
    plot_yearly_trend()
    plot_top_firms()
    plot_reasons()
    plot_class_distribution()

    logging.info("Sequential pipeline complete")

else:
    # Try to run with Prefect orchestration; if Prefect is unavailable or the flow
    # fails to import/run, automatically fall back to the sequential runner so
    # the pipeline still works without extra flags.
    sys.path.insert(0, "scripts")
    try:
        from pipeline_prefect import run_pipeline
        logging.info(f"Running Prefect flow with max_records={args.max_records}")

        # In Prefect 3, calling the flow function actually runs the workflow
        run_pipeline(args.max_records)
        logging.info("Prefect pipeline run complete")

    except Exception as e:
        logging.warning("Prefect pipeline unavailable or failed: %s", e)
        logging.info("Falling back to sequential pipeline")

        # Sequential fallback (same steps as --no-prefect)
        sys.path.insert(0, "scripts")
        from ingest_fda import ingest
        from process_fda import process
        from visualize_fda import (
            plot_yearly_trend,
            plot_top_firms,
            plot_reasons,
            plot_class_distribution,
        )

        logging.info(f"Running sequential pipeline with max_records={args.max_records}")

        ingest(max_records=args.max_records)
        process()
        plot_yearly_trend()
        plot_top_firms()
        plot_reasons()
        plot_class_distribution()

        logging.info("Sequential pipeline complete (fallback)")
