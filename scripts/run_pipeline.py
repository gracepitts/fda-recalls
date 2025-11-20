#!/usr/bin/env python3
"""CLI runner for the FDA recalls pipeline.

Usage examples:

# run with Prefect orchestration (default)
python scripts/run_pipeline.py --max-records 1000

# run sequentially without Prefect (useful for environments w/o Prefect)
python scripts/run_pipeline.py --max-records 100 --no-prefect
"""
import argparse
import sys
import logging
import os

# Ensure project root is on sys.path so `from config import ...` works when
# the script is executed from a different working directory (e.g., inside
# a container when invoking `python scripts/run_pipeline.py`). Insert the
# parent directory of `scripts/` at the front of sys.path.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import MAX_RECORDS

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Run the FDA recalls pipeline")
parser.add_argument("--max-records", type=int, default=MAX_RECORDS, help="Max records to ingest")
parser.add_argument("--no-prefect", action="store_true", help="Run sequentially without Prefect")
args = parser.parse_args()

if args.no_prefect:
    # Run steps sequentially without Prefect
    sys.path.insert(0, "scripts")
    from ingest_fda import ingest
    from process_fda import process
    from visualize_fda import plot_yearly_trend, plot_top_firms, plot_reasons, plot_class_distribution

    logging.info(f"Running sequential pipeline with max_records={args.max_records}")
    ingest(max_records=args.max_records)
    process()
    plot_yearly_trend()
    plot_top_firms()
    plot_reasons()
    plot_class_distribution()
    logging.info("Sequential pipeline complete")
else:
    # Attempt to import and run the Prefect flow
    sys.path.insert(0, "scripts")
    try:
        from pipeline_prefect import run_pipeline
    except Exception as e:
        logging.error("Failed to import Prefect-based pipeline: %s", e)
        logging.error("If you don't have Prefect available, re-run with --no-prefect")
        raise

    logging.info(f"Running Prefect flow with max_records={args.max_records}")
    # Run the Prefect flow; in Prefect 3 calling the flow function executes it.
    run_pipeline(args.max_records)
    logging.info("Prefect pipeline run complete")
