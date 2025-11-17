"""Process FDA recall data and store in DuckDB."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import duckdb
import pandas as pd

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "process_fda.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def create_database_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create the database schema for FDA recalls.

    Args:
        conn: DuckDB connection
    """
    logger.info("Creating database schema")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS recalls (
            id VARCHAR PRIMARY KEY,
            recall_type VARCHAR NOT NULL,
            recall_number VARCHAR,
            status VARCHAR,
            distribution_pattern TEXT,
            product_description TEXT,
            reason_for_recall TEXT,
            recall_initiation_date DATE,
            report_date DATE,
            classification VARCHAR,
            openfda_json TEXT,
            recalling_firm VARCHAR,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            voluntary_mandated VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    logger.info("Database schema created")


def process_recall_data(recalls: Dict[str, List]) -> pd.DataFrame:
    """
    Process raw recall data into a DataFrame.

    Args:
        recalls: Dictionary with recall data by type

    Returns:
        Processed DataFrame
    """
    logger.info("Processing recall data")
    all_records = []

    for recall_type, records in recalls.items():
        for record in records:
            processed_record = {
                "id": record.get("recall_number", "") + "_" + recall_type,
                "recall_type": recall_type,
                "recall_number": record.get("recall_number"),
                "status": record.get("status"),
                "distribution_pattern": record.get("distribution_pattern"),
                "product_description": record.get("product_description"),
                "reason_for_recall": record.get("reason_for_recall"),
                "recall_initiation_date": record.get("recall_initiation_date"),
                "report_date": record.get("report_date"),
                "classification": record.get("classification"),
                "openfda_json": str(record.get("openfda", {})),
                "recalling_firm": record.get("recalling_firm"),
                "city": record.get("city"),
                "state": record.get("state"),
                "country": record.get("country"),
                "voluntary_mandated": record.get("voluntary_mandated"),
            }
            all_records.append(processed_record)

    df = pd.DataFrame(all_records)
    logger.info(f"Processed {len(df)} records")
    return df


def store_in_duckdb(df: pd.DataFrame) -> None:
    """
    Store processed data in DuckDB.

    Args:
        df: DataFrame with processed recall data
    """
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Storing data in DuckDB at {config.DB_PATH}")

    try:
        conn = duckdb.connect(str(config.DB_PATH))
        create_database_schema(conn)

        # Insert data (replace duplicates)
        conn.execute("DELETE FROM recalls WHERE id IN (SELECT id FROM df)")
        conn.execute("INSERT INTO recalls SELECT * FROM df")

        # Get statistics
        count = conn.execute("SELECT COUNT(*) FROM recalls").fetchone()[0]
        logger.info(f"Database now contains {count} total recalls")

        conn.close()
    except Exception as e:
        logger.error(f"Error storing data in DuckDB: {e}")
        raise


def process_and_store(recalls: Dict[str, List]) -> None:
    """
    Main processing function.

    Args:
        recalls: Dictionary with recall data by type
    """
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting FDA recalls processing")

    df = process_recall_data(recalls)
    store_in_duckdb(df)

    logger.info("Processing complete")


if __name__ == "__main__":
    # Example usage - normally would be called from pipeline
    from ingest_fda import ingest_all_recalls

    logger.info("Running standalone process")
    recalls = ingest_all_recalls(save_raw=False, limit=10)
    process_and_store(recalls)
