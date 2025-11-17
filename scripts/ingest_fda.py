"""Ingest FDA recall data from the FDA API."""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "ingest_fda.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def fetch_recalls(
    recall_type: str, limit: int = 100, skip: int = 0
) -> Optional[Dict]:
    """
    Fetch recalls from FDA API.

    Args:
        recall_type: Type of recall ('food', 'drug', or 'device')
        limit: Number of records to fetch
        skip: Number of records to skip

    Returns:
        JSON response from API or None if error
    """
    if recall_type not in config.FDA_RECALLS_ENDPOINTS:
        logger.error(f"Invalid recall type: {recall_type}")
        return None

    endpoint = config.FDA_RECALLS_ENDPOINTS[recall_type]
    params = {"limit": limit, "skip": skip}

    try:
        logger.info(f"Fetching {recall_type} recalls (limit={limit}, skip={skip})")
        response = requests.get(
            endpoint, params=params, timeout=config.API_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {recall_type} recalls: {e}")
        return None


def save_raw_data(data: Dict, recall_type: str, timestamp: str) -> None:
    """
    Save raw JSON data to file.

    Args:
        data: JSON data to save
        recall_type: Type of recall
        timestamp: Timestamp string for filename
    """
    config.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    filename = config.RAW_DATA_DIR / f"{recall_type}_{timestamp}.json"

    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved raw data to {filename}")
    except Exception as e:
        logger.error(f"Error saving raw data: {e}")


def ingest_all_recalls(save_raw: bool = True, limit: int = 100) -> Dict[str, List]:
    """
    Ingest all recall types from FDA API.

    Args:
        save_raw: Whether to save raw JSON files
        limit: Number of records to fetch per type

    Returns:
        Dictionary with recall data by type
    """
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_recalls = {}

    for recall_type in config.FDA_RECALLS_ENDPOINTS.keys():
        data = fetch_recalls(recall_type, limit=limit)
        if data and "results" in data:
            all_recalls[recall_type] = data["results"]
            logger.info(f"Fetched {len(data['results'])} {recall_type} recalls")

            if save_raw:
                save_raw_data(data, recall_type, timestamp)
        else:
            logger.warning(f"No data fetched for {recall_type}")
            all_recalls[recall_type] = []

    return all_recalls


if __name__ == "__main__":
    logger.info("Starting FDA recalls ingestion")
    recalls = ingest_all_recalls(save_raw=True, limit=100)
    total = sum(len(v) for v in recalls.values())
    logger.info(f"Ingestion complete. Total recalls: {total}")
