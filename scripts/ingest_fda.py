"""
Script to ingest FDA recall data from the FDA API
"""
import json
import logging
from datetime import datetime
from pathlib import Path

import requests

import sys
sys.path.append(str(Path(__file__).parent.parent))

import config

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "ingest_fda.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_recalls(endpoint: str, product_type: str, limit: int = 100) -> list:
    """
    Fetch recall data from FDA API
    
    Args:
        endpoint: FDA API endpoint URL
        product_type: Type of product (food, drug, device)
        limit: Number of records to fetch per request
        
    Returns:
        List of recall records
    """
    logger.info(f"Fetching {product_type} recalls from FDA API")
    
    all_results = []
    skip = 0
    
    try:
        while True:
            params = {
                "limit": limit,
                "skip": skip
            }
            
            response = requests.get(
                endpoint,
                params=params,
                timeout=config.API_TIMEOUT
            )
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                break
                
            all_results.extend(results)
            logger.info(f"Fetched {len(results)} records (total: {len(all_results)})")
            
            skip += limit
            
            # Limit total records for demo purposes
            if len(all_results) >= 1000:
                logger.info("Reached 1000 records limit")
                break
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        
    return all_results


def save_raw_data(data: list, product_type: str) -> Path:
    """
    Save raw data to JSON file
    
    Args:
        data: List of recall records
        product_type: Type of product
        
    Returns:
        Path to saved file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = config.RAW_DATA_DIR / f"{product_type}_recalls_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
        
    logger.info(f"Saved {len(data)} records to {filename}")
    return filename


def main():
    """Main function to ingest FDA recall data"""
    logger.info("Starting FDA recall data ingestion")
    
    # Define endpoints to fetch
    endpoints = [
        (config.FDA_FOOD_RECALL_ENDPOINT, "food"),
        (config.FDA_DRUG_RECALL_ENDPOINT, "drug"),
        (config.FDA_DEVICE_RECALL_ENDPOINT, "device"),
    ]
    
    for endpoint, product_type in endpoints:
        logger.info(f"Processing {product_type} recalls")
        recalls = fetch_recalls(endpoint, product_type, config.API_LIMIT)
        
        if recalls:
            save_raw_data(recalls, product_type)
        else:
            logger.warning(f"No data fetched for {product_type}")
    
    logger.info("FDA recall data ingestion completed")


if __name__ == "__main__":
    main()
