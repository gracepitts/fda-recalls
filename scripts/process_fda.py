"""
Script to process FDA recall data and load into DuckDB
"""
import json
import logging
from pathlib import Path
from typing import List

import duckdb
import pandas as pd

import sys
sys.path.append(str(Path(__file__).parent.parent))

import config

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "process_fda.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_raw_data(product_type: str) -> List[dict]:
    """
    Load the most recent raw data file for a product type
    
    Args:
        product_type: Type of product (food, drug, device)
        
    Returns:
        List of recall records
    """
    pattern = f"{product_type}_recalls_*.json"
    files = sorted(config.RAW_DATA_DIR.glob(pattern))
    
    if not files:
        logger.warning(f"No raw data files found for {product_type}")
        return []
        
    latest_file = files[-1]
    logger.info(f"Loading data from {latest_file}")
    
    with open(latest_file, "r") as f:
        data = json.load(f)
        
    return data


def process_recalls(recalls: List[dict], product_type: str) -> pd.DataFrame:
    """
    Process recall records into a structured DataFrame
    
    Args:
        recalls: List of recall records
        product_type: Type of product
        
    Returns:
        Processed DataFrame
    """
    logger.info(f"Processing {len(recalls)} {product_type} recalls")
    
    df = pd.DataFrame(recalls)
    
    # Add product type column
    df["product_type"] = product_type
    
    # Convert date fields
    date_columns = ["recall_initiation_date", "report_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    logger.info(f"Processed DataFrame shape: {df.shape}")
    return df


def load_to_duckdb(df: pd.DataFrame, table_name: str = "recalls"):
    """
    Load DataFrame into DuckDB
    
    Args:
        df: DataFrame to load
        table_name: Name of the table
    """
    logger.info(f"Loading data to DuckDB table '{table_name}'")
    
    con = duckdb.connect(str(config.DUCKDB_PATH))
    
    try:
        # Create table or replace if exists
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM df
        """)
        
        # Insert new data (in real scenario, handle duplicates)
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM df
        """)
        
        # Get row count
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        logger.info(f"Table '{table_name}' now has {result[0]} rows")
        
    finally:
        con.close()


def main():
    """Main function to process FDA recall data"""
    logger.info("Starting FDA recall data processing")
    
    product_types = ["food", "drug", "device"]
    all_dfs = []
    
    for product_type in product_types:
        logger.info(f"Processing {product_type} recalls")
        
        # Load raw data
        recalls = load_raw_data(product_type)
        
        if not recalls:
            continue
            
        # Process data
        df = process_recalls(recalls, product_type)
        all_dfs.append(df)
    
    if all_dfs:
        # Combine all dataframes
        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Combined DataFrame shape: {combined_df.shape}")
        
        # Load to DuckDB
        load_to_duckdb(combined_df)
    else:
        logger.warning("No data to process")
    
    logger.info("FDA recall data processing completed")


if __name__ == "__main__":
    main()
