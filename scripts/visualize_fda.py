"""
Script to visualize FDA recall data
"""
import logging
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import sys
sys.path.append(str(Path(__file__).parent.parent))

import config

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "visualize_fda.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_data_from_duckdb(query: str) -> pd.DataFrame:
    """
    Load data from DuckDB using a query
    
    Args:
        query: SQL query to execute
        
    Returns:
        DataFrame with query results
    """
    con = duckdb.connect(str(config.DUCKDB_PATH), read_only=True)
    
    try:
        df = con.execute(query).df()
        return df
    finally:
        con.close()


def plot_recalls_by_product_type():
    """Create bar chart of recalls by product type"""
    logger.info("Creating recalls by product type visualization")
    
    query = """
        SELECT 
            product_type,
            COUNT(*) as count
        FROM recalls
        GROUP BY product_type
        ORDER BY count DESC
    """
    
    df = load_data_from_duckdb(query)
    
    fig = px.bar(
        df,
        x="product_type",
        y="count",
        title="FDA Recalls by Product Type",
        labels={"product_type": "Product Type", "count": "Number of Recalls"}
    )
    
    output_file = config.DATA_DIR / "recalls_by_type.html"
    fig.write_html(str(output_file))
    logger.info(f"Saved visualization to {output_file}")
    
    return fig


def plot_recalls_over_time():
    """Create time series of recalls"""
    logger.info("Creating recalls over time visualization")
    
    query = """
        SELECT 
            DATE_TRUNC('month', recall_initiation_date) as month,
            product_type,
            COUNT(*) as count
        FROM recalls
        WHERE recall_initiation_date IS NOT NULL
        GROUP BY month, product_type
        ORDER BY month
    """
    
    df = load_data_from_duckdb(query)
    
    fig = px.line(
        df,
        x="month",
        y="count",
        color="product_type",
        title="FDA Recalls Over Time",
        labels={"month": "Month", "count": "Number of Recalls", "product_type": "Product Type"}
    )
    
    output_file = config.DATA_DIR / "recalls_over_time.html"
    fig.write_html(str(output_file))
    logger.info(f"Saved visualization to {output_file}")
    
    return fig


def plot_recalls_by_classification():
    """Create pie chart of recalls by classification"""
    logger.info("Creating recalls by classification visualization")
    
    query = """
        SELECT 
            classification,
            COUNT(*) as count
        FROM recalls
        WHERE classification IS NOT NULL
        GROUP BY classification
        ORDER BY count DESC
    """
    
    df = load_data_from_duckdb(query)
    
    fig = px.pie(
        df,
        values="count",
        names="classification",
        title="FDA Recalls by Classification"
    )
    
    output_file = config.DATA_DIR / "recalls_by_classification.html"
    fig.write_html(str(output_file))
    logger.info(f"Saved visualization to {output_file}")
    
    return fig


def main():
    """Main function to create visualizations"""
    logger.info("Starting FDA recall data visualization")
    
    # Check if database exists
    if not config.DUCKDB_PATH.exists():
        logger.error(f"Database not found at {config.DUCKDB_PATH}")
        logger.error("Please run process_fda.py first to create the database")
        return
    
    try:
        # Create visualizations
        plot_recalls_by_product_type()
        plot_recalls_over_time()
        plot_recalls_by_classification()
        
        logger.info("All visualizations created successfully")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {e}")
        raise
    
    logger.info("FDA recall data visualization completed")


if __name__ == "__main__":
    main()
