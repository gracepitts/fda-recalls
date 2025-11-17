"""
Configuration file for FDA Recalls data pipeline
"""
import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
SCRIPTS_DIR = BASE_DIR / "scripts"
LOGS_DIR = BASE_DIR / "logs"

# Database
DUCKDB_PATH = DATA_DIR / "fda_recalls.duckdb"

# FDA API endpoints
FDA_API_BASE_URL = "https://api.fda.gov"
FDA_FOOD_RECALL_ENDPOINT = f"{FDA_API_BASE_URL}/food/enforcement.json"
FDA_DRUG_RECALL_ENDPOINT = f"{FDA_API_BASE_URL}/drug/enforcement.json"
FDA_DEVICE_RECALL_ENDPOINT = f"{FDA_API_BASE_URL}/device/enforcement.json"

# API settings
API_LIMIT = 100  # Number of records per API call
API_TIMEOUT = 30  # Timeout in seconds

# Logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
