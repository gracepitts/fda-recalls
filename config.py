"""Configuration for FDA Recalls project."""

import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent.absolute()

# Data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "fda_recalls.duckdb"

# Logs directory
LOGS_DIR = BASE_DIR / "logs"

# FDA API Configuration
FDA_API_BASE_URL = "https://api.fda.gov"
FDA_RECALLS_ENDPOINTS = {
    "food": f"{FDA_API_BASE_URL}/food/enforcement.json",
    "drug": f"{FDA_API_BASE_URL}/drug/enforcement.json",
    "device": f"{FDA_API_BASE_URL}/device/enforcement.json",
}

# API rate limiting
API_RATE_LIMIT = 240  # requests per minute
API_TIMEOUT = 30  # seconds

# Database configuration
DB_TIMEOUT = 60  # seconds
