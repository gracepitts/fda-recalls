# General project configuration

FDA_ENFORCEMENT_ENDPOINT = "https://api.fda.gov/drug/enforcement.json"
# Additional enforcement endpoints we may aggregate to increase dataset size
FDA_ENFORCEMENT_ENDPOINTS = [
	"https://api.fda.gov/drug/enforcement.json",
	"https://api.fda.gov/food/enforcement.json",
]

# API paging
LIMIT_PER_REQUEST = 100           # OpenFDA max limit per page is typically 100
MAX_RECORDS = 100000              # Stop once you’ve collected this many
REQUESTS_PER_MIN = 180            # Be nice to the API

# Storage
DUCKDB_PATH = "data/fda_recalls.duckdb"
RAW_DUMP_DIR = "data/raw"         # optional: set to None to skip writing raw files

# Prefect / logging
LOG_DIR = "logs"

