#Overall configuration

FDA_ENFORCEMENT_ENDPOINT = "https://api.fda.gov/drug/enforcement.json"
# added more data sources
FDA_ENFORCEMENT_ENDPOINTS = [
	"https://api.fda.gov/drug/enforcement.json",
	"https://api.fda.gov/food/enforcement.json",
]

# API paging
LIMIT_PER_REQUEST = 100       
MAX_RECORDS = 100000              
REQUESTS_PER_MIN = 180           

# Storage
DUCKDB_PATH = "data/fda_recalls.duckdb"
RAW_DUMP_DIR = "data/raw"        

#Logging
LOG_DIR = "logs"

