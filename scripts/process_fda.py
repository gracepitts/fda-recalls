import os
import logging
import duckdb
import pandas as pd
from datetime import datetime
from config import DUCKDB_PATH, LOG_DIR

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "process.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CREATE_PROCESSED_SQL = """
CREATE TABLE IF NOT EXISTS enforcement_clean AS
SELECT * FROM (SELECT
    CAST(NULL AS VARCHAR) AS recall_number,
    CAST(NULL AS DATE)   AS recall_initiation_dt,
    CAST(NULL AS DATE)   AS report_dt,
    CAST(NULL AS VARCHAR) AS classification,
    CAST(NULL AS VARCHAR) AS reason_for_recall,
    CAST(NULL AS VARCHAR) AS recalling_firm,
    CAST(NULL AS VARCHAR) AS state,
    CAST(NULL AS VARCHAR) AS country,
    CAST(NULL AS VARCHAR) AS distribution_pattern,
    CAST(NULL AS VARCHAR) AS status
) WHERE 1=0;
"""

TRUNCATE_SQL = "DELETE FROM enforcement_clean;"

INSERT_CLEAN_SQL = """
INSERT INTO enforcement_clean
SELECT
  recall_number,
  TRY_CAST(recall_initiation_date AS DATE) AS recall_initiation_dt,
  TRY_CAST(report_date AS DATE)            AS report_dt,
  UPPER(TRIM(classification))              AS classification,
  TRIM(reason_for_recall)                  AS reason_for_recall,
  UPPER(TRIM(recalling_firm))              AS recalling_firm,
  UPPER(TRIM(state))                       AS state,
  UPPER(TRIM(country))                     AS country,
  TRIM(distribution_pattern)               AS distribution_pattern,
  UPPER(TRIM(status))                      AS status
FROM enforcement_raw;
"""

CREATE_VIEWS_SQL = """
CREATE OR REPLACE VIEW v_yearly_counts AS
SELECT
  EXTRACT(YEAR FROM report_dt) AS year,
  COUNT(*) AS recalls
FROM enforcement_clean
WHERE report_dt IS NOT NULL
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW v_top_firms AS
SELECT
  recalling_firm,
  COUNT(*) AS recalls
FROM enforcement_clean
WHERE recalling_firm IS NOT NULL AND recalling_firm <> ''
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

CREATE OR REPLACE VIEW v_reasons AS
SELECT
  reason_for_recall,
  COUNT(*) AS recalls
FROM enforcement_clean
WHERE reason_for_recall IS NOT NULL AND reason_for_recall <> ''
GROUP BY 1
ORDER BY 2 DESC
LIMIT 25;

CREATE OR REPLACE VIEW v_class_distribution AS
SELECT
  classification,
  COUNT(*) AS recalls
FROM enforcement_clean
WHERE classification IS NOT NULL AND classification <> ''
GROUP BY 1
ORDER BY 2 DESC;
"""

def process():
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(CREATE_PROCESSED_SQL)
    conn.execute(TRUNCATE_SQL)
    conn.execute(INSERT_CLEAN_SQL)
    conn.execute(CREATE_VIEWS_SQL)
    conn.close()
    logging.info("Processing complete. Clean table and views updated.")

if __name__ == "__main__":
    process()

