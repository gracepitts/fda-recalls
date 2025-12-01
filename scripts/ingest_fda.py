import os  # For directory and path handling
import time  # For managing request delays and timing
import math  # Math utilities (may not be heavily used)
import json  # Writing raw response dumps for traceability
import logging  # Logging ingestion progress + errors
import requests  # API requests to OpenFDA
import duckdb  # Local fast SQL database
import pandas as pd  # Data manipulation + DataFrame structure
from dateutil import parser  # Date parsing utility (not used directly here)
from config import (  # Project configuration settings
    FDA_ENFORCEMENT_ENDPOINT,
    FDA_ENFORCEMENT_ENDPOINTS,
    LIMIT_PER_REQUEST,
    MAX_RECORDS,
    REQUESTS_PER_MIN,
    DUCKDB_PATH,
    RAW_DUMP_DIR,
    LOG_DIR
)

# Ensure DuckDB directory exists
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

# Ensure raw dumps directory exists — remove file if a conflict exists
if RAW_DUMP_DIR:
    try:
        os.makedirs(RAW_DUMP_DIR, exist_ok=True)
    except FileExistsError:
        if os.path.isfile(RAW_DUMP_DIR):
            os.remove(RAW_DUMP_DIR)
            os.makedirs(RAW_DUMP_DIR, exist_ok=True)
        else:
            raise

# Ensure log directory exists
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except FileExistsError:
    if os.path.isfile(LOG_DIR):
        os.remove(LOG_DIR)
        os.makedirs(LOG_DIR, exist_ok=True)
    else:
        raise

# Logging setup — writes logs to ingest.log for debugging and run history
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ingest.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Schema for the enforcement_raw table — ensures compatibility across API changes
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS enforcement_raw AS
SELECT * FROM (SELECT
    CAST(NULL AS VARCHAR) AS recall_number,
    CAST(NULL AS VARCHAR) AS reason_for_recall,
    CAST(NULL AS VARCHAR) AS status,
    CAST(NULL AS VARCHAR) AS classification,
    CAST(NULL AS VARCHAR) AS product_description,
    CAST(NULL AS VARCHAR) AS code_info,
    CAST(NULL AS VARCHAR) AS recalling_firm,
    CAST(NULL AS VARCHAR) AS report_date,
    CAST(NULL AS VARCHAR) AS recall_initiation_date,
    CAST(NULL AS VARCHAR) AS state,
    CAST(NULL AS VARCHAR) AS distribution_pattern,
    CAST(NULL AS VARCHAR) AS country,
    CAST(NULL AS VARCHAR) AS city,
    CAST(NULL AS VARCHAR) AS event_id,
    CAST(NULL AS VARCHAR) AS product_quantity,
    CAST(NULL AS VARCHAR) AS voluntary_mandated,
    CAST(NULL AS VARCHAR) AS initial_firm_notification,
    CAST(NULL AS VARCHAR) AS openfda__nui,
    CAST(NULL AS VARCHAR) AS openfda__spl_set_id,
    CAST(NULL AS VARCHAR) AS source
) WHERE 1=0;
"""

# SQL statement for inserting incoming batches by column name
UPSERT_SQL = """
INSERT INTO enforcement_raw BY NAME
SELECT
    recall_number,
    reason_for_recall,
    status,
    classification,
    product_description,
    code_info,
    recalling_firm,
    report_date,
    recall_initiation_date,
    state,
    distribution_pattern,
    country,
    city,
    event_id,
    product_quantity,
    voluntary_mandated,
    initial_firm_notification,
    openfda__nui,
    openfda__spl_set_id,
    source
FROM df
"""

def normalize_record(r: dict) -> dict:
    # Normalize nested openfda fields to flat schema
    openfda = r.get("openfda", {})
    nui = None
    if isinstance(openfda, dict):
        nui_arr = openfda.get("nui")
        if isinstance(nui_arr, list) and nui_arr:
            nui = nui_arr[0]
    spl_set_id = None
    if isinstance(openfda, dict):
        ss = openfda.get("spl_set_id")
        if isinstance(ss, list) and ss:
            spl_set_id = ss[0]

    # Return flattened data row
    return {
        "recall_number": r.get("recall_number"),
        "reason_for_recall": r.get("reason_for_recall"),
        "status": r.get("status"),
        "classification": r.get("classification"),
        "product_description": r.get("product_description"),
        "code_info": r.get("code_info"),
        "recalling_firm": r.get("recalling_firm"),
        "report_date": r.get("report_date"),
        "recall_initiation_date": r.get("recall_initiation_date"),
        "state": r.get("state"),
        "distribution_pattern": r.get("distribution_pattern"),
        "country": r.get("country"),
        "city": r.get("city"),
        "event_id": r.get("event_id"),
        "product_quantity": r.get("product_quantity"),
        "voluntary_mandated": r.get("voluntary_mandated"),
        "initial_firm_notification": r.get("initial_firm_notification"),
        "openfda__nui": nui,
        "openfda__spl_set_id": spl_set_id
    }

def ingest(max_records=MAX_RECORDS, endpoints: list | None = None, query: str | None = None):
    # Connect safely to DB — handles corrupted DB files by recreating
    try:
        conn = duckdb.connect(DUCKDB_PATH)
    except Exception:
        if os.path.exists(DUCKDB_PATH):
            os.remove(DUCKDB_PATH)
        conn = duckdb.connect(DUCKDB_PATH)

    conn.execute(CREATE_SQL)

    # Ensure legacy tables have source column
    try:
        conn.execute("ALTER TABLE enforcement_raw ADD COLUMN IF NOT EXISTS source VARCHAR;")
    except Exception:
        try:
            conn.execute("ALTER TABLE enforcement_raw ADD COLUMN source VARCHAR;")
        except Exception:
            pass

    if endpoints is None:
        endpoints = FDA_ENFORCEMENT_ENDPOINTS

    params_base = {"limit": LIMIT_PER_REQUEST}
    if query:
        params_base["search"] = query

    total = 0
    t0 = time.time()
    sleep_seconds = max(60.0 / REQUESTS_PER_MIN, 0.35)

    # Loop through endpoints extracting records
    for endpoint in endpoints:
        if total >= max_records:
            break
        try:
            source_name = endpoint.split('/')[3]
        except Exception:
            source_name = endpoint

        skip = 0
        while total < max_records:
            params = params_base.copy()
            params["skip"] = skip

            resp = requests.get(endpoint, params=params, timeout=30)
            if resp.status_code != 200:
                logging.warning(f"HTTP {resp.status_code} at skip={skip}: {resp.text[:200]}")
                if resp.status_code in (429, 503):  # Rate-limit and retry
                    time.sleep(5)
                    continue
                break

            payload = resp.json()
            results = payload.get("results", [])
            if not results:
                break

            normalized = [normalize_record(r) for r in results]
            df = pd.DataFrame(normalized)
            df["source"] = source_name

            # Deduplicate by event_id to prevent storage bloat + errors
            try:
                existing_rows = conn.execute("SELECT event_id FROM enforcement_raw WHERE event_id IS NOT NULL").fetchall()
                existing_ids = set(r[0] for r in existing_rows if r[0] is not None)
            except Exception:
                existing_ids = set()
            df = df[~df["event_id"].isin(existing_ids)]

            # Save original JSON files for reproducibility checks later
            if RAW_DUMP_DIR:
                dump_path = os.path.join(RAW_DUMP_DIR, f"{source_name}_batch_{skip}.json")
                with open(dump_path, "w") as f:
                    json.dump(results, f)

            # Write DataFrame into DuckDB if not empty
            if not df.empty:
                conn.register("df", df)
                conn.execute(UPSERT_SQL)

            n = len(results)
            total += n
            skip += n
            logging.info(f"Ingested batch: {n} (total={total}) skip={skip}")

            # Stop loop if hitting final partial page
            if n < LIMIT_PER_REQUEST:
                break

            time.sleep(sleep_seconds)

    conn.close()
    logging.info(f"Done. Total records ingested: {total}. Elapsed: {time.time() - t0:.1f}s")

if __name__ == "__main__":
    ingest()  # Default full dataset ingestion

def ingest_by_years(start_year: int, end_year: int, max_records: int = MAX_RECORDS, endpoints: list | None = None):
    """Ingest each year independently — avoids API skip limits & ensures alignment of endpoints by year."""
    if endpoints is None:
        endpoints = FDA_ENFORCEMENT_ENDPOINTS

    total = 0
    t0 = time.time()

    for year in range(start_year, end_year + 1):
        if total >= max_records:
            break
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        year_query = f"report_date:[{start_date} TO {end_date}]"

        for endpoint in endpoints:
            if total >= max_records:
                break
            source_name = endpoint.split('/')[3] if '/' in endpoint else endpoint
            skip = 0
            while total < max_records:
                params = {"limit": LIMIT_PER_REQUEST, "skip": skip, "search": year_query}
                resp = requests.get(endpoint, params=params, timeout=30)

                if resp.status_code != 200:
                    if resp.status_code in (429, 503):
                        time.sleep(5)
                        continue
                    break

                payload = resp.json()
                results = payload.get("results", [])
                if not results:
                    break

                normalized = [normalize_record(r) for r in results]
                df = pd.DataFrame(normalized)
                df["source"] = source_name

                # Deduplicate
                try:
                    with duckdb.connect(DUCKDB_PATH) as conn:
                        existing_rows = conn.execute("SELECT event_id FROM enforcement_raw WHERE event_id IS NOT NULL").fetchall()
                        existing_ids = set(r[0] for r in existing_rows if r[0] is not None)
                except Exception:
                    existing_ids = set()
                df = df[~df["event_id"].isin(existing_ids)]

                # Save raw JSON
                if RAW_DUMP_DIR:
                    dump_path = os.path.join(RAW_DUMP_DIR, f"{source_name}_year_{year}_batch_{skip}.json")
                    with open(dump_path, "w") as f:
                        json.dump(results, f)

                # Insert into DuckDB
                if not df.empty:
                    with duckdb.connect(DUCKDB_PATH) as conn:
                        conn.execute(CREATE_SQL)
                        try:
                            conn.execute("ALTER TABLE enforcement_raw ADD COLUMN IF NOT EXISTS source VARCHAR;")
                        except Exception:
                            try:
                                conn.execute("ALTER TABLE enforcement_raw ADD COLUMN source VARCHAR;")
                            except Exception:
                                pass
                        conn.register("df", df)
                        conn.execute(UPSERT_SQL)

                n = len(results)
                total += n
                skip += n

                if n < LIMIT_PER_REQUEST:
                    break

                time.sleep(max(60.0 / REQUESTS_PER_MIN, 0.35))

    return total

def ingest_by_time_windows(start_year: int, end_year: int, window_months: int = 6,
                           max_records: int = MAX_RECORDS, endpoints: list | None = None):
    """Ingest using sliding monthly windows — increases depth of fetch without exceeding skip limit."""
    if endpoints is None:
        endpoints = FDA_ENFORCEMENT_ENDPOINTS

    from datetime import datetime
    from calendar import monthrange

    # Helper — iterates month windows until coverage is complete
    def month_range_iter(start_ym: tuple[int, int], end_ym: tuple[int, int], step_months: int):
        y, m = start_ym
        end_y, end_m = end_ym
        while (y, m) <= (end_y, end_m):
            total_months = y * 12 + (m - 1) + (step_months - 1)
            ey = total_months // 12
            em = (total_months % 12) + 1
            yield (y, m), (ey, em)
            nxt_total = y * 12 + (m - 1) + step_months
            y = nxt_total // 12
            m = (nxt_total % 12) + 1

    total = 0
    t0 = time.time()
    start_ym = (start_year, 1)
    end_ym = (end_year, 12)

    # Loop through month ranges and ingest
    for (sy, sm), (ey, em) in month_range_iter(start_ym, end_ym, window_months):
        if total >= max_records:
            break

        start_date = f"{sy}{sm:02d}01"
        last_day = monthrange(ey, em)[1]
        end_date = f"{ey}{em:02d}{last_day:02d}"
        window_query = f"report_date:[{start_date} TO {end_date}]"

        for endpoint in endpoints:
            if total >= max_records:
                break
            try:
                source_name = endpoint.split('/')[3]
            except Exception:
                source_name = endpoint

            skip = 0
            while total < max_records:
                params = {"limit": LIMIT_PER_REQUEST, "skip": skip, "search": window_query}
                resp = requests.get(endpoint, params=params, timeout=30)

                if resp.status_code != 200:
                    if resp.status_code in (429, 503):
                        time.sleep(5)
                        continue
                    break

                payload = resp.json()
                results = payload.get("results", [])
                if not results:
                    break

                normalized = [normalize_record(r) for r in results]
                df = pd.DataFrame(normalized)
                df["source"] = source_name

                # Deduplicate against existing DB
                try:
                    with duckdb.connect(DUCKDB_PATH) as conn:
                        existing_rows = conn.execute("SELECT event_id FROM enforcement_raw WHERE event_id IS NOT NULL").fetchall()
                        existing_ids = set(r[0] for r in existing_rows if r[0] is not None)
                except Exception:
                    existing_ids = set()
                df = df[~df["event_id"].isin(existing_ids)]

                # Write raw batch file
                if RAW_DUMP_DIR:
                    dump_path = os.path.join(RAW_DUMP_DIR, f"{source_name}_{sy}{sm:02d}_{ey}{em:02d}_batch_{skip}.json")
                    with open(dump_path, "w") as f:
                        json.dump(results, f)

                # Insert batch into DB safely
                if not df.empty:
                    with duckdb.connect(DUCKDB_PATH) as conn:
                        conn.execute(CREATE_SQL)
                        try:
                            conn.execute("ALTER TABLE enforcement_raw ADD COLUMN IF NOT EXISTS source VARCHAR;")
                        except Exception:
                            try:
                                conn.execute("ALTER TABLE enforcement_raw ADD COLUMN source VARCHAR;")
                            except Exception:
                                pass
                        conn.register("df", df)
                        conn.execute(UPSERT_SQL)

                n = len(results)
                total += n
                skip += n

                if n < LIMIT_PER_REQUEST:
                    break

                time.sleep(max(60.0 / REQUESTS_PER_MIN, 0.35))

    return total
    logging.info(f"Done. Total records ingested: {total}. Elapsed: {time.time() - t0:.1f}s")