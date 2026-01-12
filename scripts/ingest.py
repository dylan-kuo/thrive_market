import pandas as pd
import duckdb
import os
import requests
import logging

logging.basicConfig(level=logging.INFO)

DATA_DIR = '/opt/airflow/data'
EXCEL_PATH = os.path.join(DATA_DIR, 'raw_data.xlsx')
DB_PATH = os.path.join(DATA_DIR, 'thrive.duckdb')
DOWNLOAD_URL = "https://thrivemarket-candidate-test.s3.amazonaws.com/tc_raw_data.xlsx"

def download_data():
    """Task 1: Downloads the file from S3."""
    logging.info(f"Downloading data from {DOWNLOAD_URL}...")
    try:
        response = requests.get(DOWNLOAD_URL, timeout=60)
        response.raise_for_status()
        with open(EXCEL_PATH, 'wb') as f:
            f.write(response.content)
        logging.info(f"Download complete. Saved to {EXCEL_PATH}")
    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise e

def validate_source():
    """Task 2: Validates structure AND loads to Raw DB."""
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Source file missing at {EXCEL_PATH}")

    # 1. Validation Logic
    try:
        xl = pd.ExcelFile(EXCEL_PATH)
        required_sheets = ['TC_Data', 'Sales', 'Customers']
        missing = [s for s in required_sheets if s not in xl.sheet_names]
        
        if missing:
            raise ValueError(f"CRITICAL: Missing sheets: {missing}")
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        raise e

    # 2. Load to DuckDB (Raw Layer)
    logging.info("Source Validated. Loading to DuckDB...")
    con = duckdb.connect(DB_PATH)
    
    for sheet in required_sheets:
        df = xl.parse(sheet)
        # Standardize columns (remove spaces, upper case)
        df.columns = [c.strip().replace(' ', '_').upper() for c in df.columns]
        
        table_name = f"raw_{sheet.lower()}"
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        logging.info(f"Loaded {len(df)} rows into {table_name}")
    
    con.close()