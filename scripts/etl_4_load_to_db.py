# scripts/etl_4_load_to_db.py
# ETL Block 4 - Load to Database
# Input : data/processed/*.csv
# Output: PostgreSQL Database
# Log : logs/load_to_db_log.txt (append each run)

import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime


# -------------------------------------
# Paths & Config
# -------------------------------------
DATA_DIR = Path("data/processed")
LOGS_DIR = Path("logs")
LOG_FILE = LOGS_DIR / "load_to_db_log.txt"

for d in [LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# -------------------------------------
# Logging Function
# -------------------------------------


def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {msg}\n")

# -------------------------------------
# Database Connection
# -------------------------------------


DB_URL = "postgresql+psycopg2://postgres:geleng123@localhost:5432/healthcare_dw"
engine = create_engine(DB_URL)

log("Connected to PostgreSQL database successfully.")

# -------------------------------------
# Load to DB Functions
# -------------------------------------

tables = {
    "dim_patients": DATA_DIR / "dim_patients.csv",
    "dim_doctors": DATA_DIR / "dim_doctors.csv",
    "dim_departments": DATA_DIR / "dim_departments.csv",
    "dim_diagnoses": DATA_DIR / "dim_diagnoses.csv",
    "dim_dates": DATA_DIR / "dim_dates.csv",
    "fact_visits": DATA_DIR / "fact_visits.csv"
}

for table_name, file_path in tables.items():
    if not file_path.exists():
        log(f"File {file_path} does not exist. Skipping load for {table_name}.")
        continue

    try:
        df = pd.read_csv(file_path)
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            schema=None  # Use default schema
        )
        log(
            f"✅ Succesfully loaded data into table {table_name} from {file_path} with {len(df)} rows.")

    except Exception as e:
        log(f"❌ Failed to load {table_name}: {e}")

log("All tables processed.")

# -------------------------------------
# Validation Check
# -------------------------------------
with engine.connect() as conn:
    for t in tables.keys():
        try:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            log(f"📊 Table {t} has {count} rows")
        except Exception as e:
            log(f"⚠️ Could not count {t}: {e}")

print("✅ ETL Block 4 complete. Check logs/etl_block4_load_log.txt for summary.")
