# scripts/etl_2_data_cleaning.py
# ETL Block 2 - Data Cleaning
# Input : data/raw/*.csv
# Output: data/clean/*_clean.csv
# Log : docs/data_cleaning_log.txt (overwrite each run)

import pandas as pd
from pathlib import Path
from datetime import datetime

# -------------------------------------
# Paths
# -------------------------------------
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
LOGS_DIR = Path("logs")
for d in [DATA_DIR, RAW_DIR, CLEAN_DIR, LOGS_DIR]:
    d.mkdir(exist_ok=True)

LOG_FILE = LOGS_DIR / "data_cleaning_log.txt"


def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


# -------------------------------------
# Helpers
# -------------------------------------
NULL_TOKENS = ["", " ", "NA", "N/A", "NULL", "null", "None", "-"]


def standardize_missing(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Standardize missing values in specified columns."""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].replace(NULL_TOKENS, pd.NA)
    return df


def dedupe(df: pd.DataFrame, subset: list[str] | None = None, label: str = "") -> pd.DataFrame:
    """Remove duplicate rows from the DataFrame."""
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    after = len(df)
    log(f"Removed {before - after} duplicates ({label or subset})")
    return df


def clip_non_negative(df: pd.DataFrame, cols: list[str]):
    """Convert to numeric and Clip negative values to zero in specified numeric columns."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            df[c] = df[c].clip(lower=0)
    return df


# -------------------------------------
# Cleaning functions
# -------------------------------------
def clean_patients(df: pd.DataFrame) -> pd.DataFrame:
    log("\n[patients.csv] Cleaning started")
    df.columns = [c.strip() for c in df.columns]

    # standardize missing & text trim
    df = standardize_missing(df, ["insurance_type", "city", "gender"])
    for c in ("patient_name", "gender", "city", "insurance_type"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # insurance_type: default values
    if "insurance_type" in df.columns:
        df["insurance_type"] = df["insurance_type"].fillna("Uninsured")

    # age -> int, use median for missing/invalid values
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors='coerce')
        med = int(df["age"].median()) if df["age"].notna().any() else 0
        df["age"] = df["age"].fillna(med).astype(int).clip(lower=0, upper=120)

    # gender normalization
    if "gender" in df.columns:
        df["gender"] = df["gender"].str.title()
        df["gender"] = df["gender"].where(
            df["gender"].isin(["Male", "Female"]), other="Unknown")

    # patient_id to int
    if "patient_id" in df.columns:
        df["patient_id"] = pd.to_numeric(
            df["patient_id"], errors='coerce').astype("Int64")

    # deduplicate by patient_id if exists, else full row
    df = dedupe(df, subset=[
                "patient_id"] if "patient_id" in df.columns else None, label="patients")

    log("[patients.csv] Cleaning completed")
    return df


def clean_doctors(df: pd.DataFrame) -> pd.DataFrame:
    log("\n[doctors.csv] Cleaning started")
    df.columns = [c.strip() for c in df.columns]

    # standardize missing & text trim
    df = standardize_missing(df, ["specialization", "status"])
    for c in ("doctor_name", "department_name", "specialization", "status"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # status: default values
    if "status" in df.columns:
        df["status"] = df["status"].where(
            df["status"].isin(["Active", "On Leave"]), other="Active")

    # years_experience -> int, default to 0 for missing/invalid
    if "years_experience" in df.columns:
        df["years_experience"] = pd.to_numeric(
            df["years_experience"], errors='coerce').fillna(0).astype(int).clip(lower=0)

    # doctor_id to int
    if "doctor_id" in df.columns:
        df["doctor_id"] = pd.to_numeric(
            df["doctor_id"], errors='coerce').astype("Int64")

    # deduplicate by doctor_id if exists, else full row
    df = dedupe(df, subset=[
                "doctor_id"] if "doctor_id" in df.columns else None, label="doctors")

    log("[doctors.csv] Cleaning completed")
    return df


def clean_departments(df: pd.DataFrame) -> pd.DataFrame:
    # Continue later
    return df


def clean_diagnoses(df: pd.DataFrame) -> pd.DataFrame:
    # Continue later
    return df


def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Continue later
    return df


def clean_visits(df: pd.DataFrame) -> pd.DataFrame:
    # Continue later
    return df


# -------------------------------------
# Main ETL Process
# -------------------------------------
if __name__ == "__main__":

    log(f"\n===== ETL Block 2 Test Run: {datetime.now()} =====")

    # Load raw CSVs
    patients_file = RAW_DIR / "patients.csv"
    doctors_file = RAW_DIR / "doctors.csv"

    # Try read each dataset
    try:
        patients_df = pd.read_csv(patients_file)
        log("[patients.csv] Loaded successfully")
    except Exception as e:
        log(f"[patients.csv] Load failed: {e}")
        patients_df = pd.DataFrame()

    try:
        doctors_df = pd.read_csv(doctors_file)
        log("[doctors.csv] Loaded successfully")
    except Exception as e:
        log(f"[doctors.csv] Load failed: {e}")
        doctors_df = pd.DataFrame()

    # Run cleaning functions if data available
    if not patients_df.empty:
        clean_patients_df = clean_patients(patients_df)
        clean_patients_df.to_csv(
            CLEAN_DIR / "clean_patients.csv", index=False)
        log("[clean_patients.csv] Saved to data/clean")

    if not doctors_df.empty:
        clean_doctors_df = clean_doctors(doctors_df)
        clean_doctors_df.to_csv(
            CLEAN_DIR / "clean_doctors.csv", index=False)
        log("[clean_doctors.csv] Saved to data/clean")

    log("ETL Block 2 test completed successfully")
    print("✅ Data cleaning complete → see data/clean and logs/data_cleaning_log.txt")
