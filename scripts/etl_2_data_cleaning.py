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
    df = standardize_missing(
        df, ["patient_name", "insurance_type", "city", "gender"])
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
    df = standardize_missing(
        df, ["doctor_name", "department_name", "specialization", "status"])
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
    log("\n[departments.csv] Cleaning started")
    df.columns = [c.strip() for c in df.columns]

    # standardize missing & text trim
    df = standardize_missing(df, ["department_name", "head_doctor"])
    for c in ("department_name", "head_doctor"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # department_id to int
    if "department_id" in df.columns:
        df["department_id"] = pd.to_numeric(
            df["department_id"], errors='coerce').astype("Int64")

    # floor_number to int, default to 0 for missing/invalid
    if "floor_number" in df.columns:
        df["floor_number"] = pd.to_numeric(
            df["floor_number"], errors='coerce').fillna(0).astype(int).clip(lower=0)

    # deduplicate by department_id if exists, else full row
    df = dedupe(df, subset=[
                "department_id"] if "department_id" in df.columns else None, label="departments")

    log("[departments.csv] Cleaning completed")
    return df


def clean_diagnoses(df: pd.DataFrame) -> pd.DataFrame:
    log("\n[diagnoses.csv] Cleaning started")
    df.columns = [c.strip() for c in df.columns]

    # standardize missing, diagnosis_code trim and uppercase
    df = standardize_missing(df, ["diagnosis_code", "diagnosis_name"])
    if "diagnosis_code" in df.columns:
        df["diagnosis_code"] = df["diagnosis_code"].astype(
            "string").str.strip().str.upper()

    # diagnosis_name trim
    if "diagnosis_name" in df.columns:
        df["diagnosis_name"] = df["diagnosis_name"].astype(
            "string").str.strip()

    # diagnosis_id to int
    if "diagnosis_id" in df.columns:
        df["diagnosis_id"] = pd.to_numeric(
            df["diagnosis_id"], errors='coerce').astype("Int64")

    # deduplicate by diagnosis_code if exists, else full row
    df = dedupe(df, subset=[
                "diagnosis_code"] if "diagnosis_code" in df.columns else None, label="diagnoses")

    log("[diagnoses.csv] Cleaning completed")
    return df


def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    log("\n[dates.csv] Cleaning started")
    df.columns = [c.strip() for c in df.columns]

    # standardize missing
    df = standardize_missing(
        df, ["year", "month", "day", "quarter", "day_name"])

    # parse date column and drop invalid
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors='coerce')
        df = df.dropna(subset=["date"])

        # rebuild date components
        if "year" in df.columns:
            df["year"] = df["date"].dt.year
        if "month" in df.columns:
            df["month"] = df["date"].dt.month
        if "day" in df.columns:
            df["day"] = df["date"].dt.day
        if "quarter" in df.columns:
            df["quarter"] = df["date"].dt.quarter
        if "day_name" in df.columns:
            df["day_name"] = df["date"].dt.day_name()

    # date_id to int
    if "date_id" in df.columns:
        df["date_id"] = pd.to_numeric(
            df["date_id"], errors='coerce').astype("Int64")

    # deduplicate by date if exists, else full row
    df = dedupe(df, subset=[
                "date"] if "date" in df.columns else None, label="dates")

    log("[dates.csv] Cleaning completed")
    return df


def clean_visits(df: pd.DataFrame) -> pd.DataFrame:
    log("\n[visits.csv] Cleaning started")
    df.columns = [c.strip() for c in df.columns]

    # text trim
    for c in ("department_name", "diagnosis_code", "visit_type"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # visit_date to datetime
    if "visit_date" in df.columns:
        df["visit_date"] = pd.to_datetime(df["visit_date"], errors='coerce')

    # handling non negative for payment related columns and rating
    df = clip_non_negative(
        df, ["total_cost", "insurance_coverage", "patient_payment", "visit_duration_days"])
    if "patient_payment" in df.columns and "total_cost" in df.columns and "insurance_coverage" in df.columns:
        # if null / mismatch, recalculate with handling negative values
        mask_na = df["patient_payment"].isna()
        df.loc[mask_na, "patient_payment"] = (
            df.loc[mask_na, "total_cost"] - df.loc[mask_na, "insurance_coverage"]).clip(lower=0)
        df["patient_payment"] = df["patient_payment"].clip(lower=0)
    if "satisfaction_rating" in df.columns:
        df["satisfaction_rating"] = pd.to_numeric(
            df["satisfaction_rating"], errors='coerce').fillna(3).round().astype(int).clip(lower=1, upper=5)

    # visit_type normalization
    if "visit_type" in df.columns:
        df["visit_type"] = df["visit_type"].map({
            "Rawat Jalan": "Rawat Jalan",
            "Rawat Inap": "Rawat Inap",
            "IGD": "IGD"
        }).fillna("Rawat Jalan")

    # visit_id to int
    if "visit_id" in df.columns:
        df["visit_id"] = pd.to_numeric(
            df["visit_id"], errors='coerce').astype("Int64")

    # deduplicate by visit_id if exists, else full row
    df = dedupe(df, subset=[
                "visit_id"] if "visit_id" in df.columns else None, label="visits")

    log("[visits.csv] Cleaning completed")
    return df


# -------------------------------------
# Main ETL Process
# -------------------------------------
if __name__ == "__main__":

    # Clear previous log
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"Data Cleaning Log - {datetime.now()}\n")
        f.write("="*40 + "\n")

    log(f"\n===== ETL Block 2 Run: {datetime.now()} =====")
    print("🚀 Starting ETL Block 2 Cleaning...")

    # map of filename to cleaning function
    CLEANING_JOBS = {
        "patients.csv": clean_patients,
        "doctors.csv": clean_doctors,
        "departments.csv": clean_departments,
        "diagnoses.csv": clean_diagnoses,
        "dates.csv": clean_dates,
        "visits.csv": clean_visits,
    }

    # Process each CSV file
    for filename, func in CLEANING_JOBS.items():
        try:
            raw_path = RAW_DIR / filename
            clean_path = CLEAN_DIR / filename.replace(".csv", "_clean.csv")

            if not raw_path.exists():
                log(f"❌ File not found: {raw_path}")
                continue

            df = pd.read_csv(raw_path)
            log(f"{filename} Loaded succesfully (rows: {len(df)})")

            cleaned_df = func(df)
            cleaned_df.to_csv(clean_path, index=False, encoding="utf-8")

            log(f"✅ Cleaned data saved to {clean_path} (rows: {len(cleaned_df)})")
            print(f"✅ {filename} cleaned and saved to {clean_path.name}")

        except Exception as e:
            log(f"❌ Cleaning failed for {filename}: {e}")
            print(f"⚠️ Error processing for {filename}: {e}")

    log("\n===== ETL Block 2 Completed Succesfully =====")
    print("🎉 All cleaning completed -> check data/clean and logs/data_cleaning_log.txt")
