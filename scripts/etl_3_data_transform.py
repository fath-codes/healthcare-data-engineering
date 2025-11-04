# scripts/etl_3_data_transform.py
# ETL Block 3 - Data Transformation
# Input : data/clean/*_clean.csv
# Output: data/processed/*.csv
# Log : logs/data_transform_log.txt (overwrite each run)

import pandas as pd
from pathlib import Path
from datetime import datetime

# -------------------------------------
# Paths
# -------------------------------------
DATA_DIR = Path("data")
CLEAN_DIR = DATA_DIR / "clean"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = Path("logs")
for d in [DATA_DIR, CLEAN_DIR, PROCESSED_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "data_transform_log.txt"


def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()}: {msg}\n")

# -------------------------------------
# Transform Helpers Functions
# -------------------------------------


def make_date_id(date_series: pd.Series) -> pd.Series:
    """Convert datetime to int YYYYMMDD format for joining to dim_dates."""
    return pd.to_datetime(date_series, errors="coerce").dt.strftime("%Y%m%d").astype("Int64")

# -------------------------------------
# Main Transform Function
# -------------------------------------


def transform_dimensions():
    """Prepare all dimension tables  from cleaned data."""
    log(f"\n[Transform - Dimension] {datetime.now():%Y-%m-%d %H:%M:%S} Starting dimension transformation")

    dims = {}

    # DIM_PATIENTS
    df_pat = pd.read_csv(CLEAN_DIR / "patients_clean.csv")
    dims["dim_patients"] = df_pat[[
        "patient_id",
        "patient_name",
        "gender",
        "age",
        "city",
        "insurance_type"
    ]].copy()

    # DIM_DOCTORS
    df_doc = pd.read_csv(CLEAN_DIR / "doctors_clean.csv")
    dims["dim_doctors"] = df_doc[[
        "doctor_id",
        "doctor_name",
        "department_name",
        "specialization",
        "status",
        "years_experience"
    ]].copy()

    # DIM_DEPARTMENTS
    df_dept = pd.read_csv(CLEAN_DIR / "departments_clean.csv")
    dims["dim_departments"] = df_dept[[
        "department_id",
        "department_name",
        "head_doctor",
        "floor_number"
    ]].copy()

    # DIM_DIAGNOSES
    df_diag = pd.read_csv(CLEAN_DIR / "diagnoses_clean.csv")
    dims["dim_diagnoses"] = df_diag[[
        "diagnosis_id",
        "diagnosis_name",
        "diagnosis_code"
    ]].copy()

    # DIM_DATES
    df_dates = pd.read_csv(CLEAN_DIR / "dates_clean.csv")
    dims["dim_dates"] = df_dates[[
        "date_id",
        "date",
        "year",
        "month",
        "day",
        "quarter",
        "day_name"
    ]].copy()

    # Save dimension tables
    for dim_name, df_dim in dims.items():
        output_path = PROCESSED_DIR / f"{dim_name}.csv"
        df_dim.to_csv(output_path, index=False)
        log(
            f"✅ [Transform - Dimension] Saved {dim_name} to {output_path} with {len(df_dim)} rows.")
        print(f"✅ {dim_name} created -> {output_path.name}")

    return dims


def transform_fact(dims):
    """Prepare fact table from cleaned data and transformed dimensions."""
    # Integrate dimension keys (foreign keys) into fact table (visits)

    log(f"\n[Transform - Fact] {datetime.now():%Y-%m-%d %H:%M:%S} Starting fact transformation")

    df_vis = pd.read_csv(CLEAN_DIR / "visits_clean.csv")

    # Generate date_id from visit_date for joining to dim_dates
    df_vis["date_id"] = make_date_id(df_vis["visit_date"])

    # Join dim lookups (example: diagnosis_code → diagnosis_id)
    df_diag = dims["dim_diagnoses"]
    df_vis = df_vis.merge(
        df_diag[["diagnosis_code", "diagnosis_id"]],
        on="diagnosis_code",
        how="left"
    )

    # Join doctors to get department_id (if available)
    df_doc = dims["dim_doctors"]
    df_dep = dims["dim_departments"]
    df_vis = df_vis.merge(
        df_doc[["doctor_id", "department_name"]],
        on="doctor_id",
        how="left",
        suffixes=("", "_doc")
    )
    df_vis = df_vis.merge(
        df_dep[["department_name", "department_id"]],
        on="department_name",
        how="left"
    )

    # Select & reorder final fact columns
    fact = df_vis[[
        "visit_id",
        "patient_id",
        "doctor_id",
        "department_id",
        "diagnosis_id",
        "date_id",
        "visit_type",
        "visit_duration_days",
        "total_cost",
        "insurance_coverage",
        "patient_payment",
        "satisfaction_rating"
    ]].copy()

    out = PROCESSED_DIR / "fact_visits.csv"
    fact.to_csv(out, index=False)
    log(f"✅ Saved {out.name} ({len(fact)} rows)")
    print(f"✅ fact_visits created → {out.name}")


# -------------------------------------
# Main
# -------------------------------------
if __name__ == "__main__":
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"[Data Transform Log — {datetime.now():%Y-%m-%d %H:%M:%S}]\n")

    print("🚀 Starting ETL Block 3 — Transform")
    dims = transform_dimensions()
    transform_fact(dims)

    log("\n✅ Transform process completed successfully.")
    print("\n✅ All transformations complete → see data/processed and logs/data_transform_log.txt")
