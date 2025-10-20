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
