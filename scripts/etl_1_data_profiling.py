import pandas as pd
from pathlib import Path
from datetime import datetime


def write_report(text: str, report_path: Path):
    """Helper function to write text to the report file."""
    with open(REPORT_FILE, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def profile_file(csv_path: Path, report_path: Path):
    """profiling a single CSV file"""
    try:

        parse_dates = []
        if csv_path.name == "dates.csv":
            parse_dates = ["date"]
        elif csv_path.name == "visits.csv":
            parse_dates = ["visit_date"]

        df = pd.read_csv(csv_path, parse_dates=parse_dates, encoding="utf-8")

        write_report(f"=== Profiling {csv_path.name} ===", report_path)
        write_report(
            f"Rows: {df.shape[0]}, Columns: {df.shape[1]}", report_path)
        write_report("Columns and dtypes\n" +
                     df.dtypes.to_string(), report_path)
        write_report("\nMissing Values per Column:\n" +
                     df.isna().sum().to_string(), report_path)
        write_report(f"\nDuplicates: {df.duplicated().sum()}", report_path)

        num = df.select_dtypes(include=['number'])
        if not num.empty:
            write_report("\nNumeric Summary (describe):\n" +
                         num.describe().T.to_string(), report_path)

        write_report(
            f"\nSample Data:\n{df.head(3).to_string(index=False)}", report_path)
        write_report("-" * 100 + "\n", report_path)

    except FileNotFoundError:
        write_report(f"❌ File not found: {csv_path}", report_path)
    except pd.errors.EmptyDataError:
        write_report(f"⚠️ Empty File: {csv_path.name}", report_path)
    except Exception as e:
        write_report(
            f"❌ Unkown error reading {csv_path.name}: {e}", report_path)


if __name__ == "__main__":
    try:
        DATA_DIR = Path("data")
        RAW_DIR = DATA_DIR / "raw"
        DOCS_DIR = Path("docs")
        for d in [DATA_DIR, RAW_DIR, DOCS_DIR]:
            d.mkdir(exist_ok=True)
        REPORT_FILE = DOCS_DIR / "data_quality_report.txt"

        # Make a report file (overwrite if exists)
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(
                f"[Data Profiling Report — {datetime.now():%Y-%m-%d %H:%M:%S}]\n\n")

        if not RAW_DIR.exists():
            raise FileNotFoundError(
                f"Raw data directory not found: {RAW_DIR.resolve()}")

        csv_files = sorted(RAW_DIR.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in {RAW_DIR.resolve()}")

        for csv_file in csv_files:
            profile_file(csv_file, REPORT_FILE)

        write_report("✅ Profiling complete.", REPORT_FILE)
        print(f"✅ Data profiling completed. Report saved to {REPORT_FILE}")

    except Exception as e:
        print(f"❌ Error during profiling: {e}")
