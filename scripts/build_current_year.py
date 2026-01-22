import glob
import os
import shutil
from datetime import datetime

import pandas as pd

YEAR = datetime.utcnow().year

OUT_DIR = "site"
OUT_CSV = f"{OUT_DIR}/current_year.csv"

# If you keep mappings in site already, this safely no ops
MAPPINGS_SRC = "site/mappings.json"
MAPPINGS_DST = f"{OUT_DIR}/mappings.json"

EXPECTED_COLS = [
    "hash_id",
    "company",
    "clean_name",
    "notice_date",
    "effective_date",
    "employee_count",
    "city",
    "state",
    "source_url",
]

def ensure_site_files():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Ensure mappings.json exists
    if not os.path.exists(MAPPINGS_DST):
        with open(MAPPINGS_DST, "w", encoding="utf-8") as f:
            f.write("{}")

    # If src and dst differ, copy. If same path, do nothing.
    try:
        if os.path.exists(MAPPINGS_SRC):
            if os.path.abspath(MAPPINGS_SRC) != os.path.abspath(MAPPINGS_DST):
                shutil.copy(MAPPINGS_SRC, MAPPINGS_DST)
    except Exception:
        pass

def read_state_year_csv(path: str):
    try:
        df = pd.read_csv(path)
        return df
    except Exception:
        return None

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Add missing columns
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""

    df = df[EXPECTED_COLS].copy()

    # Normalize types
    df["employee_count"] = pd.to_numeric(df["employee_count"], errors="coerce").fillna(0).astype(int)
    for c in ["hash_id", "company", "clean_name", "notice_date", "effective_date", "city", "state", "source_url"]:
        df[c] = df[c].fillna("").astype(str).str.strip()

    # Drop fully empty junk rows
    df = df[df["company"].str.len() > 0]

    return df

def write_empty():
    pd.DataFrame(columns=EXPECTED_COLS).to_csv(OUT_CSV, index=False)

def main():
    ensure_site_files()

    parts = []
    for path in glob.glob(f"data/*/{YEAR}.csv"):
        df = read_state_year_csv(path)
        if df is None or df.empty:
            continue
        parts.append(normalize_df(df))

    if not parts:
        write_empty()
        print(f"Built {OUT_CSV} with 0 rows.")
        return

    merged = pd.concat(parts, ignore_index=True)

    # Deduplicate by hash_id
    merged = merged.drop_duplicates(subset=["hash_id"], keep="last")

    # Sort newest first, blanks last
    merged["_nd"] = pd.to_datetime(merged["notice_date"], errors="coerce")
    merged = merged.sort_values(by=["_nd"], ascending=False).drop(columns=["_nd"])

    merged.to_csv(OUT_CSV, index=False)
    print(f"Built {OUT_CSV} with {len(merged)} rows.")

if __name__ == "__main__":
    main()
