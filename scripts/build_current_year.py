import glob
import os
import json
from datetime import datetime

import pandas as pd

OUT_DIR = "site"
OUT_CSV = f"{OUT_DIR}/current_year.csv"

HEADER = "hash_id,company,clean_name,notice_date,effective_date,employee_count,city,state,source_url\n"

PLACEHOLDER_URL_SUBSTRINGS = [
    "warn-worker-adjustment-and-retraining-notification",
]

WANTED_COLS = [
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

    # Ensure mappings exists (but do not copy it)
    mappings_path = os.path.join(OUT_DIR, "mappings.json")
    if not os.path.exists(mappings_path):
        with open(mappings_path, "w", encoding="utf-8") as f:
            json.dump({}, f)

def load_all_data_parts():
    parts = []
    for path in glob.glob("data/*/*.csv"):
        try:
            df_part = pd.read_csv(path, dtype=str).fillna("")
            parts.append(df_part)
        except Exception as e:
            print(f"Skipping {path}: {e}")
    return parts

def drop_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    if "source_url" not in df.columns:
        return df
    su = df["source_url"].astype(str)
    mask = pd.Series(False, index=df.index)
    for bad in PLACEHOLDER_URL_SUBSTRINGS:
        mask = mask | su.str.contains(bad, na=False)
    return df[~mask]

def main():
    ensure_site_files()

    parts = load_all_data_parts()
    if not parts:
        with open(OUT_CSV, "w", encoding="utf-8") as f:
            f.write(HEADER)
        print("No data found. Wrote empty current_year.csv")
        return

    df = pd.concat(parts, ignore_index=True)

    # Ensure all expected columns exist
    for c in WANTED_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[WANTED_COLS]

    # Remove placeholder landing-page rows
    df = drop_placeholders(df)

    # Deduplicate by hash_id
    if "hash_id" in df.columns:
        df = df.drop_duplicates(subset=["hash_id"])

    # Sort by notice_date desc where possible
    df["_nd"] = pd.to_datetime(df["notice_date"], errors="coerce")
    df = df.sort_values(by="_nd", ascending=False).drop(columns=["_nd"])

    # Save as comma separated CSV
    df.to_csv(OUT_CSV, index=False, sep=",")
    print(f"Built {OUT_CSV} with {len(df)} rows.")

if __name__ == "__main__":
    main()
