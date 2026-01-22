import glob
import os
import json
import shutil
from datetime import datetime

import pandas as pd

OUT_DIR = "site"
OUT_CSV = f"{OUT_DIR}/current_year.csv"
MAPPINGS_SRC = "site/mappings.json"
MAPPINGS_DST = f"{OUT_DIR}/mappings.json"

HEADER = "hash_id,company,clean_name,notice_date,effective_date,employee_count,city,state,source_url\n"

# Any URLs like this are not real notices. They are landing pages.
PLACEHOLDER_URL_SUBSTRINGS = [
    "warn-worker-adjustment-and-retraining-notification",
]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Ensure mappings.json exists in site/
    if os.path.exists(MAPPINGS_SRC):
        shutil.copy(MAPPINGS_SRC, MAPPINGS_DST)
    else:
        with open(MAPPINGS_DST, "w", encoding="utf-8") as f:
            json.dump({}, f)

    parts = []
    for path in glob.glob("data/*/*.csv"):
        try:
            df_part = pd.read_csv(path, dtype=str).fillna("")
            parts.append(df_part)
        except Exception as e:
            print(f"Skipping {path}: {e}")

    if not parts:
        with open(OUT_CSV, "w", encoding="utf-8") as f:
            f.write(HEADER)
        print("No data found.")
        return

    df = pd.concat(parts, ignore_index=True)

    # Keep only expected columns if extras appear
    wanted = ["hash_id","company","clean_name","notice_date","effective_date","employee_count","city","state","source_url"]
    for c in wanted:
        if c not in df.columns:
            df[c] = ""
    df = df[wanted]

    # Remove placeholder rows by URL pattern
    if "source_url" in df.columns:
        su = df["source_url"].astype(str)
        mask = pd.Series(False, index=df.index)
        for bad in PLACEHOLDER_URL_SUBSTRINGS:
            mask = mask | su.str.contains(bad, na=False)
        df = df[~mask]

    # Drop duplicates
    if "hash_id" in df.columns:
        df = df.drop_duplicates(subset=["hash_id"])

    # Sort by notice_date if present
    df["_nd"] = pd.to_datetime(df["notice_date"], errors="coerce")
    df = df.sort_values(by="_nd", ascending=False).drop(columns=["_nd"])

    # Save as a real comma separated CSV
    df.to_csv(OUT_CSV, index=False, sep=",")
    print(f"Built {OUT_CSV} with {len(df)} rows.")

if __name__ == "__main__":
    main()
