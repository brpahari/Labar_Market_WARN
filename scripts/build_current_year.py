import glob
import pandas as pd
import shutil
import os
import json
from datetime import datetime

YEAR = datetime.utcnow().year
OUT_DIR = "site"
OUT_CSV = f"{OUT_DIR}/current_year.csv"
MAPPINGS_SRC = "site/mappings.json"
MAPPINGS_DST = f"{OUT_DIR}/mappings.json"

HEADER = "hash_id,company,clean_name,notice_date,effective_date,employee_count,city,state,source_url\n"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    if os.path.exists(MAPPINGS_SRC):
        shutil.copy(MAPPINGS_SRC, MAPPINGS_DST)
    else:
        with open(MAPPINGS_DST, "w", encoding="utf-8") as f:
            json.dump({}, f)

    parts = []
    for path in glob.glob(f"data/*/{YEAR}.csv"):
        try:
            parts.append(pd.read_csv(path, dtype=str).fillna(""))
        except Exception as e:
            print(f"Skipping {path}: {e}")

    if not parts:
        with open(OUT_CSV, "w", encoding="utf-8") as f:
            f.write(HEADER)
        print("No data found for this year.")
        return

    df = pd.concat(parts, ignore_index=True)
    if "hash_id" in df.columns:
        df = df.drop_duplicates(subset=["hash_id"])

    if "notice_date" in df.columns:
        df["notice_date_sort"] = pd.to_datetime(df["notice_date"], errors="coerce")
        df = df.sort_values(by="notice_date_sort", ascending=False).drop(columns=["notice_date_sort"])

    df.to_csv(OUT_CSV, index=False)
    print(f"Built {OUT_CSV} with {len(df)} rows.")

if __name__ == "__main__":
    main()
