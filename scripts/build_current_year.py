import glob
import os
import json
import pandas as pd
from datetime import datetime, timedelta

OUT_DIR = "site"
OUT_CSV = f"{OUT_DIR}/current_year.csv"
MAPPINGS = f"{OUT_DIR}/mappings.json"
HISTORY = f"{OUT_DIR}/history_snapshot.json"

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
    if not os.path.exists(MAPPINGS):
        with open(MAPPINGS, "w", encoding="utf-8") as f:
            json.dump({}, f)
    if not os.path.exists(HISTORY):
        with open(HISTORY, "w", encoding="utf-8") as f:
            json.dump([], f)

def read_csv_guess_sep(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.read_csv(path, dtype=str, sep="\t").fillna("")

def main():
    ensure_site_files()

    files = sorted(glob.glob("data/*/*.csv"))
    parts = []
    for p in files:
        try:
            df = read_csv_guess_sep(p)
            if len(df) > 0:
                parts.append(df)
        except Exception:
            pass

    if not parts:
        pd.DataFrame(columns=WANTED_COLS).to_csv(OUT_CSV, index=False)
        print("Built", OUT_CSV, "with 0 rows")
        return

    df = pd.concat(parts, ignore_index=True)

    for c in WANTED_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[WANTED_COLS]

    if "hash_id" in df.columns:
        df = df.drop_duplicates(subset=["hash_id"], keep="first")

    df["notice_dt"] = pd.to_datetime(df["notice_date"], errors="coerce")
    cutoff = datetime.utcnow() - timedelta(days=365)
    df = df[df["notice_dt"].notna()]
    df = df[df["notice_dt"] >= cutoff]

    df = df.sort_values(by="notice_dt", ascending=False).drop(columns=["notice_dt"])

    df.to_csv(OUT_CSV, index=False, sep=",")
    print("Built", OUT_CSV, "with", len(df), "rows")

if __name__ == "__main__":
    main()
