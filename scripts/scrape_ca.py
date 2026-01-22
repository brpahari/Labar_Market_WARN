import os
import json
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "CA"
YEAR = datetime.utcnow().year

OUT_DIR = "data/ca"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

URL = "https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report.xlsx"

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

def parse_date(val) -> str:
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")

def load_mappings():
    if not os.path.exists(MAPPINGS_FILE):
        return {}
    try:
        with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def pick_sheet_name(sheet_names):
    # CA file has 3 sheets. We want Detailed WARN Report.
    preferred = [
        "Detailed WARN Report",
        "Detailed WARN report",
        "Detailed Warn Report",
        "Detailed WARN",
        "Detailed",
    ]
    for p in preferred:
        if p in sheet_names:
            return p
    # fallback, the detailed sheet is often the last one
    return sheet_names[-1]

def pick_col(cols, candidates):
    cols_l = {str(c).strip().lower(): str(c).strip() for c in cols}
    for cand in candidates:
        c = cand.lower()
        if c in cols_l:
            return cols_l[c]
    # fuzzy contains
    for cand in candidates:
        c = cand.lower()
        for k, orig in cols_l.items():
            if c in k:
                return orig
    return None

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = load_mappings()

    resp = requests.get(URL, timeout=90)
    resp.raise_for_status()

    xls = pd.ExcelFile(BytesIO(resp.content))
    print("CA sheet names:", xls.sheet_names)

    chosen = pick_sheet_name(xls.sheet_names)
    print("CA chosen sheet:", chosen)

    df_raw = pd.read_excel(xls, sheet_name=chosen)
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    print("CA columns:", df_raw.columns.tolist())

    col_company = pick_col(df_raw.columns, ["Company", "Company Name", "Employer", "Employer Name"])
    col_city = pick_col(df_raw.columns, ["City", "Location City", "Worksite City"])
    col_notice = pick_col(df_raw.columns, ["Notice Date", "Received Date", "Date Received", "WARN Received Date"])
    col_effective = pick_col(df_raw.columns, ["Effective Date", "Layoff Date", "Separation Date", "Closure/Layoff Date"])
    col_count = pick_col(df_raw.columns, ["No. of Employees", "Number of Employees", "Employees Affected", "Total Affected"])

    print("CA matched cols:", col_company, col_city, col_notice, col_effective, col_count)

    if not col_company or not col_notice:
        print("CA required columns missing on chosen sheet")
        return

    rows = []
    for _, r in df_raw.iterrows():
        company = str(r.get(col_company, "")).strip()
        if not company:
            continue

        notice_date = parse_date(r.get(col_notice, ""))
        if not notice_date:
            continue

        effective_date = parse_date(r.get(col_effective, "")) if col_effective else ""
        city = str(r.get(col_city, "")).strip() if col_city else ""

        emp = 0
        if col_count:
            try:
                emp = int(str(r.get(col_count, "0")).replace(",", "").strip() or "0")
            except Exception:
                emp = 0

        clean_name = apply_clean_name(company, mappings)
        source_url = URL
        hash_id = make_hash_id(company, notice_date, effective_date, city, source_url)

        rows.append({
            "hash_id": hash_id,
            "company": company,
            "clean_name": clean_name,
            "notice_date": notice_date,
            "effective_date": effective_date,
            "employee_count": str(emp),
            "city": city,
            "state": STATE,
            "source_url": source_url,
        })

    if not rows:
        print("CA parsed sheet but produced 0 rows")
        return

    df = pd.DataFrame(rows)

    for c in WANTED_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[WANTED_COLS]

    added = upsert_append_csv(OUT_FILE, df)
    print(f"CA added {added} rows -> {OUT_FILE}")

if __name__ == "__main__":
    main()
