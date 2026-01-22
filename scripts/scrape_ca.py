import os
import json
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "CA"
YEAR = datetime.utcnow().year

OUT_DIR = "data/ca"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

URL = "https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report.xlsx"

def parse_date(val):
    try:
        return pd.to_datetime(val, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return ""

def pick_column(columns, candidates):
    lower = {c.lower(): c for c in columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = {}
    if os.path.exists(MAPPINGS_FILE):
        try:
            with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
                mappings = json.load(f)
        except Exception:
            mappings = {}

    resp = requests.get(URL, timeout=60)
    resp.raise_for_status()

    df_raw = pd.read_excel(BytesIO(resp.content))
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    col_company = pick_column(df_raw.columns, ["Company", "Company Name", "Employer"])
    col_city = pick_column(df_raw.columns, ["City", "Location City"])
    col_notice = pick_column(df_raw.columns, ["Notice Date", "Received Date"])
    col_effective = pick_column(df_raw.columns, ["Effective Date", "Layoff Date"])
    col_count = pick_column(df_raw.columns, ["No. of Employees", "Employees Affected"])

    if not col_company or not col_notice:
        print("CA required columns missing")
        print(df_raw.columns.tolist())
        return

    rows = []

    for _, r in df_raw.iterrows():
        company = str(r.get(col_company, "")).strip()
        if not company:
            continue

        notice_date = parse_date(r.get(col_notice))
        if not notice_date:
            continue

        effective_date = parse_date(r.get(col_effective)) if col_effective else ""
        city = str(r.get(col_city, "")).strip() if col_city else ""

        emp = 0
        if col_count:
            try:
                emp = int(str(r.get(col_count)).replace(",", ""))
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
            "source_url": source_url
        })

    if not rows:
        print("CA scraper produced no rows")
        return

    df = pd.DataFrame(rows)
    added = upsert_append_csv(OUT_FILE, df)
    print(f"CA added {added} rows")

if __name__ == "__main__":
    main()
