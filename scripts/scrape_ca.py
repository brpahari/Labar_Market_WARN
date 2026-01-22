import os
import json
import re
from io import BytesIO
from datetime import datetime
import html as htmlmod

import requests
import pandas as pd

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "CA"
YEAR = datetime.utcnow().year

OUT_DIR = "data/ca"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"

MAPPINGS_FILE = "site/mappings.json"

# This workbook actually contains the detailed table
CA_XLSX_URL = "https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report1.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def load_mappings() -> dict:
    if not os.path.exists(MAPPINGS_FILE):
        return {}
    try:
        with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def parse_date(val) -> str:
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")

def extract_city_from_address(addr: str) -> str:
    a = str(addr or "").strip()
    if not a:
        return ""

    a = htmlmod.unescape(a)
    a = re.sub(r"\s+", " ", a).strip()

    # Try pattern "... <City> CA <zip>"
    m = re.search(r"\s([A-Za-z][A-Za-z \-\.']+)\sCA\s\d{5}(-\d{4})?$", a)
    if m:
        return m.group(1).strip()

    # Fallback if format is "street  city CA zip" with double spaces in the original
    parts = re.split(r"\s{2,}", str(addr or "").strip())
    if len(parts) >= 2:
        tail = parts[-1]
        m2 = re.search(r"^(.+?)\sCA\s\d{5}", tail)
        if m2:
            return m2.group(1).strip()

    return ""

def pick_sheet_name(sheet_names):
    # There is a trailing space in the official file
    for s in sheet_names:
        if str(s).strip().lower() == "detailed warn report":
            return s
    return sheet_names[-1]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = load_mappings()

    resp = requests.get(CA_XLSX_URL, headers=HEADERS, timeout=90)
    resp.raise_for_status()

    xls = pd.ExcelFile(BytesIO(resp.content))
    sheet = pick_sheet_name(xls.sheet_names)

    # header=1 is critical for this workbook
    df = pd.read_excel(xls, sheet_name=sheet, header=1)

    cols = {c: str(c).strip().lower() for c in df.columns}

    def find_col(*needles):
        for c, lc in cols.items():
            ok = True
            for n in needles:
                if n not in lc:
                    ok = False
                    break
            if ok:
                return c
        return None

    col_notice = find_col("notice", "date")
    col_effective = find_col("effective", "date")
    col_company = find_col("company")
    col_count = find_col("employees")
    col_address = find_col("address")

    if not col_notice or not col_company:
        print("CA required columns missing", list(df.columns))
        return

    rows = []
    for _, r in df.iterrows():
        company = htmlmod.unescape(str(r.get(col_company, "")).strip())
        if not company:
            continue

        notice_date = parse_date(r.get(col_notice, ""))
        if not notice_date:
            continue

        # Keep only current calendar year in the state file
        if not notice_date.startswith(str(YEAR) + "-"):
            continue

        effective_date = parse_date(r.get(col_effective, "")) if col_effective else ""

        emp = 0
        if col_count:
            try:
                emp = int(str(r.get(col_count, "0")).replace(",", "").strip() or "0")
            except Exception:
                emp = 0

        city = ""
        if col_address:
            city = extract_city_from_address(r.get(col_address, ""))

        clean_name = apply_clean_name(company, mappings)
        source_url = CA_XLSX_URL
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
        print("CA parsed but produced 0 rows for", YEAR)
        return

    out = pd.DataFrame(rows)
    added = upsert_append_csv(OUT_FILE, out)
    print("CA added", added, "rows ->", OUT_FILE)

if __name__ == "__main__":
    main()
