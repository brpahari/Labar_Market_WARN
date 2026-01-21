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

headers = {"User-Agent": "Mozilla/5.0"}
session = requests.Session()

def find_col(cols, needles):
    cols_l = [c.lower().strip() for c in cols]
    for n in needles:
        n = n.lower()
        for i, c in enumerate(cols_l):
            if n == c:
                return cols[i]
    for n in needles:
        n = n.lower()
        for i, c in enumerate(cols_l):
            if n in c:
                return cols[i]
    return None

def to_iso_date(x):
    try:
        return pd.to_datetime(x, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return ""

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = {}
    if os.path.exists(MAPPINGS_FILE):
        with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
            mappings = json.load(f)

    resp = session.get(URL, headers=headers, timeout=60)
    resp.raise_for_status()
    df_src = pd.read_excel(BytesIO(resp.content), engine="openpyxl")

    c_company = find_col(df_src.columns, ["company", "company name", "employer", "employer name"])
    c_city = find_col(df_src.columns, ["city"])
    c_notice = find_col(df_src.columns, ["notice date", "received date", "date received"])
    c_effective = find_col(df_src.columns, ["effective date", "layoff date", "impact date", "closure start date"])
    c_count = find_col(df_src.columns, ["number of employees", "no. of employees", "employees affected", "affected employees"])

    if not c_company or not c_city:
        print("CA required columns not found")
        return

    out = pd.DataFrame()
    out["company"] = df_src[c_company].astype(str).fillna("")
    out["clean_name"] = out["company"].apply(lambda x: apply_clean_name(x, mappings))
    out["city"] = df_src[c_city].astype(str).fillna("").str.title()
    out["state"] = STATE

    if c_notice:
        out["notice_date"] = df_src[c_notice].apply(to_iso_date)
    else:
        out["notice_date"] = ""

    if c_effective:
        out["effective_date"] = df_src[c_effective].apply(to_iso_date)
    else:
        out["effective_date"] = ""

    if c_count:
        out["employee_count"] = pd.to_numeric(df_src[c_count], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        out["employee_count"] = "0"

    out["source_url"] = URL
    out["hash_id"] = out.apply(
        lambda r: make_hash_id(r["company"], r["notice_date"], r["effective_date"], r["city"], r["source_url"]),
        axis=1
    )

    out = out[["hash_id","company","clean_name","notice_date","effective_date","employee_count","city","state","source_url"]]
    added = upsert_append_csv(OUT_FILE, out)
    print(f"CA added {added} rows")

if __name__ == "__main__":
    main()
