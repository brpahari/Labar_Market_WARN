import os
import json
import re
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from bs4 import BeautifulSoup

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "TX"
YEAR = datetime.utcnow().year
OUT_DIR = "data/tx"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

PAGE_URL = "https://www.twc.texas.gov/data-reports/warn-notice-data"

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

def pick_sheet_link(html):
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if re.search(r"\.(xlsx|xls|csv)$", href, flags=re.IGNORECASE):
            if href.startswith("/"):
                href = "https://www.twc.texas.gov" + href
            links.append(href)
    for u in links:
        if str(YEAR) in u:
            return u
    return links[0] if links else ""

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = {}
    if os.path.exists(MAPPINGS_FILE):
        with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
            mappings = json.load(f)

    html = session.get(PAGE_URL, headers=headers, timeout=60).text
    data_url = pick_sheet_link(html)
    if not data_url:
        print("TX data link not found")
        return

    resp = session.get(data_url, headers=headers, timeout=60)
    resp.raise_for_status()

    if data_url.lower().endswith(".csv"):
        df_src = pd.read_csv(BytesIO(resp.content), dtype=str).fillna("")
    else:
        df_src = pd.read_excel(BytesIO(resp.content), engine="openpyxl").fillna("")

    c_company = find_col(df_src.columns, ["employer", "company", "company name"])
    c_city = find_col(df_src.columns, ["city", "location"])
    c_notice = find_col(df_src.columns, ["notice date", "received date"])
    c_effective = find_col(df_src.columns, ["layoff date", "effective date", "separation date"])
    c_count = find_col(df_src.columns, ["number affected", "employees affected", "affected", "number of workers", "employees"])

    if not c_company or not c_city:
        print("TX required columns not found")
        return

    out = pd.DataFrame()
    out["company"] = df_src[c_company].astype(str).fillna("")
    out["clean_name"] = out["company"].apply(lambda x: apply_clean_name(x, mappings))
    out["city"] = df_src[c_city].astype(str).fillna("").str.title()
    out["state"] = STATE
    out["notice_date"] = df_src[c_notice].apply(to_iso_date) if c_notice else ""
    out["effective_date"] = df_src[c_effective].apply(to_iso_date) if c_effective else ""

    if c_count:
        out["employee_count"] = pd.to_numeric(df_src[c_count], errors="coerce").fillna(0).astype(int).astype(str)
    else:
        out["employee_count"] = "0"

    out["source_url"] = data_url
    out["hash_id"] = out.apply(
        lambda r: make_hash_id(r["company"], r["notice_date"], r["effective_date"], r["city"], r["source_url"]),
        axis=1
    )

    out = out[["hash_id","company","clean_name","notice_date","effective_date","employee_count","city","state","source_url"]]
    added = upsert_append_csv(OUT_FILE, out)
    print(f"TX added {added} rows from {data_url}")

if __name__ == "__main__":
    main()
