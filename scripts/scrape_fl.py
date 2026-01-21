import os
import json
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "FL"
YEAR = datetime.utcnow().year
OUT_DIR = "data/fl"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

URL = f"https://reactwarn.floridajobs.org/NoticeList.aspx?year={YEAR}"

headers = {"User-Agent": "Mozilla/5.0"}
session = requests.Session()

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

    html = session.get(URL, headers=headers, timeout=60).text
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table")
    if table is None:
        print("FL table not found")
        return

    rows = []
    trs = table.find_all("tr")
    if not trs:
        print("FL no rows")
        return

    headers_row = [th.get_text(" ", strip=True) for th in trs[0].find_all(["th","td"])]
    for tr in trs[1:]:
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if not tds or len(tds) < 2:
            continue
        row = dict(zip(headers_row, tds))
        rows.append(row)

    if not rows:
        print("FL no data rows")
        return

    df_src = pd.DataFrame(rows)

    def find_col(needles):
        cols = list(df_src.columns)
        cols_l = [c.lower() for c in cols]
        for n in needles:
            n = n.lower()
            for i, c in enumerate(cols_l):
                if n in c:
                    return cols[i]
        return None

    c_company = find_col(["company", "employer"])
    c_city = find_col(["city"])
    c_notice = find_col(["notice"])
    c_effective = find_col(["effective", "layoff", "separation"])
    c_count = find_col(["affected", "employees", "number"])

    if not c_company:
        print("FL company column not found")
        return

    out = pd.DataFrame()
    out["company"] = df_src[c_company].astype(str).fillna("")
    out["clean_name"] = out["company"].apply(lambda x: apply_clean_name(x, mappings))
    out["city"] = df_src[c_city].astype(str).fillna("").str.title() if c_city else ""
    out["state"] = STATE
    out["notice_date"] = df_src[c_notice].apply(to_iso_date) if c_notice else ""
    out["effective_date"] = df_src[c_effective].apply(to_iso_date) if c_effective else ""

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
    print(f"FL added {added} rows")

if __name__ == "__main__":
    main()
