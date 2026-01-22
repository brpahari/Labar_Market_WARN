import os
import re
import json
from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "NY"
BASE = "https://dol.ny.gov"
YEAR = datetime.utcnow().year

OUT_DIR = "data/ny"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

LISTING_URL = f"{BASE}/warn-notices"  # redirects to legacy-warn-notices

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def parse_date_any(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    s = s.replace("\u2013", "-")
    s = re.sub(r"\s+", " ", s)

    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", s)
    if m:
        return parse_date_any(m.group(1))

    m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", s)
    if m:
        return parse_date_any(m.group(1))

    return ""

def to_int(x: str) -> int:
    if x is None:
        return 0
    s = str(x).strip()
    s = s.replace(",", "")
    m = re.search(r"-?\d+", s)
    if not m:
        return 0
    try:
        return int(m.group(0))
    except Exception:
        return 0

def load_mappings() -> dict:
    if os.path.exists(MAPPINGS_FILE):
        try:
            with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
                m = json.load(f)
                if isinstance(m, dict):
                    return m
        except Exception:
            pass
    return {}

def find_warn_table(soup: BeautifulSoup):
    # Try common Drupal patterns first
    for sel in [
        "table",
        "table.views-table",
        "table.table",
        "table.table-striped",
    ]:
        t = soup.select_one(sel)
        if t and t.find_all("tr"):
            return t
    return None

def extract_row_cells(tr):
    tds = tr.find_all(["td", "th"])
    return [td.get_text(" ", strip=True) for td in tds]

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    mappings = load_mappings()

    seen_hashes = set()
    if os.path.exists(OUT_FILE):
        try:
            df_old = pd.read_csv(OUT_FILE, dtype=str).fillna("")
            if "hash_id" in df_old.columns:
                seen_hashes = set(df_old["hash_id"].astype(str).tolist())
        except Exception:
            pass

    s = requests.Session()
    r = s.get(LISTING_URL, headers=HEADERS, timeout=60, allow_redirects=True)
    print("NY listing fetch", LISTING_URL, "status", r.status_code, "final", r.url)

    if r.status_code != 200:
        print("NY listing not 200")
        return

    soup = BeautifulSoup(r.text, "lxml")

    table = find_warn_table(soup)
    if not table:
        print("NY warn table not found on page")
        return

    # Detect header map
    header = None
    thead = table.find("thead")
    if thead:
        hr = thead.find("tr")
        if hr:
            header = [h.lower().strip() for h in extract_row_cells(hr)]

    if not header:
        # fallback use first row as header if it looks like header
        first_tr = table.find("tr")
        if first_tr:
            header = [h.lower().strip() for h in extract_row_cells(first_tr)]

    # Build column index guesses
    def idx_like(keys):
        if not header:
            return None
        for i, h in enumerate(header):
            for k in keys:
                if k in h:
                    return i
        return None

    idx_company = idx_like(["company", "employer"])
    idx_city = idx_like(["city", "location", "county"])
    idx_notice = idx_like(["notice", "date of notice"])
    idx_effective = idx_like(["effective", "layoff", "closure", "start"])
    idx_count = idx_like(["number", "affected", "employees", "workers", "total"])

    if idx_company is None:
        # many legacy tables have company as first column
        idx_company = 0

    rows_out = []
    trs = table.find_all("tr")
    for tr in trs[1:]:
        cells = extract_row_cells(tr)
        if not cells or len(cells) < 2:
            continue

        company = cells[idx_company].strip() if idx_company < len(cells) else ""
        if not company or company.lower() in ["company", "employer"]:
            continue

        city = cells[idx_city].strip() if (idx_city is not None and idx_city < len(cells)) else ""
        notice_date = parse_date_any(cells[idx_notice]) if (idx_notice is not None and idx_notice < len(cells)) else ""
        effective_date = parse_date_any(cells[idx_effective]) if (idx_effective is not None and idx_effective < len(cells)) else ""
        employee_count = to_int(cells[idx_count]) if (idx_count is not None and idx_count < len(cells)) else 0

        # If the page includes old years, keep only current year notices when possible
        if notice_date and not notice_date.startswith(str(YEAR)):
            continue

        # Find a source link in the row if present
        a = tr.find("a", href=True)
        source_url = urljoin(r.url, a["href"].strip()) if a else r.url

        clean_name = apply_clean_name(company, mappings)
        hash_id = make_hash_id(company, notice_date, effective_date, city, source_url)

        if hash_id in seen_hashes:
            continue

        rows_out.append({
            "hash_id": hash_id,
            "company": company,
            "clean_name": clean_name,
            "notice_date": notice_date,
            "effective_date": effective_date,
            "employee_count": str(employee_count),
            "city": city,
            "state": STATE,
            "source_url": source_url,
        })

    print("NY extracted rows", len(rows_out))

    if not rows_out:
        print("NY no new rows found")
        return

    df_new = pd.DataFrame(rows_out)
    added = upsert_append_csv(OUT_FILE, df_new)
    print("NY added", added, "rows ->", OUT_FILE)

if __name__ == "__main__":
    main()
