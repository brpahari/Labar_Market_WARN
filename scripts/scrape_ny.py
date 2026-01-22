import os
import json
import re
import time
from io import BytesIO
from datetime import datetime

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "NY"
BASE = "https://dol.ny.gov"
YEAR = datetime.utcnow().year

LISTING_URL = f"{BASE}/legacy-warn-notices"
OUT_DIR = "data/ny"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def parse_date_any(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    s = s.replace("\u2013", "-")
    s = re.sub(r"\s+", " ", s)

    # Remove parenthetical like (Amended 3/17/2025)
    s = re.sub(r"\(.*?\)", "", s).strip()

    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%m/%d/%Y %I:%M %p"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", s)
    if m:
        return parse_date_any(m.group(1))

    return ""

def is_pdf_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    return ("application/pdf" in ctype) or ("application/octet-stream" in ctype and resp.content[:4] == b"%PDF")

def extract_pdf_fields(pdf_bytes: bytes) -> dict:
    text = ""
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return {}
            # first page is usually enough for NY
            text = pdf.pages[0].extract_text() or ""
    except Exception:
        return {}

    def find_one(pattern: str) -> str:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        return m.group(1).strip() if m else ""

    company = find_one(r"Company:\s*(.+)")
    affected = find_one(r"Total Number of Affected Workers:\s*([0-9,]+)")
    notice = find_one(r"Date of Notice:\s*(.+)")
    closure_start = find_one(r"Closure Start Date:\s*(.+)")
    address = find_one(r"Address:\s*(.+)")

    city = ""
    if address:
        # try to capture city before NY ZIP
        m = re.search(r",\s*NY\s*\d{5}", address)
        if m:
            left = address[:m.start()]
            parts = [p.strip() for p in left.split(",") if p.strip()]
            if parts:
                city = parts[-1]

    employee_count = 0
    if affected:
        try:
            employee_count = int(affected.replace(",", ""))
        except Exception:
            employee_count = 0

    return {
        "company": company,
        "notice_date": parse_date_any(notice),
        "effective_date": parse_date_any(closure_start),
        "employee_count": employee_count,
        "city": city,
    }

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = {}
    if os.path.exists(MAPPINGS_FILE):
        try:
            with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
                mappings = json.load(f)
        except Exception:
            mappings = {}

    seen_urls = set()
    if os.path.exists(OUT_FILE):
        try:
            df_history = pd.read_csv(OUT_FILE, dtype=str).fillna("")
            if "source_url" in df_history.columns:
                seen_urls = set(df_history["source_url"].astype(str).str.strip().tolist())
        except Exception:
            seen_urls = set()

    resp = session.get(LISTING_URL, headers=headers, timeout=60, allow_redirects=True)
    print(f"NY listing fetch {LISTING_URL} status {resp.status_code} final {resp.url}")

    if resp.status_code != 200:
        print("NY listing fetch failed")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    # collect /warn- links
    warn_links = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("/warn-") or "/warn-" in href:
            url = href if href.startswith("http") else BASE + href
            warn_links.append((a.get_text(strip=True) or "", url.strip()))

    warn_links = list(dict.fromkeys(warn_links))  # unique preserve order
    print(f"NY warn links found {len(warn_links)}")

    rows = []
    for anchor_text, url in warn_links:
        if url in seen_urls:
            continue

        time.sleep(0.7)

        try:
            pdf_resp = session.get(url, headers=headers, timeout=60)
        except Exception as e:
            print(f"NY fetch failed {url} {e}")
            continue

        if pdf_resp.status_code != 200:
            continue

        if not is_pdf_response(pdf_resp):
            # Not a PDF. Skip.
            continue

        fields = extract_pdf_fields(pdf_resp.content)
        if not fields:
            continue

        company = fields.get("company") or anchor_text
        notice_date = fields.get("notice_date", "")
        effective_date = fields.get("effective_date", "")
        city = fields.get("city", "")
        employee_count = fields.get("employee_count", 0)

        # if it still looks like a placeholder, skip
        if not company:
            continue

        clean_name = apply_clean_name(company, mappings)
        hash_id = make_hash_id(company, notice_date, effective_date, city, url)

        rows.append({
            "hash_id": hash_id,
            "company": company,
            "clean_name": clean_name,
            "notice_date": notice_date,
            "effective_date": effective_date,
            "employee_count": str(employee_count),
            "city": city,
            "state": STATE,
            "source_url": url,
        })

    print(f"NY extracted rows {len(rows)}")

    if not rows:
        print("NY no new rows found")
        return

    df_new = pd.DataFrame(rows)
    added = upsert_append_csv(OUT_FILE, df_new)
    print(f"NY added {added} rows -> {OUT_FILE}")

if __name__ == "__main__":
    main()
