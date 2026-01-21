import os
import json
import re
import requests
import pandas as pd
import pdfplumber
import time
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup

from common import norm_text, apply_clean_name, make_hash_id, upsert_append_csv

STATE = "NY"
BASE = "https://dol.ny.gov"
YEAR = datetime.utcnow().year

LISTING_URL = f"{BASE}/{YEAR}-warn-notices"
OUT_DIR = f"data/ny"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

# Use session for connection pooling
session = requests.Session()
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def parse_date_any(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = s.replace("\u2013", "-")
    for fmt in ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", s)
    if m:
        return parse_date_any(m.group(1))
    return ""

def extract_pdf_fields(pdf_bytes: bytes) -> dict:
    text = ""
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return {}
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
        except:
            pass

    return {
        "company": company,
        "notice_date": parse_date_any(notice),
        "effective_date": parse_date_any(closure_start),
        "employee_count": employee_count,
        "city": city
    }

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = {}
    if os.path.exists(MAPPINGS_FILE):
        with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
            mappings = json.load(f)

    # 1. Load History (Lazy Check)
    seen_urls = set()
    if os.path.exists(OUT_FILE):
        try:
            df_history = pd.read_csv(OUT_FILE)
            if "source_url" in df_history.columns:
                seen_urls = set(df_history["source_url"].astype(str).str.strip().tolist())
        except Exception:
            pass

    try:
        html = session.get(LISTING_URL, headers=headers, timeout=60).text
    except Exception as e:
        print(f"NY listing fetch failed: {e}")
        return

    soup = BeautifulSoup(html, "html.parser")

    rows = []
    for a in soup.select("a"):
        href = a.get("href") or ""
        if href.startswith("/warn-") or "/warn-" in href:
            url = href if href.startswith("http") else BASE + href
            
            # Normalize for set comparison
            url_clean = url.strip()
            
            if url_clean in seen_urls:
                continue

            time.sleep(0.7) # Polite rate limit

            try:
                pdf_bytes = session.get(url, headers=headers, timeout=30).content
                fields = extract_pdf_fields(pdf_bytes)
            except Exception as e:
                print(f"Failed parsing {url}: {e}")
                continue

            company = fields.get("company") or a.get_text(strip=True)
            notice_date = fields.get("notice_date")
            effective_date = fields.get("effective_date")
            city = fields.get("city")
            employee_count = fields.get("employee_count", 0)

            clean_name = apply_clean_name(company, mappings)
            hash_id = make_hash_id(company, notice_date, effective_date, city, url_clean)

            rows.append({
                "hash_id": hash_id,
                "company": company,
                "clean_name": clean_name,
                "notice_date": notice_date,
                "effective_date": effective_date,
                "employee_count": employee_count,
                "city": city,
                "state": STATE,
                "source_url": url_clean
            })

    if not rows:
        print("NY no new rows found")
        return

    df = pd.DataFrame(rows)
    added = upsert_append_csv(OUT_FILE, df)
    print(f"NY added {added} rows")

if __name__ == "__main__":
    main()
