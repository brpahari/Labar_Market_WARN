import os
import re
import json
import time
import hashlib
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "NY"
BASE = "https://dol.ny.gov"
YEAR = datetime.utcnow().year

# NY has year pages like /2024-warn-notices, /2025-warn-notices, /2026-warn-notices
LISTING_URL = f"{BASE}/{YEAR}-warn-notices"

OUT_DIR = "data/ny"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"

MAPPINGS_FILE = "site/mappings.json"

session = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def parse_date_any(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    s = s.replace("\u2013", "-")
    # Common NY formats
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]:
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

def _first_match(text: str, patterns) -> str:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return ""

def extract_pdf_fields(pdf_bytes: bytes) -> dict:
    """
    NY PDFs vary. Many contain fields like:
    Date of Notice:
    Closure Start Date:
    Number of Affected Employees at Site:
    Address:
    """
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return {}
            # first page is usually enough
            text = pdf.pages[0].extract_text() or ""
    except Exception:
        return {}

    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text).strip()

    notice_raw = _first_match(
        text,
        [
            r"Date of Notice:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            r"Date of Notice:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            r"Notice Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            r"Notice Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        ],
    )

    effective_raw = _first_match(
        text,
        [
            r"Closure Start Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            r"Closure Start Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            r"Layoff Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            r"Layoff Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        ],
    )

    affected_raw = _first_match(
        text,
        [
            r"Number of Affected Employees at Site:\s*([0-9,]+)",
            r"Total Number of Affected Workers:\s*([0-9,]+)",
            r"Affected Employees:\s*([0-9,]+)",
        ],
    )

    address = _first_match(
        text,
        [
            r"Address:\s*(.+)",
            r"Impacted Site:\s*.*?Address:\s*(.+)",
        ],
    )

    # City from address like "... , New York, NY 10036" or "... , Williamsville NY, 14221"
    city = ""
    if address:
        # remove trailing extra fields after zip
        address_one = address.split("\n")[0].strip()
        # Try "..., City, NY 12345"
        m = re.search(r",\s*([^,]+),\s*NY\s*\d{5}", address_one)
        if m:
            city = m.group(1).strip()
        else:
            # Try "..., City NY, 12345"
            m = re.search(r",\s*([^,]+)\s+NY\s*,?\s*\d{5}", address_one)
            if m:
                city = m.group(1).strip()

    employee_count = 0
    if affected_raw:
        try:
            employee_count = int(affected_raw.replace(",", ""))
        except Exception:
            employee_count = 0

    return {
        "notice_date": parse_date_any(notice_raw),
        "effective_date": parse_date_any(effective_raw),
        "employee_count": employee_count,
        "city": city,
    }

def load_mappings() -> dict:
    if os.path.exists(MAPPINGS_FILE):
        try:
            with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}

def fetch_listing_html() -> str:
    r = session.get(LISTING_URL, headers=HEADERS, timeout=60)
    # If the year page does not exist yet, fail cleanly
    if r.status_code >= 400:
        print(f"NY listing not available: {LISTING_URL} status {r.status_code}")
        return ""
    return r.text

def extract_pdf_links_and_titles(html: str):
    """
    Pull PDF links from the year listing page.
    We only accept links that end with .pdf or contain /system/files/documents/ and .pdf
    """
    soup = BeautifulSoup(html, "lxml")

    items = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        url = href if href.startswith("http") else (BASE + href if href.startswith("/") else f"{BASE}/{href}")

        url_l = url.lower()
        is_pdf = url_l.endswith(".pdf") or (".pdf" in url_l and "/system/files/documents/" in url_l)
        if not is_pdf:
            continue

        title = a.get_text(" ", strip=True) or ""
        items.append((url.strip(), title.strip()))

    # de dup by url, keep first title
    seen = set()
    out = []
    for url, title in items:
        if url in seen:
            continue
        seen.add(url)
        out.append((url, title))
    return out

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = load_mappings()

    # Lazy mode: skip URLs already present
    seen_urls = set()
    if os.path.exists(OUT_FILE):
        try:
            df_hist = pd.read_csv(OUT_FILE, dtype=str).fillna("")
            if "source_url" in df_hist.columns:
                seen_urls = set(df_hist["source_url"].astype(str).str.strip().tolist())
        except Exception:
            seen_urls = set()

    html = fetch_listing_html()
    if not html:
        return

    links = extract_pdf_links_and_titles(html)
    if not links:
        print("NY no pdf links found on listing page")
        return

    new_rows = []
    for pdf_url, anchor_title in links:
        if pdf_url in seen_urls:
            continue

        # polite pacing
        time.sleep(0.6)

        try:
            pdf_bytes = session.get(pdf_url, headers=HEADERS, timeout=60).content
        except Exception as e:
            print(f"NY pdf fetch failed {pdf_url}: {e}")
            continue

        fields = extract_pdf_fields(pdf_bytes)

        # Company name is usually best from the listing link text or the filename
        company = anchor_title.strip()
        if not company or len(company) < 3:
            # fallback from filename
            fname = pdf_url.split("/")[-1]
            company = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
            company = company.replace("-", " ").replace("_", " ").strip()

        notice_date = fields.get("notice_date", "")
        effective_date = fields.get("effective_date", "")
        city = fields.get("city", "")
        employee_count = fields.get("employee_count", 0)

        # Hard filter: avoid writing junk rows with no useful fields
        # If the PDF parse yields nothing useful, skip it
        if (not notice_date) and (not effective_date) and (int(employee_count) == 0) and (not city):
            continue

        clean_name = apply_clean_name(company, mappings)
        hash_id = make_hash_id(company, notice_date, effective_date, city, pdf_url)

        new_rows.append(
            {
                "hash_id": hash_id,
                "company": company,
                "clean_name": clean_name,
                "notice_date": notice_date,
                "effective_date": effective_date,
                "employee_count": int(employee_count),
                "city": city,
                "state": STATE,
                "source_url": pdf_url,
            }
        )

    if not new_rows:
        print("NY no new rows found")
        return

    df_new = pd.DataFrame(new_rows)
    added = upsert_append_csv(OUT_FILE, df_new)
    print(f"NY added {added} rows")

if __name__ == "__main__":
    main()
