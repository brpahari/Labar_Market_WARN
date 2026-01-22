import os
import re
import json
import time
from io import BytesIO
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup

from common import apply_clean_name, make_hash_id, upsert_append_csv

STATE = "NY"
BASE = "https://dol.ny.gov"
YEAR = datetime.utcnow().year

# Try multiple known NY pages
LISTING_URLS = [
    f"{BASE}/warn-notices",
    f"{BASE}/{YEAR}-warn-notices",
    f"{BASE}/warn-notices-{YEAR}",
]

OUT_DIR = "data/ny"
OUT_FILE = f"{OUT_DIR}/{YEAR}.csv"
MAPPINGS_FILE = "site/mappings.json"

session = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def parse_date_any(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    s = s.replace("\u2013", "-")
    s = re.sub(r"\s+", " ", s)

    fmts = [
        "%B %d, %Y",
        "%b %d, %Y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
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

def is_probable_pdf(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    path = (p.path or "").lower()
    qs = (p.query or "").lower()
    if ".pdf" in path:
        return True
    if ".pdf" in qs:
        return True
    # NY often stores PDFs under system files
    if "system/files/documents" in path and ("warn" in path or "notice" in path):
        return True
    return False

def fetch_html(url: str) -> str:
    try:
        r = session.get(url, headers=HEADERS, timeout=60, allow_redirects=True)
        print("NY listing fetch", url, "status", r.status_code, "final", r.url)
        if r.status_code >= 400:
            return ""
        # some sites return html but with bot text
        txt = r.text or ""
        return txt
    except Exception as e:
        print("NY listing fetch failed", url, str(e))
        return ""

def extract_pdf_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)

        if not is_probable_pdf(full):
            continue

        title = a.get_text(" ", strip=True) or ""
        out.append((full.strip(), title.strip()))
    # de dup by url
    seen = set()
    dedup = []
    for u, t in out:
        if u in seen:
            continue
        seen.add(u)
        dedup.append((u, t))
    return dedup

def extract_pdf_fields(pdf_bytes: bytes) -> dict:
    """
    NY PDFs are not consistent. We scan first 2 pages and try multiple patterns.
    """
    text = ""
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return {}
            pages = pdf.pages[:2]
            parts = []
            for pg in pages:
                parts.append(pg.extract_text() or "")
            text = "\n".join(parts)
    except Exception:
        return {}

    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)

    def first(patterns):
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                return (m.group(1) or "").strip()
        return ""

    company = first([
        r"Company:\s*(.+)",
        r"Employer:\s*(.+)",
        r"Company Name:\s*(.+)",
    ])

    notice = first([
        r"Date of Notice:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Date of Notice:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        r"Notice Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Notice Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
    ])

    effective = first([
        r"Closure Start Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Closure Start Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        r"Layoff Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Layoff Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
        r"Effective Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"Effective Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
    ])

    affected = first([
        r"Number of Affected Employees at Site:\s*([0-9,]+)",
        r"Total Number of Affected Workers:\s*([0-9,]+)",
        r"Affected Employees:\s*([0-9,]+)",
        r"Number of Affected Workers:\s*([0-9,]+)",
    ])

    address = first([
        r"Address:\s*(.+)",
        r"Worksite Address:\s*(.+)",
    ])

    city = ""
    if address:
        one = address.split("\n")[0].strip()
        m = re.search(r",\s*([^,]+),\s*NY\s*\d{5}", one)
        if m:
            city = m.group(1).strip()
        else:
            m = re.search(r",\s*([^,]+)\s+NY\s*,?\s*\d{5}", one)
            if m:
                city = m.group(1).strip()

    employee_count = 0
    if affected:
        try:
            employee_count = int(affected.replace(",", ""))
        except Exception:
            employee_count = 0

    return {
        "company": company,
        "notice_date": parse_date_any(notice),
        "effective_date": parse_date_any(effective),
        "employee_count": employee_count,
        "city": city,
    }

def load_mappings():
    if os.path.exists(MAPPINGS_FILE):
        try:
            with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    mappings = load_mappings()

    seen_urls = set()
    if os.path.exists(OUT_FILE):
        try:
            df_history = pd.read_csv(OUT_FILE, dtype=str).fillna("")
            if "source_url" in df_history.columns:
                seen_urls = set(df_history["source_url"].astype(str).str.strip().tolist())
        except Exception:
            pass

    html = ""
    used_listing = ""
    for u in LISTING_URLS:
        html = fetch_html(u)
        if html and len(html) > 2000:
            used_listing = u
            break

    if not html:
        print("NY no listing html found")
        return

    links = extract_pdf_links(html, used_listing)
    print("NY pdf links found", len(links))

    if not links:
        print("NY no pdf links on page")
        return

    rows = []
    for pdf_url, anchor_title in links:
        if pdf_url in seen_urls:
            continue

        time.sleep(0.6)

        try:
            r = session.get(pdf_url, headers={**HEADERS, "Referer": used_listing}, timeout=60, allow_redirects=True)
            print("NY pdf fetch", r.status_code, r.url)
            if r.status_code >= 400:
                continue
            pdf_bytes = r.content
        except Exception as e:
            print("NY pdf fetch failed", pdf_url, str(e))
            continue

        fields = extract_pdf_fields(pdf_bytes)

        company = (fields.get("company") or "").strip()
        if not company:
            company = (anchor_title or "").strip()

        if not company:
            # fallback from filename
            name = (urlparse(pdf_url).path or "").split("/")[-1]
            company = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE).replace("-", " ").replace("_", " ").strip()

        notice_date = fields.get("notice_date", "")
        effective_date = fields.get("effective_date", "")
        city = fields.get("city", "")
        employee_count = fields.get("employee_count", 0)

        # Important change
        # Do not skip rows just because parsing failed
        # We still want NY to show up and the source link to work
        clean_name = apply_clean_name(company, mappings)
        hash_id = make_hash_id(company, notice_date, effective_date, city, pdf_url)

        rows.append({
            "hash_id": hash_id,
            "company": company,
            "clean_name": clean_name,
            "notice_date": notice_date,
            "effective_date": effective_date,
            "employee_count": str(int(employee_count) if employee_count else 0),
            "city": city,
            "state": STATE,
            "source_url": pdf_url
        })

    if not rows:
        print("NY no new rows to write")
        return

    df = pd.DataFrame(rows)
    added = upsert_append_csv(OUT_FILE, df)
    print("NY added", added)

if __name__ == "__main__":
    main()
