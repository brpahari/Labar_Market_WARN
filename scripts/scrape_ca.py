import os
import sys
import json
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime

# --- SETUP PATH TO FIND COMMON.PY ---
# This ensures we can import common whether running from root or scripts/
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, current_dir)

try:
    from common import apply_clean_name, make_hash_id, upsert_append_csv
except ImportError:
    print("CRITICAL ERROR: Could not import 'common.py'. Make sure it exists in the scripts folder.")
    sys.exit(1)

# --- CONFIG ---
STATE = "CA"
YEAR = datetime.utcnow().year
OUT_DIR = os.path.join("data", "ca")  # Cross-platform path
OUT_FILE = os.path.join(OUT_DIR, f"{YEAR}.csv")
MAPPINGS_FILE = os.path.join("site", "mappings.json")
URL = "https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report.xlsx"

WANTED_COLS = [
    "hash_id", "company", "clean_name", "notice_date", 
    "effective_date", "employee_count", "city", "state", "source_url"
]

def load_mappings():
    if not os.path.exists(MAPPINGS_FILE):
        return {}
    try:
        with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def parse_date(val):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    try:
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except:
        return ""

def find_header_and_data(df):
    """
    Scans a dataframe to find the row that looks like a header 
    (contains 'Company' and 'Notice Date'), then returns the cleaned data.
    """
    # 1. Scan first 20 rows for a header
    header_idx = -1
    for idx, row in df.head(20).iterrows():
        # Convert entire row to string, lowercase, to search for keywords
        row_str = " ".join([str(x) for x in row.values if pd.notna(x)]).lower()
        if "company" in row_str and "notice" in row_str:
            header_idx = idx
            break
    
    if header_idx == -1:
        return None, "Could not find header row with 'Company' and 'Notice'"

    # 2. Reload/Slice using that header
    # We take the row at header_idx as columns, and data below it
    new_columns = df.iloc[header_idx].astype(str).str.strip()
    df_data = df.iloc[header_idx+1:].copy()
    df_data.columns = new_columns
    
    return df_data, None

def main():
    print(f"--- Starting CA Scraper for {YEAR} ---")
    
    # 1. Ensure Directory Exists (Crucial fix for your missing folder)
    if not os.path.exists(OUT_DIR):
        print(f"Creating directory: {OUT_DIR}")
        os.makedirs(OUT_DIR, exist_ok=True)

    mappings = load_mappings()

    # 2. Download File
    print(f"Downloading {URL}...")
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(URL, headers=headers, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        print(f"CRITICAL: Download failed: {e}")
        return

    # 3. Read Excel
    try:
        xls = pd.ExcelFile(BytesIO(resp.content))
        print(f"Found sheets: {xls.sheet_names}")
    except Exception as e:
        print(f"CRITICAL: Could not parse Excel file. It might be corrupt or not an xlsx. Error: {e}")
        return

    # 4. Sheet Search Strategy
    # Instead of guessing the name, we check EVERY sheet until we find valid columns
    df_clean = None
    
    for sheet in xls.sheet_names:
        print(f"Checking sheet: '{sheet}'...")
        try:
            df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)
            found_df, error = find_header_and_data(df_raw)
            
            if found_df is not None:
                print(f"SUCCESS: Found valid table in '{sheet}'")
                df_clean = found_df
                break
            else:
                print(f"  Skipping '{sheet}': {error}")
        except Exception as e:
            print(f"  Error reading '{sheet}': {e}")

    if df_clean is None:
        print("CRITICAL: No valid data found in any sheet.")
        return

    # 5. Map Columns
    cols = {c.lower(): c for c in df_clean.columns}
    
    def get_col(candidates):
        for c in candidates:
            if c.lower() in cols:
                return cols[c.lower()]
        return None

    c_company = get_col(["Company Name", "Company", "Employer", "Employer Name"])
    c_city = get_col(["City", "Location City", "Worksite City"])
    c_notice = get_col(["Notice Date", "Received Date", "Date Received"])
    c_effective = get_col(["Effective Date", "Layoff Date", "Closure/Layoff Date"])
    c_count = get_col(["No. of Employees", "Employees Affected", "Total Affected", "Number of Employees"])

    print(f"Mapped Columns: Company=[{c_company}], Date=[{c_notice}], Count=[{c_count}]")

    if not c_company or not c_notice:
        print("CRITICAL: Found header row but could not map 'Company' or 'Notice Date' columns.")
        print(f"Available headers: {list(df_clean.columns)}")
        return

    # 6. Extract Rows
    rows = []
    for _, r in df_clean.iterrows():
        company = str(r.get(c_company, "")).strip()
        # Filter out junk rows (repeated headers, disclaimers)
        if not company or company.lower() == "company name" or "report as of" in company.lower():
            continue

        notice_date = parse_date(r.get(c_notice, ""))
        if not notice_date:
            continue

        city = str(r.get(c_city, "")).strip() if c_city else ""
        effective_date = parse_date(r.get(c_effective, "")) if c_effective else ""
        
        # Clean Employee Count
        emp_raw = str(r.get(c_count, "0"))
        emp = 0
        import re
        digits = re.sub(r"[^0-9]", "", emp_raw)
        if digits:
            emp = int(digits)

        clean_name = apply_clean_name(company, mappings)
        hash_id = make_hash_id(company, notice_date, effective_date, city, URL)

        rows.append({
            "hash_id": hash_id,
            "company": company,
            "clean_name": clean_name,
            "notice_date": notice_date,
            "effective_date": effective_date,
            "employee_count": str(emp),
            "city": city,
            "state": STATE,
            "source_url": URL
        })

    print(f"Extracted {len(rows)} valid rows.")

    if not rows:
        print("CRITICAL: Table found but 0 rows extracted.")
        return

    # 7. Save
    df_final = pd.DataFrame(rows)
    # Fill missing columns
    for c in WANTED_COLS:
        if c not in df_final.columns:
            df_final[c] = ""
    df_final = df_final[WANTED_COLS]

    added = upsert_append_csv(OUT_FILE, df_final)
    print(f"SUCCESS: Added {added} new rows to {OUT_FILE}")

if __name__ == "__main__":
    main()
