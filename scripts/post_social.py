import pandas as pd
import os
import requests
import json
import time

CURRENT_FILE = "site/current_year.csv"
HISTORY_FILE = "site/history_snapshot.json"

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")

def load_previous_hashes():
    if not os.path.exists(HISTORY_FILE):
        return set()
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
        return set()
    except Exception:
        return set()

def save_current_hashes(hashes):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(list(hashes), f)

def safe_post(msg):
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": msg}, timeout=20)
    except Exception:
        pass

def fmt_row(r):
    company = r.get("clean_name") or r.get("company") or "Unknown"
    city = r.get("city") or ""
    state = r.get("state") or ""
    count = r.get("employee_count") or ""
    notice_date = r.get("notice_date") or ""
    source = r.get("source_url") or ""

    loc = ", ".join([x for x in [city, state] if x])
    if loc:
        loc = " in " + loc

    count_part = ""
    try:
        n = int(str(count).strip() or "0")
        if n > 0:
            count_part = f" reporting {n} affected employees"
    except Exception:
        pass

    date_part = f" Notice date {notice_date}" if notice_date else ""

    msg = f"🚨 WARN Notice: {company}{loc}{count_part}.{date_part}"
    if source:
        msg += f"\n{source}"
    return msg

def main():
    if not os.path.exists(CURRENT_FILE):
        print("No data file found.")
        return

    df = pd.read_csv(CURRENT_FILE, dtype=str).fillna("")
    if "hash_id" not in df.columns:
        print("Missing hash_id column.")
        return

    previous = load_previous_hashes()
    current = set(df["hash_id"].astype(str).tolist())

    new_ids = list(current - previous)
    if not new_ids:
        print("No new notices to post.")
        return

    new_rows = df[df["hash_id"].isin(new_ids)].copy()

    def to_int(x):
        try:
            return int(str(x).strip())
        except Exception:
            return 0

    if "employee_count" in new_rows.columns:
        new_rows["employee_count_int"] = new_rows["employee_count"].apply(to_int)
        new_rows = new_rows.sort_values(by="employee_count_int", ascending=False)
    else:
        new_rows["employee_count_int"] = 0

    new_rows = new_rows.head(3)

    for _, r in new_rows.iterrows():
        msg = fmt_row(r.to_dict())
        print(msg)
        safe_post(msg)
        time.sleep(1)

    save_current_hashes(current)

if __name__ == "__main__":
    main()
