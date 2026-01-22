import hashlib
import os
import pandas as pd
import re

def norm_key(s: str) -> str:
    s = str(s or "").lower()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def apply_clean_name(company: str, mappings: dict) -> str:
    raw = str(company or "").strip()
    k = norm_key(raw)
    return mappings.get(k, raw)

def make_hash_id(company: str, notice_date: str, effective_date: str, city: str, source_url: str) -> str:
    raw = f"{company}|{notice_date}|{effective_date}|{city}|{source_url}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def upsert_append_csv(path: str, df_new: pd.DataFrame) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        try:
            df_old = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            df_old = pd.DataFrame()
        if len(df_old) > 0 and "hash_id" in df_old.columns:
            before = len(df_old)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
            df_all = df_all.drop_duplicates(subset=["hash_id"], keep="first")
            df_all.to_csv(path, index=False)
            return len(df_all) - before

    df_new.to_csv(path, index=False)
    return len(df_new)
