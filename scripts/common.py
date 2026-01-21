import re
import hashlib
import pandas as pd

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def apply_clean_name(company: str, mappings: dict) -> str:
    key = norm_text(company)
    return mappings.get(key, company)

def make_hash_id(company: str, notice_date: str, effective_date: str, city: str, source_url: str) -> str:
    raw = f"{company}|{notice_date}|{effective_date}|{city}|{source_url}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def upsert_append_csv(path: str, df_new: pd.DataFrame) -> int:
    try:
        df_old = pd.read_csv(path, dtype=str).fillna("")
        if "hash_id" not in df_old.columns:
            df_old["hash_id"] = ""
        old_ids = set(df_old["hash_id"].astype(str).tolist())

        df_new = df_new.copy()
        df_new["hash_id"] = df_new["hash_id"].astype(str)
        df_new = df_new[~df_new["hash_id"].isin(old_ids)]

        if df_new.empty:
            return 0

        df_out = pd.concat([df_old, df_new], ignore_index=True)
        df_out.to_csv(path, index=False)
        return len(df_new)
    except FileNotFoundError:
        df_new.to_csv(path, index=False)
        return len(df_new)
