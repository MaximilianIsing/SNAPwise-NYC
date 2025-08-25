import re
from typing import Iterable, Optional, Set, Tuple

import pandas as pd
import numpy as np


ALL_STORES_CSV = "source csv/All Food Stamp Stores.csv"
NYC_ZIPS_CSV = "source csv/NYC Zip Codes.csv"
HEALTHY_STORES_CSV = "source csv/NYC Healthy Stores.csv"
OUTPUT_NYC_STORES_CSV = "NYC Food Stamp Stores.csv"


def normalize_column_names(columns: Iterable[str]) -> list[str]:
    def _clean(name: str) -> str:
        if name is None:
            return ""
        # Replace newlines and repeated whitespace with single spaces, strip surrounding spaces
        name = re.sub(r"\s+", " ", str(name).replace("\r", " ").replace("\n", " ")).strip()
        return name

    return [_clean(c) for c in columns]


def normalize_text(value: Optional[str]) -> str:
    if pd.isna(value):
        return ""
    # Upper, remove punctuation except alphanumerics and spaces, collapse whitespace
    text = str(value).upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def to_zip5(value: Optional[str]) -> Optional[str]:
    if pd.isna(value):
        return None
    # Keep only digits, then left-pad to 5 if shorter; if empty after cleaning, return None
    digits = re.sub(r"[^0-9]", "", str(value))
    if digits == "":
        return None
    # Some rows may include Zip+4; keep first 5
    digits = digits[:5]
    if len(digits) < 5:
        digits = digits.zfill(5)
    return digits


def read_nyc_zip_codes(path: str) -> Set[str]:
    df = pd.read_csv(path, dtype=str, keep_default_na=True)
    df.columns = normalize_column_names(df.columns)
    # Expected column is "ZipCode"; fall back to best-effort detection
    zip_col_candidates = [
        "ZipCode",
        "Zip Code",
        "ZIP",
        "Zip",
    ]
    zip_col = next((c for c in zip_col_candidates if c in df.columns), None)
    if zip_col is None:
        # Fallback: any column containing "zip"
        zip_col = next((c for c in df.columns if "zip" in c.lower()), None)
    if zip_col is None:
        raise ValueError("Could not find a ZIP code column in NYC Zip Codes CSV")

    zip5 = df[zip_col].map(to_zip5)
    nyc_zip_set = set(z for z in zip5.dropna().tolist() if len(z) == 5)
    return nyc_zip_set


def read_all_stores(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=True, low_memory=False)
    df.columns = normalize_column_names(df.columns)

    # Standardize essential fields
    state_col = next((c for c in ["State"] if c in df.columns), None)
    zip_col = next((c for c in ["Zip_Code", "Zip Code", "ZIP", "Zip"] if c in df.columns), None)
    name_col = next((c for c in ["Store_Name", "Store Name", "Name"] if c in df.columns), None)
    addr_col = next((c for c in ["Store_Street_Address", "Store Street Address", "Street Address", "Address"] if c in df.columns), None)

    missing = [n for n, c in {
        "state": state_col,
        "zip": zip_col,
        "store name": name_col,
        "street address": addr_col,
    }.items() if c is None]
    if missing:
        raise ValueError(f"All Stores CSV is missing required columns: {', '.join(missing)}")

    df["zip5"] = df[zip_col].map(to_zip5)
    df["state_norm"] = df[state_col].fillna("").str.upper().str.strip()
    df["store_name_key"] = df[name_col].map(normalize_text)
    df["address_key"] = df[addr_col].map(normalize_text)
    return df


def read_healthy_stores(path: str) -> pd.DataFrame:
    # Use python engine to be tolerant of embedded newlines in header; dtype=str to preserve values
    df = pd.read_csv(path, dtype=str, keep_default_na=True, engine="python")
    df.columns = normalize_column_names(df.columns)

    # Expected columns (after normalization): "Store Name", "Street Address", "Borough", "Zip Code"
    name_col = next((c for c in ["Store Name", "Store_Name", "Name"] if c in df.columns), None)
    addr_col = next((c for c in ["Street Address", "Address", "Store Street Address"] if c in df.columns), None)
    zip_col = next((c for c in ["Zip Code", "ZipCode", "Zip", "ZIP"] if c in df.columns), None)

    missing = [n for n, c in {
        "store name": name_col,
        "street address": addr_col,
        "zip code": zip_col,
    }.items() if c is None]
    if missing:
        raise ValueError(f"Healthy Stores CSV is missing required columns: {', '.join(missing)}")

    df["zip5"] = df[zip_col].map(to_zip5)
    df["store_name_key"] = df[name_col].map(normalize_text)
    df["address_key"] = df[addr_col].map(normalize_text)
    # Keep only rows with valid zip
    df = df[df["zip5"].notna()]
    return df[["zip5", "store_name_key", "address_key"]]


def build_healthy_key_sets(df_healthy: pd.DataFrame) -> Tuple[Set[Tuple[str, str]], Set[Tuple[str, str]]]:
    by_name = set(zip(df_healthy["zip5"].tolist(), df_healthy["store_name_key"].tolist()))
    by_addr = set(zip(df_healthy["zip5"].tolist(), df_healthy["address_key"].tolist()))
    return by_name, by_addr


def flag_healthy(df_nyc: pd.DataFrame, healthy_by_name: Set[Tuple[str, str]], healthy_by_addr: Set[Tuple[str, str]]) -> pd.DataFrame:
    def _is_healthy(row: pd.Series) -> bool:
        z = row.get("zip5")
        if not isinstance(z, str) or len(z) != 5:
            return False
        name_key = row.get("store_name_key", "")
        addr_key = row.get("address_key", "")
        return (z, name_key) in healthy_by_name or (z, addr_key) in healthy_by_addr

    df_nyc["Is_Healthy_Store"] = df_nyc.apply(_is_healthy, axis=1)
    return df_nyc


 

def main() -> None:
    nyc_zips = read_nyc_zip_codes(NYC_ZIPS_CSV)

    all_stores = read_all_stores(ALL_STORES_CSV)
    helper_cols = {"zip5", "state_norm", "store_name_key", "address_key"}
    original_store_cols = [c for c in all_stores.columns if c not in helper_cols]
    # NYC filter: valid ZIP in NYC set and state is NY
    is_nyc_zip = all_stores["zip5"].isin(nyc_zips)
    is_state_ny = all_stores["state_norm"] == "NY"
    nyc_stores = all_stores[is_nyc_zip & is_state_ny].copy()
    # Drop rows without an address or store name after normalization
    nyc_stores = nyc_stores[(nyc_stores["store_name_key"] != "") & (nyc_stores["address_key"] != "")]

    healthy = read_healthy_stores(HEALTHY_STORES_CSV)
    healthy_by_name, healthy_by_addr = build_healthy_key_sets(healthy)

    nyc_stores_flagged = flag_healthy(nyc_stores, healthy_by_name, healthy_by_addr)

    # Write output with ONLY original columns plus the flag; stable column order
    output_columns = original_store_cols + ["Is_Healthy_Store"]
    nyc_stores_flagged.to_csv(OUTPUT_NYC_STORES_CSV, index=False, columns=output_columns)


if __name__ == "__main__":
    main()
