import json
import re
import time
from typing import Optional

import numpy as np
import pandas as pd
from urllib import request, error


def _openai_chat_json(api_key: str, system_prompt: str, user_prompt: str) -> dict:
    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    req = request.Request(
        url="https://api.openai.com/v1/chat/completions",
        data=json.dumps(data).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
    except error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = "{}"
        raise RuntimeError(f"OpenAI HTTP error: {e.code} {body}")
    except Exception as e:
        raise RuntimeError(f"OpenAI request failed: {e}")

    try:
        j = json.loads(body)
        content = j.get("choices", [{}])[0].get("message", {}).get("content", "")
        try:
            return json.loads(content)
        except Exception:
            m = re.search(r"\{[\s\S]*\}", content)
            if m:
                return json.loads(m.group(0))
            return {}
    except Exception:
        return {}


def _parse_bool(value) -> bool:
    try:
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return bool(int(value))
        s = str(value).strip().lower()
        return s in {"1", "true", "yes", "y", "t"}
    except Exception:
        return False


def rate_stores_with_ai(
    df: pd.DataFrame,
    api_key: str,
    delay_seconds: float = 0.15,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    total_stores = len(df)
    # Initialize health rating columns
    if "AI_Health_Score" not in df.columns:
        df["AI_Health_Score"] = np.nan
    if "AI_Health_Reason" not in df.columns:
        df["AI_Health_Reason"] = ""
    
    # Initialize economy/pricing rating columns
    if "AI_Economy_Score" not in df.columns:
        df["AI_Economy_Score"] = np.nan
    if "AI_Economy_Reason" not in df.columns:
        df["AI_Economy_Reason"] = ""

    system = (
        "You are rating food retail locations on two separate scales for SNAP users:\n\n"
        "1. HEALTH RATING (1-10 scale):\n"
        "- 10 = excellent access to healthy, fresh foods (abundant produce, whole foods, organic options)\n"
        "- 1 = very poor access to healthy foods (mostly processed, limited fresh options)\n"
        "Consider: fresh produce availability, whole foods vs ultra-processed foods, organic/natural options, store type hierarchy (grocery > super store > convenience > restaurant), store name indicators of health focus, variety of healthy choices\n\n"
        "2. ECONOMY/PRICING RATING (1-5 scale):\n"
        "- 1 = very affordable, excellent value for money (low prices, large portions, bulk options)\n"
        "- 5 = very expensive, poor value for money (high prices, small portions)\n"
        "Consider: price per serving, portion sizes, bulk purchase options, sales/discounts, SNAP benefit stretch, comparison to average market prices, value for money spent\n\n"
        "IMPORTANT: Provide specific, actionable reasons. Focus on concrete details like 'abundant fresh vegetables' or 'high prices for small portions' rather than vague statements.\n\n"
        "Output strict JSON: {\"health_score\": <integer 1-10>, \"health_reason\": \"specific reason (max 60 words)\", \"economy_score\": <integer 1-5>, \"economy_reason\": \"specific reason (max 60 words)\"}"
    )

    iterable = df.iterrows()
    if isinstance(limit, int) and limit > 0:
        iterable = list(df.head(limit).iterrows())

    for i, row in iterable:
        isHealthy = _parse_bool(row.get("Is_Healthy_Store"))

        payload = {
            "name": row.get("Store_Name", ""),
            "address": row.get("Store_Street_Address", ""),
            "city": row.get("City", ""),
            "zip": row.get("Zip_Code", ""),
            "county": row.get("County", ""),
            "storeType": row.get("Store_Type", ""),
        }

        user = f"Rate this store: {json.dumps(payload, ensure_ascii=False)}"

        try:
            j = _openai_chat_json(api_key, system, user)
            
            # Process health rating
            health_score = j.get("health_score")
            if isHealthy and isinstance(health_score, (int, float)):
                health_score += IS_HEALTHY_BONUS
            health_reason = j.get("health_reason", "")
            
            # Process economy rating
            economy_score = j.get("economy_score")
            if row.get("Store_Type") == "Restaurant Meals Program":
                economy_score += IS_RESTAURANT_BONUS
            elif row.get("Store_Type") == "Grocery Store":
                economy_score += IS_GROCERY_BONUS
            economy_reason = j.get("economy_reason", "")
            
            # Validate and set health score
            if isinstance(health_score, (int, float)):
                health_score_int = int(max(1, min(10, round(health_score))))
                df.at[i, "AI_Health_Score"] = health_score_int
                df.at[i, "AI_Health_Reason"] = str(health_reason)[:240]
            else:
                df.at[i, "AI_Health_Score"] = 5
                df.at[i, "AI_Health_Reason"] = "No AI health reason provided"
            
            # Validate and set economy score
            if isinstance(economy_score, (int, float)):
                economy_score_int = int(max(1, min(5, round(economy_score))))
                df.at[i, "AI_Economy_Score"] = economy_score_int
                df.at[i, "AI_Economy_Reason"] = str(economy_reason)[:240]
            else:
                df.at[i, "AI_Economy_Score"] = 3
                df.at[i, "AI_Economy_Reason"] = "No AI economy reason provided"
            
            print(f"Finished: {i} | {round((i/total_stores)*100, 2)}%")
                
        except Exception:
            df.at[i, "AI_Health_Score"] = 5
            df.at[i, "AI_Health_Reason"] = "AI health rating unavailable"
            df.at[i, "AI_Economy_Score"] = 3
            df.at[i, "AI_Economy_Reason"] = "AI economy rating unavailable"

        time.sleep(delay_seconds)

    return df

def print_score_distributions(rated):
    # Print score distributions
    print("\n" + "="*50)
    print("SCORE DISTRIBUTIONS")
    print("="*50)

    # Health Score Distribution (1-10)
    print("\nHEALTH SCORES (1-10 scale):")
    health_counts = rated["AI_Health_Score"].value_counts().sort_index()
    for score in range(1, 11):
        count = health_counts.get(score, 0)
        print(f"  Score {score}: {count} stores")

    # Economy Score Distribution (1-5)
    print("\nECONOMY SCORES (1-5 scale):")
    economy_counts = rated["AI_Economy_Score"].value_counts().sort_index()
    for score in range(1, 6):
        count = economy_counts.get(score, 0)
        print(f"  Score {score}: {count} stores")

    # Summary statistics
    print("\nSUMMARY STATISTICS:")
    print(f"  Total stores rated: {len(rated)}")
    print(f"  Average health score: {rated['AI_Health_Score'].mean():.2f}")
    print(f"  Average economy score: {rated['AI_Economy_Score'].mean():.2f}")
    print(f"  Health score range: {rated['AI_Health_Score'].min()} - {rated['AI_Health_Score'].max()}")
    print(f"  Economy score range: {rated['AI_Economy_Score'].min()} - {rated['AI_Economy_Score'].max()}")
    print("="*50)



# Simple configuration (edit these values)
INPUT_CSV = "NYC Food Stamp Stores.csv"            # Source CSV to rate
OUTPUT_CSV = "AI NYC Food Stamp Stores.csv"                 # Destination CSV
OPENAI_API_KEY = ""                                # Paste your API key here (or leave blank to load from api_key.txt)
MAX_NUM_STORES = 0                                    # 0 = rate all rows; otherwise only first N rows
AI_RATE_DELAY = 0.15                                # seconds between API calls
IS_HEALTHY_BONUS = 2                                # number added to health score if healthy
IS_GROCERY_BONUS = -1                                # number added to health score if grocery
IS_RESTAURANT_BONUS = 1                                # number added to health score if restaurant


if not OPENAI_API_KEY:
    try:
        with open("api_key.txt", "r", encoding="utf-8") as _fh:
            OPENAI_API_KEY = _fh.read().strip()
    except FileNotFoundError:
        raise SystemExit("API key not found. Set OPENAI_API_KEY in ai_rating.py or create api_key.txt with the key.")

print(f"Started AI Rating")

df = pd.read_csv(INPUT_CSV, dtype=str, keep_default_na=True, low_memory=False)
rated = rate_stores_with_ai(
    df,
    OPENAI_API_KEY,
    delay_seconds=AI_RATE_DELAY,
    limit=(MAX_NUM_STORES or None),
)

# Ensure all required columns exist
if "AI_Health_Score" not in rated.columns:
    rated["AI_Health_Score"] = np.nan
if "AI_Health_Reason" not in rated.columns:
    rated["AI_Health_Reason"] = ""
if "AI_Economy_Score" not in rated.columns:
    rated["AI_Economy_Score"] = np.nan
if "AI_Economy_Reason" not in rated.columns:
    rated["AI_Economy_Reason"] = ""

# Cast score columns to integers, handling NaN values
rated["AI_Health_Score"] = pd.to_numeric(rated["AI_Health_Score"], errors='coerce').fillna(0).astype(int)
rated["AI_Economy_Score"] = pd.to_numeric(rated["AI_Economy_Score"], errors='coerce').fillna(0).astype(int)

rated.to_csv(OUTPUT_CSV, index=False)
print(f"Wrote AI ratings to: {OUTPUT_CSV}")

print_score_distributions(rated)






