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
    if "AI_Health_Score" not in df.columns:
        df["AI_Health_Score"] = np.nan
    if "AI_Health_Reason" not in df.columns:
        df["AI_Health_Reason"] = ""

    system = (
        "You are rating food retail locations on a 1-10 healthiness scale for nutrition-"
        "conscious SNAP users. "
        "Consider: general menu health; "
        "Consider: presence of fresh produce/whole foods vs ultra-processed; "
        "Consider: price of the foods at that retailer, economical vs expensive; "
        "Consider: food retailers that may be manipulative with portion distribution and menu items; "
        "store type (grocery > super store > convenience > restaurant meals), "
        "name signals are important. Be conservative; 10 = excellent access to healthy foods, "
        "1 = very poor. Output strict JSON: {\"score\": <integer 1-10>, \"reason\": \"short reason\"}."
    )

    iterable = df.iterrows()
    if isinstance(limit, int) and limit > 0:
        iterable = list(df.head(limit).iterrows())

    for i, row in iterable:
        if pd.notna(row.get("AI_Health_Score")) and str(row.get("AI_Health_Score")) != "":
            continue

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
            score = j.get("score")
            if isHealthy:
                score += IS_HEALTHY_BONUS
            reason = j.get("reason", "")
            if isinstance(score, (int, float)):
                score_int = int(
                    max(
                        1, 
                        min(
                            10, 
                            round(score)
                            )
                        )
                    )
                df.at[i, "AI_Health_Score"] = score_int
                df.at[i, "AI_Health_Reason"] = str(reason)[:240]
            else:
                df.at[i, "AI_Health_Score"] = 5
                df.at[i, "AI_Health_Reason"] = "No AI reason provided"
        except Exception:
            df.at[i, "AI_Health_Score"] = 5
            df.at[i, "AI_Health_Reason"] = "AI rating unavailable"

        time.sleep(delay_seconds)

    return df


# Simple configuration (edit these values)
INPUT_CSV = "NYC Food Stamp Stores.csv"            # Source CSV to rate
OUTPUT_CSV = "NYC Food Stamp Stores.csv"                 # Destination CSV
OPENAI_API_KEY = ""                                # Paste your API key here (or leave blank to load from api_key.txt)
MAX_NUM_STORES = 0                                    # 0 = rate all rows; otherwise only first N rows
AI_RATE_DELAY = 0.15                                # seconds between API calls
IS_HEALTHY_BONUS = 3                                # number added to score if healthy


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

if "AI_Health_Score" not in rated.columns:
    rated["AI_Health_Score"] = np.nan
if "AI_Health_Reason" not in rated.columns:
    rated["AI_Health_Reason"] = ""

rated.to_csv(OUTPUT_CSV, index=False)
print(f"Wrote AI ratings to: {OUTPUT_CSV}")