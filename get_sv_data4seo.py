"""
Author: Antonio Blagojevic  
Medium: https://blog.antonioblago.com/tired-of-keyword-planner-limitations-ee2b007dfa56
Script: get_sv_data4seo.py  
Description: Automated keyword search volume collection using DataForSEO API  
Version: 1.0  
Date: 2025-07-13
"""

import os
import time
import json
import base64
import pandas as pd
import requests
import pickle
import re
import unicodedata
from dotenv import load_dotenv

load_dotenv()

# === Config ===
API_URL = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"
PICKLE_BACKUP = "backup_results.pkl"
PROGRESS_FILE = "progress.txt"
BATCH_SIZE = 1000
LOCATION_CODE = 2276
LANGUAGE_CODE = "de"
MAX_RETRIES = 3
RETRY_DELAY = 3
EXCEL_FILE = "keywords.xlsx"

AUTH_HEADER = os.getenv("d4seo")  # base64 encoded user:password

FORBIDDEN_CHARS = set("™©®✓•→←≠∞¿¡§¶…")


# === Utility Functions ===

def clean_keyword_simple(kw: str) -> str:
    for char in FORBIDDEN_CHARS:
        kw = kw.replace(char, "")
    return kw.strip()


def is_valid_keyword(kw: str) -> bool:
    try:
        kw = kw.strip()
        if not kw:
            return False

        # Remove forbidden characters
        for char in FORBIDDEN_CHARS:
            kw = kw.replace(char, "")

        kw_norm = unicodedata.normalize("NFKC", kw)
        kw_cleaned = ''.join(c for c in kw_norm if unicodedata.category(c)[0] != 'C')

        # Check for non-standard characters
        for char in kw_cleaned:
            try:
                name = unicodedata.name(char)
                if not any(part in name for part in ["LATIN", "DIGIT", "SPACE", "DASH", "HYPHEN"]):
                    return False
            except ValueError:
                return False

        # Invalid character patterns
        if re.search(r"[ãåÃÂâ€œ€]", kw_cleaned):
            return False

        # Character ratio
        valid_chars = re.findall(r"[a-zA-Z0-9äöüßéèêáàâíìîóòôúùûčšžăîâăëç\- ]", kw_cleaned.lower())
        ratio = len(valid_chars) / max(len(kw_cleaned), 1)
        if ratio < 0.7:
            return False

        if len(re.findall(r"[a-zA-Z]", kw_cleaned)) < 3:
            return False

        return True

    except Exception:
        return False


def filter_keywords(keywords: list[str], log_file="invalid_keywords.txt") -> list[str]:
    filtered = []
    invalid = []

    for kw in keywords:
        kw_clean = kw.strip().lower()
        if len(kw_clean) <= 80 and len(kw_clean.split()) <= 10 and is_valid_keyword(kw_clean):
            filtered.append(kw_clean)
        else:
            invalid.append(kw.strip())

    if invalid:
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("Invalid keywords:\n\n")
            for kw in invalid:
                f.write(f"{kw}\n")
        print(f"{len(invalid)} invalid keywords written to {log_file}")

    return filtered


def load_keywords_from_excel(filepath: str) -> list:
    df = pd.read_excel(filepath)
    return df["keyword"].dropna().astype(str).tolist()


def batch_keywords(keywords: list, size: int) -> list:
    return [keywords[i:i + size] for i in range(0, len(keywords), size)]


def send_batch_request(keywords_batch: list, date_from="2021-06-01") -> dict:
    payload = [{
        "keywords": keywords_batch,
        "location_code": LOCATION_CODE,
        "language_code": LANGUAGE_CODE,
        "sort_by": "relevance",
        "date_from": date_from,
        "search_partners": True,
        "include_adult_keywords": False
    }]
    headers = {
        'Authorization': f'Basic {AUTH_HEADER}',
        'Content-Type': 'application/json'
    }
    response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"HTTP {response.status_code}: {response.text}")


def parse_response(response_json: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    results, monthly_results = [], []

    tasks = response_json.get("tasks", [])
    for task in tasks:
        items = task.get("result", [])
        for item in items:
            keyword = item.get("keyword")
            results.append({
                "keyword": keyword,
                "search_volume": item.get("search_volume"),
                "competition": item.get("competition"),
                "cpc": item.get("cpc"),
                "currency": item.get("currency")
            })

            for ms in item.get("monthly_searches", []):
                monthly_results.append({
                    "keyword": keyword,
                    "year": ms.get("year"),
                    "month": ms.get("month"),
                    "search_volume": ms.get("search_volume")
                })

    return pd.DataFrame(results), pd.DataFrame(monthly_results)


def save_progress(index: int):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(index))


def load_progress() -> int:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return int(f.read())
    return 0


def load_existing_results():
    if os.path.exists(PICKLE_BACKUP):
        data = pd.read_pickle(PICKLE_BACKUP)
        return (
            data.get("results", pd.DataFrame()),
            data.get("monthly", pd.DataFrame()),
            data.get("api_costs", pd.DataFrame()),
            data.get("status", {}).get("progress_index", 0),
            data.get("status", {}).get("total_cost", 0.0)
        )
    else:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0, 0.0


def save_all_to_pickle(results_df, monthly_df, api_costs_df, progress_index, total_api_cost):
    data = {
        "results": results_df,
        "monthly": monthly_df,
        "api_costs": api_costs_df,
        "status": {
            "progress_index": progress_index,
            "total_cost": total_api_cost
        }
    }
    pd.to_pickle(data, PICKLE_BACKUP)


# === Main ===

def main():
    print("Loading keywords from Excel...")
    all_keywords = load_keywords_from_excel(EXCEL_FILE)
    all_keywords = [clean_keyword_simple(kw) for kw in all_keywords]
    all_keywords = filter_keywords(all_keywords)
    keyword_batches = batch_keywords(all_keywords, BATCH_SIZE)
    start_index = load_progress()

    results_df, monthly_df, api_costs_df, _, total_api_cost = load_existing_results()
    api_costs_list = []

    print(f"{len(all_keywords)} keywords loaded. Starting at batch {start_index + 1}/{len(keyword_batches)}.")

    for idx in range(start_index, len(keyword_batches)):
        batch = keyword_batches[idx]
        print(f"\nProcessing batch {idx + 1}/{len(keyword_batches)} ({len(batch)} keywords)...")

        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response_json = send_batch_request(batch)
                time.sleep(1)

                # Cost logging
                batch_cost = response_json.get("cost", 0.0)
                total_api_cost += batch_cost
                api_costs_list.append({
                    "batch_index": idx + 1,
                    "keyword_count": len(batch),
                    "cost_usd": batch_cost
                })
                print(f"Batch cost: {batch_cost:.3f} USD")

                df_keywords, df_monthly = parse_response(response_json)
                results_df = pd.concat([results_df, df_keywords], ignore_index=True)
                monthly_df = pd.concat([monthly_df, df_monthly], ignore_index=True)

                save_all_to_pickle(results_df, monthly_df, api_costs_df, idx + 1, total_api_cost)
                save_progress(idx + 1)
                success = True
                break
            except Exception as e:
                print(f"Error in batch {idx + 1} (try {attempt}): {e}")
                if attempt < MAX_RETRIES:
                    print(f"Retrying in {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                else:
                    print("Max retries reached. Stopping script.")
                    return

        if success:
            time.sleep(1.5)

    print(f"\nTotal API cost: {total_api_cost:.3f} USD")

    api_costs_df = pd.DataFrame(api_costs_list)
    api_costs_df.loc[len(api_costs_df.index)] = {
        "batch_index": "Total",
        "keyword_count": api_costs_df["keyword_count"].sum(),
        "cost_usd": total_api_cost
    }

    with pd.ExcelWriter("final_results.xlsx") as writer:
        results_df.to_excel(writer, sheet_name="Keyword Data", index=False)
        monthly_df.to_excel(writer, sheet_name="Monthly Volume", index=False)
        api_costs_df.to_excel(writer, sheet_name="API Costs", index=False)

    print("✅ Export completed: final_results.xlsx")


if __name__ == "__main__":
    main()
