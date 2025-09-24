import sys
import csv
import json
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
import pymongo
import os

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://myUserAdmin:Cunmiu123@127.0.0.1:27017/?authSource=admin"
DB_NAME = "glamira"
SOURCE_COLLECTION = "summary"

TARGET_EVENT_TYPES = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed",
    "product_view_all_recommend_clicked",
]

DEFAULT_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 1.5
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
]

MAX_WORKERS = 30
BATCH_SIZE = 1000   # số lượng product xử lý 1 lần
URLS_PER_PRODUCT = 20

CANDIDATES_JSONL = "product_name_candidates.jsonl"
FINAL_CSV = "product_names_final.csv"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, desc=None, total=None):
        for i, item in enumerate(iterable):
            yield item


@dataclass
class UrlRecord:
    product_id: str
    url: str
    source_collection: str


def _get_mongo_client() -> pymongo.MongoClient:
    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    return str(value).strip() or None


def stream_product_ids(batch_size=1000):
    """Stream product IDs theo batch để tránh tràn RAM"""
    col = _get_mongo_client()[DB_NAME][SOURCE_COLLECTION]

    pipeline = [
        {"$match": {
            "collection": {"$in": TARGET_EVENT_TYPES},
            "$or": [
                {"product_id": {"$exists": True, "$ne": None, "$type": "string"}},
                {"viewing_product_id": {"$exists": True, "$ne": None, "$type": "string"}}
            ]
        }},
        {"$project": {
            "pid": {"$ifNull": ["$product_id", "$viewing_product_id"]}}},
        {"$group": {"_id": "$pid"}},
        {"$sort": {"_id": 1}},
    ]

    cursor = col.aggregate(pipeline, allowDiskUse=True)

    batch = []
    for doc in cursor:
        batch.append(doc["_id"])
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def get_urls_for_product(product_id: str, limit: int = 10) -> List[UrlRecord]:
    col = _get_mongo_client()[DB_NAME][SOURCE_COLLECTION]
    query = {
        "$or": [{"product_id": product_id}, {"viewing_product_id": product_id}],
        "collection": {"$in": TARGET_EVENT_TYPES},
    }
    url_records, seen_urls = [], set()
    for doc in col.find(query, {"current_url": 1, "referrer_url": 1, "collection": 1}).limit(limit * 2):
        collection = doc.get("collection")
        url = _safe_str(doc.get("referrer_url") if collection ==
                        "product_view_all_recommend_clicked" else doc.get("current_url"))
        if url and url not in seen_urls:
            url_records.append(UrlRecord(product_id, url, collection))
            seen_urls.add(url)
            if len(url_records) >= limit:
                break
    return url_records


def http_get(url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)},
                                timeout=DEFAULT_TIMEOUT_SECONDS, allow_redirects=True)
            if resp.status_code == 200:
                return resp.text
        except requests.RequestException:
            pass
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
    return None


def extract_product_name(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for sel in ["h1.page-title span.base", "h1.product-title", "h1.product-name", "h1"]:
        el = soup.select_one(sel)
        if el:
            return el.get_text(strip=True)
    return None


def process_single_url(record: UrlRecord) -> Dict:
    html = http_get(record.url)
    return {
        "product_id": record.product_id,
        "url": record.url,
        "source_collection": record.source_collection,
        "product_name": extract_product_name(html) if html else None,
        "fetched_at": int(time.time()),
    }


def crawl_product_names_parallel(records: List[UrlRecord]) -> List[Dict]:
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for future in as_completed(executor.submit(process_single_url, r) for r in records):
            try:
                results.append(future.result())
            except Exception as e:
                logger.warning(f"Error: {e}")
    return results


def deduplicate_by_product_id(candidates: List[Dict]) -> List[Dict]:
    deduped = {}
    for row in candidates:
        pid = row["product_id"]
        if pid not in deduped or (deduped[pid].get("product_name") is None and row.get("product_name")):
            deduped[pid] = row
    return list(deduped.values())


def append_candidates_jsonl(candidates: List[Dict], path: str = CANDIDATES_JSONL) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for row in candidates:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_final_csv(candidates: List[Dict], path: str = FINAL_CSV) -> None:
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
                                "product_id", "product_name", "url", "source_collection", "fetched_at"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(candidates)


def main():
    total_products = 0
    for batch in stream_product_ids(batch_size=BATCH_SIZE):
        logger.info(f"🔄 Processing batch with {len(batch)} product IDs...")
        url_records = []
        for pid in batch:
            url_records.extend(get_urls_for_product(
                pid, limit=URLS_PER_PRODUCT))

        candidates = crawl_product_names_parallel(url_records)
        deduped = deduplicate_by_product_id(candidates)

        append_candidates_jsonl(deduped)
        append_final_csv(deduped)

        total_products += len(deduped)
        logger.info(
            f"✅ Finished batch. Total processed so far: {total_products}")


if __name__ == "__main__":
    main()
