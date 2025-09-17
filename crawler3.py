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

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://myUserAdmin:Cunmiu123@127.0.0.1:27018/?authSource=admin"
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
CANDIDATES_JSONL = "product_name_candidates.jsonl"
FINAL_CSV = "product_names_final.csv"
URLS_JSONL = "product_urls.jsonl"  # File lưu URLs trước khi crawl
BATCH_SIZE = 10000
MAX_PRODUCTS = 1000

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


def get_unique_product_ids() -> List[str]:
    client = _get_mongo_client()
    col = client[DB_NAME][SOURCE_COLLECTION]

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
        # {"$limit": MAX_PRODUCTS}
    ]

    product_ids = [r["_id"]
                   for r in col.aggregate(pipeline, allowDiskUse=True)]
    logger.info(f"✅ Found {len(product_ids)} unique product IDs")
    return product_ids


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


def fetch_unique_product_urls() -> List[UrlRecord]:
    product_ids = get_unique_product_ids()
    all_records = []
    for pid in tqdm(product_ids, desc="Fetching URLs"):
        all_records.extend(get_urls_for_product(pid, limit=20))
    logger.info(f"✅ Collected {len(all_records)} URL records")
    return all_records


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


def write_urls_jsonl(url_records: List[UrlRecord], path: str = URLS_JSONL) -> None:
    """Lưu tất cả URL records ra file JSONL trước khi crawl"""
    with open(path, "w", encoding="utf-8") as f:
        for record in url_records:
            row = {
                "product_id": record.product_id,
                "url": record.url,
                "source_collection": record.source_collection
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    logger.info(f"✅ Saved {len(url_records)} URL records to {path}")


def load_urls_from_jsonl(path: str = URLS_JSONL) -> List[UrlRecord]:
    """Load URL records từ file JSONL"""
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line.strip())
                records.append(UrlRecord(
                    product_id=data["product_id"],
                    url=data["url"],
                    source_collection=data["source_collection"]
                ))
        logger.info(f"✅ Loaded {len(records)} URL records from {path}")
    except FileNotFoundError:
        logger.warning(f"⚠️ File {path} not found")
    except Exception as e:
        logger.error(f"❌ Error loading URLs from {path}: {e}")
    return records


def write_candidates_jsonl(candidates: List[Dict], path: str = CANDIDATES_JSONL) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in candidates:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_final_csv(candidates: List[Dict], path: str = FINAL_CSV) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
                                "product_id", "product_name", "url", "source_collection", "fetched_at"])
        writer.writeheader()
        writer.writerows(candidates)


def main():
    import os

    # Kiểm tra xem file URLs đã tồn tại chưa
    if os.path.exists(URLS_JSONL):
        logger.info(f"📁 Found existing URLs file: {URLS_JSONL}")
        url_records = load_urls_from_jsonl()
        if not url_records:
            logger.info("🔄 URLs file empty, fetching from MongoDB...")
            try:
                _get_mongo_client().admin.command("ping")
                logger.info("✅ Connected to MongoDB")
            except Exception as e:
                logger.error(f"❌ Cannot connect to MongoDB: {e}")
                sys.exit(1)
            url_records = fetch_unique_product_urls()
            write_urls_jsonl(url_records)
    else:
        logger.info("🔄 No URLs file found, fetching from MongoDB...")
        try:
            _get_mongo_client().admin.command("ping")
            logger.info("✅ Connected to MongoDB")
        except Exception as e:
            logger.error(f"❌ Cannot connect to MongoDB: {e}")
            sys.exit(1)
        url_records = fetch_unique_product_urls()
        write_urls_jsonl(url_records)

    # Crawl product names từ URLs đã lưu
    logger.info("🕷️ Starting to crawl product names...")
    candidates = crawl_product_names_parallel(url_records)
    deduped = deduplicate_by_product_id(candidates)
    write_candidates_jsonl(deduped)
    write_final_csv(deduped)
    logger.info(f"✅ Done! Processed {len(deduped)} unique products")


if __name__ == "__main__":
    main()
