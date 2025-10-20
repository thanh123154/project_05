#!/usr/bin/env python3
"""
Data Filtering Script for Crawler3.py
Filters and merges data from 'summary' collection.
"""

import sys
import csv
import json
import time
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from tqdm import tqdm
import pymongo

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "countly"
SUMMARY_COLLECTION = "summary"

# Output files
MERGED_JSON = "tld_grouped.json"
MERGED_CSV = "merged_data.csv"
URLS_JSONL = "product_urls.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class ProductUrlRecord:
    product_id: str
    url: str
    source_collection: str
    timestamp: Optional[int] = None


def get_mongo_client() -> pymongo.MongoClient:
    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def get_summary_data() -> List[ProductUrlRecord]:
    """Extract all needed data from summary collection."""
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[SUMMARY_COLLECTION]

    all_records = []

    # Các collection con cần xử lý
    target_collections = [
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed",
        "product_view_all_recommend_clicked"
    ]

    for name in target_collections:
        logger.info(f"🔍 Filtering data for '{name}' ...")

        pipeline = [
            {"$match": {
                "collection": name,
                "$or": [
                    {"product_id": {"$exists": True, "$ne": None, "$type": "string"}},
                    {"viewing_product_id": {"$exists": True, "$ne": None, "$type": "string"}}
                ],
                "$or": [
                    {"current_url": {"$exists": True, "$ne": None, "$type": "string"}},
                    {"referrer_url": {"$exists": True, "$ne": None, "$type": "string"}}
                ]
            }},
            {"$project": {
                "product_id": {"$ifNull": ["$product_id", "$viewing_product_id"]},
                "url": {"$ifNull": ["$current_url", "$referrer_url"]},
                "time_stamp": 1,
                "collection": 1
            }}
        ]

        try:
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            count = 0

            for doc in tqdm(cursor, desc=f"Processing {name}"):
                record = ProductUrlRecord(
                    product_id=str(doc["product_id"]),
                    url=doc["url"],
                    source_collection=name,
                    timestamp=doc.get("time_stamp")
                )
                all_records.append(record)
                count += 1

            logger.info(f"✅ {name}: {count} records found")

        except Exception as e:
            logger.error(f"❌ Error processing {name}: {e}")

    logger.info(f"✅ Total extracted records: {len(all_records)}")
    return all_records


def deduplicate_records(records: List[ProductUrlRecord]) -> List[ProductUrlRecord]:
    seen = set()
    deduped = []
    for record in records:
        key = (record.product_id, record.url)
        if key not in seen:
            seen.add(key)
            deduped.append(record)
    logger.info(f"✅ Deduplicated {len(records)} → {len(deduped)} records")
    return deduped


def group_by_product_id(records: List[ProductUrlRecord]) -> Dict[str, Dict]:
    grouped = {}
    for record in records:
        pid = record.product_id
        grouped.setdefault(pid, {"product_id": pid, "current_url": {}})

        from urllib.parse import urlparse
        try:
            domain = urlparse(record.url).netloc.lower()
            tld = domain.split('.')[-1]
            country = tld if tld in ['fr', 'de', 'es', 'it', 'com', 'co.uk', 'nl', 'be'] else 'other'
        except Exception:
            country = 'other'

        grouped[pid]["current_url"].setdefault(country, []).append(record.url)

    logger.info(f"✅ Grouped {len(records)} records into {len(grouped)} products")
    return grouped


def save_to_json(grouped_data: Dict[str, Dict]):
    with open(MERGED_JSON, 'w', encoding='utf-8') as f:
        json.dump(list(grouped_data.values()), f, ensure_ascii=False, indent=2)
    logger.info(f"💾 Saved {len(grouped_data)} products → {MERGED_JSON}")


def save_to_csv(records: List[ProductUrlRecord]):
    if not records:
        logger.warning("⚠️ No data to save")
        return
    with open(MERGED_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "url", "source_collection", "timestamp"])
        writer.writeheader()
        for r in records:
            writer.writerow(vars(r))
    logger.info(f"💾 Saved {len(records)} records → {MERGED_CSV}")


def save_to_jsonl(records: List[ProductUrlRecord]):
    with open(URLS_JSONL, 'w', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps({
                "product_id": r.product_id,
                "url": r.url,
                "source_collection": r.source_collection
            }, ensure_ascii=False) + "\n")
    logger.info(f"💾 Saved {len(records)} URLs → {URLS_JSONL}")


def main():
    start = time.time()

    try:
        client = get_mongo_client()
        client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        sys.exit(1)

    records = get_summary_data()
    deduped = deduplicate_records(records)
    grouped = group_by_product_id(deduped)

    save_to_json(grouped)
    save_to_csv(deduped)
    save_to_jsonl(deduped)

    logger.info(f"🎉 Done in {time.time() - start:.2f}s — {len(deduped)} unique records, {len(grouped)} products")


if __name__ == "__main__":
    main()
