#!/usr/bin/env python3
"""
Data Filtering Script for Crawler3.py
Filters and merges data from user behavior collections and recommendation collection.
"""

import sys
import csv
import json
import time
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import pymongo
from tqdm import tqdm

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "countly"

# User behavior collections
BEHAVIOR_COLLECTIONS = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed"
]

# Recommendation collection
RECOMMENDATION_COLLECTION = "product_view_all_recommend_clicked"

# Output files
MERGED_JSON = "tld_grouped.json"
MERGED_CSV = "merged_data.csv"
URLS_JSONL = "product_urls.jsonl"

# Processing settings
BATCH_SIZE = 10000
MAX_WORKERS = 10

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
    """Get MongoDB client."""
    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def get_behavior_data() -> List[ProductUrlRecord]:
    """Extract data from user behavior collections."""
    client = get_mongo_client()
    db = client[DB_NAME]

    all_records = []

    for collection_name in BEHAVIOR_COLLECTIONS:
        logger.info(f"🔍 Processing collection: {collection_name}")
        collection = db[collection_name]

        # Query to get product_id/viewing_product_id and current_url
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"product_id": {"$exists": True,
                                        "$ne": None, "$type": "string"}},
                        {"viewing_product_id": {"$exists": True,
                                                "$ne": None, "$type": "string"}}
                    ],
                    "current_url": {"$exists": True, "$ne": None, "$type": "string"}
                }
            },
            {
                "$project": {
                    "product_id": {
                        "$ifNull": ["$product_id", "$viewing_product_id"]
                    },
                    "current_url": 1,
                    "time_stamp": 1
                }
            },
            {
                "$match": {
                    "product_id": {"$ne": None, "$ne": ""},
                    "current_url": {"$ne": None, "$ne": ""}
                }
            }
        ]

        try:
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            count = 0

            for doc in tqdm(cursor, desc=f"Processing {collection_name}"):
                record = ProductUrlRecord(
                    product_id=str(doc["product_id"]),
                    url=doc["current_url"],
                    source_collection=collection_name,
                    timestamp=doc.get("time_stamp")
                )
                all_records.append(record)
                count += 1

            logger.info(f"✅ Found {count} records in {collection_name}")

        except Exception as e:
            logger.error(f"❌ Error processing {collection_name}: {e}")
            continue

    logger.info(f"✅ Total behavior records: {len(all_records)}")
    return all_records


def get_recommendation_data() -> List[ProductUrlRecord]:
    """Extract data from recommendation collection."""
    client = get_mongo_client()
    collection = client[DB_NAME][RECOMMENDATION_COLLECTION]

    logger.info(f"🔍 Processing collection: {RECOMMENDATION_COLLECTION}")

    pipeline = [
        {
            "$match": {
                "viewing_product_id": {"$exists": True, "$ne": None, "$type": "string"},
                "referrer_url": {"$exists": True, "$ne": None, "$type": "string"}
            }
        },
        {
            "$project": {
                "viewing_product_id": 1,
                "referrer_url": 1,
                "time_stamp": 1
            }
        },
        {
            "$match": {
                "viewing_product_id": {"$ne": None, "$ne": ""},
                "referrer_url": {"$ne": None, "$ne": ""}
            }
        }
    ]

    records = []
    try:
        cursor = collection.aggregate(pipeline, allowDiskUse=True)

        for doc in tqdm(cursor, desc=f"Processing {RECOMMENDATION_COLLECTION}"):
            record = ProductUrlRecord(
                product_id=str(doc["viewing_product_id"]),
                url=doc["referrer_url"],
                source_collection=RECOMMENDATION_COLLECTION,
                timestamp=doc.get("time_stamp")
            )
            records.append(record)

        logger.info(
            f"✅ Found {len(records)} records in {RECOMMENDATION_COLLECTION}")

    except Exception as e:
        logger.error(f"❌ Error processing {RECOMMENDATION_COLLECTION}: {e}")

    return records


def deduplicate_records(records: List[ProductUrlRecord]) -> List[ProductUrlRecord]:
    """Remove duplicate records based on product_id and url."""
    seen = set()
    deduped = []

    for record in records:
        key = (record.product_id, record.url)
        if key not in seen:
            seen.add(key)
            deduped.append(record)

    logger.info(f"✅ Deduplicated: {len(records)} -> {len(deduped)} records")
    return deduped


def group_by_product_id(records: List[ProductUrlRecord]) -> Dict[str, Dict]:
    """Group records by product_id for tld_grouped.json format."""
    grouped = {}

    for record in records:
        product_id = record.product_id

        if product_id not in grouped:
            grouped[product_id] = {
                "product_id": product_id,
                "current_url": {}
            }

        # Group by country code (extract from URL domain)
        try:
            from urllib.parse import urlparse
            parsed = urlparse(record.url)
            domain = parsed.netloc.lower()

            # Extract country code from domain (e.g., .fr, .de, .com)
            if '.' in domain:
                tld = domain.split('.')[-1]
                if tld in ['fr', 'de', 'es', 'it', 'com', 'co.uk', 'nl', 'be']:
                    country = tld
                else:
                    country = 'other'
            else:
                country = 'other'

        except Exception:
            country = 'other'

        if country not in grouped[product_id]["current_url"]:
            grouped[product_id]["current_url"][country] = []

        grouped[product_id]["current_url"][country].append(record.url)

    logger.info(
        f"✅ Grouped {len(records)} records into {len(grouped)} products")
    return grouped


def save_to_json(grouped_data: Dict[str, Dict], filename: str = MERGED_JSON):
    """Save grouped data to JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(list(grouped_data.values()), f, ensure_ascii=False, indent=2)

    logger.info(f"✅ Saved {len(grouped_data)} products to {filename}")


def save_to_csv(records: List[ProductUrlRecord], filename: str = MERGED_CSV):
    """Save records to CSV file."""
    if not records:
        logger.warning("⚠️ No data to save to CSV")
        return

    fieldnames = ['product_id', 'url', 'source_collection', 'timestamp']

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            writer.writerow({
                'product_id': record.product_id,
                'url': record.url,
                'source_collection': record.source_collection,
                'timestamp': record.timestamp
            })

    logger.info(f"✅ Saved {len(records)} records to {filename}")


def save_to_jsonl(records: List[ProductUrlRecord], filename: str = URLS_JSONL):
    """Save records to JSONL file for crawler3.py."""
    with open(filename, 'w', encoding='utf-8') as f:
        for record in records:
            row = {
                "product_id": record.product_id,
                "url": record.url,
                "source_collection": record.source_collection
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info(f"✅ Saved {len(records)} records to {filename}")


def main():
    """Main processing function."""
    start_time = time.time()

    # Test MongoDB connection
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
    except Exception as e:
        logger.error(f"❌ Cannot connect to MongoDB: {e}")
        sys.exit(1)

    # Get data from behavior collections
    logger.info("📊 Extracting data from behavior collections...")
    behavior_records = get_behavior_data()

    # Get data from recommendation collection
    logger.info("📊 Extracting data from recommendation collection...")
    recommendation_records = get_recommendation_data()

    # Merge all records
    all_records = behavior_records + recommendation_records
    logger.info(f"📊 Total records before deduplication: {len(all_records)}")

    # Deduplicate
    deduped_records = deduplicate_records(all_records)

    # Group by product_id for tld_grouped.json format
    logger.info("📊 Grouping data by product_id...")
    grouped_data = group_by_product_id(deduped_records)

    # Save outputs
    logger.info("💾 Saving results...")
    save_to_json(grouped_data)
    save_to_csv(deduped_records)
    save_to_jsonl(deduped_records)

    end_time = time.time()
    logger.info(
        f"🎉 Processing completed in {end_time - start_time:.2f} seconds")
    logger.info(f"📈 Summary:")
    logger.info(f"   - Total unique records: {len(deduped_records)}")
    logger.info(f"   - Unique products: {len(grouped_data)}")
    logger.info(
        f"   - Output files: {MERGED_JSON}, {MERGED_CSV}, {URLS_JSONL}")


if __name__ == "__main__":
    main()
