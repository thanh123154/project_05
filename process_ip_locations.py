#!/usr/bin/env python3
"""
IP Location Processing Script (No Authorization)
Processes unique IPs from MongoDB collection and enriches with location data using IP2Location.
"""

import sys
import csv
import json
import time
import logging
from dataclasses import dataclass
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pymongo
from IP2Location import IP2Location

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://127.0.0.1:27017/"  # ⚠️ No username/password
DB_NAME = "countly"
SOURCE_COLLECTION = "summary"
TARGET_COLLECTION = "ip_locations"
IP2LOCATION_DB_PATH = "IP2LOCATION-LITE-DB1.BIN"

# Output files
CSV_OUTPUT = "ip_locations.csv"
JSONL_OUTPUT = "ip_locations.jsonl"

# Processing settings
BATCH_SIZE = 1000
MAX_WORKERS = 10
LOG_EVERY = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, desc=None, total=None):
        for item in iterable:
            yield item


@dataclass
class IPLocationData:
    ip: str
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    region_name: Optional[str] = None
    city_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    processed_at: Optional[int] = None


class IPLocationProcessor:
    def __init__(self, db_path: str = IP2LOCATION_DB_PATH):
        try:
            self.ip2location = IP2Location(db_path)
            logger.info(f"✅ IP2Location database loaded from {db_path}")
        except Exception as e:
            logger.error(f"❌ Failed to load IP2Location DB: {e}")
            sys.exit(1)

    def get_location_data(self, ip: str) -> IPLocationData:
        """Get location data for a single IP."""
        try:
            result = self.ip2location.get_all(ip)
            return IPLocationData(
                ip=ip,
                country_code=result.country_short,
                country_name=result.country_long,
                region_name=result.region,
                city_name=result.city,
                latitude=float(result.latitude),
                longitude=float(result.longitude),
                processed_at=int(time.time())
            )
        except Exception as e:
            logger.warning(f"⚠️ Error processing IP {ip}: {e}")
            return IPLocationData(ip=ip, processed_at=int(time.time()))


def get_mongo_client() -> pymongo.MongoClient:
    """Connect to MongoDB without authentication."""
    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def get_unique_ips(limit: Optional[int] = None) -> List[str]:
    """Get unique IP addresses from MongoDB collection."""
    client = get_mongo_client()
    collection = client[DB_NAME][SOURCE_COLLECTION]

    pipeline = [
        {"$match": {"ip": {"$exists": True, "$ne": None, "$type": "string"}}},
        {"$group": {"_id": "$ip"}},
        {"$sort": {"_id": 1}}
    ]
    if limit:
        pipeline.append({"$limit": limit})

    ips = [doc["_id"]
           for doc in collection.aggregate(pipeline, allowDiskUse=True)]
    logger.info(f"✅ Found {len(ips)} unique IPs")
    return ips


def process_ip_batch(ips: List[str], processor: IPLocationProcessor) -> List[IPLocationData]:
    return [processor.get_location_data(ip) for ip in ips]


def process_ips_parallel(ips: List[str], processor: IPLocationProcessor) -> List[IPLocationData]:
    all_results = []
    total = len(ips)
    batches = [ips[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_ip_batch, b, processor)
                   for b in batches]
        for i, f in enumerate(as_completed(futures), start=1):
            try:
                all_results.extend(f.result())
                if i % LOG_EVERY == 0 or i == len(futures):
                    logger.info(f"⏳ Progress: {i}/{len(futures)} batches done")
            except Exception as e:
                logger.error(f"❌ Error in batch: {e}")
    logger.info(f"✅ Finished processing {len(all_results)} IPs")
    return all_results


def save_to_csv(data: List[IPLocationData]):
    if not data:
        logger.warning("⚠️ No data to save to CSV")
        return

    fieldnames = ['ip', 'country_code', 'country_name', 'region_name', 'city_name',
                  'latitude', 'longitude', 'processed_at']
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in data:
            writer.writerow(d.__dict__)
    logger.info(f"✅ Saved {len(data)} records to {CSV_OUTPUT}")


def save_to_jsonl(data: List[IPLocationData]):
    if not data:
        logger.warning("⚠️ No data to save to JSONL")
        return

    with open(JSONL_OUTPUT, 'w', encoding='utf-8') as f:
        for d in data:
            f.write(json.dumps(d.__dict__, ensure_ascii=False) + "\n")
    logger.info(f"✅ Saved {len(data)} records to {JSONL_OUTPUT}")


def save_to_mongodb(data: List[IPLocationData]):
    if not data:
        logger.warning("⚠️ No data to save to MongoDB")
        return

    client = get_mongo_client()
    col = client[DB_NAME][TARGET_COLLECTION]
    docs = [d.__dict__ for d in data]

    try:
        result = col.insert_many(docs)
        logger.info(
            f"✅ Inserted {len(result.inserted_ids)} records into {TARGET_COLLECTION}")
        col.create_index("ip")
        logger.info("✅ Created index on 'ip'")
    except Exception as e:
        logger.error(f"❌ Error inserting to MongoDB: {e}")


def main():
    import os
    if not os.path.exists(IP2LOCATION_DB_PATH):
        logger.error(f"❌ Missing IP2Location DB file: {IP2LOCATION_DB_PATH}")
        sys.exit(1)

    try:
        client = get_mongo_client()
        client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
    except Exception as e:
        logger.error(f"❌ Cannot connect to MongoDB: {e}")
        sys.exit(1)

    processor = IPLocationProcessor()
    ips = get_unique_ips()
    if not ips:
        logger.warning("⚠️ No IPs found")
        sys.exit(1)

    data = process_ips_parallel(ips, processor)
    save_to_csv(data)
    save_to_jsonl(data)
    save_to_mongodb(data)
    logger.info("🎉 Done!")


if __name__ == "__main__":
    main()
