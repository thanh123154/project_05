#!/usr/bin/env python3
"""
Optimized IP Location Processing Script with Timing
Processes unique IPs from MongoDB and enriches with location data using IP2Location.
"""

import sys
import csv
import json
import time
import logging
from dataclasses import dataclass
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import os

import pymongo
from IP2Location import IP2Location
from tqdm import tqdm

# Initialize DB once at module level for multiprocessing
_ip_db = None


def _init_ip_db():
    """Initialize IP2Location database once"""
    global _ip_db
    if _ip_db is None:
        logger.info(f"📂 Initializing IP2Location DB from: {IP2LOCATION_DB_PATH}")
        _ip_db = IP2Location(IP2LOCATION_DB_PATH)
        logger.info(f"✅ IP2Location DB loaded successfully")
    return _ip_db

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "countly"
SOURCE_COLLECTION = "summary"
TARGET_COLLECTION = "ip_locations"
IP2LOCATION_DB_PATH = "IP2LOCATION-LITE-DB1.BIN"

CSV_OUTPUT = "ip_locations.csv"
JSONL_OUTPUT = "ip_locations.jsonl"

BATCH_SIZE = 500  # Mongo insert batch size
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)
RETRY_COUNT = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
    processing_time_ms: Optional[float] = None  # time to get this IP location


# -----------------------------
# Functions
# -----------------------------
def get_mongo_client() -> pymongo.MongoClient:
    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def get_unique_ips(limit: Optional[int] = None) -> List[str]:
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


def safe_float(value) -> Optional[float]:
    """Safely convert value to float, handling None and empty strings"""
    if value is None or value == '' or value == 'N/A':
        return None
    try:
        val = float(value)
        return val if not (val == -1.0 or val == -999): None  # IP2Location uses -1 for "not available"
    except (ValueError, TypeError):
        return None


def lookup_ip(ip: str) -> IPLocationData:
    """Worker function for ProcessPoolExecutor"""
    start = time.time()
    try:
        db = _init_ip_db()
        for attempt in range(RETRY_COUNT + 1):
            try:
                result = db.get_all(ip)
                end = time.time()
                
                # Debug: Show raw result for first lookup
                if 'FIRST_LOOKUP_DEBUG' not in globals():
                    globals()['FIRST_LOOKUP_DEBUG'] = True
                    logger.info(f"🔍 Raw result for IP {ip}:")
                    logger.info(f"  - country_short: '{result.country_short}' (type: {type(result.country_short)})")
                    logger.info(f"  - country_long: '{result.country_long}' (type: {type(result.country_long)})")
                    logger.info(f"  - region: '{result.region}' (type: {type(result.region)})")
                    logger.info(f"  - city: '{result.city}' (type: {type(result.city)})")
                    logger.info(f"  - latitude: '{result.latitude}' (type: {type(result.latitude)})")
                    logger.info(f"  - longitude: '{result.longitude}' (type: {type(result.longitude)})")
                
                # Extract and clean values
                country_code = result.country_short if result.country_short and result.country_short != '-' else None
                country_name = result.country_long if result.country_long and result.country_long != '-' else None
                region_name = result.region if result.region and result.region != '-' else None
                city_name = result.city if result.city and result.city != '-' else None
                latitude = safe_float(result.latitude)
                longitude = safe_float(result.longitude)
                
                return IPLocationData(
                    ip=ip,
                    country_code=country_code,
                    country_name=country_name,
                    region_name=region_name,
                    city_name=city_name,
                    latitude=latitude,
                    longitude=longitude,
                    processed_at=int(end),
                    processing_time_ms=(end - start) * 1000
                )
            except Exception as e:
                if attempt < RETRY_COUNT:
                    continue
                else:
                    logger.warning(f"⚠️ Failed IP {ip} after retries: {e}")
                    end = time.time()
                    return IPLocationData(ip=ip, processed_at=int(end),
                                          processing_time_ms=(end - start) * 1000)
    except Exception as e:
        logger.error(f"❌ Cannot load IP2Location DB in worker: {e}")
        end = time.time()
        return IPLocationData(ip=ip, processed_at=int(end),
                              processing_time_ms=(end - start) * 1000)


def process_ips_parallel(ips: List[str]) -> List[IPLocationData]:
    all_results = []
    
    # Test first IP lookup to verify DB is working
    if ips:
        logger.info(f"🧪 Testing first IP lookup: {ips[0]}")
        test_result = lookup_ip(ips[0])
        logger.info(f"🧪 Test result: country={test_result.country_code}, city={test_result.city_name}, lat={test_result.latitude}")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(lookup_ip, ip): ip for ip in ips}
        for f in tqdm(as_completed(futures), total=len(futures), desc="Processing IPs"):
            all_results.append(f.result())
    logger.info(f"✅ Finished processing {len(all_results)} IPs")
    return all_results


def save_to_csv(data: List[IPLocationData]):
    if not data:
        logger.warning("⚠️ No data to save to CSV")
        return

    fieldnames = ['ip', 'country_code', 'country_name', 'region_name', 'city_name',
                  'latitude', 'longitude', 'processed_at', 'processing_time_ms']
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

    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        docs = [d.__dict__ for d in batch]
        try:
            result = col.insert_many(docs)
            logger.info(
                f"✅ Inserted {len(result.inserted_ids)} records into {TARGET_COLLECTION}")
        except Exception as e:
            logger.error(f"❌ Error inserting batch to MongoDB: {e}")

    col.create_index("ip")
    logger.info("✅ Created index on 'ip'")


def main():
    total_start = time.time()

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

    ips = get_unique_ips()
    if not ips:
        logger.warning("⚠️ No IPs found")
        sys.exit(1)

    data = process_ips_parallel(ips)
    save_to_csv(data)
    save_to_jsonl(data)
    save_to_mongodb(data)

    total_end = time.time()
    logger.info(
        f"🎉 Done! Total processing time: {total_end - total_start:.2f} seconds")


if __name__ == "__main__":
    main()
