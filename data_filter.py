#!/usr/bin/env python3
"""
Data Filtering Script for Crawler3.py
Batch version - handles large MongoDB collections efficiently.
"""

import sys
import csv
import json
import time
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from pymongo import MongoClient
from urllib.parse import urlparse

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

# Batch settings
BATCH_SIZE = 1000  # số record xử lý mỗi batch

# Tùy chọn để lấy tất cả dữ liệu (không filter nghiêm ngặt)
STRICT_FILTERING = False  # Set True để giữ logic cũ, False để lấy nhiều dữ liệu hơn

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


def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)


def url_to_country(url: str) -> str:
    try:
        domain = urlparse(url).netloc.lower()
        tld = domain.split('.')[-1]
        return tld if tld in ['fr', 'de', 'es', 'it', 'com', 'co.uk', 'nl', 'be'] else 'other'
    except Exception:
        return 'other'


def process_in_batches():
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[SUMMARY_COLLECTION]

    logger.info(f"🔧 Configuration: STRICT_FILTERING = {STRICT_FILTERING}")
    logger.info(f"🔧 Batch size: {BATCH_SIZE}")

    grouped = {}
    seen_product_ids: Set[str] = set()
    total_records = 0

    # Ghi file CSV & JSONL ngay từ đầu (append)
    csv_file = open(MERGED_CSV, 'w', newline='', encoding='utf-8')
    csv_writer = csv.DictWriter(csv_file, fieldnames=["product_id", "url", "source_collection", "timestamp"])
    csv_writer.writeheader()

    jsonl_file = open(URLS_JSONL, 'w', encoding='utf-8')

    # Các loại sub-collection cần lấy
    # 6 collections đầu: lấy product_id/viewing_product_id + current_url
    main_collections = [
        "view_product_detail",
        "select_product_option", 
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed"
    ]
    
    # Collection đặc biệt: lấy viewing_product_id + referrer_url
    special_collection = "product_view_all_recommend_clicked"

    # Xử lý 6 collections chính
    for sub_collection in main_collections:
        logger.info(f"🔍 Processing '{sub_collection}' ...")

        # Kiểm tra tổng số documents cho collection này
        total_docs = collection.count_documents({"collection": sub_collection})
        logger.info(f"  📊 Total documents in '{sub_collection}': {total_docs:,}")

        if STRICT_FILTERING:
            # Query nghiêm ngặt (logic cũ)
            query = {
                "collection": sub_collection,
                "$and": [
                    {
                        "$or": [
                            {"product_id": {"$exists": True, "$ne": None}},
                            {"viewing_product_id": {"$exists": True, "$ne": None}}
                        ]
                    },
                    {
                        "current_url": {"$exists": True, "$ne": None}
                    }
                ]
            }
        else:
            # Query lỏng hơn - lấy tất cả documents có product_id/viewing_product_id
            query = {
                "collection": sub_collection,
                "$or": [
                    {"product_id": {"$exists": True, "$ne": None}},
                    {"viewing_product_id": {"$exists": True, "$ne": None}}
                ]
            }

        # Kiểm tra số documents match query
        matching_docs = collection.count_documents(query)
        logger.info(f"  📊 Documents matching query: {matching_docs:,}")

        cursor = collection.find(query, batch_size=BATCH_SIZE)

        batch = []
        processed_count = 0
        skipped_invalid = 0
        
        for doc in cursor:
            pid = str(doc.get("product_id") or doc.get("viewing_product_id"))
            if not pid:
                skipped_invalid += 1
                continue

            # Thử lấy URL từ nhiều field khác nhau
            url = None
            if STRICT_FILTERING:
                # Chỉ lấy current_url
                url = doc.get("current_url")
            else:
                # Thử nhiều field để lấy URL
                url = (doc.get("current_url") or 
                       doc.get("referrer_url") or 
                       doc.get("url") or 
                       doc.get("page_url"))
            
            if not url:
                skipped_invalid += 1
                continue

            record = ProductUrlRecord(
                product_id=pid,
                url=url,
                source_collection=sub_collection,
                timestamp=doc.get("time_stamp")
            )

            batch.append(record)
            total_records += 1
            processed_count += 1

            # Khi đủ batch, xử lý
            if len(batch) >= BATCH_SIZE:
                save_batch(batch, csv_writer, jsonl_file, grouped, seen_product_ids)
                batch.clear()

        # Lưu nốt phần cuối
        if batch:
            save_batch(batch, csv_writer, jsonl_file, grouped, seen_product_ids)
            batch.clear()

        logger.info(f"✅ Finished '{sub_collection}' - Processed: {processed_count:,}, Skipped invalid: {skipped_invalid:,}")

    # Xử lý collection đặc biệt: product_view_all_recommend_clicked
    logger.info(f"🔍 Processing '{special_collection}' ...")
    
    # Kiểm tra tổng số documents cho collection đặc biệt
    total_docs_special = collection.count_documents({"collection": special_collection})
    logger.info(f"  📊 Total documents in '{special_collection}': {total_docs_special:,}")
    
    if STRICT_FILTERING:
        special_query = {
            "collection": special_collection,
            "$and": [
                {
                    "viewing_product_id": {"$exists": True, "$ne": None}
                },
                {
                    "referrer_url": {"$exists": True, "$ne": None}
                }
            ]
        }
    else:
        # Query lỏng hơn cho collection đặc biệt
        special_query = {
            "collection": special_collection,
            "viewing_product_id": {"$exists": True, "$ne": None}
        }
    
    # Kiểm tra số documents match query đặc biệt
    matching_docs_special = collection.count_documents(special_query)
    logger.info(f"  📊 Documents matching special query: {matching_docs_special:,}")
    
    cursor = collection.find(special_query, batch_size=BATCH_SIZE)
    
    batch = []
    processed_count_special = 0
    skipped_invalid_special = 0
    
    for doc in cursor:
        pid = str(doc.get("viewing_product_id"))
        if not pid:
            skipped_invalid_special += 1
            continue

        # Thử lấy URL từ nhiều field khác nhau
        url = None
        if STRICT_FILTERING:
            # Chỉ lấy referrer_url
            url = doc.get("referrer_url")
        else:
            # Thử nhiều field để lấy URL
            url = (doc.get("referrer_url") or 
                   doc.get("current_url") or 
                   doc.get("url") or 
                   doc.get("page_url"))
        
        if not url:
            skipped_invalid_special += 1
            continue
            
        record = ProductUrlRecord(
            product_id=pid,
            url=url,
            source_collection=special_collection,
            timestamp=doc.get("time_stamp")
        )

        batch.append(record)
        total_records += 1
        processed_count_special += 1

        # Khi đủ batch, xử lý
        if len(batch) >= BATCH_SIZE:
            save_batch(batch, csv_writer, jsonl_file, grouped, seen_product_ids)
            batch.clear()

    # Lưu nốt phần cuối cho collection đặc biệt
    if batch:
        save_batch(batch, csv_writer, jsonl_file, grouped, seen_product_ids)
        batch.clear()

    logger.info(f"✅ Finished '{special_collection}' - Processed: {processed_count_special:,}, Skipped invalid: {skipped_invalid_special:,}")

    csv_file.close()
    jsonl_file.close()

    # Lưu file JSON tổng hợp sau cùng
    save_grouped_json(grouped)

    logger.info(f"🎉 Done. Processed {total_records} records from summary collection.")


def save_batch(batch: List[ProductUrlRecord], csv_writer, jsonl_file, grouped: Dict, seen_product_ids: Set[str]):
    """Save batch to CSV, JSONL, and update grouped data in memory."""
    saved_count = 0
    skipped_count = 0
    
    for r in batch:
        # Lưu tất cả records vào CSV và JSONL (không skip duplicate product_ids)
        csv_writer.writerow(vars(r))
        jsonl_file.write(json.dumps(vars(r), ensure_ascii=False) + "\n")
        saved_count += 1

        # Chỉ distinct trong grouped data (cho JSON output)
        pid = r.product_id
        if pid not in seen_product_ids:
            seen_product_ids.add(pid)
            grouped.setdefault(pid, {"product_id": pid, "current_url": {}})
        
        country = url_to_country(r.url)
        grouped[pid]["current_url"].setdefault(country, []).append(r.url)
    
    if saved_count > 0:
        logger.info(f"  📊 Batch: {saved_count} records saved to CSV/JSONL")


def save_grouped_json(grouped: Dict[str, Dict]):
    with open(MERGED_JSON, 'w', encoding='utf-8') as f:
        json.dump(list(grouped.values()), f, ensure_ascii=False, indent=2)
    logger.info(f"💾 Saved {len(grouped)} grouped products → {MERGED_JSON}")


def main():
    start = time.time()
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        logger.info("✅ Connected to MongoDB")
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        sys.exit(1)

    process_in_batches()
    logger.info(f"🏁 Total time: {time.time() - start:.2f}s")


if __name__ == "__main__":
    main()
