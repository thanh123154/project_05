import sys
import csv
import json
import time
import random
import logging
import gc
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Generator

import requests
from bs4 import BeautifulSoup
import os

# -----------------------------
# Configurations
# -----------------------------
# Input files from data_filter.py
FILTERED_CSV = "merged_data.csv"
FILTERED_JSONL = "product_urls.jsonl"

DEFAULT_TIMEOUT_SECONDS = 5
MAX_RETRIES = 2
RETRY_BACKOFF_BASE_SECONDS = 1
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
]

MAX_WORKERS = 20  # Giảm số workers để tránh quá tải
BATCH_SIZE = 500   # Giảm batch size để tiết kiệm RAM
URLS_PER_PRODUCT = 5  # Giảm số URL per product
MEMORY_LIMIT_MB = 1024  # Giới hạn RAM usage

# Tùy chọn để force process (bỏ qua existing products)
FORCE_PROCESS = True  # Set True để xử lý lại tất cả products

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

# -----------------------------
# Dataclasses
# -----------------------------


@dataclass
class UrlRecord:
    product_id: str
    url: str
    source_collection: str

# -----------------------------
# Helper Functions
# -----------------------------


def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    return str(value).strip() or None


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def check_memory_limit() -> bool:
    """Check if memory usage exceeds limit"""
    current_mb = get_memory_usage_mb()
    if current_mb > MEMORY_LIMIT_MB:
        logger.warning(f"⚠️ Memory usage {current_mb:.1f}MB exceeds limit {MEMORY_LIMIT_MB}MB")
        return True
    return False


def force_garbage_collection():
    """Force garbage collection to free memory"""
    gc.collect()
    logger.info(f"🧹 Garbage collection completed. Memory: {get_memory_usage_mb():.1f}MB")


def stream_filtered_data_batches(batch_size: int = BATCH_SIZE) -> Generator[List[Dict], None, None]:
    """Stream filtered data in batches to avoid loading everything into memory"""
    logger.info(f"📂 Streaming data from {FILTERED_JSONL} in batches of {batch_size}")
    
    current_batch = []
    product_ids_seen = set()
    
    if os.path.exists(FILTERED_JSONL):
        with open(FILTERED_JSONL, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line.strip())
                    product_id = record['product_id']
                    
                    # Chỉ thêm nếu chưa xử lý product_id này
                    if product_id not in product_ids_seen:
                        current_batch.append(record)
                        product_ids_seen.add(product_id)
                        
                        # Yield batch khi đủ size
                        if len(current_batch) >= batch_size:
                            yield current_batch
                            current_batch = []
                            
                            # Check memory và force GC nếu cần
                            if check_memory_limit():
                                force_garbage_collection()
                                
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Skipping invalid line {line_num}: {e}")
                    continue
    else:
        # Fallback to CSV streaming
        logger.info(f"📂 Streaming data from {FILTERED_CSV} in batches of {batch_size}")
        with open(FILTERED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1):
                product_id = row['product_id']
                
                if product_id not in product_ids_seen:
                    current_batch.append(row)
                    product_ids_seen.add(product_id)
                    
                    if len(current_batch) >= batch_size:
                        yield current_batch
                        current_batch = []
                        
                        if check_memory_limit():
                            force_garbage_collection()
    
    # Yield remaining batch
    if current_batch:
        yield current_batch
    
    logger.info(f"✅ Finished streaming. Total unique product IDs: {len(product_ids_seen)}")


def count_total_products_streaming() -> int:
    """Count total unique product IDs without loading all data into memory"""
    product_ids = set()
    
    if os.path.exists(FILTERED_JSONL):
        with open(FILTERED_JSONL, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    product_ids.add(record['product_id'])
                except (json.JSONDecodeError, KeyError):
                    continue
    else:
        with open(FILTERED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_ids.add(row['product_id'])
    
    return len(product_ids)


def process_batch_data(batch_data: List[Dict]) -> List[UrlRecord]:
    """Process a batch of data and convert to UrlRecord list"""
    url_records = []
    
    for record in batch_data:
        product_id = record.get('product_id')
        url = record.get('url')
        source_collection = record.get('source_collection', 'unknown')
        
        if product_id and url:
            url_records.append(UrlRecord(
                product_id=product_id,
                url=_safe_str(url),
                source_collection=source_collection
            ))
    
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
    
    # Thêm nhiều selector hơn để tìm product name
    selectors = [
        "h1.page-title span.base",
        "h1.product-title", 
        "h1.product-name",
        "h1",
        ".product-title",
        ".product-name", 
        ".page-title",
        "title",
        "[data-testid='product-title']",
        ".product-info h1",
        ".product-details h1",
        ".product-header h1"
    ]
    
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if text and len(text) > 3:  # Đảm bảo có nội dung thực sự
                return text
    return None


def process_single_url(record: UrlRecord) -> Dict:
    html = http_get(record.url)
    product_name = None
    status = "failed"
    
    if html:
        product_name = extract_product_name(html)
        status = "success" if product_name else "no_name_found"
    else:
        status = "no_html"
    
    return {
        "product_id": record.product_id,
        "url": record.url,
        "source_collection": record.source_collection,
        "product_name": product_name,
        "status": status,
        "fetched_at": int(time.time()),
    }


def crawl_product_names_parallel(records: List[UrlRecord]) -> List[Dict]:
    results = []
    total = len(records)
    completed = 0
    
    # Log memory usage before starting
    initial_memory = get_memory_usage_mb()
    logger.info(f"🧠 Starting crawl with {total} records. Initial memory: {initial_memory:.1f}MB")

    try:
        progress = tqdm(total=total, desc="Crawling products")
        has_progress_update = hasattr(progress, "update")
    except Exception:
        progress = None
        has_progress_update = False

    log_every = max(1, total // 20) if total > 0 else 1  # ~5% steps

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_url, r) for r in records]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.warning(f"Error: {e}")
            finally:
                completed += 1
                
                # Memory monitoring
                if completed % log_every == 0 or completed == total:
                    current_memory = get_memory_usage_mb()
                    percent = (completed * 100.0 / total) if total else 100.0
                    logger.info(f"⏳ Progress: {completed}/{total} ({percent:.1f}%) - Memory: {current_memory:.1f}MB")
                    
                    # Force GC if memory usage is high
                    if check_memory_limit():
                        force_garbage_collection()
                
                if has_progress_update and progress is not None:
                    try:
                        progress.update(1)
                    except Exception:
                        pass

    if progress is not None and hasattr(progress, "close"):
        try:
            progress.close()
        except Exception:
            pass

    # Final memory check
    final_memory = get_memory_usage_mb()
    logger.info(f"🧠 Finished crawl. Memory: {final_memory:.1f}MB (Δ: {final_memory - initial_memory:+.1f}MB)")
    
    return results


# Removed deduplicate_by_product_id function since data_filter.py already ensures unique product_ids


def append_candidates_jsonl(candidates: List[Dict], path: str = CANDIDATES_JSONL) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for row in candidates:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_final_csv(candidates: List[Dict], path: str = FINAL_CSV) -> None:
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
                                "product_id", "product_name", "url", "source_collection", "status", "fetched_at"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(candidates)

# -----------------------------
# Main
# -----------------------------


def main():
    # Count total products without loading all data
    total_expected = count_total_products_streaming()
    logger.info(f"📊 Total distinct product IDs from filtered data: {total_expected}")
    logger.info(f"🧠 Initial memory usage: {get_memory_usage_mb():.1f}MB")

    # Load existing processed products (skip if FORCE_PROCESS)
    existing_pids = set()
    if not FORCE_PROCESS and os.path.exists(FINAL_CSV):
        with open(FINAL_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_pids.add(row["product_id"])
        logger.info(f"📋 Found {len(existing_pids)} already processed products")
    elif FORCE_PROCESS:
        logger.info("🔄 FORCE_PROCESS enabled - will process all products")

    total_processed = 0
    total_with_name = 0
    batch_count = 0

    # Process data in streaming batches
    for batch_data in stream_filtered_data_batches(batch_size=BATCH_SIZE):
        batch_count += 1
        
        # Filter out already processed products
        original_batch_size = len(batch_data)
        batch_data = [record for record in batch_data 
                     if record.get('product_id') not in existing_pids]
        filtered_batch_size = len(batch_data)
        
        logger.info(f"📊 Batch {batch_count}: {original_batch_size} -> {filtered_batch_size} products (filtered: {original_batch_size - filtered_batch_size})")
        
        if not batch_data:
            logger.info(f"⏭️ Batch {batch_count}: All products already processed, skipping")
            continue

        logger.info(f"🔄 Processing batch {batch_count} with {len(batch_data)} products...")
        logger.info(f"🧠 Memory before batch: {get_memory_usage_mb():.1f}MB")
        
        # Convert batch data to URL records
        url_records = process_batch_data(batch_data)
        
        if not url_records:
            logger.warning(f"⚠️ Batch {batch_count}: No valid URL records found")
            continue

        # Process URLs in parallel
        candidates = crawl_product_names_parallel(url_records)

        # Save results
        append_candidates_jsonl(candidates)
        append_final_csv(candidates)

        # Update statistics
        processed_batch = len(candidates)
        with_name_batch = sum(1 for row in candidates if row.get("product_name"))
        
        # Thống kê chi tiết
        status_counts = {}
        for row in candidates:
            status = row.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        total_processed += processed_batch
        total_with_name += with_name_batch

        percent = (total_processed / total_expected) * 100 if total_expected else 0
        
        logger.info(f"✅ Finished batch {batch_count}. Processed: {processed_batch}, With names: {with_name_batch}")
        logger.info(f"📊 Total progress: {total_processed}/{total_expected} ({percent:.2f}% done)")
        logger.info(f"📊 Status breakdown: {status_counts}")
        logger.info(f"🧠 Memory after batch: {get_memory_usage_mb():.1f}MB")
        
        # Force garbage collection after each batch
        force_garbage_collection()
        
        # Check if we should pause due to memory
        if check_memory_limit():
            logger.warning("⚠️ Memory limit reached, forcing cleanup...")
            time.sleep(2)  # Brief pause to let system recover

    logger.info(f"🎯 DONE! {total_with_name}/{total_expected} products crawled successfully.")
    logger.info(f"🧠 Final memory usage: {get_memory_usage_mb():.1f}MB")


if __name__ == "__main__":
    main()
