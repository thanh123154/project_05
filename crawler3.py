import sys
import csv
import json
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

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

MAX_WORKERS = 50
BATCH_SIZE = 1000   # số lượng product xử lý 1 lần
URLS_PER_PRODUCT = 10

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


def load_filtered_data() -> Dict[str, List[Dict]]:
    """Load filtered data from data_filter.py output files"""
    product_urls = {}
    
    # Load from JSONL file (more efficient for large datasets)
    if os.path.exists(FILTERED_JSONL):
        logger.info(f"📂 Loading data from {FILTERED_JSONL}")
        with open(FILTERED_JSONL, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    product_id = record['product_id']
                    if product_id not in product_urls:
                        product_urls[product_id] = []
                    product_urls[product_id].append(record)
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Skipping invalid line: {e}")
                    continue
    else:
        # Fallback to CSV if JSONL doesn't exist
        logger.info(f"📂 Loading data from {FILTERED_CSV}")
        with open(FILTERED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_id = row['product_id']
                if product_id not in product_urls:
                    product_urls[product_id] = []
                product_urls[product_id].append(row)
    
    logger.info(f"✅ Loaded {len(product_urls)} distinct product IDs")
    return product_urls


def stream_product_ids_from_filtered(product_urls: Dict[str, List[Dict]], batch_size=1000):
    """Stream product IDs from filtered data in batches"""
    product_ids = list(product_urls.keys())
    
    for i in range(0, len(product_ids), batch_size):
        yield product_ids[i:i + batch_size]


def count_total_products_from_filtered(product_urls: Dict[str, List[Dict]]) -> int:
    """Count total distinct product IDs from filtered data"""
    return len(product_urls)


def get_urls_for_product_from_filtered(product_id: str, product_urls: Dict[str, List[Dict]], limit: int = URLS_PER_PRODUCT) -> List[UrlRecord]:
    """Get URLs for a product from filtered data"""
    if product_id not in product_urls:
        return []
    
    records = []
    urls = product_urls[product_id][:limit]  # Limit URLs per product
    
    for url_data in urls:
        url = url_data.get('url')
        source_collection = url_data.get('source_collection', 'unknown')
        if url:
            records.append(UrlRecord(product_id, _safe_str(url), source_collection))
    
    return records


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
                if has_progress_update and progress is not None:
                    try:
                        progress.update(1)
                    except Exception:
                        pass
                else:
                    if completed % log_every == 0 or completed == total:
                        percent = (completed * 100.0 /
                                   total) if total else 100.0
                        logger.info(
                            f"⏳ Progress: {completed}/{total} ({percent:.1f}%)")

    if progress is not None and hasattr(progress, "close"):
        try:
            progress.close()
        except Exception:
            pass

    return results


def deduplicate_by_product_id(candidates: List[Dict]) -> List[Dict]:
    """Deduplicate by product_id, keeping only one active product name per product_id"""
    deduped = {}
    for row in candidates:
        pid = row["product_id"]
        # Keep the first record with a valid product_name, or the first record if no name found
        if pid not in deduped:
            deduped[pid] = row
        elif (deduped[pid].get("product_name") is None and row.get("product_name")):
            # Replace if current has no name but new one has name
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
                                "product_id", "product_name", "url", "source_collection", "status", "fetched_at"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(candidates)

# -----------------------------
# Main
# -----------------------------


def main():
    # Load filtered data from data_filter.py output
    product_urls = load_filtered_data()
    total_expected = count_total_products_from_filtered(product_urls)
    logger.info(f"📊 Total distinct product IDs from filtered data: {total_expected}")

    # Load existing processed products
    existing_pids = set()
    if os.path.exists(FINAL_CSV):
        with open(FINAL_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_pids.add(row["product_id"])

    total_processed = 0
    total_with_name = 0

    for batch in stream_product_ids_from_filtered(product_urls, batch_size=BATCH_SIZE):
        # Skip already processed
        batch = [pid for pid in batch if pid not in existing_pids]
        if not batch:
            continue

        logger.info(f"🔄 Processing batch with {len(batch)} product IDs...")
        url_records = []
        for pid in batch:
            url_records.extend(get_urls_for_product_from_filtered(
                pid, product_urls, limit=URLS_PER_PRODUCT))

        candidates = crawl_product_names_parallel(url_records)
        deduped = deduplicate_by_product_id(candidates)

        append_candidates_jsonl(deduped)
        append_final_csv(deduped)

        processed_batch = len(deduped)
        with_name_batch = sum(1 for row in deduped if row.get("product_name"))
        
        # Thống kê chi tiết
        status_counts = {}
        for row in deduped:
            status = row.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        total_processed += processed_batch
        total_with_name += with_name_batch

        percent = (total_processed / total_expected) * \
            100 if total_expected else 0
        
        logger.info(
            f"✅ Finished batch. Total processed: {total_processed}/{total_expected} "
            f"({percent:.2f}% done). With product_name: {total_with_name}"
        )
        logger.info(f"📊 Status breakdown: {status_counts}")

    logger.info(
        f"🎯 DONE! {total_with_name}/{total_expected} products crawled successfully.")


if __name__ == "__main__":
    main()
