# -----------------------------
# File: crawl_sample10_fixed.py
# -----------------------------
import os
import json
import csv
import time
import argparse
import logging
from typing import List, Optional
from dataclasses import dataclass
from urllib.parse import urlparse

from tqdm import tqdm
import crawler3 as prod

# ===============================
# Logger setup
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ===============================
# Constants
# ===============================
SAMPLE_CANDIDATES = "product_name_candidates_sample.jsonl"
SAMPLE_FINAL = "product_names_sample.csv"
DEFAULT_N = 10


@dataclass
class UrlRecordS:
    product_id: str
    url: str
    source_collection: str


# ===============================
# Helper: read first N records
# ===============================
def read_first_n_records(n: int, domain_contains: Optional[str] = None) -> List[dict]:
    records = []
    seen = set()

    # 1️⃣ Try product_urls.jsonl first
    if os.path.exists("product_urls.jsonl"):
        logger.info("Reading from product_urls.jsonl ...")
        with open("product_urls.jsonl", "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                pid = str(obj.get("product_id"))
                url = obj.get("url")
                if pid in seen:
                    continue
                if domain_contains and domain_contains not in (url or ""):
                    continue
                if not prod.is_likely_product_url(url):
                    continue
                seen.add(pid)
                records.append(obj)
                if len(records) >= n:
                    return records

    # 2️⃣ Fallback: merged_data.csv
    if os.path.exists("merged_data.csv"):
        logger.info("Reading from merged_data.csv ...")
        with open("merged_data.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=["product_id", "url", "source_collection", "fetched_at"])
            for row in reader:
                pid = str(row.get("product_id"))
                url = row.get("url")
                if pid in seen:
                    continue
                if domain_contains and domain_contains not in (url or ""):
                    continue
                if not prod.is_likely_product_url(url):
                    continue
                seen.add(pid)
                records.append(row)
                if len(records) >= n:
                    break
    return records


# ===============================
# Main
# ===============================
def main():
    parser = argparse.ArgumentParser(description="Sample crawl (10 products)")
    parser.add_argument("--n", type=int, default=DEFAULT_N)
    parser.add_argument("--domain", type=str, default=None)
    parser.add_argument("--show-urls", action="store_true")
    args = parser.parse_args()

    records = read_first_n_records(args.n, args.domain)
    if not records:
        logger.error("No records found to crawl. Check your input files.")
        return

    url_records = prod.process_batch_data(records)

    if args.show_urls:
        logger.info("Selected URLs for sample crawl:")
        for r in url_records:
            logger.info(f"- {r.url}")

    logger.info(f"🧠 Starting sample crawl for {len(url_records)} URLs...")
    results = prod.crawl_product_names_parallel(url_records)

    prod.append_candidates_jsonl(results, path=SAMPLE_CANDIDATES)

    named_only = [r for r in results if r.get("product_name")]
    if named_only:
        prod.append_final_csv(named_only, path=SAMPLE_FINAL)
        logger.info(f"✅ Saved {len(named_only)} named products to {SAMPLE_FINAL}")
    else:
        logger.warning("⚠️ No product names extracted in sample run.")


if __name__ == "__main__":
    main()
