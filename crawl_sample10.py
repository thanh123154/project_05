import csv
import json
import logging
import argparse
from typing import Dict, List, Optional

import crawler3
from crawler3 import (
    UrlRecord,
    process_batch_data,
    crawl_product_names_parallel,
    deduplicate_by_product_id,
    is_likely_product_url,
)

INPUT_JSONL = "product_urls.jsonl"
INPUT_CSV = "merged_data.csv"
DEFAULT_SAMPLE_SIZE = 10

SAMPLE_CANDIDATES_JSONL = "product_name_candidates_sample.jsonl"
SAMPLE_FINAL_CSV = "product_names_sample.csv"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def read_first_n_records(n: int, domain_contains: Optional[str]) -> List[Dict]:
    records: List[Dict] = []
    seen_products = set()

    # Prefer JSONL if present for performance
    try:
        with open(INPUT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line.strip())
                except Exception:
                    continue
                pid = row.get("product_id")
                if not pid or pid in seen_products:
                    continue
                url = row.get("url")
                if domain_contains and (not url or domain_contains not in url):
                    continue
                if not is_likely_product_url(url):
                    continue
                seen_products.add(pid)
                records.append(row)
                if len(records) >= n:
                    return records
    except FileNotFoundError:
        pass

    # Fallback to CSV
    try:
        with open(INPUT_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row.get("product_id")
                if not pid or pid in seen_products:
                    continue
                url = row.get("url")
                if domain_contains and (not url or domain_contains not in url):
                    continue
                if not is_likely_product_url(url):
                    continue
                seen_products.add(pid)
                records.append(row)
                if len(records) >= n:
                    break
    except FileNotFoundError:
        logger.error("No input files found: product_urls.jsonl or merged_data.csv")

    return records


def append_candidates_jsonl(candidates: List[Dict], path: str = SAMPLE_CANDIDATES_JSONL) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for row in candidates:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_final_csv(candidates: List[Dict], path: str = SAMPLE_FINAL_CSV) -> None:
    file_exists = False
    try:
        with open(path, "r", encoding="utf-8"):
            file_exists = True
    except FileNotFoundError:
        file_exists = False

    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "product_id",
                "product_name",
                "url",
                "source_collection",
                "status",
                "fetched_at",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(candidates)


def main():
    parser = argparse.ArgumentParser(description="Crawl sample product names")
    parser.add_argument("--n", type=int, default=DEFAULT_SAMPLE_SIZE, help="number of products to test")
    parser.add_argument("--domain", type=str, default=None, help="filter URLs containing this substring (e.g. glamira.at)")
    parser.add_argument("--show-urls", action="store_true", help="print selected URLs before crawling")
    args = parser.parse_args()

    # Tweak crawler settings for gentle sample run
    try:
        crawler3.MAX_WORKERS = 3
        crawler3.DEFAULT_TIMEOUT_SECONDS = max(25, crawler3.DEFAULT_TIMEOUT_SECONDS)
        crawler3.DEBUG_MODE = True
    except Exception:
        pass

    sample_records = read_first_n_records(args.n, args.domain)
    if not sample_records:
        logger.error("No input records available to test.")
        return

    url_records: List[UrlRecord] = process_batch_data(sample_records)
    if not url_records:
        logger.error("No valid URL records built from input.")
        return

    if args.show_urls:
        logger.info("Selected URLs for sample crawl:")
        for r in url_records:
            logger.info(f"- {r.url}")

    logger.info(f"Testing crawl for {len(url_records)} products...")
    candidates = crawl_product_names_parallel(url_records)

    # Deduplicate and keep best per product
    unique_candidates = deduplicate_by_product_id(candidates)

    # Save all attempts to sample jsonl
    append_candidates_jsonl(candidates)

    # Only save named rows to sample CSV
    named_only = [row for row in unique_candidates if row.get("product_name")]
    if named_only:
        append_final_csv(named_only)
        logger.info(f"Saved {len(named_only)} named products to {SAMPLE_FINAL_CSV}")
    else:
        logger.info("No product names extracted in the sample run.")


if __name__ == "__main__":
    main()


