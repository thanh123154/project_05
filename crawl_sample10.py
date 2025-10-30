# -----------------------------------
# File: crawl_sample10_fixed.py
# -----------------------------------

import argparse, csv, json, os, logging
from typing import List, Optional
from dataclasses import dataclass
import crawl3_fixed as prod

logger = logging.getLogger(__name__)

SAMPLE_CANDIDATES = "product_name_candidates_sample.jsonl"
SAMPLE_FINAL = "product_names_sample.csv"
DEFAULT_N = 10

@dataclass
class UrlRecordS:
    product_id: str
    url: str
    source_collection: str

def read_first_n_records(n: int, domain_contains: Optional[str]):
    records = []
    seen = set()
    try:
        with open('product_urls.jsonl', 'r', encoding='utf-8') as f:
            for line in f:
                obj = json.loads(line)
                pid = obj.get('product_id')
                url = obj.get('url')
                if not pid or pid in seen:
                    continue
                if domain_contains and domain_contains not in (url or ''):
                    continue
                if not prod.is_likely_product_url(url):
                    continue
                seen.add(pid)
                records.append(obj)
                if len(records) >= n:
                    return records
    except Exception:
        pass
    try:
        with open('merged_data.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row.get('product_id')
                url = row.get('url')
                if not pid or pid in seen:
                    continue
                if domain_contains and domain_contains not in (url or ''):
                    continue
                if not prod.is_likely_product_url(url):
                    continue
                seen.add(pid)
                records.append(row)
                if len(records) >= n:
                    break
    except Exception:
        pass
    return records

def main():
    parser = argparse.ArgumentParser(description='Crawl sample product names')
    parser.add_argument('--n', type=int, default=DEFAULT_N)
    parser.add_argument('--domain', type=str, default=None)
    parser.add_argument('--show-urls', action='store_true')
    args = parser.parse_args()

    records = read_first_n_records(args.n, args.domain)
    if not records:
        logger.error("No records found.")
        return

    url_records = prod.process_batch_data(records)
    if args.show_urls:
        for r in url_records:
            logger.info(f"→ {r.url}")

    logger.info(f"Crawling {len(url_records)} sample products...")
    candidates = prod.crawl_product_names_parallel(url_records)
    prod.append_candidates_jsonl(candidates, path=SAMPLE_CANDIDATES)

    named_only = [r for r in candidates if r.get('product_name')]
    if named_only:
        prod.append_final_csv(named_only, path=SAMPLE_FINAL)
        logger.info(f"✅ Saved {len(named_only)} items to {SAMPLE_FINAL}")
    else:
        logger.warning("⚠️ No product names extracted in sample run.")

if __name__ == "__main__":
    main()
