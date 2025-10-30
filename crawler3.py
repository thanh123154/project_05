# -----------------------------
# File: crawl3_fixed.py
# -----------------------------
import os
import re
import json
import time
import csv
import requests
import logging
from urllib.parse import urlparse
from typing import List, Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# ===============================
# Logger setup
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ===============================
# Data model
# ===============================
@dataclass
class UrlRecord:
    product_id: str
    url: str
    source_collection: str

def distinct_records(records):
    """Remove duplicate products by product_id. Only keep the first occurrence."""
    seen = set()
    result = []
    for row in records:
        pid = str(row.get("product_id")).strip()
        if pid and pid not in seen:
            seen.add(pid)
            result.append(row)
    return result

# ===============================
# Core functions
# ===============================
def http_get(url: str) -> Optional[str]:
    """Fetch raw HTML."""
    try:
        resp = requests.get(url, timeout=15, verify=False, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
        if resp.status_code == 200 and resp.text.strip():
            return resp.text
    except Exception as e:
        logger.warning(f"HTTP error for {url}: {e}")
    return None


def extract_product_name(html: str) -> Optional[str]:
    """Extract product name from JSON-LD or fallback tags."""
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Try JSON-LD first
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                # JSON-LD may contain list
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "Product":
                            name = item.get("name")
                            if name:
                                return name.strip()
                elif isinstance(data, dict) and data.get("@type") == "Product":
                    name = data.get("name")
                    if name:
                        return name.strip()
            except Exception:
                continue

        # Fallback 1: <meta property="og:title">
        meta_title = soup.find("meta", property="og:title")
        if meta_title and meta_title.get("content"):
            return meta_title["content"].strip()

        # Fallback 2: <title>
        if soup.title and soup.title.text:
            return soup.title.text.strip()

        # Fallback 3: H1
        h1 = soup.find("h1")
        if h1 and h1.text.strip():
            return h1.text.strip()

    except Exception as e:
        logger.warning(f"extract_product_name error: {e}")
    return None


def _derive_name_from_slug(url: str) -> Optional[str]:
    parsed = urlparse(url)
    slug = parsed.path.rsplit('/', 1)[-1]
    slug = slug.split('.html', 1)[0]
    parts = [w for w in re.split(r"[-_]+", slug) if w]
    if not parts:
        return None
    name = " ".join(p.capitalize() for p in parts if p.isalpha())
    return name.strip() if name else None


def is_likely_product_url(url: str) -> bool:
    return "glamira" in (url or "") and ".html" in (url or "")


def process_batch_data(batch):
    out = []
    for row in batch:
        pid = str(row.get("product_id")).strip()
        url = row.get("url")
        sc = row.get("source_collection", "unknown")
        if pid and url and is_likely_product_url(url):
            out.append(UrlRecord(product_id=pid, url=url, source_collection=sc))
    return out


def crawl_product_names_parallel(records: List[UrlRecord], debug_limit: int = 10):
    results = []
    for idx, rec in enumerate(tqdm(records, desc="Crawling products")):
        html = http_get(rec.url)
        product_name = None
        status = "no_html"
        if html:
            product_name = extract_product_name(html)
            status = "success" if product_name else "fallback"
            if not product_name:
                product_name = _derive_name_from_slug(rec.url)
            # Save debug HTML for first few pages
            if idx < debug_limit:
                host = urlparse(rec.url).netloc.replace(".", "_")
                fname = f"debug_html_{host}_{int(time.time())}_{idx}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"🧪  Saved debug HTML to {fname}")
        results.append({
            "product_id": rec.product_id,
            "product_name": product_name or "",
            "url": rec.url,
            "source_collection": rec.source_collection,
            "status": status,
            "fetched_at": int(time.time())
        })
        logger.info(f"⏳  Progress: {idx+1}/{len(records)} ({(idx+1)/len(records)*100:.1f}%)")
    return results


def append_candidates_jsonl(rows, path="product_name_candidates.jsonl"):
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def append_final_csv(rows, path="product_names_final.csv"):
    exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "product_id", "product_name", "url", "source_collection", "status", "fetched_at"
        ])
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


# ===============================
# CLI
# ===============================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Crawl product names from URLs")
    parser.add_argument("--input", type=str, default="merged_data.csv")
    parser.add_argument("--n", type=int, default=20000)
    args = parser.parse_args()

    records = []
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=["product_id", "url", "source_collection", "fetched_at"])
            for row in reader:
                if len(records) >= args.n:
                    break
                if is_likely_product_url(row.get("url")):
                    records.append(row)
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        exit(1)

    # Loại sản phẩm trùng theo product_id
    records = distinct_records(records)
    url_records = process_batch_data(records)
    logger.info(f"Starting crawl for {len(url_records)} URLs...")
    results = crawl_product_names_parallel(url_records)
    append_candidates_jsonl(results)
    named_only = [r for r in results if r.get("product_name")]
    logger.info(f"Total input (distinct) products: {len(url_records)}")
    logger.info(f"Total successfully crawled product_name: {len(named_only)}")
    if len(named_only) < len(url_records):
        missed = [r['product_id'] for r in results if not r.get("product_name")]
        logger.warning(f"Products could not be named: {missed}")
    if named_only:
        append_final_csv(named_only)
        logger.info(f"✅ Saved {len(named_only)} named products.")
    else:
        logger.warning("⚠️ No product names extracted.")
