

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
from typing import Dict, List, Optional, Generator

import requests
try:
    import cloudscraper
    _cf_scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
except Exception:
    _cf_scraper = None
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urlparse, urlunparse

# -----------------------------
# Configurations
# -----------------------------
FILTERED_CSV = "merged_data.csv"
FILTERED_JSONL = "product_urls.jsonl"

DEFAULT_TIMEOUT_SECONDS = 12
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
]

MAX_WORKERS = 10
BATCH_SIZE = 1000
URLS_PER_PRODUCT = 5
MEMORY_LIMIT_MB = 4096

FORCE_PROCESS = True
CANDIDATES_JSONL = "product_name_candidates.jsonl"
FINAL_CSV = "product_names_final.csv"
LOG_FILE = "crawl.log"

# Logging config (file + console)
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

# debug html save limits
_DEBUG_HTML_SAVED = 0
_DEBUG_HTML_MAX = 10

# Proxy support via env
def _build_proxies() -> Optional[Dict[str, str]]:
    try:
        http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
        https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        proxy_url = os.environ.get("PROXY_URL")
        if proxy_url and not (http_proxy or https_proxy):
            http_proxy = proxy_url
            https_proxy = proxy_url
        if http_proxy or https_proxy:
            proxies = {}
            if http_proxy:
                proxies["http"] = http_proxy
            if https_proxy:
                proxies["https"] = https_proxy
            return proxies
    except Exception:
        pass
    return None

_PROXIES = _build_proxies()

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, desc=None, total=None):
        for i, item in enumerate(iterable):
            yield item

@dataclass
class UrlRecord:
    product_id: str
    url: str
    source_collection: str

# Helper utilities

def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    return str(value).strip() or None


def is_likely_product_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url)
        path = (parsed.path or "").lower()
        exclude_tokens = [
            '/cart', '/checkout', '/customer', '/account', '/login', '/logout',
            '/wishlist', '/compare', '/search', '/catalog/category', '/contact',
            '/privacy', '/terms', '/cookies', '/newsletter', '/order', '/payment',
        ]
        if any(tok in path for tok in exclude_tokens):
            return False
        if path.endswith('.html') and len(path.strip('/').split('/')) >= 1:
            return True
        product_markers = ['product', 'prod', 'glamira-']
        if any(pm in path for pm in product_markers):
            return True
    except Exception:
        return False
    return False


def estimate_accept_language(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        tld = host.rsplit('.', 1)[-1].lower() if '.' in host else ''
        mapping = {
            'de': 'de-DE,de;q=0.8,en;q=0.5',
            'at': 'de-AT,de;q=0.8,en;q=0.5',
            'ch': 'de-CH,de;q=0.8,fr-CH;q=0.6,en;q=0.5',
            'fr': 'fr-FR,fr;q=0.8,en;q=0.5',
            'be': 'fr-BE,fr;q=0.8,nl-BE;q=0.6,en;q=0.5',
            'it': 'it-IT,it;q=0.8,en;q=0.5',
            'es': 'es-ES,es;q=0.8,en;q=0.5',
            'pt': 'pt-PT,pt;q=0.8,en;q=0.5',
            'pl': 'pl-PL,pl;q=0.8,en;q=0.5',
            'cz': 'cs-CZ,cs;q=0.8,en;q=0.5',
            'au': 'en-AU,en;q=0.8',
            'uk': 'en-GB,en;q=0.8',
        }
        return mapping.get(tld, 'en-US,en;q=0.8')
    except Exception:
        return 'en-US,en;q=0.8'


def is_non_product_page(html: str) -> bool:
    lower = html.lower()
    if 'pageType' in lower and 'cart' in lower:
        return True
    if '<meta name="robots" content="noindex,nofollow"' in lower and ('warenkorb' in lower or 'cart' in lower):
        return True
    if '<title>warenkorb' in lower or 'checkout/cart/' in lower:
        return True
    return False


def _extract_canonical_url(html: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        link = soup.find('link', rel=lambda v: v and 'canonical' in str(v).lower())
        if link:
            href = (link.get('href') or '').strip()
            return href or None
    except Exception:
        return None
    return None


def get_memory_usage_mb() -> float:
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def check_memory_limit() -> bool:
    current_mb = get_memory_usage_mb()
    if current_mb > MEMORY_LIMIT_MB:
        logger.warning(f"⚠️ Memory usage {current_mb:.1f}MB exceeds limit {MEMORY_LIMIT_MB}MB")
        return True
    return False


def force_garbage_collection():
    gc.collect()
    logger.info(f"🧹 Garbage collection completed. Memory: {get_memory_usage_mb():.1f}MB")


def stream_filtered_data_batches(batch_size: int = BATCH_SIZE) -> Generator[List[Dict], None, None]:
    logger.info(f"📂 Streaming data from {FILTERED_JSONL} in batches of {batch_size}")
    current_batch = []
    if os.path.exists(FILTERED_JSONL):
        with open(FILTERED_JSONL, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line.strip())
                    current_batch.append(record)
                    if len(current_batch) >= batch_size:
                        yield current_batch
                        current_batch = []
                        if check_memory_limit():
                            force_garbage_collection()
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Skipping invalid line {line_num}: {e}")
                    continue
    else:
        logger.info(f"📂 Streaming data from {FILTERED_CSV} in batches of {batch_size}")
        with open(FILTERED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1):
                current_batch.append(row)
                if len(current_batch) >= batch_size:
                    yield current_batch
                    current_batch = []
                    if check_memory_limit():
                        force_garbage_collection()
    if current_batch:
        yield current_batch
    logger.info("✅ Finished streaming. Total records processed.")


def count_total_products_streaming() -> int:
    product_ids = set()
    if os.path.exists(FILTERED_JSONL):
        with open(FILTERED_JSONL, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line.strip())
                    product_ids.add(record['product_id'])
                except Exception:
                    continue
    else:
        with open(FILTERED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_ids.add(row['product_id'])
    return len(product_ids)


def process_batch_data(batch_data: List[Dict]) -> List[UrlRecord]:
    url_records = []
    for record in batch_data:
        product_id = record.get('product_id')
        url = record.get('url')
        source_collection = record.get('source_collection', 'unknown')
        if product_id and url and is_likely_product_url(url):
            url_records.append(UrlRecord(product_id=product_id, url=_safe_str(url), source_collection=source_collection))
    return url_records


def http_get(url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(random.uniform(0.0, 0.2))
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": estimate_accept_language(url),
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Referer": f"{urlparse(url).scheme}://{urlparse(url).hostname or ''}/",
            }
            host = urlparse(url).hostname or ""
            resp = None
            if _cf_scraper is not None and ("glamira." in host):
                try:
                    resp = _cf_scraper.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, allow_redirects=True, verify=False, proxies=_PROXIES)
                except Exception:
                    resp = None
            if resp is None:
                resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, allow_redirects=True, verify=False, proxies=_PROXIES)
            if resp.status_code == 200:
                try:
                    final_path = urlparse(resp.url).path.lower()
                    if '/checkout/cart' in final_path or '/cart' in final_path:
                        logger.debug(f"Redirected to cart for {url} -> {resp.url}")
                        return resp.text
                except Exception:
                    pass
                return resp.text
            elif resp.status_code == 404:
                logger.debug(f"404 Not Found: {url}")
                return None
            elif resp.status_code == 403:
                logger.debug(f"403 Forbidden: {url}")
                if _cf_scraper is not None:
                    try:
                        cf_resp = _cf_scraper.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, allow_redirects=True, verify=False, proxies=_PROXIES)
                        if cf_resp.status_code == 200:
                            return cf_resp.text
                    except Exception:
                        pass
                return None
            elif resp.status_code in (429, 503):
                if _cf_scraper is not None:
                    try:
                        cf_resp = _cf_scraper.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS, allow_redirects=True, verify=False, proxies=_PROXIES)
                        if cf_resp.status_code == 200:
                            return cf_resp.text
                    except Exception:
                        pass
                logger.debug(f"HTTP {resp.status_code}: {url}")
            else:
                logger.debug(f"HTTP {resp.status_code}: {url}")
        except requests.exceptions.Timeout:
            logger.debug(f"Timeout (attempt {attempt}): {url}")
        except requests.exceptions.ConnectionError:
            logger.debug(f"Connection error (attempt {attempt}): {url}")
        except requests.exceptions.RequestException as e:
            logger.debug(f"Request error (attempt {attempt}): {url} - {e}")
        except Exception as e:
            logger.debug(f"Unexpected error (attempt {attempt}): {url} - {e}")
        if attempt < MAX_RETRIES:
            sleep_time = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            time.sleep(sleep_time)
    return None


def extract_product_name(html: str) -> Optional[str]:
    lower_html = html.lower()
    bot_wall_indicators = ["cloudflare", "captcha", "attention required", "access denied"]
    if any(ind in lower_html for ind in bot_wall_indicators):
        return None
    soup = BeautifulSoup(html, "html.parser")
    for script in soup(["script", "style"]):
        script.decompose()
    try:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(script.string or "")
            except Exception:
                continue
            def extract_name(obj) -> Optional[str]:
                if not isinstance(obj, dict):
                    return None
                obj_type = obj.get("@type", "") or ""
                if str(obj_type).lower() == "product" and obj.get("name"):
                    return str(obj["name"]).strip()
                if "@graph" in obj and isinstance(obj["@graph"], list):
                    for node in obj["@graph"]:
                        n = extract_name(node)
                        if n:
                            return n
                if "offers" in obj and isinstance(obj["offers"], dict):
                    n = obj["offers"].get("name")
                    if n:
                        return str(n).strip()
                return None
            candidate = extract_name(data)
            if candidate and 3 < len(candidate) < 200:
                cleaned = candidate.replace("\n", " ").replace("\t", " ").replace("\r", " ")
                cleaned = " ".join(cleaned.split())
                for sep in [" | ", " - ", " — ", " – "]:
                    if sep in cleaned:
                        cleaned = cleaned.split(sep)[0].strip()
                for token in ["glamira", "store", "shop"]:
                    if cleaned.lower().endswith(token):
                        cleaned = cleaned[: -len(token)].strip(" -|•")
                if cleaned and 3 < len(cleaned) < 200:
                    return cleaned
    except Exception:
        pass
    try:
        matches = []
        matches += re.findall(r"window\\.dataLayer\\.push\\((\{[\s\S]*?\})\)\s*;?", html)
        matches += re.findall(r"\bdataLayer\\.push\\((\{[\s\S]*?\})\)\s*;?", html)
        for m in matches:
            try:
                obj = json.loads(m)
            except Exception:
                try:
                    fixed = re.sub(r",\s*([}\]])", r"\1", m)
                    obj = json.loads(fixed)
                except Exception:
                    continue
            if not isinstance(obj, dict):
                continue
            product = obj.get("product") or obj.get("ecommerce", {}).get("detail", {}).get("products", [{}])[0]
            if isinstance(product, dict):
                candidate = product.get("name") or product.get("sku") or product.get("parent_sku")
                if candidate and 2 < len(str(candidate)) < 200:
                    cleaned = str(candidate).strip()
                    cleaned = cleaned.replace("\n", " ").replace("\t", " ").replace("\r", " ")
                    cleaned = " ".join(cleaned.split())
                    return cleaned
    except Exception:
        pass
    try:
        win_prod_match = re.search(r"window\\.product\\s*=\\s*\\{[\s\S]*?\\};", html)
        if win_prod_match:
            block = win_prod_match.group(0)
            sku_match = re.search(r"product\\.sku\\s*=\\s*['\"]([^'\"]+)['\"]", block)
            if sku_match:
                sku_val = sku_match.group(1).strip()
                if 2 < len(sku_val) < 200:
                    return sku_val
    except Exception:
        pass
    selectors = [
        "h1.page-title span.base",
        "h1.page-title",
        ".page-title",
        "h1.product-title",
        "h1.product-name",
        ".product-title",
        ".product-name",
        ".product-info h1",
        ".product-details h1",
        ".product-header h1",
        "[itemprop='name']",
        "[data-testid='product-title']",
        "[data-testid='product-name']",
        "[data-role='product-name']",
        "meta[property='og:title']",
        "meta[name='title']",
        "h1",
        "title",
    ]
    for sel in selectors:
        try:
            text = None
            if sel.startswith("meta"):
                el = soup.select_one(sel)
                if el:
                    text = el.get('content', '').strip()
            else:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(strip=True)
            if text and len(text) > 3 and len(text) < 300:
                text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
                text = ' '.join(text.split())
                for sep in [" | ", " - ", " — ", " – "]:
                    if sep in text:
                        text = text.split(sep)[0].strip()
                marketing_prefixes = ["Kaufen Sie ", "Achetez ", "Acheter ", "Buy ", "Compra ", "Compre "]
                for pref in marketing_prefixes:
                    if text.startswith(pref):
                        text = text[len(pref):].strip()
                skip_phrases = ["404", "not found", "error", "page not found", "home", "shop", "store", "catalog", "products"]
                if not any(phrase in text.lower() for phrase in skip_phrases):
                    return text
        except Exception as e:
            logger.debug(f"Error extracting with selector '{sel}': {e}")
            continue
    return None


def _derive_name_from_slug(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        path = parsed.path
        if not path or ".html" not in path:
            return None
        slug = path.rsplit('/', 1)[-1]
        slug = slug.split('.html', 1)[0]
        prefixes = ['glamira-', 'bague-', 'ring-', 'anneau-', 'verlobungsring-', 'eheringe-', 'pierscionki-', 'prsten-', 'collier-', 'pendant-', 'necklace-', 'earring-']
        for p in prefixes:
            if slug.startswith(p):
                slug = slug[len(p):]
                break
        words = [w for w in re.split(r"[-_]+", slug) if w]
        if not words:
            return None
        name = " ".join(w.capitalize() if w.isalpha() else w for w in words)
        if 2 < len(name) < 200:
            return name
    except Exception:
        return None
    return None


def process_single_url(record: UrlRecord) -> Dict:
    global _DEBUG_HTML_SAVED
    html = http_get(record.url)
    product_name = None
    status = "failed"
    if html:
        if is_non_product_page(html):
            recovered_html = None
            new_url = _extract_canonical_url(html)
            if not new_url:
                try:
                    parsed = urlparse(record.url)
                    new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
                except Exception:
                    new_url = None
            if new_url and new_url != record.url:
                recovered_html = http_get(new_url)
                if recovered_html and not is_non_product_page(recovered_html):
                    html = recovered_html
                    record = UrlRecord(product_id=record.product_id, url=new_url, source_collection=record.source_collection)
                else:
                    recovered_html = None
        if is_non_product_page(html):
            if _DEBUG_HTML_SAVED < _DEBUG_HTML_MAX:
                try:
                    parsed = urlparse(record.url)
                    host = (parsed.hostname or 'unknown').replace(':', '_')
                    fname = f"debug_html_{host}_{int(time.time())}_{_DEBUG_HTML_SAVED}.html"
                    with open(fname, 'w', encoding='utf-8') as f:
                        f.write(html)
                    logger.info(f"🧪 Saved debug HTML to {fname}")
                    _DEBUG_HTML_SAVED += 1
                except Exception:
                    pass
            return {"product_id": record.product_id, "url": record.url, "source_collection": record.source_collection, "product_name": None, "status": "non_product_page", "fetched_at": int(time.time())}
        product_name = extract_product_name(html)
        if product_name:
            status = "success"
        else:
            slug_name = _derive_name_from_slug(record.url)
            if slug_name:
                product_name = slug_name
                status = "slug_heuristic"
            else:
                status = "no_name_found"
                if _DEBUG_HTML_SAVED < _DEBUG_HTML_MAX:
                    try:
                        parsed = urlparse(record.url)
                        host = (parsed.hostname or 'unknown').replace(':', '_')
                        fname = f"debug_html_{host}_{int(time.time())}_{_DEBUG_HTML_SAVED}.html"
                        with open(fname, 'w', encoding='utf-8') as f:
                            f.write(html)
                        logger.info(f"🧪 Saved debug HTML to {fname}")
                        _DEBUG_HTML_SAVED += 1
                    except Exception:
                        pass
    else:
        status = "no_html"
        logger.debug(f"No HTML for {record.url[:50]}...")
    return {"product_id": record.product_id, "url": record.url, "source_collection": record.source_collection, "product_name": product_name, "status": status, "fetched_at": int(time.time())}


def crawl_product_names_parallel(records: List[UrlRecord]) -> List[Dict]:
    results = []
    total = len(records)
    completed = 0
    initial_memory = get_memory_usage_mb()
    logger.info(f"🧠 Starting crawl with {total} records. Initial memory: {initial_memory:.1f}MB")
    try:
        progress = tqdm(total=total, desc="Crawling products")
        has_progress_update = hasattr(progress, "update")
    except Exception:
        progress = None
        has_progress_update = False
    log_every = max(1, total // 20) if total > 0 else 1
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_single_url, r) for r in records]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.warning(f"Error: {e}")
            finally:
                completed += 1
                if completed % log_every == 0 or completed == total:
                    current_memory = get_memory_usage_mb()
                    percent = (completed * 100.0 / total) if total else 100.0
                    logger.info(f"⏳ Progress: {completed}/{total} ({percent:.1f}%) - Memory: {current_memory:.1f}MB")
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
    final_memory = get_memory_usage_mb()
    logger.info(f"🧠 Finished crawl. Memory: {final_memory:.1f}MB (Δ: {final_memory - initial_memory:+.1f}MB)")
    return results


def deduplicate_by_product_id(candidates: List[Dict]) -> List[Dict]:
    if not candidates:
        return []
    grouped = {}
    for row in candidates:
        pid = row.get("product_id")
        if not pid:
            continue
        status = row.get("status", "unknown")
        has_name = bool(row.get("product_name"))
        if pid not in grouped:
            grouped[pid] = row
        else:
            existing_status = grouped[pid].get("status", "unknown")
            existing_has_name = bool(grouped[pid].get("product_name"))
            if has_name and not existing_has_name:
                grouped[pid] = row
            elif has_name and existing_has_name:
                if status == "success" and existing_status != "success":
                    grouped[pid] = row
    unique_list = list(grouped.values())
    original_count = len(candidates)
    unique_count = len(unique_list)
    if original_count != unique_count:
        logger.info(f"🔗 Deduplicated: {original_count} -> {unique_count} unique products (removed {original_count - unique_count} duplicates)")
    return unique_list


def append_candidates_jsonl(candidates: List[Dict], path: str = CANDIDATES_JSONL) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for row in candidates:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_final_csv(candidates: List[Dict], path: str = FINAL_CSV) -> None:
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "product_name", "url", "source_collection", "status", "fetched_at"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(candidates)


# Main loop

def main():
    total_expected = count_total_products_streaming()
    logger.info(f"📊 Total distinct product IDs from filtered data: {total_expected}")
    logger.info(f"🧠 Initial memory usage: {get_memory_usage_mb():.1f}MB")
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
    for batch_data in stream_filtered_data_batches(batch_size=BATCH_SIZE):
        batch_count += 1
        original_batch_size = len(batch_data)
        batch_data = [record for record in batch_data if record.get('product_id') not in existing_pids]
        filtered_batch_size = len(batch_data)
        logger.info(f"📊 Batch {batch_count}: {original_batch_size} -> {filtered_batch_size} products (filtered: {original_batch_size - filtered_batch_size})")
        if not batch_data:
            logger.info(f"⏭️ Batch {batch_count}: All products already processed, skipping")
            continue
        logger.info(f"🔄 Processing batch {batch_count} with {len(batch_data)} products...")
        logger.info(f"🧠 Memory before batch: {get_memory_usage_mb():.1f}MB")
        url_records = process_batch_data(batch_data)
        if not url_records:
            logger.warning(f"⚠️ Batch {batch_count}: No valid URL records found")
            continue
        candidates = crawl_product_names_parallel(url_records)
        unique_candidates = deduplicate_by_product_id(candidates)
        append_candidates_jsonl(candidates)
        named_only = [row for row in unique_candidates if row.get("product_name")]
        if named_only:
            append_final_csv(named_only)
        for row in unique_candidates:
            existing_pids.add(row.get("product_id"))
        processed_batch = len(unique_candidates)
        with_name_batch = len(named_only)
        status_counts = {}
        for row in unique_candidates:
            status = row.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        total_processed += processed_batch
        total_with_name += with_name_batch
        percent = (total_processed / total_expected) * 100 if total_expected else 0
        logger.info(f"✅ Finished batch {batch_count}. Processed: {processed_batch}, With names: {with_name_batch}")
        logger.info(f"📊 Total progress: {total_processed}/{total_expected} ({percent:.2f}% done)")
        logger.info(f"📊 Status breakdown: {status_counts}")
        logger.info(f"🧠 Memory after batch: {get_memory_usage_mb():.1f}MB")
        force_garbage_collection()
        if check_memory_limit():
            logger.warning("⚠️ Memory limit reached, forcing cleanup...")
            time.sleep(2)
    logger.info(f"🎯 DONE! {total_with_name}/{total_expected} products crawled successfully.")
    logger.info(f"🧠 Final memory usage: {get_memory_usage_mb():.1f}MB")

if __name__ == "__main__":
    main()
