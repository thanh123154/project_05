import os
import re
import json
import time
import csv
import requests
import logging
from urllib.parse import urlparse
from typing import List, Optional, Dict
from dataclasses import dataclass
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings
import argparse

# Cài đặt để bỏ qua cảnh báo SSL
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
                # JSON-LD có thể chứa list
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
    """Derive product name from URL slug as a last resort."""
    parsed = urlparse(url)
    slug = parsed.path.rsplit('/', 1)[-1]
    slug = slug.split('.html', 1)[0]
    # Tách slug bằng dấu gạch ngang/dấu gạch dưới
    parts = [w for w in re.split(r"[-_]+", slug) if w]
    if not parts:
        return None
    # Ghép lại, chỉ giữ lại các từ là chữ cái, viết hoa chữ cái đầu
    name = " ".join(p.capitalize() for p in parts if p.isalpha())
    return name.strip() if name else None


def is_likely_product_url(url: str) -> bool:
    """Tình trạng lọc URL hiện tại của bạn."""
    return "glamira" in (url or "") and ".html" in (url or "")


def crawl_product_names_parallel(records: List[UrlRecord], debug_limit: int = 10) -> List[Dict]:
    """Crawl sản phẩm và trích xuất tên. Đã sửa lỗi logic trạng thái."""
    results = []
    for idx, rec in enumerate(tqdm(records, desc="Crawling products")):
        html = http_get(rec.url)
        product_name = None
        status = "no_html"
        
        if html:
            # 1. Thử trích xuất từ HTML/JSON-LD
            product_name = extract_product_name(html)
            
            if product_name:
                # ✅ SỬA LỖI: Gán trạng thái thành công khi tìm thấy tên
                status = "success" 
            else:
                # 2. Thử slug heuristic
                slug_name = _derive_name_from_slug(rec.url)
                if slug_name:
                    product_name = slug_name
                    status = "slug_heuristic"
                else:
                    status = "no_name_found"

            # Lưu debug HTML cho các trang đầu tiên
            if idx < debug_limit:
                host = urlparse(rec.url).netloc.replace(".", "_")
                fname = f"debug_html_{host}_{int(time.time())}_{rec.product_id}.html"
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"🧪 Saved debug HTML to {fname}")
                
        results.append({
            "product_id": rec.product_id,
            "product_name": product_name or "",
            "url": rec.url,
            "source_collection": rec.source_collection,
            "status": status,
            "fetched_at": int(time.time())
        })
        # Ghi log tiến trình ít hơn để tránh quá tải console
        if (idx + 1) % 100 == 0 or idx + 1 == len(records):
            logger.info(f"⏳ Progress: {idx+1}/{len(records)} ({(idx+1)/len(records)*100:.1f}%)")
            
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
# CLI (ĐÃ SỬA LỖI LỌC DỮ LIỆU ĐẦU VÀO)
# =================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl product names from URLs")
    parser.add_argument("--input", type=str, default="merged_data.csv", help="Input CSV file path")
    parser.add_argument("--n", type=int, default=20000, help="Max number of *distinct* products to crawl")
    args = parser.parse_args()

    # SỬ DỤNG DICT ĐỂ GOM TẤT CẢ URL VÀ CHỌN URL TỐT NHẤT CHO MỖI PRODUCT_ID
    best_records: Dict[str, UrlRecord] = {} 
    total_rows_read = 0

    logger.info(f"Reading input data from {args.input}...")

    try:
        # Đọc dữ liệu thô
        with open(args.input, "r", encoding="utf-8") as f:
            # LƯU Ý: Đảm bảo fieldnames khớp với cấu trúc file CSV gốc của bạn
            reader = csv.DictReader(f, fieldnames=["product_id", "url", "source_collection", "fetched_at"])
            
            # Sử dụng tqdm để hiển thị tiến trình đọc file
            for row in tqdm(reader, desc="Processing input data"):
                total_rows_read += 1
                pid = str(row.get("product_id")).strip()
                url = row.get("url")
                
                if not pid or not url:
                    continue

                # Logic chọn URL TỐT NHẤT cho mỗi ID:
                is_good_url = is_likely_product_url(url)
                
                # A. Nếu chưa có bản ghi nào cho ID này, chọn URL hiện tại
                if pid not in best_records:
                    best_records[pid] = UrlRecord(pid, url, row.get("source_collection", "unknown"))
                
                # B. Nếu đã có bản ghi, CHỈ CẬP NHẬT nếu URL MỚI TỐT HƠN URL CŨ
                # (Tức là URL hiện tại là URL không hợp lệ, nhưng URL mới hợp lệ)
                elif is_good_url and not is_likely_product_url(best_records[pid].url):
                    best_records[pid] = UrlRecord(pid, url, row.get("source_collection", "unknown"))
                
                # Dừng nếu đã đạt đến giới hạn N (dành cho ID duy nhất)
                if len(best_records) >= args.n:
                    break
        
    except Exception as e:
        logger.error(f"Failed to read input file {args.input}: {e}")
        exit(1)

    # Lấy danh sách các bản ghi tốt nhất để crawl (Đã lọc ID duy nhất)
    url_records = list(best_records.values())

    logger.info(f"Total rows read from input: {total_rows_read}")
    logger.info(f"Total input (distinct) products: {len(url_records)}") 
    
    # -----------------------------------------------------------------
    # BẮT ĐẦU CRAWL
    # -----------------------------------------------------------------
    logger.info(f"Starting crawl for {len(url_records)} URLs...")
    results = crawl_product_names_parallel(url_records)
    
    # Xử lý kết quả
    append_candidates_jsonl(results)
    
    named_only = [r for r in results if r.get("product_name")]
    
    logger.info(f"Total successfully crawled product_name: {len(named_only)}")
    
    if len(named_only) < len(url_records):
        missed = [r['product_id'] for r in results if not r.get("product_name")]
        logger.warning(f"Products could not be named: {missed[:10]}... (Total: {len(missed)})")
    
    if named_only:
        append_final_csv(named_only)
        logger.info(f"✅ Saved {len(named_only)} named products to product_names_final.csv.")
    else:
        logger.warning("⚠️ No product names extracted.")