#!/usr/bin/env python3
"""
Debug script để kiểm tra tại sao tỷ lệ thành công thấp
"""

import json
import requests
from bs4 import BeautifulSoup
import random
import time
import urllib.parse
from collections import defaultdict
import re

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

TRACKING_PARAMS = {
    "gclid",
    "fbclid",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
}


def sanitize_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        filtered = [(k, v) for (k, v) in query if k not in TRACKING_PARAMS]
        new_query = urllib.parse.urlencode(filtered, doseq=True)
        rebuilt = parsed._replace(query=new_query)
        return urllib.parse.urlunsplit(rebuilt)
    except Exception:
        return url


DOMAIN_LANG = {
    ".fr": "fr-FR,fr;q=0.9,en;q=0.8",
    ".be": "fr-BE,fr;q=0.9,nl;q=0.8,en;q=0.7",
    ".pl": "pl-PL,pl;q=0.9,en;q=0.8",
    ".cz": "cs-CZ,cs;q=0.9,en;q=0.8",
    ".ch": "de-CH,de;q=0.9,fr-CH,fr;q=0.8,it-CH,it;q=0.7,en;q=0.6",
    ".pt": "pt-PT,pt;q=0.9,en;q=0.8",
    ".at": "de-AT,de;q=0.9,en;q=0.8",
    ".za": "en-ZA,en;q=0.9",
    ".com.au": "en-AU,en;q=0.9",
}


def guess_accept_language(domain: str) -> str:
    for tld, lang in DOMAIN_LANG.items():
        if domain.endswith(tld):
            return lang
    return "en-US,en;q=0.9"


def build_headers(referrer: str | None = None, domain: str | None = None) -> dict:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": guess_accept_language(domain or ""),
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Chromium";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if referrer:
        headers["Referer"] = referrer
    return headers


def get_with_retries(url: str, timeout: int = 10, max_retries: int = 3):
    url = sanitize_url(url)
    sess = requests.Session()
    domain = urllib.parse.urlsplit(url).netloc
    headers = build_headers(domain=domain)
    scraper = None
    try:
        import cloudscraper  # type: ignore

        scraper = cloudscraper.create_scraper()
    except Exception:
        scraper = None

    last_resp = None
    for attempt in range(1, max_retries + 1):
        try:
            # Warm-up: visit homepage to receive cookies and geolocation
            homepage = f"https://{domain}/"
            try:
                sess.get(homepage, headers=headers, timeout=timeout, allow_redirects=True)
                # small randomized delay
                time.sleep(0.4 + random.random() * 0.6)
            except Exception:
                pass

            # Use homepage as referer
            headers_with_ref = dict(headers)
            headers_with_ref["Referer"] = homepage

            resp = sess.get(url, headers=headers_with_ref, timeout=timeout, allow_redirects=True)
            last_resp = resp
            if resp.status_code == 200:
                return resp, headers, url, False
            if resp.status_code in (403, 429, 503) and scraper is not None:
                try:
                    scr_resp = scraper.get(url, headers=headers_with_ref, timeout=timeout, allow_redirects=True)
                    if scr_resp.status_code == 200:
                        return scr_resp, headers, url, True
                except Exception:
                    pass
        except requests.RequestException:
            pass
        if attempt < max_retries:
            time.sleep(1 * (2 ** (attempt - 1)))
    return last_resp, headers, url, False

def test_url(url: str):
    """Test một URL để xem tại sao không lấy được product name"""
    print(f"\n🔍 Testing URL: {url}")
    
    try:
        resp, headers_used, final_url, used_scraper = get_with_retries(url)
        if resp is None:
            print("❌ No response received")
            return {
                "url": url,
                "final_url": url,
                "domain": urllib.parse.urlsplit(url).netloc,
                "status_code": None,
                "content_len": 0,
                "used_scraper": used_scraper,
                "name_found": False,
            }
        print(f"Status Code: {resp.status_code}")
        print(f"Final URL: {resp.url}")
        print(f"Content Length: {len(resp.text)}")
        print(f"Used cloudscraper: {used_scraper}")
        # Print a small snippet to detect WAF block pages
        snippet = resp.text[:300].replace("\n", " ")
        print(f"Snippet: {snippet[:200]}{'...' if len(snippet)>200 else ''}")
        
        name_found = False
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Test các selector
            selectors = [
                "h1.page-title span.base",
                "h1.product-title", 
                "h1.product-name",
                "h1",
                ".product-title",
                ".product-name", 
                ".page-title",
                "title"
            ]
            
            found_selectors = []
            for sel in selectors:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(strip=True)
                    if text and len(text) > 3:
                        found_selectors.append(f"{sel}: '{text}'")
                        if not name_found:
                            name_found = True
            
            if found_selectors:
                print("✅ Found selectors:")
                for sel in found_selectors:
                    print(f"  - {sel}")
            else:
                print("❌ No product name found")
                print("Available h1 tags:")
                for h1 in soup.find_all('h1'):
                    print(f"  - {h1.get_text(strip=True)}")
                print("Available title tag:")
                title = soup.find('title')
                if title:
                    print(f"  - {title.get_text(strip=True)}")
        else:
            print(f"❌ HTTP Error: {resp.status_code}")
        return {
            "url": url,
            "final_url": resp.url,
            "domain": urllib.parse.urlsplit(resp.url).netloc,
            "status_code": resp.status_code,
            "content_len": len(resp.text) if hasattr(resp, "text") and resp.text is not None else 0,
            "used_scraper": used_scraper,
            "name_found": name_found,
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "url": url,
            "final_url": url,
            "domain": urllib.parse.urlsplit(url).netloc,
            "status_code": None,
            "content_len": 0,
            "used_scraper": False,
            "name_found": False,
        }

def main():
    print("🐛 Debug Crawler - Testing sample URLs")
    
    # Đọc một vài URLs từ file để test
    try:
        with open("product_urls.jsonl", "r", encoding="utf-8") as f:
            urls_to_test = []
            for i, line in enumerate(f):
                if i >= 10:  # Test 10 URLs đầu
                    break
                try:
                    data = json.loads(line.strip())
                    urls_to_test.append(data['url'])
                except:
                    continue
            
            print(f"📊 Testing {len(urls_to_test)} sample URLs...")
            results = []
            for url in urls_to_test:
                res = test_url(url)
                if isinstance(res, dict):
                    results.append(res)

            # Tổng kết
            total = len(results)
            ok = sum(1 for r in results if r.get("status_code") == 200)
            name_ok = sum(1 for r in results if r.get("name_found"))
            used_scraper = sum(1 for r in results if r.get("used_scraper"))
            print("\n===== Summary =====")
            print(f"Total tested: {total}")
            print(f"HTTP 200: {ok} ({(ok/total*100 if total else 0):.1f}%)")
            print(f"Name found: {name_ok} ({(name_ok/total*100 if total else 0):.1f}%)")
            print(f"Used cloudscraper success: {used_scraper}")

            # Breakdown theo domain
            by_domain = defaultdict(lambda: {"count":0, "ok":0, "name":0})
            for r in results:
                d = r.get("domain") or ""
                by_domain[d]["count"] += 1
                if r.get("status_code") == 200:
                    by_domain[d]["ok"] += 1
                if r.get("name_found"):
                    by_domain[d]["name"] += 1

            print("\nPer-domain:")
            for d, stats in sorted(by_domain.items(), key=lambda x: -x[1]["count"]):
                cnt = stats["count"]
                okd = stats["ok"]
                named = stats["name"]
                print(f"- {d}: total={cnt}, 200={okd} ({(okd/cnt*100 if cnt else 0):.1f}%), name={named} ({(named/cnt*100 if cnt else 0):.1f}%)")
                
    except FileNotFoundError:
        print("❌ File product_urls.jsonl not found. Run data_filter.py first.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

