#!/usr/bin/env python3
"""
Debug script để kiểm tra tại sao tỷ lệ thành công thấp
"""

import json
import requests
from bs4 import BeautifulSoup
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

def test_url(url: str):
    """Test một URL để xem tại sao không lấy được product name"""
    print(f"\n🔍 Testing URL: {url}")
    
    try:
        resp = requests.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, 
                          timeout=10, allow_redirects=True)
        print(f"Status Code: {resp.status_code}")
        print(f"Final URL: {resp.url}")
        print(f"Content Length: {len(resp.text)}")
        
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
            
    except Exception as e:
        print(f"❌ Error: {e}")

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
            
            for url in urls_to_test:
                test_url(url)
                
    except FileNotFoundError:
        print("❌ File product_urls.jsonl not found. Run data_filter.py first.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()

