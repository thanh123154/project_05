#!/usr/bin/env python3
"""
Test script để test crawl một vài URLs để debug vấn đề
"""

import sys
import json
import csv
from crawler3 import process_single_url, UrlRecord

def test_single_urls():
    """Test crawl một vài URLs để debug"""
    print("🧪 Testing single URL crawling...")
    
    # Lấy một vài URLs từ input data
    test_urls = []
    
    # Thử đọc từ JSONL trước
    try:
        with open("product_urls.jsonl", "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 5:  # Chỉ lấy 5 URLs đầu
                    break
                try:
                    record = json.loads(line.strip())
                    test_urls.append(record)
                except:
                    continue
    except FileNotFoundError:
        # Fallback to CSV
        try:
            with open("merged_data.csv", "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= 5:
                        break
                    test_urls.append(row)
        except FileNotFoundError:
            print("❌ No input files found!")
            return
    
    if not test_urls:
        print("❌ No test URLs found!")
        return
    
    print(f"📊 Testing {len(test_urls)} URLs...")
    
    for i, url_data in enumerate(test_urls, 1):
        url = url_data.get('url', '')
        product_id = url_data.get('product_id', '')
        
        print(f"\n🔍 Test {i}: {url[:60]}...")
        print(f"   Product ID: {product_id}")
        
        # Tạo UrlRecord
        record = UrlRecord(
            product_id=product_id,
            url=url,
            source_collection=url_data.get('source_collection', 'test')
        )
        
        # Test crawl
        result = process_single_url(record)
        
        print(f"   Status: {result['status']}")
        if result['product_name']:
            print(f"   Product Name: {result['product_name']}")
        else:
            print(f"   No product name found")
        
        # Nếu có vấn đề, hiển thị thêm thông tin
        if result['status'] == 'no_html':
            print(f"   ⚠️ Could not fetch HTML - possible issues:")
            print(f"      - URL invalid or blocked")
            print(f"      - Network timeout")
            print(f"      - Server error")
        elif result['status'] == 'no_name_found':
            print(f"   ⚠️ HTML fetched but no product name found - possible issues:")
            print(f"      - Website structure changed")
            print(f"      - Product page not found")
            print(f"      - Selectors need updating")

if __name__ == "__main__":
    test_single_urls()
