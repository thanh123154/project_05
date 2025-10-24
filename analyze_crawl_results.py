#!/usr/bin/env python3
"""
Script để phân tích kết quả crawl và tìm vấn đề
"""

import csv
import json
import os
from collections import Counter

def analyze_crawl_results():
    """Phân tích kết quả crawl để tìm vấn đề"""
    print("🔍 Analyzing crawl results...")
    
    # Kiểm tra input data
    input_files = ["product_urls.jsonl", "merged_data.csv"]
    input_data = []
    
    for file in input_files:
        if os.path.exists(file):
            print(f"📂 Found input file: {file}")
            if file.endswith('.jsonl'):
                with open(file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            input_data.append(record)
                        except:
                            continue
            else:
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        input_data.append(row)
            break
    
    if not input_data:
        print("❌ No input data found!")
        return
    
    print(f"📊 Total input records: {len(input_data)}")
    
    # Kiểm tra output data
    output_file = "product_names_final.csv"
    if not os.path.exists(output_file):
        print("❌ No output file found!")
        return
    
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        output_data = list(reader)
    
    print(f"📊 Total output records: {len(output_data)}")
    
    # Phân tích status
    status_counts = Counter()
    for row in output_data:
        status = row.get('status', 'unknown')
        status_counts[status] += 1
    
    print(f"📊 Status breakdown:")
    for status, count in status_counts.most_common():
        print(f"   {status}: {count}")
    
    # Phân tích URLs
    print(f"\n🔍 URL Analysis:")
    sample_urls = []
    for row in output_data[:10]:
        url = row.get('url', '')
        status = row.get('status', '')
        sample_urls.append((url[:50] + '...' if len(url) > 50 else url, status))
    
    for url, status in sample_urls:
        print(f"   {url} -> {status}")
    
    # Kiểm tra product names
    with_names = [row for row in output_data if row.get('product_name')]
    print(f"\n📊 Products with names: {len(with_names)}")
    
    if with_names:
        print("   Sample product names:")
        for row in with_names[:5]:
            name = row.get('product_name', '')
            print(f"     {name[:50]}...")
    
    # Phân tích vấn đề
    print(f"\n🔍 Problem Analysis:")
    success_rate = len(with_names) / len(output_data) * 100 if output_data else 0
    print(f"   Success rate: {success_rate:.1f}%")
    
    if success_rate < 5:
        print("   ❌ Very low success rate - possible issues:")
        print("      - URLs might be invalid or blocked")
        print("      - Website structure changed")
        print("      - Network/timeout issues")
        print("      - User-Agent blocked")
    
    # Kiểm tra source collections
    source_counts = Counter()
    for row in output_data:
        source = row.get('source_collection', 'unknown')
        source_counts[source] += 1
    
    print(f"\n📊 Source collections:")
    for source, count in source_counts.most_common():
        print(f"   {source}: {count}")

if __name__ == "__main__":
    analyze_crawl_results()
