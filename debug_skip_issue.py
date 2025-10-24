#!/usr/bin/env python3
"""
Debug script để kiểm tra tại sao tất cả batch đều bị skip
"""

import csv
import json
import os
from typing import Set, List, Dict

FILTERED_JSONL = "product_urls.jsonl"
FILTERED_CSV = "merged_data.csv"
FINAL_CSV = "product_names_final.csv"

def check_existing_products() -> Set[str]:
    """Check what products are already processed"""
    existing_pids = set()
    if os.path.exists(FINAL_CSV):
        with open(FINAL_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_pids.add(row["product_id"])
    return existing_pids

def check_input_data() -> List[Dict]:
    """Check what products are in input data"""
    input_data = []
    
    if os.path.exists(FILTERED_JSONL):
        print(f"📂 Reading from {FILTERED_JSONL}")
        with open(FILTERED_JSONL, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line.strip())
                    input_data.append(record)
                    if line_num <= 5:  # Show first 5 records
                        print(f"  Sample {line_num}: {record.get('product_id')} - {record.get('url')[:50]}...")
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"  Invalid line {line_num}: {e}")
                    continue
    else:
        print(f"📂 Reading from {FILTERED_CSV}")
        with open(FILTERED_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1):
                input_data.append(row)
                if row_num <= 5:  # Show first 5 records
                    print(f"  Sample {row_num}: {row.get('product_id')} - {row.get('url')[:50]}...")
    
    return input_data

def analyze_skip_issue():
    """Analyze why all batches are being skipped"""
    print("🔍 Debugging skip issue...")
    
    # Check existing products
    existing_pids = check_existing_products()
    print(f"📋 Found {len(existing_pids)} already processed products")
    if len(existing_pids) > 0:
        print(f"  Sample existing IDs: {list(existing_pids)[:5]}")
    
    # Check input data
    input_data = check_input_data()
    print(f"📊 Total input records: {len(input_data)}")
    
    # Get unique product IDs from input
    input_pids = set()
    for record in input_data:
        pid = record.get('product_id')
        if pid:
            input_pids.add(pid)
    
    print(f"📊 Unique product IDs in input: {len(input_pids)}")
    if len(input_pids) > 0:
        print(f"  Sample input IDs: {list(input_pids)[:5]}")
    
    # Check overlap
    overlap = existing_pids.intersection(input_pids)
    print(f"📊 Overlap between existing and input: {len(overlap)}")
    
    # Check what would be processed
    new_pids = input_pids - existing_pids
    print(f"📊 New product IDs to process: {len(new_pids)}")
    
    if len(new_pids) == 0:
        print("❌ PROBLEM: No new products to process!")
        print("   This means all input products are already in the output file.")
        
        # Check if output file is too large
        if os.path.exists(FINAL_CSV):
            file_size = os.path.getsize(FINAL_CSV)
            print(f"   Output file size: {file_size:,} bytes ({file_size/1024/1024:.1f}MB)")
            
            # Check if we should reset
            if file_size > 100 * 1024 * 1024:  # > 100MB
                print("   ⚠️ Output file is very large. Consider backing up and starting fresh.")
    else:
        print(f"✅ Found {len(new_pids)} new products to process")
        print(f"   Sample new IDs: {list(new_pids)[:5]}")

if __name__ == "__main__":
    analyze_skip_issue()
