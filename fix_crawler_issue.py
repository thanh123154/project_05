#!/usr/bin/env python3
"""
Script để fix vấn đề crawler bị skip tất cả batch
"""

import os
import csv
import json

def analyze_and_fix():
    """Analyze the issue and provide solutions"""
    print("🔍 Analyzing crawler skip issue...")
    
    # Check if output files exist
    output_files = ["product_names_final.csv", "product_name_candidates.jsonl"]
    existing_files = [f for f in output_files if os.path.exists(f)]
    
    if not existing_files:
        print("✅ No output files found - crawler should work normally")
        return
    
    print(f"📋 Found existing output files: {existing_files}")
    
    # Check file sizes
    for file in existing_files:
        if os.path.exists(file):
            size = os.path.getsize(file)
            print(f"   {file}: {size:,} bytes ({size/1024/1024:.1f}MB)")
    
    # Check if CSV has data
    if os.path.exists("product_names_final.csv"):
        with open("product_names_final.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            print(f"📊 CSV has {len(rows)} rows")
            
            if len(rows) > 0:
                print(f"   Sample product IDs: {[row['product_id'] for row in rows[:3]]}")
    
    print("\n🔧 Solutions:")
    print("1. Set FORCE_PROCESS = True in crawler3.py (line 39)")
    print("2. Or backup and reset output files:")
    print("   python reset_crawler.py")
    print("3. Or run debug script to see details:")
    print("   python debug_skip_issue.py")

if __name__ == "__main__":
    analyze_and_fix()
