#!/usr/bin/env python3
"""
Script để backup và reset output files của crawler
"""

import os
import shutil
import time
from datetime import datetime

def backup_and_reset():
    """Backup existing output files and reset for fresh start"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"backup_{timestamp}"
    
    output_files = [
        "product_name_candidates.jsonl",
        "product_names_final.csv"
    ]
    
    print(f"🔄 Creating backup directory: {backup_dir}")
    os.makedirs(backup_dir, exist_ok=True)
    
    for file in output_files:
        if os.path.exists(file):
            backup_path = os.path.join(backup_dir, file)
            shutil.copy2(file, backup_path)
            print(f"📦 Backed up {file} -> {backup_path}")
            
            # Get file size
            size = os.path.getsize(file)
            print(f"   Size: {size:,} bytes ({size/1024/1024:.1f}MB)")
            
            # Remove original
            os.remove(file)
            print(f"🗑️ Removed {file}")
        else:
            print(f"ℹ️ {file} not found, skipping")
    
    print(f"✅ Backup completed. Files backed up to {backup_dir}/")
    print("🚀 You can now run crawler3.py with fresh start")

if __name__ == "__main__":
    backup_and_reset()
