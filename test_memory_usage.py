#!/usr/bin/env python3
"""
Test script để kiểm tra memory usage của crawler3.py
"""

import psutil
import gc
import time

def get_memory_usage_mb():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def test_memory_monitoring():
    """Test memory monitoring functions"""
    print("🧠 Memory Usage Test")
    print(f"Initial memory: {get_memory_usage_mb():.1f}MB")
    
    # Simulate memory usage
    data = []
    for i in range(1000):
        data.append(f"test_data_{i}" * 100)  # Create some memory usage
        if i % 100 == 0:
            current_memory = get_memory_usage_mb()
            print(f"After {i} iterations: {current_memory:.1f}MB")
    
    print(f"Peak memory: {get_memory_usage_mb():.1f}MB")
    
    # Test garbage collection
    del data
    gc.collect()
    print(f"After GC: {get_memory_usage_mb():.1f}MB")

if __name__ == "__main__":
    test_memory_monitoring()
