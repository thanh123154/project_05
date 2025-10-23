#!/usr/bin/env python3
"""
Debug script để kiểm tra dữ liệu trong MongoDB
"""

import sys
from pymongo import MongoClient

MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "countly"
SUMMARY_COLLECTION = "summary"

def debug_mongodb_data():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[SUMMARY_COLLECTION]
        
        print("🔍 Debugging MongoDB data...")
        
        # Kiểm tra tổng số documents
        total_docs = collection.count_documents({})
        print(f"📊 Total documents in summary collection: {total_docs:,}")
        
        # Kiểm tra các collection types
        pipeline = [
            {'$group': {'_id': '$collection', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]
        collections = list(collection.aggregate(pipeline))
        print(f"\n📊 Collections in summary:")
        for c in collections[:15]:
            print(f"  {c['_id']}: {c['count']:,}")
        
        # Kiểm tra các collections được target trong data_filter.py
        target_collections = [
            "view_product_detail",
            "select_product_option", 
            "select_product_option_quality",
            "add_to_cart_action",
            "product_detail_recommendation_visible",
            "product_detail_recommendation_noticed",
            "product_view_all_recommend_clicked"
        ]
        
        print(f"\n🎯 Target collections analysis:")
        for col in target_collections:
            total = collection.count_documents({"collection": col})
            with_product_id = collection.count_documents({
                "collection": col,
                "$or": [
                    {"product_id": {"$exists": True, "$ne": None}},
                    {"viewing_product_id": {"$exists": True, "$ne": None}}
                ]
            })
            with_url = collection.count_documents({
                "collection": col,
                "$or": [
                    {"current_url": {"$exists": True, "$ne": None}},
                    {"referrer_url": {"$exists": True, "$ne": None}}
                ]
            })
            print(f"  {col}:")
            print(f"    Total: {total:,}")
            print(f"    With product_id/viewing_product_id: {with_product_id:,}")
            print(f"    With URL fields: {with_url:,}")
        
        # Kiểm tra sample documents
        print(f"\n📄 Sample documents:")
        samples = list(collection.find({}).limit(5))
        for i, doc in enumerate(samples):
            print(f"\nSample {i+1}:")
            print(f"  collection: {doc.get('collection')}")
            print(f"  product_id: {doc.get('product_id')}")
            print(f"  viewing_product_id: {doc.get('viewing_product_id')}")
            print(f"  current_url: {doc.get('current_url')}")
            print(f"  referrer_url: {doc.get('referrer_url')}")
            print(f"  url: {doc.get('url')}")
            print(f"  page_url: {doc.get('page_url')}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    debug_mongodb_data()
