#!/usr/bin/env python3
"""
Script tạo index cho collection MongoDB
"""

import pymongo
import logging

# -----------------------------
# Configurations
# -----------------------------
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "countly"
COLLECTION_NAME = "summary"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logger.info(f"✅ Connected to MongoDB database: {DB_NAME}")
    except Exception as e:
        logger.error(f"❌ Cannot connect to MongoDB: {e}")
        return

    try:
        # Tạo index trên trường 'ip'
        index_name = collection.create_index("ip")
        logger.info(f"✅ Created index '{index_name}' on field 'ip'")
    except Exception as e:
        logger.error(f"❌ Error creating index: {e}")


if __name__ == "__main__":
    main()
