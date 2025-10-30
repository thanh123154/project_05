import csv
from collections import Counter

INPUT_FILE = "merged_data.csv"

def count_distinct_product_ids(path=INPUT_FILE):
    counted = Counter()
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=["product_id", "url", "source_collection", "fetched_at"])
            for row in reader:
                pid = str(row.get("product_id")).strip()
                if pid:
                    counted[pid] += 1
    except Exception as e:
        print(f"Error reading file: {e}")
        return
    print(f"Total product_id in file: {sum(counted.values())}")
    print(f"Distinct product_id count: {len(counted)}")
    repeated = [item for item in counted.items() if item[1] > 1]
    if repeated:
        print("Top repeated product_id (up to 10):")
        for pid, count in sorted(repeated, key=lambda x: -x[1])[:10]:
            print(f"  - {pid}: {count} times")
    else:
        print("All product_id are unique.")

if __name__ == "__main__":
    count_distinct_product_ids()
