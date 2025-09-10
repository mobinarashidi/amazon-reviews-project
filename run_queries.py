import os
import json
from elasticsearch import Elasticsearch

# Config
ES_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
INDEX = os.getenv("INDEX_NAME", "amazon-music-reviews")

QUERY_DIR = "queries"   # folder containing query JSON files
OUTPUT_DIR = "queries_outputs"      # folder to save results

# Connect to Elasticsearch
es = Elasticsearch(ES_URL)

# Ensure output dir exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Iterate over all JSON query files
for fname in os.listdir(QUERY_DIR):
    if not fname.endswith(".json"):
        continue

    path = os.path.join(QUERY_DIR, fname)
    with open(path, "r", encoding="utf-8") as f:
        query = json.load(f)

    print(f"Running {fname} ...")

    # Run query
    res = es.search(index=INDEX, body=query)

    # Convert ObjectApiResponse -> dict (for ES client v8+)
    res_dict = res.body if hasattr(res, "body") else res

    # Save output as JSON
    out_path = os.path.join(OUTPUT_DIR, fname.replace(".json", "_result.json"))
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(res_dict, f, ensure_ascii=False, indent=2)

    print(f"Saved → {out_path}")

print("\nAll queries executed. Results saved in 'out/' folder.")
