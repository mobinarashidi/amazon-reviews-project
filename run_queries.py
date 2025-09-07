import os
import json
import time
import csv
from elasticsearch import Elasticsearch, exceptions
from concurrent.futures import ProcessPoolExecutor, TimeoutError

# Config
ES_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
INDEX = os.getenv("INDEX_NAME", "amazon-music-reviews")

QUERY_DIR = "queries"  # folder containing query JSON files
OUTPUT_DIR = "queries_outputs" # folder to save results
CSV_REPORT = "queries_report.csv" # Final report file

TIMEOUT_PER_QUERY = 180  # Maximum allowed time per query

def run_query(fname):
    # Connect to Elasticsearch
    es = Elasticsearch(ES_URL)

    # Load query from file
    path = os.path.join(QUERY_DIR, fname)
    with open(path, "r", encoding="utf-8") as f:
        query = json.load(f)

    start_time = time.time()
    try:
        # Execute the query
        res = es.search(index=INDEX, body=query, request_timeout=TIMEOUT_PER_QUERY)
        latency = time.time() - start_time # Time until response received

        # Get response body
        res_dict = res.body if hasattr(res, "body") else res

        # Save result to output file
        out_path = os.path.join(OUTPUT_DIR, fname.replace(".json", "_result.json"))
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(res_dict, f, ensure_ascii=False, indent=2)

        total_time = time.time() - start_time # Total time including saving
        hits = len(res_dict.get('hits', {}).get('hits', [])) # Number of results

        status = "Success"
        return fname, latency, total_time, hits, status

    # Handle different types of errors
    except (exceptions.ConnectionError, exceptions.ConnectionTimeout, exceptions.TransportError) as e:
        total_time = time.time() - start_time
        status = f"Failed-ES"
        return fname, None, total_time, 0, status
    except exceptions.BadRequestError as e:
        total_time = time.time() - start_time
        status = f"Failed-BadRequest"
        return fname, None, total_time, 0, status
    except Exception as e:
        total_time = time.time() - start_time
        status = f"Failed-Other"
        return fname, None, total_time, 0, status


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True) # Create output folder if not exists
    results = []

    # Run each query in a separate process
    with ProcessPoolExecutor(max_workers=1) as executor:
        for fname in os.listdir(QUERY_DIR):
            if not fname.endswith(".json"):
                continue
            future = executor.submit(run_query, fname)
            try:
                result = future.result(timeout=TIMEOUT_PER_QUERY)
            except TimeoutError:
                result = (fname, None, TIMEOUT_PER_QUERY, 0, "Timeout")
            results.append(result)

    # Save results to CSV report
    with open(CSV_REPORT, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Query", "Latency(s)", "TotalTime(s)", "Hits", "Status"])
        for r in results:
            writer.writerow(r)

    # Print
    for r in results:
        print(f"{r[0]:40} | Latency: {r[1] if r[1] is not None else '-':>6} | "
              f"Total: {r[2]:6.2f}s | Hits: {r[3]:5} | Status: {r[4]}")

    print(f"\nAll queries executed. CSV report saved -> {CSV_REPORT}")
