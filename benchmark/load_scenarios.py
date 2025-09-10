import os
import json
import time
import csv
import math
import random
from pathlib import Path
from statistics import mean
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch

# Settings
ES_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
INDEX_NAME = os.getenv("INDEX_NAME", "amazon-music-reviews")
QUERY_DIR = os.getenv("QUERY_DIR", "../queries")
OUT_DIR = os.getenv("OUT_DIR", "../scenarios_outputs")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "180"))  # seconds
DURATION_PER_CLIENT = float(os.getenv("DURATION_PER_CLIENT", "10"))  # seconds
WARMUP_REQUESTS = int(os.getenv("WARMUP_REQUESTS", "5"))
RANDOM_SEED = int(os.getenv("SEED", "42"))

# changing the number of clients in each scenario(the choice of queries are all random)
SCENARIOS = [
    {"name": "C01__clients_1", "clients": 1},
    {"name": "C02__clients_2", "clients": 2},
    {"name": "C03__clients_4", "clients": 4},
    {"name": "C04__clients_6", "clients": 6},
    {"name": "C05__clients_8", "clients": 8},
    {"name": "C06__clients_10", "clients": 10},
    {"name": "C07__clients_12", "clients": 12},
    {"name": "C08__clients_16", "clients": 16},
    {"name": "C09__clients_20", "clients": 20},
    {"name": "C10__clients_24", "clients": 24},
]

random.seed(RANDOM_SEED)


# Set query parameters for ElasticSearch client
def load_query_files(qdir: Path):
    files = sorted([p for p in qdir.glob("*.json")])
    queries = []
    for p in files:
        with p.open("r", encoding="utf-8") as f:
            q = json.load(f)
        # ES 9.x named-args
        kw = {}
        if "query" in q:
            kw["query"] = q["query"]
        if "aggs" in q:
            kw["aggs"] = q["aggs"]
        if "size" in q:
            kw["size"] = q["size"]
        if "sort" in q:
            kw["sort"] = q["sort"]
        if "_source" in q:
            kw["_source"] = q["_source"]
        if "from" in q:
            kw["from_"] = q["from"]
        kw["track_total_hits"] = q.get("track_total_hits", True)
        queries.append((p.name, kw))
    return queries


# getting the percentile of times
def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


# Sending random request by clients
def run_client(es: Elasticsearch, queries, duration_s, per_req_rows, scenario_name):
    start_time = time.perf_counter()
    while (time.perf_counter() - start_time) < duration_s:
        qname, kwargs = random.choice(queries)
        t0 = time.perf_counter()
        status = "Success"
        took = ""
        total_hits = ""
        try:
            res = es.search(index=INDEX_NAME, **kwargs)
            res_body = res.body
            took = res_body.get("took", "")
            th = res_body.get("hits", {}).get("total", 0)
            total_hits = th.get("value", th if isinstance(th, int) else "")
        except Exception as e:
            status = f"{type(e).__name__}: {e}"
        latency = (time.perf_counter() - t0)
        per_req_rows.append([
            scenario_name, qname, time.time(), latency, took, total_hits, status
        ])


# some simple queries to warm up
def warmup(es: Elasticsearch, queries):
    n = min(WARMUP_REQUESTS, len(queries))
    for i in range(n):
        try:
            _, kwargs = queries[i]
            es.search(index=INDEX_NAME, **kwargs)
        except Exception:
            pass


def run_scenario(es: Elasticsearch, scenario, queries, outdir: Path):
    name = scenario["name"]
    clients = scenario["clients"]

    # erase the cache to start fair
    try:
        es.indices.clear_cache(index=INDEX_NAME)
    except Exception:
        pass

    warmup(es, queries)

    per_req_rows = []  # [scenario, query_name, epoch, latency_s, took_ms, total_hits, status]

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=clients) as ex:
        futures = [ex.submit(run_client, es, queries, DURATION_PER_CLIENT, per_req_rows, name)
                   for _ in range(clients)]
        for _ in as_completed(futures):
            pass
    elapsed = time.perf_counter() - t0

    # log each request
    per_req_dir = outdir / "per_request_logs"
    per_req_dir.mkdir(parents=True, exist_ok=True)
    per_req_file = per_req_dir / f"{name}.csv"
    with per_req_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "query", "epoch", "latency_s", "took_ms", "total_hits", "status"])
        w.writerows(per_req_rows)

    # set the results
    latencies = [r[3] for r in per_req_rows if r[6] == "Success"]
    tooks_ms = [r[4] for r in per_req_rows if r[6] == "Success" and isinstance(r[4], (int, float))]
    total_requests = len(per_req_rows)
    success = sum(1 for r in per_req_rows if r[6] == "Success")
    errors = total_requests - success
    rps = total_requests / elapsed if elapsed > 0 else 0.0

    # calculate latencies and percentile to log in the csv file
    summary = {
        "scenario": name,
        "clients": clients,
        "duration_s": round(elapsed, 3),
        "requests": total_requests,
        "success": success,
        "errors": errors,
        "rps": round(rps, 2),
        "lat_avg_ms": round(mean(latencies) * 1000, 2) if latencies else "",
        "lat_p50_ms": round(percentile(latencies, 50) * 1000, 2) if latencies else "",
        "lat_p90_ms": round(percentile(latencies, 90) * 1000, 2) if latencies else "",
        "lat_p95_ms": round(percentile(latencies, 95) * 1000, 2) if latencies else "",
        "lat_p99_ms": round(percentile(latencies, 99) * 1000, 2) if latencies else "",
        "took_avg_ms": round(mean(tooks_ms), 2) if tooks_ms else "",
        "took_p95_ms": round(percentile(tooks_ms, 95), 2) if tooks_ms else "",
    }
    return summary


def main():
    qdir = Path(QUERY_DIR)
    outdir = Path(OUT_DIR)
    outdir.mkdir(parents=True, exist_ok=True)
    if not qdir.exists():
        raise SystemExit(f"Query directory not found: {qdir.resolve()}")

    # ElasticSearch client with global timeout
    es = Elasticsearch(ES_URL).options(request_timeout=REQUEST_TIMEOUT)

    # loading queries from the directory
    queries = load_query_files(qdir)
    if not queries:
        raise SystemExit("No queries found.")

    summaries = []
    for sc in SCENARIOS:
        print(f"Running scenario: {sc['name']}  (clients={sc['clients']}, selector=random)")
        s = run_scenario(es, sc, queries, outdir)
        summaries.append(s)
        print(f"  -> done in {s['duration_s']}s | rps={s['rps']} | avg_lat={s['lat_avg_ms']} ms | errors={s['errors']}")

    # output the results in scenarios_report.csv
    summary_csv = outdir / "scenarios_report.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "scenario", "clients", "duration_s",
            "requests", "success", "errors", "rps",
            "lat_avg_ms", "lat_p50_ms", "lat_p90_ms", "lat_p95_ms", "lat_p99_ms",
            "took_avg_ms", "took_p95_ms",
        ])
        w.writeheader()
        w.writerows(summaries)

    print(f"\nSummary saved -> {summary_csv}")
    print(f"Per-request logs -> {outdir}/per_request_logs/")


if __name__ == "__main__":
    main()
