import os
import json
import csv
from pathlib import Path
from elasticsearch import Elasticsearch

# Config
ES_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
INDEX_NAME = os.getenv("INDEX_NAME", "amazon-music-reviews")
QUERY_DIR = os.getenv("QUERY_DIR", "queries")
CSV_REPORT = os.getenv("CSV_REPORT", "queries_report.csv")
OUT_DIR = os.getenv("OUT_DIR", "queries_outputs")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "180"))


# Convert json queries to dicts
def load_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


# Extract hits.total.value from response
def total_hits_from(res_body: dict) -> int:
    th = res_body.get("hits", {}).get("total", 0)
    if isinstance(th, dict):
        return int(th.get("value", 0))
    return int(th or 0)


# Set query parameters for ElasticSearch client
def build_kwargs(q: dict) -> dict:
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
        kw["from_"] = q["from"]  # ES Python client uses from_
    kw["track_total_hits"] = q.get("track_total_hits", True)
    return kw


def main():
    qdir = Path(QUERY_DIR)
    outdir = Path(OUT_DIR)
    outdir.mkdir(parents=True, exist_ok=True)
    if not qdir.exists():
        raise SystemExit(f"Query directory not found: {qdir.resolve()}")

    # ES client with global timeout
    es = Elasticsearch(ES_URL).options(request_timeout=REQUEST_TIMEOUT)

    # Clear cache before running
    try:
        es.indices.clear_cache(index=INDEX_NAME)
        print(f"Cleared cache for index: {INDEX_NAME}")
    except Exception as e:
        print(f"WARNING: clear_cache failed: {type(e).__name__}: {e}")

    qfiles = sorted(qdir.glob("*.json"))

    with open(CSV_REPORT, "w", newline="", encoding="utf-8") as fcsv:
        writer = csv.writer(fcsv)
        writer.writerow(["Query", "Took(ms)", "TotalHits", "Status"])

        for qp in qfiles:
            took_ms, total_hits, status = "", "", "Success"
            try:
                # loading the query
                q = load_json(qp)
                # setting arguments
                kwargs = build_kwargs(q)

                # performing the query
                res = es.search(index=INDEX_NAME, **kwargs)
                res_body: dict = res.body

                took_ms = res_body.get("took", "")
                total_hits = total_hits_from(res_body)

                # save raw response body
                out_path = outdir / f"{qp.stem}_response.json"
                with out_path.open("w", encoding="utf-8") as outf:
                    json.dump(res_body, outf, ensure_ascii=False, indent=2)

            except Exception as e:
                status = f"{type(e).__name__}: {e}"

            writer.writerow([qp.name, took_ms, total_hits, status])
            print(f"{qp.name:35} | took={took_ms} ms | total hits={total_hits} | {status}")

    print(f"\nSaved -> {CSV_REPORT}")
    print(f"Raw responses -> {outdir}/")


if __name__ == "__main__":
    main()
