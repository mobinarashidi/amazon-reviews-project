import argparse
import gzip
import os
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from tqdm import tqdm

# We only import normalize() from our local parser module.
# normalize() converts a parsed review block into a clean dict:
#   - casts price/score/time, adds time_iso, computes helpfulness, etc.
from parser import normalize


def parse_snap(fh):
    """Parse a SNAP-formatted reviews text stream (already opened as text).
    The file consists of blocks of "key: value" lines separated by a blank line.

    Example block:
        product/productId: B00002066I
        product/title: a-ha
        review/userId: A1RSDE90N6RSZF
        review/helpfulness: 3/4
        review/score: 5.0
        review/time: 939772800
        review/summary: Inspiring
        review/text: I hope a lot of people ...

    We accumulate lines into 'block' until we hit a blank line, then yield
    normalize(block).
    """
    block = {}
    for line in fh:
        line = line.strip()

        # Detect the end of a review by reaching a blank line
        if not line:
            if block:
                yield normalize(block)
                block = {}
            continue

        # Each data line is "key: value" --> so we split them and add them to the block
        if ": " in line:
            k, v = line.split(": ", 1)
            block[k] = v
    if block:
        # Normalize each block
        yield normalize(block)


def actions(docs, index):
    """
    Convert normalized docs into bulk actions for Elasticsearch.
    We build a stable _id to eliminate duplicates (idempotent indexing):
        _id = productId::userId::time  (when all are present)
    Otherwise, ES will auto-generate an _id.
    """
    for doc in docs:
        # build the id using productID, userID and time (if any is missing ES will generate id itself)
        pid = doc.get("productId")
        uid = doc.get("userId")
        t = doc.get("time")
        _id = f"{pid}::{uid}::{t}" if pid and uid and t else None

        action = {"_index": index, "_source": doc}
        if _id:
            action["_id"] = _id
        yield action


def main():
    # CLI arguments
    p = argparse.ArgumentParser(description="Stream Amazon reviews into Elasticsearch")
    p.add_argument("--dataset", required=True, help="Path to Music.txt or Music.txt.gz")
    p.add_argument("--index", default="amazon-music-reviews", help="Target index name")
    p.add_argument("--url", default=os.getenv("ELASTIC_URL", "http://localhost:9200"))
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BULK_BATCH_SIZE", "1000")))
    args = p.parse_args()

    # ES client
    es = Elasticsearch(args.url)

    # Select the appropriate file opener (gzip vs plain text)
    dataset_path = args.dataset
    if dataset_path.endswith(".gz"):
        opener = lambda: gzip.open(dataset_path, "rt", encoding="utf-8", errors="ignore")
    else:
        opener = lambda: open(dataset_path, "rt", encoding="utf-8", errors="ignore")

    # Streaming bulk index with progress bar
    with opener() as fh:
        docs = parse_snap(fh)
        successes = 0
        failures = 0
        for ok, res in tqdm(
                streaming_bulk(es, actions(docs, args.index), chunk_size=args.batch_size),
                desc="Indexing",
                unit="docs"
        ):
            if ok:
                successes += 1
            else:
                failures += 1

    print(f"\nDone. Indexed: {successes}, Failed: {failures}")
    if failures > 0:
        # Giving non-zero output status to show there was failure
        sys.exit(1)


if __name__ == "__main__":
    main()
