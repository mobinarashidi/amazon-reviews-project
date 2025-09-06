"""
Utilities to normalize a single SNAP Amazon review "block" (dict of raw strings)
into a clean, typed document ready for indexing in Elasticsearch.

Input format (per review block, lines separated by a blank line):
    product/productId: B00002066I
    product/title: a-ha
    product/price: 15.99
    review/userId: A1RSDE90N6RSZF
    review/profileName: Joseph M. Kotow
    review/helpfulness: 3/4
    review/score: 5.0
    review/time: 939772800
    review/summary: Inspiring
    review/text: I hope a lot of people ...

Output keys (normalized):
    - productId        (str)
    - title            (str | None)
    - price            (float | None)
    - userId           (str | None)
    - profileName      (str | None)
    - helpfulness_raw  (str | None)  # original "x/y"
    - helpfulness      (float | None) # x/y in [0,1], None if y == 0 or parse fails
    - score            (float | None)
    - time             (int | None)   # unix epoch seconds
    - time_iso         (str | None)   # ISO8601 UTC (e.g., "1999-10-14T00:00:00Z")
    - summary_text     (str | None)
    - review_text      (str | None)

Design goals:
    - Be robust to dirty inputs (empty fields, malformed numbers).
    - Never raise on bad rows: coerce to None where appropriate.
    - Keep raw helpfulness as reference, but also provide a normalized float.
"""
from datetime import datetime, timezone

# Mapping from raw SNAP keys to our normalized document keys.
KEYS = {
    "product/productId": "productId",
    "product/title": "title",
    "product/price": "price",
    "review/userId": "userId",
    "review/profileName": "profileName",
    "review/helpfulness": "helpfulness_raw",
    "review/score": "score",
    "review/time": "time",
    "review/summary": "summary_text",
    "review/text": "review_text",
}


def normalize(block: dict) -> dict:
    d = {}

    # Map and copy raw values from the input block into d
    for k, v in block.items():
        if k in KEYS:
            # strip whitespace if value is a string, otherwise keep the format
            d[KEYS[k]] = v.strip() if isinstance(v, str) else v

    # Price -> float or None
    try:
        d["price"] = float(d.get("price")) if d.get("price") not in (None, "") else None
    except:
        d["price"] = None

    # Score -> float or None
    try:
        d["score"] = float(d.get("score")) if d.get("score") not in (None, "") else None
    except:
        d["score"] = None

    # Time -> int epoch + derived ISO UTC string
    try:
        t = int(d.get("time"))
        d["time"] = t
        d["time_iso"] = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except:
        # If invalid, nullify both fields
        d["time"] = None
        d["time_iso"] = None

    # Helpfulness "x/y" -> float ratio or None
    h = d.get("helpfulness_raw")
    if h:
        try:
            num, den = h.split("/")
            num, den = int(num), int(den)
            # Only compute ratio if denominator > 0
            d["helpfulness"] = (num / den) if den > 0 else None
        except:
            d["helpfulness"] = None
    else:
        d["helpfulness"] = None

    return d
