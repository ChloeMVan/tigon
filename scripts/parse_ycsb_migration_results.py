#!/usr/bin/env python3
"""
Parse YCSB migration policy experiment result files and output a CSV.
"""
import csv
import re
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "ycsb_migration_policy_experiments"
OUTPUT_CSV = RESULTS_DIR / "ycsb_migration_experiments.csv"

# Filename pattern: policy_Clock_query_rmw_cross10_zipf0.5_rw10.txt
FILENAME_RE = re.compile(
    r"policy_(?P<policy>\w+)_query_(?P<query_type>\w+)_cross(?P<cross>\d+)_zipf(?P<zipf>[\d.]+)_rw(?P<rw>\d+)\.txt"
)

# Content patterns
CACHE_HIT_RE = re.compile(r"cache hit rate: ([\d.]+)%")
TOTAL_COMMIT_RE = re.compile(r"total_commit: ([\d.]+)")
# Worker 0 latency: 204 us (50%) 222 us (75%) 377 us (95%) 451 us (99%)
WORKER_LATENCY_RE = re.compile(
    r"Worker 0 latency: (\d+) us \(50%\) (\d+) us \(75%\) (\d+) us \(95%\) (\d+) us \(99%\)"
)
# txn commit latency: 204 us (50%) 222 us (75%) 374 us (95%) 449 us (99%) avg 220 us
AVG_LATENCY_RE = re.compile(r"txn commit latency: .*? avg (\d+) us")


def parse_filename(name: str) -> dict | None:
    m = FILENAME_RE.match(name)
    if not m:
        return None
    return {
        "migration_policy": m.group("policy"),
        "query_type": m.group("query_type"),
        "cross_value": int(m.group("cross")),
        "zipf_theta_value": float(m.group("zipf")),
        "rw_ratio": int(m.group("rw")),
    }


def parse_file(path: Path) -> dict | None:
    text = path.read_text()
    row = {}

    # cache hit rate
    m = CACHE_HIT_RE.search(text)
    row["cache_hit"] = float(m.group(1)) if m else None

    # total_commit
    m = TOTAL_COMMIT_RE.search(text)
    row["total_commit"] = float(m.group(1)) if m else None

    # Worker 0 latency -> 75th, 95th, 99th (and we could use 50th as median)
    m = WORKER_LATENCY_RE.search(text)
    if m:
        row["lat_75_us"] = int(m.group(2))
        row["lat_95_us"] = int(m.group(3))
        row["lat_99_us"] = int(m.group(4))
    else:
        row["lat_75_us"] = row["lat_95_us"] = row["lat_99_us"] = None

    # avg latency from txn commit latency
    m = AVG_LATENCY_RE.search(text)
    row["avg_latency_us"] = int(m.group(1)) if m else None

    return row


def main():
    rows = []
    for path in sorted(RESULTS_DIR.glob("*.txt")):
        name = path.name
        config = parse_filename(name)
        if not config:
            continue
        metrics = parse_file(path)
        if not metrics or metrics.get("total_commit") is None:
            continue
        row = {
            "migration_policy": config["migration_policy"],
            "query_type": config["query_type"],
            "cross_value": config["cross_value"],
            "zipf_theta_value": config["zipf_theta_value"],
            "rw_ratio": config["rw_ratio"],
            "cache_hit": metrics["cache_hit"],
            "avg_latency_us": metrics["avg_latency_us"],
            "lat_75_us": metrics["lat_75_us"],
            "lat_95_us": metrics["lat_95_us"],
            "lat_99_us": metrics["lat_99_us"],
            "total_commit": metrics["total_commit"],
        }
        rows.append(row)

    fieldnames = [
        "migration_policy",
        "query_type",
        "cross_value",
        "zipf_theta_value",
        "rw_ratio",
        "cache_hit",
        "avg_latency_us",
        "lat_75_us",
        "lat_95_us",
        "lat_99_us",
        "total_commit",
    ]
    with open(OUTPUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
