import os
import re
import glob
import pandas as pd


def extract_filename_metadata(filepath):
    name = os.path.basename(filepath)

    pattern = re.compile(
        r"policy_(?P<policy>[^_]+)_query_(?P<query_type>[^_]+)"
        r"_cross(?P<cross_ratio>[0-9.]+)"
        r"_zipf(?P<zipf_theta>[0-9.]+)"
        r"_rw(?P<rw_ratio>[0-9.]+)\.txt"
    )

    m = pattern.match(name)
    if not m:
        return {"filename": name}

    return {
        "filename": name,
        "policy": m.group("policy"),
        "query_type": m.group("query_type"),
        "cross_ratio": float(m.group("cross_ratio")),
        "zipf_theta": float(m.group("zipf_theta")),
        "rw_ratio": float(m.group("rw_ratio")),
    }


def extract_value(text, key):
    """
    Extracts a numeric value
    """
    m = re.search(rf"{re.escape(key)}:\s*([0-9]+(?:\.[0-9]+)?)", text)
    if m:
        return float(m.group(1))
    return None


def extract_round_trip_latency(text):
    m = re.search(
        r"round_trip_latency\s+([0-9]+(?:\.[0-9]+)?)\s+\(50th\)\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s+\(75th\)\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s+\(95th\)\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s+\(99th\)",
        text
    )
    if not m:
        return {
            "round_trip_latency_50th": None,
            "round_trip_latency_75th": None,
            "round_trip_latency_95th": None,
            "round_trip_latency_99th": None,
        }

    return {
        "round_trip_latency_50th": float(m.group(1)),
        "round_trip_latency_75th": float(m.group(2)),
        "round_trip_latency_95th": float(m.group(3)),
        "round_trip_latency_99th": float(m.group(4)),
    }


def extract_metrics(text):
    result = {
        "num_clflush": extract_value(text, "num_clflush"),
        "num_clwb": extract_value(text, "num_clwb"),
        "num_cache_hit": extract_value(text, "num_cache_hit"),
        "num_cache_miss": extract_value(text, "num_cache_miss"),
        "cache_hit_rate": extract_value(text, "cache hit rate"),
        "total_commit": extract_value(text, "total_commit"),
        "total_size_index_usage": extract_value(text, "total_size_index_usage"),
        "total_size_metadata_usage": extract_value(text, "total_size_metadata_usage"),
        "total_size_data_usage": extract_value(text, "total_size_data_usage"),
        "total_size_transport_usage": extract_value(text, "total_size_transport_usage"),
        "total_size_misc_usage": extract_value(text, "total_size_misc_usage"),
        "total_hw_cc_usage": extract_value(text, "total_hw_cc_usage"),
        "total_usage": extract_value(text, "total_usage"),
    }

    result.update(extract_round_trip_latency(text))
    return result


def parse_file(filepath):
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    row = {}
    row.update(extract_filename_metadata(filepath))
    row.update(extract_metrics(text))
    return row


def build_dataframe(folder):
    rows = []
    for path in glob.glob(os.path.join(folder, "*.txt")):
        rows.append(parse_file(path))

    df = pd.DataFrame(rows)

    sort_cols = ["policy", "query_type", "cross_ratio", "zipf_theta", "rw_ratio"]
    existing_sort_cols = [c for c in sort_cols if c in df.columns]
    if existing_sort_cols:
        df = df.sort_values(existing_sort_cols).reset_index(drop=True)

    return df


if __name__ == "__main__":
    folder = "ycsb_migration_policy_experiments"
    df = build_dataframe(folder)
    print(df)
    # df.to_csv("experiment_results.csv", index=False)
    df.to_pickle("experiment_results.pkl")
