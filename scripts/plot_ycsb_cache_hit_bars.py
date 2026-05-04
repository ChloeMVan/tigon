#!/usr/bin/env python3
"""
Bar graphs: cache hit rate, avg latency, or total commit (y) vs rw_ratio, zipf_theta, or cross_value (x).
Each group has 4 bars: LRU-rmw, LRU-scan, Clock-rmw, Clock-scan.

Modes:
  - by rw_ratio: fix zipf_theta=0.7, cross=10.
  - by zipf_theta: fix rw_ratio=10, cross=10.
  - by cross: fix rw_ratio=10, zipf_theta=0.7.

Metrics: cache_hit, avg_latency, total_commit (default: all).

Requires: matplotlib (pip install matplotlib)
"""
import argparse
import csv
import matplotlib.pyplot as plt
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "ycsb_migration_policy_experiments"
CSV_PATH = RESULTS_DIR / "ycsb_migration_experiments.csv"

# Bar order within each group: (policy, query_type)
BAR_ORDER = [
    ("LRU", "rmw"),
    ("LRU", "scan"),
    ("Clock", "rmw"),
    ("Clock", "scan"),
]

RW_RATIOS = [10, 50, 90]
ZIPF_THETAS = [0.5, 0.7, 0.99]

COLORS = ["#2ecc71", "#27ae60", "#3498db", "#2980b9"]

# (csv_column, ylabel, ylim_low, ylim_high) — None for auto
METRICS = [
    ("cache_hit", "Cache hit rate (%)", 0, 105),
    ("avg_latency_us", "Avg latency (µs)", None, None),
    ("total_commit", "Total commit", None, None),
]


def load_full_csv():
    """Load all rows from CSV as list of dicts."""
    with open(CSV_PATH, newline="") as f:
        return list(csv.DictReader(f))


def _row_metrics(row):
    """Extract metric dict from a CSV row."""
    return {
        "cache_hit": float(row["cache_hit"]),
        "avg_latency_us": float(row["avg_latency_us"]),
        "total_commit": float(row["total_commit"]),
    }


def load_data_by_rw_ratio():
    """Dict (rw_ratio, policy, query_type) -> metrics for cross=10, zipf=0.7."""
    data = {}
    for row in load_full_csv():
        if int(row["cross_value"]) != 10 or float(row["zipf_theta_value"]) != 0.7:
            continue
        key = (int(row["rw_ratio"]), row["migration_policy"], row["query_type"])
        data[key] = _row_metrics(row)
    return data


def load_data_by_zipf_theta():
    """Dict (zipf_theta, policy, query_type) -> metrics for cross=10, rw_ratio=10."""
    data = {}
    for row in load_full_csv():
        if int(row["cross_value"]) != 10 or int(row["rw_ratio"]) != 10:
            continue
        z = float(row["zipf_theta_value"])
        key = (z, row["migration_policy"], row["query_type"])
        data[key] = _row_metrics(row)
    return data


def load_data_by_cross():
    """Dict (cross_value, policy, query_type) -> metrics for rw_ratio=10, zipf_theta=0.7."""
    data = {}
    for row in load_full_csv():
        if int(row["rw_ratio"]) != 10 or float(row["zipf_theta_value"]) != 0.7:
            continue
        c = int(row["cross_value"])
        key = (c, row["migration_policy"], row["query_type"])
        data[key] = _row_metrics(row)
    return data


def make_legend(ax):
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS[0], label="LRU, rmw"),
        Patch(facecolor=COLORS[1], label="LRU, scan"),
        Patch(facecolor=COLORS[2], label="Clock, rmw"),
        Patch(facecolor=COLORS[3], label="Clock, scan"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", ncol=2)


def _plot_grouped_bars(data, groups, group_labels, xlabel, metric_key, ylabel, title, output_stem, ylim_low=None, ylim_high=None):
    """Draw one bar chart and save as PDF + PNG."""
    n_groups = len(groups)
    n_bars_per_group = len(BAR_ORDER)
    bar_width = 0.2
    group_width = n_bars_per_group * bar_width
    gap_between_groups = 0.3

    fig, ax = plt.subplots(figsize=(10, 5))
    for i, group_val in enumerate(groups):
        group_center = i * (group_width + gap_between_groups)
        for j, (policy, query_type) in enumerate(BAR_ORDER):
            key = (group_val, policy, query_type)
            if key not in data or metric_key not in data[key]:
                continue
            x = group_center + j * bar_width
            ax.bar(x, data[key][metric_key], bar_width, color=COLORS[j])

    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_xticks(
        [i * (group_width + gap_between_groups) + (n_bars_per_group * bar_width) / 2 - bar_width / 2 for i in range(n_groups)]
    )
    ax.set_xticklabels(group_labels)
    if ylim_low is not None and ylim_high is not None:
        ax.set_ylim(ylim_low, ylim_high)
    elif ylim_low is not None:
        ax.set_ylim(bottom=ylim_low)
    elif ylim_high is not None:
        ax.set_ylim(top=ylim_high)
    ax.grid(axis="y", alpha=0.3)
    make_legend(ax)
    plt.title(title)
    plt.tight_layout()
    out = RESULTS_DIR / output_stem
    plt.savefig(out.with_suffix(".pdf"), bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), bbox_inches="tight", dpi=150)
    plt.close()
    print(f"Saved {out}.pdf and .png")


def plot_by_rw_ratio(metrics_to_plot):
    data = load_data_by_rw_ratio()
    if not data:
        raise SystemExit("No rows with zipf_theta_value=0.7 and cross_value=10")
    groups = RW_RATIOS
    group_labels = [f"RW ratio = {r}" for r in groups]
    subtitle = "(zipf θ = 0.7, cross = 10)"
    for metric_key, ylabel, ylim_lo, ylim_hi in METRICS:
        if metric_key not in metrics_to_plot:
            continue
        title = f"{ylabel} by migration policy and query type\n{subtitle}"
        stem = f"{metric_key}_by_rw_ratio"
        _plot_grouped_bars(data, groups, group_labels, "Read–write ratio", metric_key, ylabel, title, stem, ylim_lo, ylim_hi)


def plot_by_zipf_theta(metrics_to_plot):
    data = load_data_by_zipf_theta()
    if not data:
        raise SystemExit("No rows with rw_ratio=10 and cross_value=10")
    groups = sorted(set(k[0] for k in data.keys()))
    if not groups:
        raise SystemExit("No data for rw_ratio=10, cross=10")
    group_labels = [str(z) for z in groups]
    subtitle = "(rw ratio = 10, cross = 10)"
    for metric_key, ylabel, ylim_lo, ylim_hi in METRICS:
        if metric_key not in metrics_to_plot:
            continue
        title = f"{ylabel} by migration policy and query type\n{subtitle}"
        stem = f"{metric_key}_by_zipf_theta"
        _plot_grouped_bars(data, groups, group_labels, "Zipf θ", metric_key, ylabel, title, stem, ylim_lo, ylim_hi)


def plot_by_cross(metrics_to_plot):
    data = load_data_by_cross()
    if not data:
        raise SystemExit("No rows with rw_ratio=10 and zipf_theta_value=0.7")
    groups = sorted(set(k[0] for k in data.keys()))
    if not groups:
        raise SystemExit("No data for rw_ratio=10, zipf_theta=0.7")
    group_labels = [str(c) for c in groups]
    subtitle = "(rw ratio = 10, zipf θ = 0.7)"
    for metric_key, ylabel, ylim_lo, ylim_hi in METRICS:
        if metric_key not in metrics_to_plot:
            continue
        title = f"{ylabel} by migration policy and query type\n{subtitle}"
        stem = f"{metric_key}_by_cross"
        _plot_grouped_bars(data, groups, group_labels, "Cross", metric_key, ylabel, title, stem, ylim_lo, ylim_hi)


def main():
    parser = argparse.ArgumentParser(description="Plot bar charts for cache hit, avg latency, total commit.")
    parser.add_argument(
        "mode",
        nargs="?",
        default="all",
        choices=("rw", "zipf", "cross", "all", "both"),
        help="Plot by rw_ratio (zipf=0.7, cross=10), zipf_theta (rw=10, cross=10), cross (rw=10, zipf=0.7), or all/both (default).",
    )
    parser.add_argument(
        "--metric",
        action="append",
        dest="metrics",
        choices=("cache_hit", "avg_latency", "total_commit"),
        help="Metric to plot (repeat for multiple). Default: all.",
    )
    args = parser.parse_args()
    mode = "all" if args.mode == "both" else args.mode
    if args.metrics is None:
        metrics_to_plot = {"cache_hit", "avg_latency_us", "total_commit"}
    else:
        metrics_to_plot = set()
        for m in args.metrics:
            if m == "avg_latency":
                metrics_to_plot.add("avg_latency_us")
            else:
                metrics_to_plot.add(m)
    if mode in ("rw", "all"):
        plot_by_rw_ratio(metrics_to_plot)
    if mode in ("zipf", "all"):
        plot_by_zipf_theta(metrics_to_plot)
    if mode in ("cross", "all"):
        plot_by_cross(metrics_to_plot)


if __name__ == "__main__":
    main()
