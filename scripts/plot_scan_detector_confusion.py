#!/usr/bin/env python3
"""
Plot ScanDetector confusion matrices from YCSB migration experiment results.

Reads results/ycsb_migration_policy_experiments/ycsb_migration_experiments.csv
and generates one confusion matrix PNG per AgingAutoScan run, plus a summary
bar chart of precision/recall/F1 across all configurations.

Output directory: results/ycsb_migration_policy_experiments/confusion_matrices/

Usage:
  python3 scripts/plot_scan_detector_confusion.py
"""
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "ycsb_migration_policy_experiments"
CSV_PATH    = RESULTS_DIR / "ycsb_migration_experiments.csv"
OUT_DIR     = RESULTS_DIR / "confusion_matrices"


def load_rows():
    if not CSV_PATH.exists():
        sys.exit(f"CSV not found: {CSV_PATH}\nRun parse_ycsb_migration_results.py first.")
    with open(CSV_PATH, newline="") as f:
        rows = list(csv.DictReader(f))
    auto = [r for r in rows if r["migration_policy"] == "AgingAutoScan" and r["sd_tp"] not in ("", None)]
    if not auto:
        sys.exit("No AgingAutoScan rows with scan_detector data found in the CSV.")
    return auto


def to_int(v):
    return int(v) if v not in ("", None) else 0


def to_float(v):
    return float(v) if v not in ("", None) else float("nan")


def label(row):
    return (f"query={row['query_type']}  zipf={row['zipf_theta_value']}"
            f"  cross={row['cross_value']}  rw={row['rw_ratio']}")


def plot_confusion(row, out_path):
    tp = to_int(row["sd_tp"])
    fp = to_int(row["sd_fp"])
    tn = to_int(row["sd_tn"])
    fn = to_int(row["sd_fn"])
    total = tp + fp + tn + fn

    matrix     = np.array([[tp, fn], [fp, tn]], dtype=float)
    matrix_pct = matrix / total * 100 if total > 0 else matrix

    prec = to_float(row["sd_precision"])
    rec  = to_float(row["sd_recall"])
    f1   = to_float(row["sd_f1"])
    acc  = to_float(row["sd_accuracy"])

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))

    for ax, data, fmt, title_suffix in [
        (axes[0], matrix,     ".0f", "counts"),
        (axes[1], matrix_pct, ".1f", "% of total"),
    ]:
        im = ax.imshow(data, cmap="Blues", vmin=0)
        for i in range(2):
            for j in range(2):
                val = data[i, j]
                ax.text(j, i, f"{val:{fmt}}" + ("%" if "%" in title_suffix else ""),
                        ha="center", va="center",
                        color="white" if val > data.max() * 0.6 else "black",
                        fontsize=13, fontweight="bold")
        ax.set_xticks([0, 1])
        ax.set_yticks([0, 1])
        ax.set_xticklabels(["Predicted\nScan", "Predicted\nPoint"], fontsize=10)
        ax.set_yticklabels(["Actual\nScan", "Actual\nPoint"], fontsize=10)
        ax.set_xlabel("Predicted", fontsize=10)
        ax.set_ylabel("Actual", fontsize=10)
        ax.set_title(f"Confusion matrix ({title_suffix})", fontsize=11)
        plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

    metrics_text = (f"Precision={prec:.4f}   Recall={rec:.4f}"
                    f"   F1={f1:.4f}   Accuracy={acc:.4f}"
                    f"   Total moves={total:,}")
    fig.suptitle(f"ScanDetector — {label(row)}\n{metrics_text}", fontsize=10, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight", dpi=120)
    plt.close(fig)


def plot_summary(rows, out_path):
    labels   = [label(r) for r in rows]
    precs    = [to_float(r["sd_precision"]) for r in rows]
    recalls  = [to_float(r["sd_recall"])    for r in rows]
    f1s      = [to_float(r["sd_f1"])        for r in rows]

    x   = np.arange(len(rows))
    w   = 0.25
    fig, ax = plt.subplots(figsize=(max(8, len(rows) * 1.8), 5))
    ax.bar(x - w, precs,   w, label="Precision", color="#4C72B0")
    ax.bar(x,     recalls, w, label="Recall",    color="#55A868")
    ax.bar(x + w, f1s,     w, label="F1",        color="#C44E52")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=8)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("Score")
    ax.set_title("ScanDetector — Precision / Recall / F1 across configurations")
    ax.legend()
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight", dpi=120)
    plt.close(fig)
    print(f"  summary chart -> {out_path}")


def main():
    rows = load_rows()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for row in rows:
        fname = (f"confusion_query{row['query_type']}_zipf{row['zipf_theta_value']}"
                 f"_cross{row['cross_value']}_rw{row['rw_ratio']}.png")
        out_path = OUT_DIR / fname
        plot_confusion(row, out_path)
        print(f"  {label(row)}  ->  {out_path.name}")

    if len(rows) > 1:
        plot_summary(rows, OUT_DIR / "summary_precision_recall_f1.png")

    print(f"\nDone. {len(rows)} confusion matrix(es) written to {OUT_DIR}")


if __name__ == "__main__":
    main()
