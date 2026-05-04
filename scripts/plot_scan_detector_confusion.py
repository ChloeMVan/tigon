#!/usr/bin/env python3
"""
Parse AgingAutoScan result files for [ScanDetect] lines and plot
a confusion matrix grid across workload configurations.

Usage:
    python3 scripts/plot_scan_detector_confusion.py [RESULTS_DIR]

RESULTS_DIR defaults to results/ycsb_migration_policy_experiments.
Only files whose filename contains "AgingAutoScan" are processed.
"""

import os
import re
import sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# ── regex patterns ────────────────────────────────────────────────────────────

# Header comment lines in the result file
RE_QUERY   = re.compile(r"#.*query_type=(\w+)")
RE_ZIPF    = re.compile(r"#.*zipf_theta=([\d.]+)")
RE_CROSS   = re.compile(r"#.*cross_ratio=(\d+)")
RE_RW      = re.compile(r"#.*rw_ratio=(\d+)")

# The [ScanDetect] summary line emitted by PolicyAgingAutoScan::print_statistics()
RE_DETECT  = re.compile(
    r"\[ScanDetect\]"
    r".*tp=(\d+)"
    r".*tn=(\d+)"
    r".*fp=(\d+)"
    r".*fn=(\d+)"
    r".*precision=([\d.]+)"
    r".*recall=([\d.]+)"
    r".*f1=([\d.]+)"
    r".*accuracy=([\d.]+)"
)

# ── file parsing ──────────────────────────────────────────────────────────────

def parse_file(path):
    meta = {}
    result = None
    with open(path) as f:
        for line in f:
            m = RE_QUERY.search(line);  meta.setdefault("query",  m.group(1)) if m else None
            m = RE_ZIPF.search(line);   meta.setdefault("zipf",   m.group(1)) if m else None
            m = RE_CROSS.search(line);  meta.setdefault("cross",  m.group(1)) if m else None
            m = RE_RW.search(line);     meta.setdefault("rw",     m.group(1)) if m else None
            m = RE_DETECT.search(line)
            if m:
                result = {
                    "tp": int(m.group(1)), "tn": int(m.group(2)),
                    "fp": int(m.group(3)), "fn": int(m.group(4)),
                    "precision": float(m.group(5)), "recall": float(m.group(6)),
                    "f1": float(m.group(7)), "accuracy": float(m.group(8)),
                }
    if result is None or len(meta) < 4:
        return None
    return {**meta, **result}

# ── confusion matrix panel ────────────────────────────────────────────────────

def draw_confusion(ax, tp, tn, fp, fn, title, precision, recall, f1, accuracy):
    total = tp + tn + fp + fn
    cm = np.array([[tp, fn], [fp, tn]], dtype=float)

    # show counts and row-normalised percentages
    row_sums = cm.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    cm_norm = cm / row_sums

    im = ax.imshow(cm_norm, cmap="Blues", vmin=0, vmax=1)

    labels = [["TP", "FN"], ["FP", "TN"]]
    counts = [[tp, fn], [fp, tn]]
    for i in range(2):
        for j in range(2):
            pct = cm_norm[i, j] * 100
            ax.text(j, i,
                    f"{labels[i][j]}\n{counts[i][j]:,}\n({pct:.1f}%)",
                    ha="center", va="center", fontsize=8,
                    color="white" if cm_norm[i, j] > 0.6 else "black")

    ax.set_xticks([0, 1])
    ax.set_yticks([0, 1])
    ax.set_xticklabels(["Pred: Scan", "Pred: Point"], fontsize=7)
    ax.set_yticklabels(["GT: Scan", "GT: Point"], fontsize=7)
    ax.set_title(f"{title}\nP={precision:.2f} R={recall:.2f} F1={f1:.2f} Acc={accuracy:.2f}",
                 fontsize=7, pad=3)

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 \
        else os.path.join(os.path.dirname(__file__), "..",
                          "results", "ycsb_migration_policy_experiments")
    results_dir = os.path.abspath(results_dir)

    rows = []
    for fname in sorted(os.listdir(results_dir)):
        if "AgingAutoScan" not in fname or not fname.endswith(".txt"):
            continue
        rec = parse_file(os.path.join(results_dir, fname))
        if rec:
            rows.append(rec)

    if not rows:
        print(f"No AgingAutoScan result files with [ScanDetect] lines found in {results_dir}")
        print("Make sure you have run the benchmark with policy=AgingAutoScan and rebuilt.")
        sys.exit(1)

    # group by query type for separate figures
    query_types = sorted(set(r["query"] for r in rows))

    for query in query_types:
        subset = [r for r in rows if r["query"] == query]

        # axes: rows = zipf theta, cols = cross ratio
        zipfs   = sorted(set(r["zipf"]  for r in subset))
        crosses = sorted(set(r["cross"] for r in subset), key=int)
        rws     = sorted(set(r["rw"]    for r in subset), key=int)

        # one figure per rw_ratio so the grid stays readable
        for rw in rws:
            data = [r for r in subset if r["rw"] == rw]
            nrows, ncols = len(zipfs), len(crosses)
            fig, axes = plt.subplots(nrows, ncols,
                                     figsize=(3.5 * ncols, 3.5 * nrows),
                                     squeeze=False)
            fig.suptitle(
                f"Scan Detector Confusion Matrix\n"
                f"query={query}  rw_ratio={rw}",
                fontsize=11, y=1.01
            )

            for ri, zipf in enumerate(zipfs):
                for ci, cross in enumerate(crosses):
                    match = [r for r in data if r["zipf"] == zipf and r["cross"] == cross]
                    ax = axes[ri][ci]
                    if not match:
                        ax.axis("off")
                        continue
                    r = match[0]
                    draw_confusion(
                        ax,
                        r["tp"], r["tn"], r["fp"], r["fn"],
                        f"zipf={zipf}  cross={cross}",
                        r["precision"], r["recall"], r["f1"], r["accuracy"]
                    )

            # shared axis labels
            for ci, cross in enumerate(crosses):
                axes[0][ci].set_xlabel(f"cross={cross}", fontsize=8, labelpad=2)
            for ri, zipf in enumerate(zipfs):
                axes[ri][0].set_ylabel(f"zipf={zipf}", fontsize=8, labelpad=2)

            plt.tight_layout()
            out = os.path.join(results_dir,
                               f"scan_detector_confusion_query{query}_rw{rw}.png")
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved: {out}")

    # summary table to stdout
    print(f"\n{'query':6} {'zipf':6} {'cross':6} {'rw':4}  "
          f"{'TP':>8} {'TN':>8} {'FP':>8} {'FN':>8}  "
          f"{'Prec':>6} {'Rec':>6} {'F1':>6} {'Acc':>6}")
    print("-" * 90)
    for r in sorted(rows, key=lambda x: (x["query"], x["zipf"], int(x["cross"]), int(x["rw"]))):
        print(f"{r['query']:6} {r['zipf']:6} {r['cross']:6} {r['rw']:4}  "
              f"{r['tp']:>8,} {r['tn']:>8,} {r['fp']:>8,} {r['fn']:>8,}  "
              f"{r['precision']:>6.3f} {r['recall']:>6.3f} {r['f1']:>6.3f} {r['accuracy']:>6.3f}")


if __name__ == "__main__":
    main()
