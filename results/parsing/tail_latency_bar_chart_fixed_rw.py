import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


df = pd.read_pickle("experiment_results.pkl")

#fixed rmw
plot_df = df[(df["policy"] == "Clock") & (df["query_type"] == "rmw")].copy()


numeric_cols = [
    "cross_ratio", "zipf_theta", "rw_ratio", "cache_hit_rate", "total_commit",
    "round_trip_latency_50th", "round_trip_latency_95th", "round_trip_latency_99th",
    "total_hw_cc_usage"
]

for col in numeric_cols:
    plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")

rw_value = 50
fig_df = plot_df[plot_df["rw_ratio"] == rw_value].copy()

grouped = (
    fig_df.groupby(["cross_ratio", "zipf_theta"], as_index=False)[
        ["round_trip_latency_50th", "round_trip_latency_95th", "round_trip_latency_99th"]
    ].mean()
)

grouped = grouped.sort_values(["cross_ratio", "zipf_theta"]).reset_index(drop=True)
labels = [f"c={r.cross_ratio}, z={r.zipf_theta}" for r in grouped.itertuples()]

x = np.arange(len(grouped))
width = 0.25

plt.figure(figsize=(12, 5))
plt.bar(x - width, grouped["round_trip_latency_50th"], width, label="p50")
plt.bar(x,         grouped["round_trip_latency_95th"], width, label="p95")
plt.bar(x + width, grouped["round_trip_latency_99th"], width, label="p99")

plt.xticks(x, labels, rotation=45, ha="right")
plt.ylabel("Round Trip Latency")
plt.title(f"Figure 3: Tail Latency by Workload (rw_ratio={rw_value}, query_type=rmw)")
plt.legend()
plt.grid(True, axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("graphs/tail_latency.png", dpi=300, bbox_inches="tight")