import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt


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

#rw fixed
rw_value = 50
fig_df = plot_df[plot_df["rw_ratio"] == rw_value].copy()

grouped = (
    fig_df.groupby(["cross_ratio", "zipf_theta"], as_index=False)["total_hw_cc_usage"]
    .mean()
)

zipf_values = sorted(grouped["zipf_theta"].unique())

plt.figure(figsize=(8, 5))

for z in zipf_values:
    sub = grouped[grouped["zipf_theta"] == z].sort_values("cross_ratio")
    plt.plot(
        sub["cross_ratio"],
        sub["total_hw_cc_usage"],
        marker="o",
        label=f"zipf={z}"
    )

plt.xlabel("Cross Ratio")
plt.ylabel("Total HW Cache-Coherent Usage")
plt.title(f"Figure 4: HW Cache-Coherent Memory Usage vs Cross Ratio (rw_ratio={rw_value}, query_type=rmw)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("graphs/mem.png", dpi=300, bbox_inches="tight")