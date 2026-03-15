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
    
import matplotlib.pyplot as plt

# rw fixed
rw_value = 50
fig_df = plot_df[plot_df["rw_ratio"] == rw_value].copy()

grouped = (
    fig_df.groupby(["zipf_theta", "cross_ratio"], as_index=False)["cache_hit_rate"]
    .mean()
)

cross_values = sorted(grouped["cross_ratio"].unique())

plt.figure(figsize=(8, 5))

for c in cross_values:
    sub = grouped[grouped["cross_ratio"] == c].sort_values("zipf_theta")
    plt.plot(
        sub["zipf_theta"],
        sub["cache_hit_rate"],
        marker="o",
        label=f"cross={c}"
    )

plt.xlabel("Zipf Skew (zipf_theta)")
plt.ylabel("Cache Hit Rate")
plt.title(f"Figure 2: Cache Hit Rate vs Zipf Skew (rw_ratio={rw_value}, query_type=rmw)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("graphs/cache_hit.png", dpi=300, bbox_inches="tight")