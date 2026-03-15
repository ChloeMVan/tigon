import pandas as pd

# Load data
df = pd.read_pickle("experiment_results.pkl")

# Optional: focus on one query type
# Change 'rmw' if needed, or comment this out to keep all query types
df = df[df["query_type"] == "rmw"].copy()

# Normalize policy names just in case
df["policy"] = df["policy"].astype(str).str.strip().str.upper()

# Keep only CLOCK and LRU
df = df[df["policy"].isin(["CLOCK", "LRU"])].copy()

# Make sure numeric columns are numeric
numeric_cols = [
    "cross_ratio",
    "zipf_theta",
    "rw_ratio",
    "total_commit",
    "cache_hit_rate"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Aggregate in case there are multiple runs per configuration
agg = (
    df.groupby(["policy", "cross_ratio", "zipf_theta", "rw_ratio"], as_index=False)
      .agg(
          throughput=("total_commit", "mean"),
          hit_rate=("cache_hit_rate", "mean")
      )
)

# Split Clock and LRU
clock = agg[agg["policy"] == "CLOCK"].copy()
lru = agg[agg["policy"] == "LRU"].copy()

# Merge side-by-side
merged = pd.merge(
    clock,
    lru,
    on=["cross_ratio", "zipf_theta", "rw_ratio"],
    suffixes=("_clock", "_lru")
)

# Compute speedup and hit-rate improvement
merged["speedup_lru_vs_clock"] = merged["throughput_lru"] / merged["throughput_clock"]
merged["hit_rate_diff"] = merged["hit_rate_lru"] - merged["hit_rate_clock"]

# Final table
table = merged[[
    "cross_ratio",
    "zipf_theta",
    "rw_ratio",
    "throughput_clock",
    "throughput_lru",
    "speedup_lru_vs_clock",
    "hit_rate_clock",
    "hit_rate_lru",
    "hit_rate_diff"
]].copy()

# Rename columns for presentation
table.columns = [
    "cross_ratio",
    "zipf_theta",
    "rw_ratio",
    "Clock Throughput",
    "LRU Throughput",
    "Speedup (LRU/Clock)",
    "Clock Hit Rate",
    "LRU Hit Rate",
    "Hit Rate Diff (LRU-Clock)"
]

table = table[
    table["cross_ratio"].isin([10, 50, 90]) &
    table["zipf_theta"].isin([0.5, 0.7, 0.9]) &
    table["rw_ratio"].isin([50])
]

# Sort for readability
table = table.sort_values(["cross_ratio", "zipf_theta", "rw_ratio"]).reset_index(drop=True)

# Optional formatting
pd.set_option("display.float_format", lambda x: f"{x:.3f}")

print(table)

# Save to CSV
table.to_csv("lru_vs_clock_performance_table.csv", index=False)

# Optional: export to LaTeX for paper/proposal use
latex_table = table.to_latex(index=False, float_format="%.3f")
with open("lru_vs_clock_performance_table.tex", "w") as f:
    f.write(latex_table)
print(table.to_string(index=False))

print("\nSaved:")
print("- lru_vs_clock_performance_table.csv")
print("- lru_vs_clock_performance_table.tex")