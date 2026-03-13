#!/bin/bash
#
# Run YCSB experiments varying migration policy (Clock, LRU), query type (scan, rmw),
# cross_ratio (10, 50, 90), zipf_theta (0.5, 0.7, 0.99), rw_ratio (10, 50, 90).
# Results written to .txt files for throughput, latency, and cache hit rate analysis.
# Execute from the Tigon repository root.
#
# Usage: ./scripts/run_ycsb_migration_experiments.sh
#

set -euo pipefail

# --- Configuration (2-VM setup) ---
HOST_NUM=2
WORKER_NUM=3
KEYS=200000
RESULTS_DIR="results/ycsb_migration_policy_experiments"
REPEATS=1

# Fixed YCSB/run.sh parameters
PROTOCOL="TwoPLPasha"
USE_CXL_TRANS=1
USE_OUTPUT_THREAD=0
ENABLE_MIGRATION_OPTIMIZATION=1
WHEN_TO_MOVE_OUT="Reactive"
HW_CC_BUDGET=$((1024 * 1024 * 200))
ENABLE_SCC=1
SCC_MECH="WriteThrough"
PRE_MIGRATE="None"
TIME_TO_RUN=30
TIME_TO_WARMUP=10
LOGGING_TYPE="BLACKHOLE"
EPOCH_LEN=20000
MODEL_CXL_SEARCH=0
GATHER_OUTPUT=0

# Sweep dimensions
POLICIES=(Clock LRU)
QUERY_TYPES=(scan rmw)
CROSS_RATIOS=(10 50 90)
ZIPF_THETAS=(0.5 0.7 0.99)
RW_RATIOS=(10 50 90)

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." &>/dev/null && pwd)
mkdir -p "$REPO_ROOT/$RESULTS_DIR"
RESULTS_ABS="$REPO_ROOT/$RESULTS_DIR"

# run.sh YCSB: PROTOCOL HOST_NUM WORKER_NUM QUERY_TYPE KEYS RW_RATIO ZIPF_THETA CROSS_RATIO
#             USE_CXL_TRANS USE_OUTPUT_THREAD ENABLE_MIGRATION_OPTIMIZATION MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET
#             ENABLE_SCC SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE EPOCH_LEN MODEL_CXL_SEARCH GATHER_OUTPUT
run_one() {
  local QUERY_TYPE=$1
  local RW_RATIO=$2
  local ZIPF_THETA=$3
  local CROSS_RATIO=$4
  local POLICY=$5
  local REPEAT_ID=$6
  local LABEL=$7

  local BUDGET_MB=$((HW_CC_BUDGET / 1024 / 1024))
  local SAFE_LABEL=$(echo "$LABEL" | tr ' ' '_' | tr -cd '[:alnum:]_.-')
  local OUT_FILE="${RESULTS_ABS}/${SAFE_LABEL}.txt"

  echo "=========================================================="
  echo "Running: $LABEL"
  echo "  output: $OUT_FILE"
  echo "=========================================================="

  {
    echo "# YCSB migration policy experiment"
    echo "# query_type=$QUERY_TYPE rw_ratio=$RW_RATIO zipf_theta=$ZIPF_THETA cross_ratio=$CROSS_RATIO"
    echo "# policy=$POLICY when_to_move_out=$WHEN_TO_MOVE_OUT hw_cc_budget_mb=$BUDGET_MB"
    echo "# host_num=$HOST_NUM worker_num=$WORKER_NUM keys=$KEYS repeat=$REPEAT_ID"
    echo "# time_to_run=$TIME_TO_RUN time_to_warmup=$TIME_TO_WARMUP"
    echo "# timestamp=$(date -Iseconds)"
    echo "---"
  } > "$OUT_FILE"

  cd "$REPO_ROOT"
  ./scripts/run.sh YCSB "$PROTOCOL" "$HOST_NUM" "$WORKER_NUM" "$QUERY_TYPE" \
    "$KEYS" "$RW_RATIO" "$ZIPF_THETA" "$CROSS_RATIO" \
    "$USE_CXL_TRANS" "$USE_OUTPUT_THREAD" "$ENABLE_MIGRATION_OPTIMIZATION" "$POLICY" "$WHEN_TO_MOVE_OUT" "$HW_CC_BUDGET" \
    "$ENABLE_SCC" "$SCC_MECH" "$PRE_MIGRATE" "$TIME_TO_RUN" "$TIME_TO_WARMUP" "$LOGGING_TYPE" "$EPOCH_LEN" "$MODEL_CXL_SEARCH" "$GATHER_OUTPUT" \
    >> "$OUT_FILE" 2>&1

  echo "  done."
}

# --- Main sweep: policy × query_type × cross × zipf × rw_ratio × repeats ---
echo "YCSB migration experiments (2 VMs)"
echo "  policies: ${POLICIES[*]}"
echo "  query_types: ${QUERY_TYPES[*]}"
echo "  cross_ratio: ${CROSS_RATIOS[*]}"
echo "  zipf_theta: ${ZIPF_THETAS[*]}"
echo "  rw_ratio: ${RW_RATIOS[*]}"
echo "  repeats: $REPEATS"
echo "Results directory: $RESULTS_ABS"
echo ""

total=0
for policy in "${POLICIES[@]}"; do
  for query in "${QUERY_TYPES[@]}"; do
    for cross in "${CROSS_RATIOS[@]}"; do
      for zipf in "${ZIPF_THETAS[@]}"; do
        for rw in "${RW_RATIOS[@]}"; do
          for ((rep = 1; rep <= REPEATS; rep++)); do
            label="policy_${policy}_query_${query}_cross${cross}_zipf${zipf}_rw${rw}"
            [[ $REPEATS -gt 1 ]] && label="${label}_rep${rep}"
            run_one "$query" "$rw" "$zipf" "$cross" "$policy" "$rep" "$label"
            ((total++)) || true
          done
        done
      done
    done
  done
done

echo ""
echo "Done. $total runs written to $RESULTS_ABS"
echo ""
echo "For throughput, latency, and cache hit rate, parse each .txt for:"
echo "  - Throughput: total_commit (Global Stats), or commit: per window"
echo "  - Cache hit rate: 'cache hit rate: X%' (SCCManager), or local_cxl_access/local_access"
echo "  - Latency: round_trip_latency (50th/75th/95th/99th), or Worker latency percentiles"
