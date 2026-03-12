#!/bin/bash
#
# Run all YCSB MIGRATION_POLICY experiments (2 VMs) and write results to .txt files
# for later graphing. Execute from the Tigon repository root.
#
# Usage:
#   ./scripts/run_ycsb_migration_experiments.sh              # run all experiments
#   ./scripts/run_ycsb_migration_experiments.sh 1           # only experiment 1 (policy comparison)
#   ./scripts/run_ycsb_migration_experiments.sh 2            # only experiment 2 (eviction & budget)
#   ./scripts/run_ycsb_migration_experiments.sh 3           # only experiment 3 (skew & cross)
#   ./scripts/run_ycsb_migration_experiments.sh 4           # only experiment 4 (insert-only)
#

set -euo pipefail

# --- Configuration (2-VM setup per plan) ---
HOST_NUM=2
WORKER_NUM=3
KEYS=200000
RESULTS_DIR="results/ycsb_migration_policy_experiments"
REPEATS_POLICY=2
REPEATS_OTHER=1

# Shared YCSB/run.sh parameters (24 args after YCSB)
PROTOCOL="TwoPLPasha"
USE_CXL_TRANS=1
USE_OUTPUT_THREAD=0
ENABLE_MIGRATION_OPTIMIZATION=1
ENABLE_SCC=1
SCC_MECH="WriteThrough"
PRE_MIGRATE="None"
TIME_TO_RUN=30
TIME_TO_WARMUP=10
LOGGING_TYPE="BLACKHOLE"
EPOCH_LEN=20000
MODEL_CXL_SEARCH=0
GATHER_OUTPUT=0

# Experiment selector: 0 = all, 1–4 = that experiment only
RUN_EXP="${1:-0}"

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
  local WHEN_TO_MOVE_OUT=$6
  local HW_CC_BUDGET=$7
  local REPEAT_ID=$8
  local LABEL=$9

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

# --- Experiment 1: Policy comparison (scan, rmw, insert × Clock, LRU, FIFO, NoMoveOut) ---
experiment_1_policy_comparison() {
  local WHEN_TO_MOVE_OUT="Reactive"
  local HW_CC_BUDGET=$((1024 * 1024 * 200))
  local CROSS_RATIO=0
  local RW_RATIO=80
  local ZIPF_RMW=0
  local ZIPF_INSERT=0

  for workload in scan rmw insert; do
    for policy in Clock LRU FIFO NoMoveOut; do
      for ((rep = 1; rep <= REPEATS_POLICY; rep++)); do
        local zipf_val=0
        [[ "$workload" == "rmw" ]] && zipf_val=$ZIPF_RMW
        [[ "$workload" == "insert" ]] && zipf_val=$ZIPF_INSERT
        local rw=80
        [[ "$workload" != "rmw" ]] && rw=0
        run_one "$workload" "$rw" "$zipf_val" "$CROSS_RATIO" "$policy" "$WHEN_TO_MOVE_OUT" "$HW_CC_BUDGET" "$rep" \
          "exp1_policy_${workload}_${policy}_rep${rep}"
      done
    done
  done
}

# --- Experiment 2: When-to-evict and budget (LRU; scan, rmw, insert) ---
experiment_2_eviction_budget() {
  local POLICY="LRU"
  local BUDGETS_MB=(32 64 128 200)
  local WHEN_OPTS=("Reactive" "OnDemand")
  local RW_RATIO=80
  local ZIPF_THETA=0
  local CROSS_RATIO=0

  for workload in scan rmw insert; do
    for when in "${WHEN_OPTS[@]}"; do
      for mb in "${BUDGETS_MB[@]}"; do
        for ((rep = 1; rep <= REPEATS_OTHER; rep++)); do
          local budget=$((1024 * 1024 * mb))
          run_one "$workload" "$RW_RATIO" "$ZIPF_THETA" "$CROSS_RATIO" "$POLICY" "$when" "$budget" "$rep" \
            "exp2_eviction_${workload}_${when}_${mb}MB_rep${rep}"
        done
      done
    done
  done
}

# --- Experiment 3: Skew and cross-partition (LRU, Clock; scan, rmw, insert) ---
experiment_3_skew_cross() {
  local WHEN_TO_MOVE_OUT="Reactive"
  local HW_CC_BUDGET=$((1024 * 1024 * 200))
  local RW_RATIO=80
  local ZIPF_VALS=(0 0.5 0.99)
  local CROSS_VALS=(0 20 50)
  local POLICIES=(LRU Clock)

  for workload in scan rmw insert; do
    for policy in "${POLICIES[@]}"; do
      for zipf in "${ZIPF_VALS[@]}"; do
        for cross in "${CROSS_VALS[@]}"; do
          for ((rep = 1; rep <= REPEATS_OTHER; rep++)); do
            run_one "$workload" "$RW_RATIO" "$zipf" "$cross" "$policy" "$WHEN_TO_MOVE_OUT" "$HW_CC_BUDGET" "$rep" \
              "exp3_skew_${workload}_${policy}_zipf${zipf}_cross${cross}_rep${rep}"
          done
        done
      done
    done
  done
}

# --- Experiment 4: Insert-only (all policies, optional budgets) ---
experiment_4_insert_only() {
  local WHEN_TO_MOVE_OUT="Reactive"
  local HW_CC_BUDGET=$((1024 * 1024 * 200))
  local CROSS_RATIO=0
  local RW_RATIO=0
  local ZIPF_THETA=0

  for policy in Clock LRU FIFO NoMoveOut; do
    for ((rep = 1; rep <= REPEATS_POLICY; rep++)); do
      run_one "insert" "$RW_RATIO" "$ZIPF_THETA" "$CROSS_RATIO" "$policy" "$WHEN_TO_MOVE_OUT" "$HW_CC_BUDGET" "$rep" \
        "exp4_insert_${policy}_rep${rep}"
    done
  done
}

# --- Main ---
echo "YCSB migration policy experiments (2 VMs)"
echo "Results directory: $RESULTS_ABS"
echo ""

if [[ "$RUN_EXP" == "1" ]]; then
  echo "=== Experiment 1: Policy comparison ==="
  experiment_1_policy_comparison
elif [[ "$RUN_EXP" == "2" ]]; then
  echo "=== Experiment 2: Eviction & budget ==="
  experiment_2_eviction_budget
elif [[ "$RUN_EXP" == "3" ]]; then
  echo "=== Experiment 3: Skew & cross-partition ==="
  experiment_3_skew_cross
elif [[ "$RUN_EXP" == "4" ]]; then
  echo "=== Experiment 4: Insert-only ==="
  experiment_4_insert_only
else
  echo "=== Experiment 1: Policy comparison ==="
  experiment_1_policy_comparison
  echo ""
  echo "=== Experiment 2: Eviction & budget ==="
  experiment_2_eviction_budget
  echo ""
  echo "=== Experiment 3: Skew & cross-partition ==="
  experiment_3_skew_cross
  echo ""
  echo "=== Experiment 4: Insert-only ==="
  experiment_4_insert_only
fi

echo ""
echo "Done. Results written to $RESULTS_ABS"
echo ""
echo "For graphing, grep/parse each .txt for:"
echo "  - Throughput: total_commit (Global Stats), or commit: per window"
echo "  - Cache hit rate: 'cache hit rate: X%' (SCCManager), or local_cxl_access/local_access from Coordinator"
echo "  - Latency: round_trip_latency (50th/75th/95th/99th), or Worker latency percentiles"
