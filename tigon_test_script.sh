#!/bin/bash
# Execute from the Tigon root directory

set -euo pipefail

# --- WORKLOAD PARAMETERS ---
HOST_NUM=8
WORKER_NUM=3
KEYS=1000000 

RESULT_DIR="results/eviction_stress_test"
mkdir -p "$RESULT_DIR"

# --- CXL BUDGETS ---
# Shrink the budget to see when Clock starts failing
BUDGETS=(
  $((1024*1024*200))  # 200MB
  $((1024*1024*128))  # 128MB
  $((1024*1024*64))   # 64MB
  $((1024*1024*32))   # 32MB
  $((1024*1024*16))   # 16MB
  $((1024*1024*10))   # 10MB
)

POLICIES=("Clock" "LRU")

REPEATS=3
                                                                                                        
# Shared Tigon/YCSB parameters (must match run.sh YCSB order: 24 args after YCSB)
# run.sh YCSB: PROTOCOL HOST_NUM WORKER_NUM QUERY_TYPE KEYS RW_RATIO ZIPF_THETA CROSS_RATIO
#   USE_CXL_TRANS USE_OUTPUT_THREAD ENABLE_MIGRATION_OPTIMIZATION MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET
#   ENABLE_SCC SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE EPOCH_LEN MODEL_CXL_SEARCH_OVERHEAD GATHER_OUTPUT
USE_CXL_TRANS=1
USE_OUTPUT_THREAD=0
ENABLE_MIGRATION_OPTIMIZATION=1
WHEN_TO_MOVE_OUT="OnDemand"
ENABLE_SCC=1
SCC_MECH="WriteThrough"
PRE_MIGRATE="None"
TIME_TO_RUN=30
TIME_TO_WARMUP=10
LOGGING_TYPE="BLACKHOLE"
EPOCH_LEN=20000
MODEL_CXL_SEARCH=0
GATHER_OUTPUTS=0
SYSTEM="TwoPLPasha"

echo "Starting Eviction Test..."
echo "Results will be saved to $RESULT_DIR"

run_one() {
  local TEST_NAME=$1
  local QUERY_TYPE=$2
  local ZIPF_THETA=$3
  local CROSS_RATIO=$4
  local RW_RATIO=$5
  local POLICY=$6
  local BUDGET=$7
  local REPEAT_ID=$8

  local BUDGET_MB=$((BUDGET/1024/1024))
  local LOG_FILE="${RESULT_DIR}/${TEST_NAME}_${QUERY_TYPE}_rw${RW_RATIO}_zipf${ZIPF_THETA}_cross${CROSS_RATIO}_${POLICY}_${BUDGET_MB}MB_rep${REPEAT_ID}.log"

  echo "=========================================================="
  echo "Running: test=${TEST_NAME}"
  echo "  workload=${QUERY_TYPE}"
  echo "  rw_ratio=${RW_RATIO}"
  echo "  zipf=${ZIPF_THETA}"
  echo "  cross=${CROSS_RATIO}"
  echo "  policy=${POLICY}"
  echo "  budget=${BUDGET_MB}MB"
  echo "  repeat=${REPEAT_ID}/${REPEATS}"
  echo "=========================================================="

  {
    echo "# test_name=${TEST_NAME}"
    echo "# query_type=${QUERY_TYPE}"
    echo "# rw_ratio=${RW_RATIO}"
    echo "# zipf_theta=${ZIPF_THETA}"
    echo "# cross_ratio=${CROSS_RATIO}"
    echo "# policy=${POLICY}"
    echo "# hwcc_budget_mb=${BUDGET_MB}"
    echo "# repeat=${REPEAT_ID}"
    echo "# host_num=${HOST_NUM}"
    echo "# worker_num=${WORKER_NUM}"
    echo "# keys=${KEYS}"
    echo "# time_to_run=${TIME_TO_RUN}"
    echo "# time_to_warmup=${TIME_TO_WARMUP}"
    echo "# timestamp=$(date -Iseconds)"
    echo
  } | tee "$LOG_FILE"

  # Full YCSB call matching: ./scripts/run.sh YCSB TwoPLPasha 8 3 rmw 300000 50 0.7 10 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 10 BLACKHOLE 20000 0 0
  ./scripts/run.sh YCSB "$SYSTEM" "$HOST_NUM" "$WORKER_NUM" "$QUERY_TYPE" \
    "$KEYS" "$RW_RATIO" "$ZIPF_THETA" "$CROSS_RATIO" \
    "$USE_CXL_TRANS" "$USE_OUTPUT_THREAD" "$ENABLE_MIGRATION_OPTIMIZATION" "$POLICY" "$WHEN_TO_MOVE_OUT" "$BUDGET" \
    "$ENABLE_SCC" "$SCC_MECH" "$PRE_MIGRATE" "$TIME_TO_RUN" "$TIME_TO_WARMUP" "$LOGGING_TYPE" "$EPOCH_LEN" "$MODEL_CXL_SEARCH" "$GATHER_OUTPUTS" \
    | tee -a "$LOG_FILE"

  sleep 5
}

run_experiment() {
  local TEST_NAME=$1
  local QUERY_TYPE=$2
  local ZIPF_THETA=$3
  local CROSS_RATIO=$4
  local RW_RATIO=$5

  for POLICY in "${POLICIES[@]}"; do
    for BUDGET in "${BUDGETS[@]}"; do
      for ((rep=1; rep<=REPEATS; rep++)); do
        run_one "$TEST_NAME" "$QUERY_TYPE" "$ZIPF_THETA" "$CROSS_RATIO" "$RW_RATIO" "$POLICY" "$BUDGET" "$rep"
      done
    done
  done
}

# run_experiment "test_name" "query_type" zipf_theta cross_ratio rw_ratio

echo "=== 1. CONTROL BASELINE ==="
run_experiment "control_mixed_zipf099_cross20_rw50" "mixed" 0.7 20 50

echo "=== 2. CAPACITY STRESS ==="
run_experiment "stress_mixed_zipf05_cross100_rw50" "mixed" 0.5 100 50

echo "=== 3. SCAN POLLUTION STRESS ==="
run_experiment "stress_scan_zipf0_cross100_rw50" "scan" 0 100 50

echo "=== 4. WRITE-HEAVY DIRTY EVICTION  ==="
run_experiment "stress_rmw_zipf05_cross100_rw10" "rmw" 0.5 100 10

echo "=== 5. READ-HEAVY HOTSPOT REUSE ==="
run_experiment "stress_mixed_zipf099_cross80_rw95" "mixed" 0.99 80 95

echo "Done."
echo "Logs are in $RESULT_DIR"