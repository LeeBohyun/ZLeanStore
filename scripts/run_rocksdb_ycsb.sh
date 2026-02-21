#!/usr/bin/env bash
set -euo pipefail

# ----------------------------------------------------------
# run_rocksdb_ycsb.sh
#
# Phase 0: Prepare SSD (blkdiscard + mkfs.ext4 + mount).
# Phase 1: Run RocksDB_YCSB to LOAD FLAGS_ycsb_record_count records
#          (exec_seconds = 0 -> effectively load-only).
# Phase 2: Run RocksDB_YCSB again on the same DB to RUN the workload
#          for RUN_DURATION seconds.
#
# Logs and monitoring outputs go under RESULT_BASE/<timestamp>/.
# ----------------------------------------------------------

usage() {
  cat <<EOF
Usage: $0 [options]

Paths / binaries (defaults = your current setup):

  -B  RocksDB_YCSB binary path
      [default: \$HOME/git/LeanStoreOOPW/build/benchmark/RocksDB_YCSB]

  -R  Result base directory
      [default: /home/lee/result/rocksdb_ycsb/bigcache]

  -D  Datadir / mountpoint (filesystem on the SSD)
      [default: /home/lee/test_data]

  -W  WAL directory
      [default: /home/lee/test_log]

  -S  SMART script path
      [default: /home/lee/scripts/get_smart_info.sh]

Device / sudo:

  -n  SMART device / SSD under test
      [default: /dev/nvme2n1]

  -P  Sudo password (for blkdiscard/mkfs/mount).
      If omitted, you will be prompted.

Workload knobs (optional; have good defaults):

  -y  YCSB dataset size in GB    [default: 800]
  -c  YCSB record count          [default: 4160000000]
  -t  Run duration (seconds)     [default: 10800]
  -w  Worker count               [default: 64]
  -z  Zipf theta                 [default: 0.8]
  -r  Read ratio (0–100)         [default: 50]

Example:
  $0 -P your_sudo_pw -n /dev/nvme2n1 -D /home/lee/test_data
EOF
  exit 1
}

# ---------------- Defaults ----------------

SUDO_PASS="${SUDO_PASS:-}"          # can also be set via env

REPO_ROOT="${HOME}/git/LeanStoreOOPW"
ROCKSDB_YCSB_BIN="${REPO_ROOT}/build/benchmark/RocksDB_YCSB"

SMART_DEVICE="/dev/nvme2n1"         # SSD under test
DATADIR="/home/lee/test_data"       # mountpoint of the SSD
ROCKSDB_DIR=""                      # will be derived from DATADIR
WAL_DIR="/home/lee/test_log"

SMART_SCRIPT="/home/lee/scripts/get_smart_info.sh"

RESULT_BASE="/home/lee/result/rocksdb_ycsb/bigcache"

# Workload parameters
YCSB_DATASET_SIZE_GB=800
YCSB_RECORD_COUNT=4160000000
YCSB_READ_RATIO=50
YCSB_ZIPF_THETA=0.8
WORKER_COUNT=64

LOAD_EXEC_SECONDS=0
RUN_DURATION=10800

# Monitoring knobs
DU_INTERVAL=1                       # seconds between du samples
MONITOR_MAX_SECS=$(( RUN_DURATION + 7200 ))   # run + 2h cushion

# RocksDB tuning knobs (still here if you wire them up later)
WRITE_BUFFER_MB=512
MAX_WRITE_BUFFERS=6
BLOCK_SIZE_KB=16
SST_SIZE_MB=64
L0_FILE_NUM_COMPACTION_TRIGGER=4
L0_SLOWDOWN_WRITES_TRIGGER=20
L0_STOP_WRITES_TRIGGER=36
ENABLE_BLOOM=1
ENABLE_STATISTICS=1

# ---------------- Parse CLI args ----------------

while getopts "B:R:D:W:S:n:P:y:c:t:w:z:r:h" opt; do
  case "$opt" in
    B) ROCKSDB_YCSB_BIN=$OPTARG ;;
    R) RESULT_BASE=$OPTARG ;;
    D) DATADIR=$OPTARG ;;
    W) WAL_DIR=$OPTARG ;;
    S) SMART_SCRIPT=$OPTARG ;;
    n) SMART_DEVICE=$OPTARG ;;
    P) SUDO_PASS=$OPTARG ;;
    y) YCSB_DATASET_SIZE_GB=$OPTARG ;;
    c) YCSB_RECORD_COUNT=$OPTARG ;;
    t) RUN_DURATION=$OPTARG ;;
    w) WORKER_COUNT=$OPTARG ;;
    z) YCSB_ZIPF_THETA=$OPTARG ;;
    r) YCSB_READ_RATIO=$OPTARG ;;
    h) usage ;;
    *) usage ;;
  esac
done

if [[ -z "$SUDO_PASS" ]]; then
  read -s -p "Enter sudo password: " SUDO_PASS
  echo
fi

if [[ -z "$ROCKSDB_DIR" ]]; then
  ROCKSDB_DIR="${DATADIR}/rocksdb_ycsb"
fi

# ---------------- Basic checks ----------------

if [[ ! -x "$ROCKSDB_YCSB_BIN" ]]; then
  echo "ERROR: RocksDB_YCSB not found or not executable at: $ROCKSDB_YCSB_BIN" >&2
  echo "Build it with:  cmake --build build -j  (from LeanStoreOOPW root)" >&2
  exit 1
fi

# ---------------- Prep result dir ----------------

mkdir -p "$RESULT_BASE"
TS="$(date +%F_%H-%M-%S)"
RUN_DIR="$RESULT_BASE/$TS"
mkdir -p "$RUN_DIR"

YCSB_LOAD_LOG="$RUN_DIR/rocksdb_ycsb_load.csv"
YCSB_RUN_LOG="$RUN_DIR/rocksdb_ycsb_run.csv"
DU_LOG="$RUN_DIR/du_bytes.csv"
IOSTAT_LOG="$RUN_DIR/iostat.log"
SMART_LOG="$RUN_DIR/smart.log"

# Record run meta
{
  echo "timestamp: $TS"
  echo "binary: $ROCKSDB_YCSB_BIN"
  echo "repo_root: $REPO_ROOT"
  echo "datadir (mountpoint): $DATADIR"
  echo "rocksdb_dir (FLAGS_db_dir): $ROCKSDB_DIR"
  echo "wal_dir (FLAGS_wal_path): $WAL_DIR"
  echo "smart_device: $SMART_DEVICE"
  echo "result_base: $RESULT_BASE"
  echo "ycsb_dataset_size_gb: $YCSB_DATASET_SIZE_GB"
  echo "ycsb_record_count: $YCSB_RECORD_COUNT"
  echo "ycsb_read_ratio: $YCSB_READ_RATIO"
  echo "ycsb_zipf_theta: $YCSB_ZIPF_THETA"
  echo "worker_count: $WORKER_COUNT"
  echo "load_exec_seconds: $LOAD_EXEC_SECONDS"
  echo "run_exec_seconds: $RUN_DURATION"
  echo "write_buffer_mb: $WRITE_BUFFER_MB"
  echo "max_write_buffers: $MAX_WRITE_BUFFERS"
  echo "block_size_kb: $BLOCK_SIZE_KB"
  echo "sst_size_mb: $SST_SIZE_MB"
  echo "l0_trigger: $L0_FILE_NUM_COMPACTION_TRIGGER"
  echo "l0_slowdown_trigger: $L0_SLOWDOWN_WRITES_TRIGGER"
  echo "l0_stop_trigger: $L0_STOP_WRITES_TRIGGER"
  echo "enable_bloom: $ENABLE_BLOOM"
  echo "enable_statistics: $ENABLE_STATISTICS"
} > "$RUN_DIR/run.meta"

echo "==> Logs: $RUN_DIR"

# ---------------- Cleanup handler ----------------

pids=()

cleanup() {
  echo "==> Stopping background loggers..."
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# ---------------- Prepare filesystem on SSD ----------------

echo "==> Preparing SSD on $SMART_DEVICE..."

mkdir -p "$DATADIR"
echo "$SUDO_PASS" | sudo -S umount "$DATADIR" 2>/dev/null || true
echo "$SUDO_PASS" | sudo -S blkdiscard -f "$SMART_DEVICE"
echo "$SUDO_PASS" | sudo -S mkfs.ext4 -F "$SMART_DEVICE"
echo "$SUDO_PASS" | sudo -S mount "$SMART_DEVICE" "$DATADIR"
echo "$SUDO_PASS" | sudo -S chown -R "$USER:$USER" "$DATADIR"

# Fresh RocksDB dir + WAL dir
rm -rf "$ROCKSDB_DIR" "$WAL_DIR"
mkdir -p "$ROCKSDB_DIR" "$WAL_DIR"

# ---------------- Background monitors ----------------

# 1) du sampler
(
  echo "timestamp,physical_bytes,logical_bytes" > "$DU_LOG"
  end=$(( $(date +%s) + MONITOR_MAX_SECS ))
  while [[ "$(date +%s)" -lt "$end" ]]; do
    physical=$(du -sb "$ROCKSDB_DIR" 2>/dev/null | awk '{print $1}')
    logical=$(du --apparent-size -sb "$ROCKSDB_DIR" 2>/dev/null | awk '{print $1}')
    printf "%s,%s,%s\n" "$(date +%F' '%T)" "$physical" "$logical" >> "$DU_LOG"
    sleep "$DU_INTERVAL"
  done
) &
pids+=("$!")

# 2) iostat
if command -v iostat >/dev/null 2>&1; then
  (
    timeout "$MONITOR_MAX_SECS" iostat -x 1 "$SMART_DEVICE"
  ) > "$IOSTAT_LOG" 2>&1 &
  pids+=("$!")
else
  echo "iostat not found; install 'sysstat' to enable." > "$IOSTAT_LOG"
fi

# 3) SMART logging
if [[ -x "$SMART_SCRIPT" ]]; then
  (
    end=$(( $(date +%s) + MONITOR_MAX_SECS ))
    while [[ "$(date +%s)" -lt "$end" ]]; do
      echo "===== $(date +%F' '%T) START =====" >> "$SMART_LOG"
      "$SMART_SCRIPT" "$SUDO_PASS" "$RUN_DIR" "$SMART_DEVICE" >> "$SMART_LOG" 2>&1 || true
      echo "===== $(date +%F' '%T) END =====" >> "$SMART_LOG"
      sleep 300   # every 5 minutes
    done
  ) &
  pids+=("$!")
else
  echo "$SMART_SCRIPT not found or not executable" > "$SMART_LOG"
fi

# ---------------- PHASE 1: LOAD ONLY ----------------

echo "==> Phase 1: RocksDB_YCSB LOAD (record_count = $YCSB_RECORD_COUNT)"

YCSB_LOAD_CMD=(
  "$ROCKSDB_YCSB_BIN"

  --db_dir="$ROCKSDB_DIR"
  --wal_path="$WAL_DIR"

  --worker_count="$WORKER_COUNT"
  --ycsb_dataset_size_gb="$YCSB_DATASET_SIZE_GB"
  --ycsb_record_count="$YCSB_RECORD_COUNT"
  --ycsb_read_ratio="$YCSB_READ_RATIO"
  --ycsb_zipf_theta="$YCSB_ZIPF_THETA"
  --ycsb_exec_seconds="$LOAD_EXEC_SECONDS"
)

printf "    %q " "${YCSB_LOAD_CMD[@]}"; echo
(
  cd "$REPO_ROOT" && "${YCSB_LOAD_CMD[@]}"
) | tee "$YCSB_LOAD_LOG"

# ---------------- PHASE 2: RUN ONLY ----------------

echo "==> Phase 2: RocksDB_YCSB RUN (duration = ${RUN_DURATION}s, record_count = $YCSB_RECORD_COUNT)"

YCSB_RUN_CMD=(
  "$ROCKSDB_YCSB_BIN"

  --db_dir="$ROCKSDB_DIR"
  --wal_path="$WAL_DIR"

  --worker_count="$WORKER_COUNT"
  --ycsb_dataset_size_gb="$YCSB_DATASET_SIZE_GB"
  --ycsb_record_count="$YCSB_RECORD_COUNT"
  --ycsb_read_ratio="$YCSB_READ_RATIO"
  --ycsb_zipf_theta="$YCSB_ZIPF_THETA"
  --ycsb_exec_seconds="$RUN_DURATION"
)

printf "    %q " "${YCSB_RUN_CMD[@]}"; echo
(
  cd "$REPO_ROOT" && "${YCSB_RUN_CMD[@]}"
) | tee "$YCSB_RUN_LOG"

# ---------------- Final size snapshot ----------------

physical=$(du -sb "$ROCKSDB_DIR" 2>/dev/null | awk '{print $1}')
logical=$(du --apparent-size -sb "$ROCKSDB_DIR" 2>/dev/null | awk '{print $1}')
printf "%s,%s,%s\n" "$(date +%F' '%T)" "$physical" "$logical" >> "$DU_LOG"

echo "==> Done. Results in $RUN_DIR"
echo "    LOAD log : $YCSB_LOAD_LOG"
echo "    RUN log  : $YCSB_RUN_LOG"
echo "    du       : $DU_LOG"
echo "    iostat   : $IOSTAT_LOG"
echo "    SMART    : $SMART_LOG"

pkill -f get_smart_info.sh 2>/dev/null || true
