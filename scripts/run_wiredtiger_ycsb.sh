#!/usr/bin/env bash
set -euo pipefail

########################################
# Usage / argument parsing
########################################
usage() {
  cat <<EOF
Usage: $0 [options]

Paths / binaries:

  -B  WiredTiger_YCSB binary
      [default: \$HOME/git/LeanStoreOOPW/build/benchmark/WiredTiger_YCSB]

  -d  Data device (SSD under test)
      [default: /dev/nvme2n1]

  -m  Mountpoint of the SSD
      [default: /home/lee/test_data]

  -o  Base output directory
      [default: /home/lee/result/ycsb/s0/wiredtiger/800/tuned]

  -w  WAL directory
      [default: /home/lee/test_log]

  -s  SMART monitoring script
      [default: /home/lee/scripts/get_smart_info.sh]

Passwords / sudo:

  -P  Sudo/SMART password (for blkdiscard/mkfs/mount/chown)
      If omitted, you will be prompted.

Workload knobs (optional; have defaults):

  -r  YCSB recordcount          [default: 4160000000]
  -z  Zipf theta                [default: 0.8]
  -R  Read ratio (0–100)        [default: 50]
  -t  Run duration (seconds)    [default: 10800]
  -l  Load threads              [default: 64]
  -T  Run threads               [default: 64]

Example:
  $0 -P your_sudo_pw -d /dev/nvme2n1 -m /home/lee/test_data
EOF
  exit 1
}

########################################
# Default settings (can be overridden)
########################################

# Path to WiredTiger YCSB benchmark binary
WT_BENCH_BIN="${WT_BENCH_BIN:-$HOME/git/LeanStoreOOPW/build/benchmark/WiredTiger_YCSB}"

# Where to store the WiredTiger DB (inside the mounted SSD)
DB_SUBDIR="${DB_SUBDIR:-wiredtiger_ycsb}"

# Data device / mountpoint for WiredTiger
TEST_DEVICE="${TEST_DEVICE:-/dev/nvme2n1}"
TEST_MOUNTPOINT="${TEST_MOUNTPOINT:-/home/lee/test_data}"

# Owner of the mounted filesystem (after mkfs/mount)
FS_OWNER="${FS_OWNER:-$USER}"
FS_GROUP="${FS_GROUP:-$USER}"

# YCSB-style dataset parameters
RECORDCOUNT="${RECORDCOUNT:-4160000000}"
FIELDLENGTH="${FIELDLENGTH:-120}"
ZIPFIAN_CONSTANT="${ZIPFIAN_CONSTANT:-0.8}"
READ_RATIO="${READ_RATIO:-50}"

# Concurrency and duration
LOAD_THREADS="${LOAD_THREADS:-64}"
RUN_THREADS="${RUN_THREADS:-64}"
MAX_EXEC_SECS="${MAX_EXEC_SECS:-10800}"

# WiredTiger cache sizes (GB) for load and run
WT_CACHE_LOAD_GB="${WT_CACHE_LOAD_GB:-160}"
WT_CACHE_RUN_GB="${WT_CACHE_RUN_GB:-80}"

# Monitoring
SMART_SCRIPT="${SMART_SCRIPT:-/home/lee/scripts/get_smart_info.sh}"
SMART_INTERVAL_SEC="${SMART_INTERVAL_SEC:-480}"        # every 5 minutes
SMART_DEVICE="${SMART_DEVICE:-}"                       # will default to TEST_DEVICE
SMART_PASSWORD="${SMART_PASSWORD:-}"                   # will ask if empty
IOSTAT_INTERVAL_SEC="${IOSTAT_INTERVAL_SEC:-10}"

# WAL dir (was hardcoded before)
WAL_DIR="${WAL_DIR:-/home/lee/test_log}"

# Output base directory (actual OUTDIR will be base + timestamp)
BASE_OUT="${BASE_OUT:-/home/lee/result/ycsb/s0/wiredtiger/800/tuned}"

########################################
# Parse CLI arguments
########################################

while getopts "B:d:m:o:w:s:P:r:z:R:t:l:T:h" opt; do
  case "$opt" in
    B) WT_BENCH_BIN=$OPTARG ;;
    d) TEST_DEVICE=$OPTARG ;;
    m) TEST_MOUNTPOINT=$OPTARG ;;
    o) BASE_OUT=$OPTARG ;;
    w) WAL_DIR=$OPTARG ;;
    s) SMART_SCRIPT=$OPTARG ;;
    P) SMART_PASSWORD=$OPTARG ;;
    r) RECORDCOUNT=$OPTARG ;;
    z) ZIPFIAN_CONSTANT=$OPTARG ;;
    R) READ_RATIO=$OPTARG ;;
    t) MAX_EXEC_SECS=$OPTARG ;;
    l) LOAD_THREADS=$OPTARG ;;
    T) RUN_THREADS=$OPTARG ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Defaults that depend on other vars
if [[ -z "$SMART_DEVICE" ]]; then
  SMART_DEVICE="$TEST_DEVICE"
fi

if [[ -z "$SMART_PASSWORD" ]]; then
  read -s -p "Enter sudo/SMART password: " SMART_PASSWORD
  echo
fi

# Output paths
OUTDIR="$BASE_OUT/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTDIR"

LOAD_LOG="$OUTDIR/load.csv"
RUN_LOG="$OUTDIR/run_a.csv"
IOSTAT_LOG="$OUTDIR/iostat.log"
SMART_LOG="$OUTDIR/smart.log"
WT_STATS_LOG="$OUTDIR/wiredtiger_stats.log"   # stdout/stats from WT binary

echo "=== Output directory: $OUTDIR ==="

########################################
# PRECHECKS
########################################

[ -x "$WT_BENCH_BIN" ] || { echo "ERROR: WT binary '$WT_BENCH_BIN' not found or not executable."; exit 1; }
command -v iostat  >/dev/null 2>&1 || { echo "ERROR: iostat not found (install sysstat)."; exit 1; }
[ -x "$SMART_SCRIPT" ] || { echo "ERROR: SMART script '$SMART_SCRIPT' not found or not executable."; exit 1; }

########################################
# DEVICE RESET & MOUNT
########################################

echo "[INFO] Preparing test device $TEST_DEVICE and mountpoint $TEST_MOUNTPOINT"

# Unmount data FS if currently mounted
if mountpoint -q "$TEST_MOUNTPOINT"; then
  echo "[INFO] Unmounting $TEST_MOUNTPOINT"
  echo "$SMART_PASSWORD" | sudo -S umount "$TEST_MOUNTPOINT"
fi

# Discard the device to reset SSD state
echo "[INFO] blkdiscard on $TEST_DEVICE"
echo "$SMART_PASSWORD" | sudo -S blkdiscard -f "$TEST_DEVICE"

# mkfs – recreate the filesystem each run
echo "[INFO] Creating filesystem on $TEST_DEVICE"
echo "$SMART_PASSWORD" | sudo -S mkfs.ext4 "$TEST_DEVICE"

# Mount the device
echo "[INFO] Mounting $TEST_DEVICE -> $TEST_MOUNTPOINT"
echo "$SMART_PASSWORD" | sudo -S mount "$TEST_DEVICE" "$TEST_MOUNTPOINT"

# Ensure user can write to the mount
echo "[INFO] chown $FS_OWNER:$FS_GROUP $TEST_MOUNTPOINT"
echo "$SMART_PASSWORD" | sudo -S chown "$FS_OWNER:$FS_GROUP" "$TEST_MOUNTPOINT"

# DB path used by WiredTiger (FLAGS_db_path / FLAGS_db_dir)
DB_PATH="$TEST_MOUNTPOINT/$DB_SUBDIR"
mkdir -p "$DB_PATH"

# Clean WAL directory
rm -rf "$WAL_DIR"/*
mkdir -p "$WAL_DIR"

########################################
# START MONITORS
########################################

echo "[INFO] Starting SMART monitor (every ${SMART_INTERVAL_SEC}s) for $SMART_DEVICE -> $SMART_LOG"
(
  while true; do
    echo "===== $(date -Is) =====" >> "$SMART_LOG"
    "$SMART_SCRIPT" "$SMART_PASSWORD" "$OUTDIR" "$SMART_DEVICE" >> "$SMART_LOG" 2>&1 || true

    # Simple FS usage logging
    USED_PCT=$(df --output=pcent "$TEST_MOUNTPOINT" | tail -1 | tr -dc '0-9')
    echo "[MONITOR] FS usage on $TEST_MOUNTPOINT: ${USED_PCT}%" >> "$SMART_LOG"

    sleep "$SMART_INTERVAL_SEC"
  done
) &
SMART_PID=$!

echo "[INFO] Starting iostat monitor (every ${IOSTAT_INTERVAL_SEC}s) -> $IOSTAT_LOG"
(
  while true; do
    echo "===== $(date -Is) =====" >> "$IOSTAT_LOG"
    iostat -x 1 1 >> "$IOSTAT_LOG" 2>&1 || echo "[WARN] iostat failed" >> "$IOSTAT_LOG"
    sleep "$IOSTAT_INTERVAL_SEC"
  done
) &
IOSTAT_PID=$!

cleanup() {
  echo "[INFO] Stopping monitors..."
  kill "$IOSTAT_PID" 2>/dev/null || true
  kill "$SMART_PID" 2>/dev/null || true

  # Unmount data FS if currently mounted
  if mountpoint -q "$TEST_MOUNTPOINT"; then
    echo "[INFO] Unmounting $TEST_MOUNTPOINT"
    echo "$SMART_PASSWORD" | sudo -S umount "$TEST_MOUNTPOINT"
  fi
}
trap cleanup EXIT

########################################
# LOAD PHASE (exec_seconds = 0)
########################################

echo "[INFO] LOAD PHASE: recordcount=$RECORDCOUNT fieldlength=$FIELDLENGTH threads=$LOAD_THREADS WT_cache=${WT_CACHE_LOAD_GB}GB"

(
  cd "$(dirname "$WT_BENCH_BIN")"

  time LD_LIBRARY_PATH="$HOME/wt-install/lib" "$WT_BENCH_BIN" \
    --db_path="$DB_PATH" \
    --bm_physical_gb="$WT_CACHE_LOAD_GB" \
    --ycsb_record_count="$RECORDCOUNT" \
    --worker_count="$LOAD_THREADS" \
    --wal_path="$WAL_DIR" \
    --ycsb_exec_seconds=0 \
    --ycsb_dataset_size_gb=0 \
    --db_dir="$DB_PATH"
) 2>&1 | tee "$LOAD_LOG"

echo "[INFO] DB size after load:"
du -sh "$DB_PATH" || true

########################################
# RUN PHASE (exec_seconds > 0)
########################################

echo "[INFO] RUN PHASE: threads=$RUN_THREADS duration=${MAX_EXEC_SECS}s WT_cache=${WT_CACHE_RUN_GB}GB zipf_theta=$ZIPFIAN_CONSTANT read_ratio=$READ_RATIO"

(
  cd "$(dirname "$WT_BENCH_BIN")"

  time LD_LIBRARY_PATH="$HOME/wt-install/lib" "$WT_BENCH_BIN" \
    --db_path="$DB_PATH" \
    --bm_physical_gb="$WT_CACHE_RUN_GB" \
    --ycsb_record_count="$RECORDCOUNT" \
    --worker_count="$RUN_THREADS" \
    --wal_path="$WAL_DIR" \
    --ycsb_exec_seconds="$MAX_EXEC_SECS" \
    --ycsb_zipf_theta="$ZIPFIAN_CONSTANT" \
    --ycsb_read_ratio="$READ_RATIO" \
    --db_dir="$DB_PATH"
) 2>&1 | tee "$RUN_LOG" | tee "$WT_STATS_LOG"

echo
echo "=== All results stored in: $OUTDIR ==="
echo "  Load log:       $LOAD_LOG"
echo "  Run log:        $RUN_LOG"
echo "  iostat log:     $IOSTAT_LOG"
echo "  SMART log:      $SMART_LOG"
echo "  WT stats (csv): $WT_STATS_LOG"

pkill -f get_smart_info.sh 2>/dev/null || true
