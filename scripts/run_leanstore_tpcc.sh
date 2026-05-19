#!/usr/bin/env bash
set -euo pipefail

########################################
# Usage / argument parsing
########################################
usage() {
  cat <<EOF
Usage: $0 -c CURRENT_DIR -t BIN_DIR -r RESULT_DIR -s SRC_DIR -d DEVICE [-p SUDO_PWD]

  -c  Current scripts dir (e.g., /home/lee/scripts)
  -t  Benchmark binary dir (e.g., /home/lee/git/ZLeanStore/build/benchmark)
  -r  Result dir (e.g., /home/lee/result/tpcc/test)
  -s  Source dir (e.g., /home/lee/git/ZLeanStore/src)
  -d  NVMe namespace device (e.g., /dev/nvme2n1)
  -p  Sudo password (optional; if omitted you'll be prompted)

Example:
  $0 -c /home/lee/scripts \\
     -t /home/lee/git/ZLeanStore/build/benchmark \\
     -r /home/lee/result/tpcc/test \\
     -s /home/lee/git/ZLeanStore/src \\
     -d /dev/nvme2n1
EOF
  exit 1
}

CURRENT_DIR=""
BIN_DIR=""
RESULT_DIR=""
SRC_DIR=""
DEVICE=""
SUDO_PWD=""

while getopts "c:t:r:s:d:p:h" opt; do
  case "$opt" in
    c) CURRENT_DIR=$OPTARG ;;
    t) BIN_DIR=$OPTARG ;;
    r) RESULT_DIR=$OPTARG ;;
    s) SRC_DIR=$OPTARG ;;
    d) DEVICE=$OPTARG ;;
    p) SUDO_PWD=$OPTARG ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Basic validation
[[ -z "$CURRENT_DIR" || -z "$BIN_DIR" || -z "$RESULT_DIR" || -z "$SRC_DIR" || -z "$DEVICE" ]] && usage

if [[ -z "$SUDO_PWD" ]]; then
  read -s -p "Enter sudo password: " SUDO_PWD
  echo
fi

########################################
# Static / experiment parameters
########################################

exec_time=300
thread_cnt=64
ver=0

ssdsz="894"
dbsz=894

# Each TPC-C warehouse holds ~100 MB of initial data; sizes below roughly
# match the YCSB script's 20/160/320/480/640/800 GB working sets.
ssdsize=(
  "--tpcc_warehouse_count=200  --bm_physical_gb=2   --max_wal_capacity_gb=2   --simulator_mode=true  --max_ssd_capacity_gb=25  --SSD_OP=0.625  --simulator_SSD_gc_unit_mb=8"
  "--tpcc_warehouse_count=1600 --bm_physical_gb=16  --max_wal_capacity_gb=16"
  "--tpcc_warehouse_count=3200 --bm_physical_gb=32  --max_wal_capacity_gb=32"
  "--tpcc_warehouse_count=4800 --bm_physical_gb=48  --max_wal_capacity_gb=48"
  "--tpcc_warehouse_count=6400 --bm_physical_gb=64  --max_wal_capacity_gb=64"
  "--tpcc_warehouse_count=8000 --bm_physical_gb=80  --max_wal_capacity_gb=40"
)

version=(
  "--use_out_of_place_write=true  --garbage_collector_cnt=1  --use_compression=true  --compression_algorithm=0  --block_size_mb=8  --use_binpacking=true  --batch_writes=true  --write_buffer_partition_cnt=2  --bm_evict_batch_size=64  --use_edt=true  --gc_victim_block_selection_algorithm=0  --max_open_block_cnt=16  --use_SSDWA1_pattern=false"
  "--use_out_of_place_write=false --batch_writes=false --bm_evict_batch_size=64"
)

########################################
# Helpers
########################################

run_dir() { echo "$RESULT_DIR/$1"; }

prep_device() {
  echo "Start DD of SSD: blkdiscard + chmod on $DEVICE"
  echo "$SUDO_PWD" | sudo -S blkdiscard "$DEVICE" || echo "Warning: blkdiscard failed/skip"
  echo "$SUDO_PWD" | sudo -S chmod 777 "$DEVICE" || echo "Warning: chmod failed/skip"
  sleep 10
}

save_run_config() {
  local outdir="$1"
  local j="$2"
  local i="$3"

  # Save config snapshots (best-effort)
  cp "$SRC_DIR/config.cc" "$CURRENT_DIR/run_leanstore_tpcc.sh" "$RESULT_DIR" 2>/dev/null || true

  {
    echo "$j"
    echo "$i"
    echo "standard TPC-C (5 tx, batch_delete disabled)"
  } >> "$outdir/ver.out"
}

start_iostat() {
  local outdir="$1"
  iostat -x 60 1080 > "$outdir/iostat.out" &
  echo $!
}

stop_pid() {
  local pid="$1"
  if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
    kill "$pid" 2>/dev/null || true
  fi
}

########################################
# Main loops
########################################

for j in "${ssdsize[@]}"; do
  for i in "${version[@]}"; do
    outdir="$(run_dir "$ver")"
    mkdir -p "$outdir"

    echo
    echo "========================================"
    echo "ver=$ver"
    echo "ssdsize args: $j"
    echo "version args: $i"
    echo "========================================"
    echo "Start TPC-C/LeanStore"

    prep_device
    save_run_config "$outdir" "$j" "$i"

    # iostat in background
    IOSTAT_PID="$(start_iostat "$outdir")"

    # Run workload — standard TPC-C, 6th batch-delete tx disabled via
    # --tpcc_batch_delete_window=0.
    cd "$BIN_DIR"
    ./LeanStore_TPCC \
      --SSD_OP=0.0625 \
      --tpcc_exec_seconds="$exec_time" \
      --tpcc_batch_delete_window=0 \
      --max_ssd_capacity_gb="$ssdsz" \
      --bm_virtual_gb="$dbsz" \
      --db_path="$DEVICE" \
      --wal_path="" \
      ${i} ${j} \
      --measure_waf=true \
      --user_pwd="$SUDO_PWD" \
      --worker_count="$thread_cnt" \
      | tee "$outdir/trx.csv"

    sleep 20

    # Cleanup
    killall -9 LeanStore_TPCC 2>/dev/null || true
    pkill -f LeanStore_TPCC 2>/dev/null || true
    stop_pid "$IOSTAT_PID"

    echo "Done evaluation"
    sleep 300

    ver=$((ver + 1))
  done
  ver=$((ver + 160))
done
