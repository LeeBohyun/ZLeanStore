#!/usr/bin/env bash
set -euo pipefail

########################################
# Usage / argument parsing
########################################
usage() {
  cat <<EOF
Usage: $0 -c CURRENT_DIR -b BUILD_DIR -t YCSB_DIR -r RESULT_DIR -s SRC_DIR -d DEVICE [-p SUDO_PWD]

  -c  Current scripts dir (e.g., /home/smrc/scripts)
  -b  Build dir (e.g., /home/smrc/ZLeanStore/build)
  -t  TPCC/YCSB binary dir (e.g., /home/smrc/ZLeanStore/build/benchmark)
  -r  Result dir (e.g., /home/smrc/result/ycsb/nvme4/testfdp)
  -s  Source dir (e.g., /home/smrc/ZLeanStore/src)
  -d  NVMe namespace device (e.g., /dev/nvme4n1)
  -p  Sudo password (optional; if omitted you'll be prompted)

Example:
  $0 -c /home/smrc/scripts \\
     -b /home/smrc/ZLeanStore/build \\
     -t /home/smrc/ZLeanStore/build/benchmark \\
     -r /home/smrc/result/ycsb/nvme4/testfdp \\
     -s /home/smrc/ZLeanStore/src \\
     -d /dev/nvme4n1
EOF
  exit 1
}

CURRENT_DIR=""
BUILD_DIR=""
YCSB_DIR=""
RESULT_DIR=""
SRC_DIR=""
DEVICE=""
PWD_SUDO=""

while getopts "c:b:t:r:s:d:p:h" opt; do
  case "$opt" in
    c) CURRENT_DIR=$OPTARG ;;
    b) BUILD_DIR=$OPTARG ;;
    t) YCSB_DIR=$OPTARG ;;
    r) RESULT_DIR=$OPTARG ;;
    s) SRC_DIR=$OPTARG ;;
    d) DEVICE=$OPTARG ;;
    p) PWD_SUDO=$OPTARG ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Basic validation
[[ -z "$CURRENT_DIR" || -z "$BUILD_DIR" || -z "$YCSB_DIR" || -z "$RESULT_DIR" || -z "$SRC_DIR" || -z "$DEVICE" ]] && usage

if [[ -z "$PWD_SUDO" ]]; then
  read -s -p "Enter sudo password: " PWD_SUDO
  echo
fi

########################################
# Static / experiment parameters
########################################

exec_time=24000
thread_cnt=32
ver=3
skew=(0.8)
dbsz=1788
ssdsz="1788"
devicesz="$ssdsz"
ctrls="0x7"

ssdsize=(
  # For simulation:
  # "--ycsb_dataset_size_gb=20 --ycsb_record_count=72740178 --bm_physical_gb=2 --max_wal_capacity_gb=2 --simulator_mode=true --max_ssd_capacity_gb=25 --SSD_OP=0.625"
  "--ycsb_dataset_size_gb=1600 --bm_physical_gb=80 --max_wal_capacity_gb=80 --ycsb_record_count=5794060165"
)

version=(
  "--use_out_of_place_write=true --garbage_collector_cnt=4 --use_compression=true --use_binpacking=true --batch_writes=true --write_buffer_partition_cnt=3 --bm_evict_batch_size=64 --use_edt=false --block_size_mb=8496 --gc_victim_block_selection_algorithm=0 --max_open_block_cnt=8  --use_FDP=true"
  "--use_out_of_place_write=true --garbage_collector_cnt=4 --use_compression=true --use_binpacking=true --batch_writes=true --write_buffer_partition_cnt=3 --bm_evict_batch_size=64 --use_edt=false --block_size_mb=513  --gc_victim_block_selection_algorithm=0 --max_open_block_cnt=16 --use_FDP=true"
  "--use_out_of_place_write=true --garbage_collector_cnt=4 --use_compression=true --use_binpacking=true --batch_writes=true --write_buffer_partition_cnt=3 --bm_evict_batch_size=64 --use_edt=false --block_size_mb=8496 --gc_victim_block_selection_algorithm=0 --max_open_block_cnt=16 --use_FDP=true"
  "--use_out_of_place_write=true --garbage_collector_cnt=4 --use_compression=true --use_binpacking=true --batch_writes=true --write_buffer_partition_cnt=3 --bm_evict_batch_size=64 --use_edt=false --block_size_mb=513  --gc_victim_block_selection_algorithm=0 --max_open_block_cnt=16 --use_FDP=true --use_SSDWA1_pattern=true"
)

########################################
# Main loops
########################################

for z in "${skew[@]}"; do
  for j in "${ssdsize[@]}"; do
    for i in "${version[@]}"; do
      result_subdir="$RESULT_DIR/$ver"
      mkdir -p "$result_subdir"

      echo "$i"
      echo "Start YCSB/LeanStore"
      echo "Start NS recreate + FDP setup"

      ctrl="${DEVICE%n*}"      # e.g. /dev/nvme4
      old_nsid="${DEVICE##*n}" # extract current nsid from /dev/nvme4nX

      # Detach & delete current namespace
      echo "$PWD_SUDO" | sudo -S nvme detach-ns "$ctrl" --namespace-id="$old_nsid" || echo "Warning: detach ns $old_nsid failed/skip"
      echo "$PWD_SUDO" | sudo -S nvme delete-ns "$ctrl" --namespace-id="$old_nsid" || echo "Warning: delete ns $old_nsid failed/skip"

      # Enable FDP on the controller
      echo "[3/8] Enabling FDP..."
      echo "$PWD_SUDO" | sudo -S nvme fdp feature "$ctrl" -e 1 -c 0 || { echo "Failed to enable FDP"; exit 1; }

      # Capacity from controller -> 4KiB blocks
      echo "[4/8] Fetching capacity in blocks from controller..."
      blocks=$(
        { echo "$PWD_SUDO" | sudo -S nvme id-ctrl "$ctrl"; } \
        | sed 's/,//g' \
        | awk '/tnvmcap|nvmcap/{
            for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/) cap=$i
          }
          END{ if(cap>0) printf("%.0f", cap/4096); }'
      )
      if [[ -z "${blocks:-}" || ! "$blocks" =~ ^[0-9]+$ ]]; then
        echo "Error: Failed to parse block count. Value was: $blocks"
        exit 1
      fi
      echo "Capacity (4KiB blocks): $blocks"

      # Create NS; robustly extract numeric nsid
      echo "[5/8] Creating namespace..."
      create_out=$(echo "$PWD_SUDO" | sudo -S nvme create-ns "$ctrl" -b 4096 --nsze="$blocks" --ncap="$blocks" -p 0,1,2,3,4,5,6,7 -n 8) \
        || { echo "Failed to create namespace"; exit 1; }
      new_nsid=$(echo "$create_out" | grep -Eo '[0-9]+' | tail -1)
      if [[ -z "${new_nsid:-}" ]]; then
        new_nsid=$( { echo "$PWD_SUDO" | sudo -S nvme list-ns "$ctrl"; } | awk 'NF && $1 ~ /^[0-9]+$/ {ns=$1} END{print ns}')
      fi
      if [[ -z "${new_nsid:-}" ]]; then
        echo "Error: could not determine new nsid from output: $create_out"
        exit 1
      fi
      echo "New namespace id: $new_nsid"

      # Attach NS
      echo "[6/8] Attaching namespace $new_nsid..."
      echo "$PWD_SUDO" | sudo -S nvme attach-ns "$ctrl" --namespace-id="$new_nsid" --controllers="$ctrls" \
        || { echo "Failed to attach namespace"; exit 1; }

      # Update device path, prep the ns device
      echo "[7/8] Preparing namespace device $DEVICE ..."
      echo "$PWD_SUDO" | sudo -S blkdiscard "$DEVICE" || echo "blkdiscard warning"
      echo "$PWD_SUDO" | sudo -S chmod 777 "$DEVICE"

      # FDP namespace-level config
      echo "$PWD_SUDO" | sudo -S nvme fdp update "$ctrl" --namespace-id="$new_nsid" --pids=0,1,2,3,4,5,6,7

      # FDP configs (controller)
      echo "[8/8] Querying FDP configuration..."
      echo "$PWD_SUDO" | sudo -S nvme fdp configs "$ctrl" -e 1 || { echo "Failed to retrieve FDP config"; exit 1; }

      echo "$PWD_SUDO" | sudo -S nvme fdp set-event "$ctrl" -n "$new_nsid" -p 0 -e enable -t 0

      echo "$DEVICE is used to pass down the config"

      # Save run config
      cp "$SRC_DIR/config.cc" "$CURRENT_DIR/run_ycsb_nvme4.sh" "$result_subdir" 2>/dev/null || true
      {
        echo "$z"
        echo "$j"
        echo "$i"
      } >> "$result_subdir/ver.out"

      # iostat in background
      iostat -x 60 1080 > "$result_subdir/iostat.out" &
      IOSTAT_PID=$!

      cd "$CURRENT_DIR"
      ./calc_waf.sh "$PWD_SUDO" "$result_subdir" "$ctrl" 1 &
      WAF_PID=$!

      # Run workload
      cd "$YCSB_DIR"
      echo "$PWD_SUDO" | sudo -S ./LeanStore_YCSB \
        --SSD_OP=0.06 \
        --max_ssd_capacity_gb="$devicesz" \
        --bm_virtual_gb="$dbsz" \
        --db_path="$DEVICE" \
        --wal_path="" \
        --ycsb_exec_seconds="$exec_time" \
        ${i} ${j} \
        --measure_waf=true \
        --user_pwd="$PWD_SUDO" \
        --ycsb_read_ratio=50 \
        --ycsb_zipf_theta="$z" \
        --worker_count="$thread_cnt" \
        | tee "$result_subdir"/trx.csv

      sleep 20
      killall -9 LeanStore_YCSB || true

      # stop iostat
      if ps -p "$IOSTAT_PID" >/dev/null 2>&1; then
        kill "$IOSTAT_PID" || true
      fi

      [ -n "${WAF_PID:-}" ] && kill "$WAF_PID" 2>/dev/null || true

      echo "Done evaluation"
      sleep 300
      ver=$((ver + 1))
    done
    ver=$((ver + 230))
  done
  ver=$((ver + 1000))
done
