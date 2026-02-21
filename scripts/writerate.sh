#!/usr/bin/env bash
set -euo pipefail

### ---------- USER SETTINGS ----------
DEVICES=( "/dev/nvme11n1" )       # List of SSD devices
NUMJOBS=8                         # Number of parallel fio jobs
BLOCK_SIZES=(4096)                # Block size(s) in bytes
WRATE_MB=( 35 70 140 280 420 560 700 840 980 1120 1260 )  # target write MB/s per device # 280 420 560 700 840 980 1120 1260 1400

SMART_SCRIPT="/home/lee/scripts/get_smart_info.sh"
RESULT_BASE="/home/lee/scripts/fio/result"  # base directory for logs and results

PWD=$1

### ---------- ROOT CHECK ----------
if [ "$(id -u)" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

mkdir -p "$RESULT_BASE"

for DEVICE in "${DEVICES[@]}"; do
    if [ ! -b "$DEVICE" ]; then
        echo "ERROR: $DEVICE is not a block device"
        exit 1
    fi

    echo "===== Device: $DEVICE ====="

    # Discard device
    echo "[INFO] blkdiscard $DEVICE"
    blkdiscard "$DEVICE"

    DEVICE_NAME=$(basename "$DEVICE")

    # Per-device summary file (stays in RESULT_BASE for convenience)
    OUTPUT_FILE="${RESULT_BASE}/writerate_results_${DEVICE_NAME}.txt"
    : > "$OUTPUT_FILE"

    echo "Results will be stored in $OUTPUT_FILE"
    echo "[INFO] Device: $DEVICE" | tee -a "$OUTPUT_FILE"

    # --------- Determine device size ---------
    DEV_SIZE_BYTES=$(blockdev --getsize64 "$DEVICE")
    if [ -z "$DEV_SIZE_BYTES" ] || [ "$DEV_SIZE_BYTES" -le 0 ]; then
        echo "ERROR: Could not determine size of $DEVICE"
        exit 1
    fi
    echo "[INFO] $DEVICE size: $DEV_SIZE_BYTES bytes" | tee -a "$OUTPUT_FILE"

    JOB_SIZE=$(( DEV_SIZE_BYTES / NUMJOBS ))

    # --------- Prefill: sequential write over full device ---------
    echo "[INFO] Prefilling $DEVICE sequentially..." | tee -a "$OUTPUT_FILE"
    fio --name="prefill_${DEVICE_NAME}" \
        --filename="$DEVICE" \
        --direct=1 \
        --ioengine=libaio \
        --rw=write \
        --bs=1M \
        --size="$JOB_SIZE" \
        --offset_increment="$JOB_SIZE" \
        --iodepth=128 \
        --numjobs="$NUMJOBS" \
        --output="${RESULT_BASE}/prefill_${DEVICE_NAME}.json" \
        --output-format=json

    # echo "[INFO] Prefill complete for $DEVICE" | tee -a "$OUTPUT_FILE"

    # --------- Random write phases for each rate ---------
    for BS in "${BLOCK_SIZES[@]}"; do
        for RATE_MB in "${WRATE_MB[@]}"; do
            echo
            echo "=== Random write on $DEVICE (bs=${BS}B, target=${RATE_MB} MB/s total) ===" | tee -a "$OUTPUT_FILE"

            # TOTAL_BYTES = 2 × device capacity (total across all jobs)
            TOTAL_BYTES=$(( 2 * DEV_SIZE_BYTES ))
            RATE_BYTES_PER_SEC=$(( RATE_MB * 1024 * 1024 ))
            if [ "$RATE_BYTES_PER_SEC" -le 0 ]; then
                echo "ERROR: Invalid RATE_MB=$RATE_MB"
                exit 1
            fi

            # Estimate runtime for the "2× capacity" case (just info)
            EST_RUNTIME=$(( TOTAL_BYTES / RATE_BYTES_PER_SEC ))
            [ "$EST_RUNTIME" -lt 1 ] && EST_RUNTIME=1

            # fio --rate is *per job* in KiB/s
            # total rate ~ RATE_MB MB/s -> per-job MB/s = RATE_MB / NUMJOBS
            # per-job KiB/s = (RATE_MB * 1024) / NUMJOBS
            RATE_KIB_PER_JOB=$(( RATE_MB * 1024 / NUMJOBS ))
            if [ "$RATE_KIB_PER_JOB" -lt 1 ]; then
                RATE_KIB_PER_JOB=1
            fi

            # per-job io_size so that total ≈ 2 × capacity
            # used only for RATE_MB >= 700
            IO_SIZE_PER_JOB=$(( TOTAL_BYTES / NUMJOBS ))
            if [ "$IO_SIZE_PER_JOB" -le 0 ]; then
                echo "ERROR: Computed IO_SIZE_PER_JOB <= 0"
                exit 1
            fi

            echo "[INFO] Total bytes to write (all jobs, 2×cap case):   $TOTAL_BYTES" | tee -a "$OUTPUT_FILE"
            echo "[INFO] Per-job io_size (2×cap case):                 $IO_SIZE_PER_JOB bytes" | tee -a "$OUTPUT_FILE"
            echo "[INFO] Estimated runtime at ${RATE_MB} MB/s (2×cap): ~${EST_RUNTIME}s" | tee -a "$OUTPUT_FILE"
            echo "[INFO] Per-job rate limit:                            ${RATE_KIB_PER_JOB} KiB/s" | tee -a "$OUTPUT_FILE"

            JOB_NAME="writerate_${DEVICE_NAME}_bs${BS}_rate${RATE_MB}"

            # Per-writerate experiment directory:
            #   $RESULT_BASE/<device>/rate_<MB>MB_bs<BS>/
            EXP_BASE_DIR="${RESULT_BASE}/${DEVICE_NAME}/rate_${RATE_MB}MB_bs${BS}"
            mkdir -p "$EXP_BASE_DIR"

            # fio JSON goes here
            JSON_OUT="${EXP_BASE_DIR}/${JOB_NAME}.json"

            # SMART result directory (this is what get_smart_info.sh sees as its "result dir")
            SMART_DIR="${EXP_BASE_DIR}/smart"
            mkdir -p "$SMART_DIR"

            # --------- Start SMART logger for THIS experiment ---------
            SMART_PID=""
            if [ -x "$SMART_SCRIPT" ]; then
                echo "[INFO] Starting SMART logger in $SMART_DIR for $JOB_NAME" | tee -a "$OUTPUT_FILE"
                (
                    cd "$(dirname "$SMART_SCRIPT")"
                    ./$(basename "$SMART_SCRIPT") "$PWD" "$SMART_DIR" "$DEVICE"
                ) &
                SMART_PID=$!
                echo "$SMART_PID" > "${SMART_DIR}/smart.pid"
            else
                echo "[WARN] SMART script $SMART_SCRIPT not found or not executable; skipping SMART logging" | tee -a "$OUTPUT_FILE"
            fi

            # --------------------- Run fio job -------------------------
            # Below 700 MB/s: run for 1 hour (time-based)
            # 700 MB/s and above: run until 2× capacity written (io_size-based)
            if (( RATE_MB < 700 )); then
                echo "[INFO] RATE_MB=${RATE_MB} < 700 -> running time-based for 1 hour" | tee -a "$OUTPUT_FILE"
                fio --name="$JOB_NAME" \
                    --filename="$DEVICE" \
                    --direct=1 \
                    --ioengine=libaio \
                    --rw=randwrite \
                    --bs="$BS" \
                    --size="$DEV_SIZE_BYTES" \
                    --iodepth=64 \
                    --numjobs="$NUMJOBS" \
                    --rate="${RATE_KIB_PER_JOB}k" \
                    --time_based=1 \
                    --runtime=3600 \
                    --output="$JSON_OUT" \
                    --output-format=json
            else
                echo "[INFO] RATE_MB=${RATE_MB} >= 700 -> running until 2× capacity written" | tee -a "$OUTPUT_FILE"
                fio --name="$JOB_NAME" \
                    --filename="$DEVICE" \
                    --direct=1 \
                    --ioengine=libaio \
                    --rw=randwrite \
                    --bs="$BS" \
                    --size="$DEV_SIZE_BYTES" \
                    --io_size="$IO_SIZE_PER_JOB" \
                    --iodepth=64 \
                    --numjobs="$NUMJOBS" \
                    --rate="${RATE_KIB_PER_JOB}k" \
                    --output="$JSON_OUT" \
                    --output-format=json
            fi

            echo "[INFO] Finished $JOB_NAME" | tee -a "$OUTPUT_FILE"

            # --------- Stop SMART logger for THIS experiment ----------
            if [ -n "${SMART_PID:-}" ]; then
                echo "[INFO] Stopping SMART logger (PID $SMART_PID) for $JOB_NAME" | tee -a "$OUTPUT_FILE"
                kill "$SMART_PID" 2>/dev/null || true
                wait "$SMART_PID" 2>/dev/null || true
            fi
        done
    done

    echo "===== Finished all tests for $DEVICE =====" | tee -a "$OUTPUT_FILE"
done

echo "All tests completed!"
