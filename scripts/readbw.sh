#!/bin/bash

DEVICES=( "/blk/h0" )  # List of SSD devices # "/blk/intel1" "/blk/s0" 
TEST_SIZE=$((128 * 1024 * 1024 * 1024))  # 128GB test size
NUMJOBS=32  # Number of parallel threads
RUNTIME=300  # Test runtime in seconds
#BLOCK_SIZES=(512 1024 1536 2048 2560 3072 3584 4096 4608 5120 5632 6144 6656 7168 7680 8192)  # 0.5 KiB to 8 KiB
BLOCK_SIZES=( 4096 )  # 0.5 KiB to 8 KiB


# Ensure the script runs as root
if [ "$(id -u)" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# Loop through each device
for DEVICE in "${DEVICES[@]}"; do
    DEVICE_NAME=$(basename "$DEVICE")  # Extract device name for output file naming
    OUTPUT_FILE="latency_throughput_iops_results_${DEVICE_NAME}.txt"
    
    # Remove previous results
    # rm -f *_latency.json $OUTPUT_FILE

    # echo "Discarding existing data on $DEVICE..."
    # sudo blkdiscard $DEVICE
    
     echo "Filling $DEVICE with sequential writes before testing random reads..."
    
    # # Prefill the device with sequential writes using fio
    sudo fio --name=write_test \
        --filename=$DEVICE \
        --direct=1 \
        --ioengine=libaio \
        --rw=write \
        --bs=1M \
        --size=$TEST_SIZE \
        --iodepth=64 \
        --numjobs=$NUMJOBS \
        --output-format=json \
        --offset_increment=$((TEST_SIZE / NUMJOBS)) > "${DEVICE_NAME}_write.json"

    echo "Sequential write complete for $DEVICE. Proceeding with random read tests..."

    # Measure Read Latency, Throughput, and IOPS for each block size
    for BS in "${BLOCK_SIZES[@]}"; do
        echo "Measuring ${BS}-byte read latency, throughput & IOPS on $DEVICE with $NUMJOBS threads..."
        
        fio --name=read_test_${BS} \
            --filename=$DEVICE \
            --direct=1 \
            --ioengine=libaio \
            --rw=randread \
            --bs=${BS} \
            --size=$TEST_SIZE \
            --iodepth=64 \
            --numjobs=$NUMJOBS \
            --time_based \
            --runtime=$RUNTIME \
            --output-format=json \
            --offset_align=${BS} > "${DEVICE_NAME}_${BS}_read.json"

        echo "${BS}-byte read results on $DEVICE:" | tee -a $OUTPUT_FILE
        
        # Extract and log mean latency (in nanoseconds)
        LATENCY=$(jq '[.jobs[].read.lat_ns.mean // 0] | add / (length | if . == 0 then 1 else . end)' "${DEVICE_NAME}_${BS}_read.json")
        echo "Mean Latency: $LATENCY ns" | tee -a $OUTPUT_FILE
        
        # Extract and log throughput (in MB/s)
        THROUGHPUT=$(jq '[.jobs[].read.bw_bytes // 0] | add / (length | if . == 0 then 1 else . end) / (1024 * 1024)' "${DEVICE_NAME}_${BS}_read.json")
        echo "Throughput: $THROUGHPUT MB/s" | tee -a $OUTPUT_FILE

        # Extract and log IOPS
        IOPS=$(jq '[.jobs[].read.iops // 0] | add / (length | if . == 0 then 1 else . end)' "${DEVICE_NAME}_${BS}_read.json")
        echo "IOPS: $IOPS" | tee -a $OUTPUT_FILE

        echo "" | tee -a $OUTPUT_FILE
    done

    echo "Results for $DEVICE saved to $OUTPUT_FILE"
done

echo "All tests completed!"
