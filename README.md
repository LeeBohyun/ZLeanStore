# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Implemented Features
- [x] Out-of-place write optimizations for SSDs  (NEW)
- [x] Virtual-memory assisted buffer manager with explicit OS pagetable management  [SIDMOG'23]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE'19]
- [x] Variable-length key/values B-Tree with prefix compression and hints  [BTW'23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD'20]

## Compiling
Install dependencies:

### Core
`sudo apt-get install cmake libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev libgcrypt-dev liburing-dev libzstd-dev libbenchmark-dev libssl-dev libzstd-dev zlib1g-dev`

**exmap**: stored in `share_libs/exmap`
- Run `sudo ./load.sh`

### Third-party libraries
 
**Databases**: `sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev`

## How to build

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`


## How to configure
- In src/config.cc file, you can configure LeanStore according to your needs
- For example, ``use_out_of_place_write=false``

## How to run TPC-C
- relevant folder and files: ``/ZLeanStore/benchmark/src/include/benchmark/tpcc`` ``/ZLeanStore/benchmark/main/leanstore_tpcc.cc``
- steady tpcc option: ``--tpcc_batch_delete_window=2100`` 
- growing vanilla tpcc:  ``--tpcc_batch_delete_window=0`` 

- For example:  ``./build/benchmark/LeanStore_TPCC --tpcc_warehouse_count=100 --tpcc_exec_seconds=1800 --tpcc_batch_delete_window=2100``


## Running YCSB-A

```bash
cd build/benchmark
./LeanStore_YCSB \
  --max_ssd_capacity_gb=16 \
  --bm_virtual_gb=18 \
  --db_path=/dev/nvme1n1 \
  --wal_path="" \
  --use_out_of_place_write=true \
  --garbage_collector_cnt=16 \
  --use_binpacking=true \
  --batch_writes=true \
  --worker_count=64 \
  --ycsb_exec_seconds=1200 \
  --ycsb_read_ratio=50 \
  --ycsb_zipf_theta=0.8 \
  --measure_waf=true \
  --user_pwd=YOUR_PASSWORD \
  | tee trx.csv
```


## Running LeanStore in SSD Simulator Mode

LeanStore provides a *simulator mode* that mimics SSD behavior, allowing users to evaluate write amplification and space management strategies **without requiring a real block device**. This mode is ideal for research and benchmarking purposes where hardware-level changes are emulated in software.

### Example: Running a Skewed Workload (YCSB-A, zipf 0.6) on SSD simulator, but it still issues IO request, it is just that you can assume that your SSD is smaller for example

```bash
./LeanStore_Flashbench \
  --simulator_mode=true \
  --initial_dataset_size_gb= 40 \
  --bm_physical_gb=8 \
  --max_wal_capacity_gb=8 \
  --block_size_mb=128 \
  --simulator_SSD_gc_unit_mb=128 \
  --SSD_OP=0.125 \
  --measure_waf=true \
  --exec_seconds=1800 \
  --read_ratio=50 \
  --zipf_factor=0.6 \
  --worker_count=8 \
  --user_pwd=YOUR_PASSWORD \
  | tee trx.csv
