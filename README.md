# ZLeanStore

LeanStore is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs.
ZLeanStore is an out-of-place write version of [LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) that co-designs the DBMS and SSD layers to minimize total write amplification (WAF). It is the artifact for the paper:

> **How to Write to SSDs**
> Bohyun Lee, Tobias Ziegler, and Viktor Leis.
> *Proceedings of the VLDB Endowment (PVLDB)*, Vol. 19, 2026, p. 1469.
> [VLDB PDF](https://www.vldb.org/pvldb/vol19/p1469-lee.pdf) · [extended version](https://arxiv.org/abs/2603.09927) · [Code](https://github.com/LeeBohyun/ZLeanStore)


## Implemented Features Including Previous Works

- [x] SSD write optimizations [VLDB'26]
- [x] Virtual-memory assisted buffer manager with explicit OS pagetable management [SIGMOD'23]
- [x] Optimistic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE'19]
- [x] Variable-length key/values B-Tree with prefix compression and hints [BTW'23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD'20]

## Implemented Features In This Paper
ZLeanStore extends original LeanStore with four out-of-place optimizations that collectively reduce write amplification across both the DB and SSD layers, improving throughput by 1.65--2.24x and reducing flash writes per transaction by 6.2--9.8x on YCSB-A:

- **Page-wise compression & page packing** -- Compresses each 4 KiB page independently (LZ4/ZSTD) and bin-packs compressed pages into 4 KiB-aligned slots, cutting write volume while preserving fast, single-I/O reads.
- **Grouping by Deathtime (GDT)** -- Estimates each page's invalidation time and groups pages with similar deathtimes into the same zone during GC, reducing the valid-page ratio of victim zones and therefore DB WAF.
- **Aligning DB and SSD GC units** -- Sizes database zones to match the SSD's internal garbage-collection (erase) unit so that DB GC invalidates entire superblocks, eliminating SSD-internal GC writes. The GC unit can be inferred from FDP Reclaim Unit size or estimated via a ZNS-like probing pattern.
- **NoWA pattern** -- A write pattern for commodity (non-ZNS) SSDs that guarantees SSD WAF = 1 by ensuring the SSD always has a fully-invalidated superblock available before its GC triggers. Works by detecting and compensating for write-frequency imbalances among concurrently appended zones.

The design also natively supports **ZNS SSDs** (zone-append interface, SSD WAF = 1 by construction) and **FDP-enabled SSDs** (placement hints replace NoWA to avoid multiplexing).

Total WAF = DB WAF x SSD WAF. The key insight is that optimizing only one layer can counterintuitively worsen the other; both must be addressed jointly.

## Requirements

- **OS**: Linux (requires io_uring, NVMe device access)
- **Compiler**: GCC or Clang with C++20 support
- **Architecture**: x86\_64 with AVX2 support, or ARM64 with CRC+crypto extensions

## Compiling

Install dependencies:

### Build tools

```
sudo apt-get install build-essential make git pkg-config
```

### Core libraries

```
sudo apt-get install cmake libtbb-dev libfmt-dev libgflags-dev libgtest-dev \
  libgmock-dev libgcrypt-dev liburing-dev libzstd-dev libbenchmark-dev \
  libssl-dev zlib1g-dev
```

**exmap** (optional): stored in `share_libs/exmap` if you want to enable exmap
- Install kernel headers first: `sudo apt-get install linux-headers-$(uname -r)`
- Then run `sudo ./load.sh` inside `share_libs/exmap/`

### Third-party libraries

**Databases** (for comparison benchmarks):

```
sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev
```

### Script runtime tools (for SSD characterization and benchmarking)

```
sudo apt-get install nvme-cli smartmontools fio jq sysstat
```

## How to Build

```bash
mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
```

## How to Configure

Configuration flags are defined in `src/config.cc`. Key flags:

| Flag | Description |
|------|-------------|
| `--use_out_of_place_write` | Enable out-of-place writes (default: `false` for in-place baseline) |
| `--garbage_collector_cnt` | Number of GC threads |
| `--use_compression` | Enable page-wise LZ4 compression |
| `--use_binpacking` | Enable 4 KiB-aligned page packing |
| `--batch_writes` | Batch evictions into sequential I/Os |
| `--use_edt` | Enable Estimated Deathtime (GDT) placement |
| `--block_size_mb` | DB zone (GC unit) size in MB -- align to SSD GC unit |
| `--use_SSDWA1_pattern` | Enable the NoWA write pattern (SSD WAF = 1) |
| `--use_FDP` | Use FDP placement hints instead of NoWA |
| `--simulator_mode` | Run with a simulated smaller SSD capacity |
| `--ycsb_dataset_size_gb` | Logical dataset size in GB |
| `--ycsb_record_count` | Number of YCSB records to load |
| `--bm_physical_gb` | Buffer pool size in GB (set to 5--20% of dataset for I/O-bound runs) |
| `--max_wal_capacity_gb` | Maximum WAL size in GB |
| `--SSD_OP` | SSD over-provisioning ratio |

## Running YCSB-A

`--db_path` accepts either a raw block device (e.g. `/dev/nvme1n1`) or a file path on a mounted filesystem (e.g. `/mnt/ssd/leanstore.db`). Raw device access bypasses the filesystem for maximum throughput, but requires root or appropriate device permissions and **will destroy all data on the device**. Using a filesystem path is safer and does not require root, though it adds a small layer of indirection.

```bash
cd build/benchmark
./LeanStore_YCSB \
  --max_ssd_capacity_gb=16 \
  --bm_virtual_gb=18 \
  --db_path=/dev/nvme1n1 \
  --wal_path="" \
  --use_out_of_place_write=true \
  --garbage_collector_cnt=16 \
  --use_compression=true \
  --use_binpacking=true \
  --batch_writes=true \
  --use_edt=true \
  --block_size_mb=512 \
  --use_SSDWA1_pattern=false \
  --worker_count=64 \
  --ycsb_exec_seconds=1200 \
  --ycsb_read_ratio=50 \
  --ycsb_zipf_theta=0.8 \
  --measure_waf=true \
  --user_pwd=YOUR_PASSWORD \
  | tee trx.csv
```

## Running TPC-C

```bash
./build/benchmark/LeanStore_TPCC \
  --tpcc_warehouse_count=100 \
  --tpcc_exec_seconds=1800 \
  --tpcc_batch_delete_window=0
```

- Steady TPC-C: `--tpcc_batch_delete_window=2100`
- Growing (vanilla) TPC-C: `--tpcc_batch_delete_window=0`
- Relevant code: `benchmark/src/include/benchmark/tpcc/`, `benchmark/main/leanstore_tpcc.cc`

## Running in SSD Simulator Mode

Simulator mode emulates a smaller SSD capacity in software, allowing WAF evaluation without dedicating an entire device. I/O is still issued to the underlying device, but the space manager behaves as if the SSD is smaller.

```bash
./LeanStore_YCSB \
  --simulator_mode=true \
  --ycsb_dataset_size_gb=40 \
  --bm_physical_gb=8 \
  --max_wal_capacity_gb=8 \
  --block_size_mb=32 \
  --simulator_SSD_gc_unit_mb=32 \
  --SSD_OP=0.125 \
  --measure_waf=true \
  --ycsb_exec_seconds=1800 \
  --ycsb_read_ratio=50 \
  --ycsb_zipf_theta=0.6 \
  --worker_count=8 \
  --user_pwd=YOUR_PASSWORD \
  | tee trx.csv
```

## Comparison Benchmarks (DBMS YCSB)

The `scripts/` directory contains automation for running YCSB workloads on multiple database systems to compare WAF and throughput. See [`scripts/README.md`](scripts/README.md) for full documentation.

| Script | DBMS |
|--------|------|
| `run_leanstore_ycsb_example.sh` | ZLeanStore (raw device) |
| `run_leanstore_ycsb_fdp.sh` | ZLeanStore with FDP namespace setup |
| `run_mysql_ycsb.sh` | MySQL / InnoDB |
| `run_pg_ycsb.sh` | PostgreSQL |
| `run_rocksdb_ycsb.sh` | RocksDB |
| `run_wiredtiger_ycsb.sh` | WiredTiger |

## SSD Characterization Scripts

Scripts for measuring SSD read performance, inferring GC unit size, and validating the NoWA pattern. These use the `iob` (I/O benchmark) and `sim` (SSD simulator) tools from [ssdiq_zleanstore](https://github.com/LeeBohyun/ssdiq_zleanstore). See [`scripts/README.md`](scripts/README.md) for details.

| Script | Purpose |
|--------|---------|
| `readlat.sh` | Measure random-read latency at multiple block sizes (fio, QD=1) |
| `readbw.sh` | Measure random-read bandwidth (fio, QD=64) |
| `findssdgcunitsize.sh` | Infer the SSD's GC erase-unit size via ZNS-like probing |
| `testnowa.sh` | Validate NoWA achieves SSD WAF = 1 on real hardware |
| `testnowasim.sh` | Validate NoWA and ZNS patterns in the SSD simulator |
| `calcssdwaf.sh` | Continuously sample NVMe SMART counters and compute SSD WAF |
| `getsmartinfo.sh` | Collect raw NVMe media/host write counters |

## Citation

```bibtex
@article{lee2026howtowrite,
  title   = {How to Write to SSDs},
  author  = {Lee, Bohyun and Ziegler, Tobias and Leis, Viktor},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {19},
  year    = {2026},
  pages   = {1469},
  url     = {https://www.vldb.org/pvldb/vol19/p1469-lee.pdf}
}

@misc{lee2026howtowrite_arxiv,
  title         = {How to Write to SSDs},
  author        = {Lee, Bohyun and Ziegler, Tobias and Leis, Viktor},
  year          = {2026},
  eprint        = {2603.09927},
  archivePrefix = {arXiv},
  primaryClass  = {cs.DB}
}
```
