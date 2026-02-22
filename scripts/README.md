# Scripts

Automation scripts for SSD benchmarking, WAF measurement, and DBMS YCSB experiments.

The `iob` and `sim` binaries referenced by several scripts come from the [ssdiq\_zleanstore](https://github.com/LeeBohyun/ssdiq_zleanstore) project:

- **`iob`** (I/O Benchmark) — performs real I/O on an NVMe device using various write patterns (uniform, zipf, ZNS, NOWA, etc.) and measures throughput, latency, and write amplification on actual hardware.
- **`sim`** (SSD Simulator) — models SSD internals (garbage collection, zone management, wear leveling) in software, enabling rapid exploration of different workload patterns and GC strategies without requiring a physical device.

Both binaries should be built from the ssdiq\_zleanstore repo and placed in this directory (or on `$PATH`) before running the scripts that use them.

---

## SSD Characterization

### `readlat.sh` — Random Read Latency

Measures random-read latency, throughput, and IOPS at multiple block sizes using `fio`.

1. Discards (`blkdiscard`) the target device.
2. Prefills the device with sequential writes (128 GB, 1 M block size).
3. Runs `randread` at queue depth 1 (`iodepth=1`) with 1 thread per block size (512 B, 4 KiB, 8 KiB) for 600 s each.
4. Extracts mean latency (ns), throughput (MB/s), and IOPS from fio JSON output via `jq`.

```bash
sudo ./readlat.sh
```

Edit the `DEVICES`, `BLOCK_SIZES`, and `RUNTIME` variables at the top of the script to match your setup.

---

### `readbw.sh` — Random Read Bandwidth

Similar to `readlat.sh` but targets maximum bandwidth with deeper queues.

1. Prefills the device with sequential writes (128 GB, 32 threads).
2. Runs `randread` at queue depth 64 with 32 threads for 300 s per block size.
3. Reports mean latency, throughput, and IOPS.

```bash
sudo ./readbw.sh
```

Edit `DEVICES`, `BLOCK_SIZES`, `NUMJOBS`, and `RUNTIME` at the top.

---

### `findssdgcunitsize.sh` — Find SSD GC Unit (Erase Unit) Size

Determines the SSD's internal garbage-collection (erase) unit size by running ZNS-like sequential write patterns with increasing zone sizes and observing when WAF drops to ~1.0.

1. Sanitizes the NVMe device (`nvme sanitize`).
2. Iterates over zone sizes (512 K, 1 G, 2 G, ... 32 G) using the `iob` tool with the `zns-noshuffle` pattern.
3. Post-processes the `iob` log to find the smallest `pagesPerZone` where WAF stays below 1.1, then converts that to the physical zone size in GB.

Requires: `iob` binary in the current directory, `sudo` access.

```bash
./findssdgcunitsize.sh /dev/nvme2n1 output_prefix [UNI_BS] [SEQ_BS]
```

| Argument | Description | Default |
|----------|-------------|---------|
| `$1` | NVMe device (e.g. `/dev/nvme2n1`) | required |
| `$2` | Output file prefix | required |
| `$3` | Uniform block size | `4K` |
| `$4` | Sequential block size | `512K` |

---

### `testnowa.sh` — Test NOWA Pattern on Real Hardware

Validates that the NOWA (No Write Amplification) write pattern achieves SSD WAF of ~1 on a real device using `iob`.

1. Sanitizes the NVMe device.
2. Runs `iob` with `nowa-noshuffle` pattern, 256 K block size, 512 K zone size, and 16384 active zones.

Requires: `iob` binary in the current directory, `sudo` access.

```bash
./testnowa.sh /dev/nvme2n1 output_prefix [UNI_BS] [SEQ_BS]
```

Arguments are the same as `findssdgcunitsize.sh`.

---

### `testnowasim.sh` — Test NOWA Pattern in Simulator

Runs the NOWA and ZNS write patterns in the SSD simulator (`sim`) to verify WAF behavior without needing real hardware.

1. Runs `sim` with `nowa-noshuffle` (512 K BS, 512 K zone, 2048 active zones, 500 writes).
2. Runs `sim` with `zns-noshuffle` (256 K BS, 4 M zone, 1 active zone, 500 writes).

The simulator's erase granularity is 8 MB, so ZNS WAF reaches 1.0 when zone size equals 8 MB.

Requires: `sim` binary in the current directory.

```bash
./testnowasim.sh
```

No arguments needed. Outputs CSV files to the current directory.

---

## SSD WAF Measurement

### `calcssdwaf.sh` — Calculate SSD Write Amplification Factor

Continuously samples flash-level and host-level write counters from the NVMe device, converts both to bytes, and computes per-interval, running-average, and cumulative WAF.

- Flash writes: `nvme ocp smart-add-log` ("Physical media units written")
- Host writes: `smartctl -A` ("Data Units Written")
- WAF = delta\_flash\_bytes / delta\_host\_bytes

```bash
./calcssdwaf.sh /tmp/results /dev/nvme2n1 [options]
```

| Option | Description | Default |
|--------|-------------|---------|
| `--duration SECONDS` | Stop after N seconds | unlimited |
| `--interval SECONDS` | Sampling interval | 1 |
| `--stop-on-stable` | Stop when flash counter stops changing | off |
| `--stable-samples N` | Number of identical samples to trigger stop | 300 |
| `--flash-unit-bytes N` | Bytes per flash counter unit | 512000 |
| `--host-unit-bytes N` | Bytes per host DUW unit | 512000 |
| `--sudo-mode auto\|require\|never` | How to handle sudo | auto |

Outputs: `flash_raw.out`, `host_raw.out`, `flash_bytes.out`, `host_bytes.out`, `waf.csv`, `blocksize.txt`, `usedspace.out`.

---

### `getsmartinfo.sh` — Collect NVMe SMART Data (Data Collection Only)

Collects raw NVMe media/host write counters and logs them to files. Runs in a loop and exits when the flash-write counter has been stable for 1800 consecutive samples.

**Note:** This script has an inconsistency — it stores flash writes as raw units but host writes as bytes. Use `calcssdwaf.sh` instead for correct WAF computation.

```bash
./getsmartinfo.sh <sudo_password> <result_dir> <device>
```

---

## DBMS YCSB Benchmarks

All `run_*` scripts follow a similar structure:
1. **Prepare the SSD** — `blkdiscard`, `mkfs.ext4`, `mount` (or raw device access for LeanStore).
2. **Load phase** — Populate the database with YCSB records (`ycsb_exec_seconds=0`).
3. **Run phase** — Execute the YCSB workload for a configured duration.
4. **Monitor** — Background `iostat` and SMART logging throughout the run.
5. **Collect results** — Transaction throughput CSV, iostat logs, SMART logs.

### `run_leanstore_ycsb_example.sh` — LeanStore YCSB

Runs LeanStore\_YCSB directly on a raw NVMe device (no filesystem). Tests multiple dataset sizes and ZLeanStore version configurations (out-of-place write on/off, compression, bin-packing, etc.).

Includes a `--simulator_mode=true` configuration for testing at reduced scale.

```bash
./run_leanstore_ycsb_example.sh \
  -c /path/to/scripts \
  -t /path/to/build/benchmark \
  -r /path/to/results \
  -s /path/to/src \
  -d /dev/nvme2n1 \
  -p <sudo_password>
```

---

### `run_leanstore_ycsb_fdp.sh` — LeanStore YCSB with FDP

Same as the LeanStore example but additionally sets up NVMe Flexible Data Placement (FDP). Before each run it:
1. Detaches and deletes the existing NVMe namespace.
2. Enables FDP on the controller.
3. Creates a new namespace with FDP placement handles.
4. Runs the YCSB workload with `--use_FDP=true`.

```bash
./run_leanstore_ycsb_fdp.sh \
  -c /path/to/scripts \
  -b /path/to/build \
  -t /path/to/build/benchmark \
  -r /path/to/results \
  -s /path/to/src \
  -d /dev/nvme4n1 \
  -p <sudo_password>
```

---

### `run_mysql_ycsb.sh` — MySQL (InnoDB) YCSB

Runs the MySQL\_YCSB benchmark against a locally installed MySQL server.

1. Formats the SSD and mounts it as the MySQL datadir.
2. Initializes MySQL (`--initialize-insecure`) if needed.
3. Load phase: starts MySQL with a large buffer pool (160 GB), loads records.
4. Run phase: restarts MySQL with the production config (`my.cnf`), runs the workload.
5. Monitors with `iostat` and SMART logging.

```bash
./run_mysql_ycsb.sh \
  -P <sudo_password> \
  -c /path/to/scripts \
  -t /path/to/build/benchmark \
  -r /path/to/results \
  -s /path/to/src \
  -d /dev/nvme2n1 \
  -M /path/to/mysql-local \
  -F /path/to/my.cnf
```

Run `./run_mysql_ycsb.sh -h` for all options.

---

### `run_pg_ycsb.sh` — PostgreSQL YCSB

Runs the Postgres\_YCSB benchmark against a local PostgreSQL cluster.

1. Initializes a PostgreSQL cluster with `initdb` (commented out by default; enable for fresh runs).
2. Load phase: starts Postgres with large `shared_buffers` (commented out by default).
3. Run phase: starts Postgres with `shared_buffers=80GB` and runs the workload.
4. WAL is relocated to a separate directory.

```bash
./run_pg_ycsb.sh \
  -P <sudo_password> \
  -c /path/to/scripts \
  -b /path/to/build/benchmark \
  -r /path/to/results \
  -s /path/to/src \
  -d /dev/nvme2n1 \
  -g /path/to/pg/bin
```

Run `./run_pg_ycsb.sh -h` for all options.

---

### `run_rocksdb_ycsb.sh` — RocksDB YCSB

Runs the RocksDB\_YCSB benchmark on a freshly formatted SSD.

1. Formats and mounts the SSD.
2. Load phase: populates the RocksDB database.
3. Run phase: runs the workload for the configured duration.
4. Monitors disk usage (`du`), `iostat`, and SMART data throughout.

```bash
./run_rocksdb_ycsb.sh \
  -P <sudo_password> \
  -n /dev/nvme2n1 \
  -D /path/to/mountpoint
```

Run `./run_rocksdb_ycsb.sh -h` for all options.

---

### `run_wiredtiger_ycsb.sh` — WiredTiger YCSB

Runs the WiredTiger\_YCSB benchmark on a freshly formatted SSD.

1. Formats and mounts the SSD.
2. Load phase: populates the WiredTiger database (160 GB cache).
3. Run phase: runs with 80 GB cache for the configured duration.
4. Monitors with `iostat` and SMART logging; cleans up on exit.

Requires `LD_LIBRARY_PATH` pointing to WiredTiger libraries (`$HOME/wt-install/lib`).

```bash
./run_wiredtiger_ycsb.sh \
  -P <sudo_password> \
  -d /dev/nvme2n1 \
  -m /path/to/mountpoint
```

Run `./run_wiredtiger_ycsb.sh -h` for all options.
