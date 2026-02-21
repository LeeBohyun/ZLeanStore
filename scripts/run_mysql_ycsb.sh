#!/usr/bin/env bash
set -euo pipefail

########################################
# Usage / argument parsing
########################################
usage() {
  cat <<EOF
Usage: $0 [options]

Required (or they fall back to defaults shown below):

  -c  Current scripts dir        [default: /home/lee/scripts]
  -t  TPCC/YCSB binary dir       [default: /home/lee/git/LeanStoreOOPW/build/benchmark]
  -r  Result dir                 [default: /home/lee/result/ycsb/m0/mysql/800]
  -s  Source dir                 [default: /home/lee/git/LeanStoreOOPW/src]
  -d  NVMe device                [default: /dev/nvme2n1]

  -M  MySQL base dir             [default: /home/lee/mysql-local]
  -F  MySQL my.cnf path          [default: /home/lee/mysql-server/my.cnf]
  -D  MySQL datadir              [default: /home/lee/test_data]
  -L  LeanStore mountpoint       [default: /home/lee/test_data]
  -l  InnoDB log dir             [default: /home/lee/test_log/mysql]

  -P  Sudo password (for nvme, mkfs, mount, etc.)
      If omitted, you will be prompted.

Optional:

  -S  SMART info script          [default: /home/lee/scripts/get_smart_info.sh]
  -n  SMART device               [default: same as -d]
  -p  SMART password             [default: same as -P]
  -h  Show this help and exit

Example:

  $0 -P mysudo \
     -c /home/lee/scripts \
     -t /home/lee/git/LeanStoreOOPW/build/benchmark \
     -r /home/lee/result/ycsb/m0/mysql/800 \
     -s /home/lee/git/LeanStoreOOPW/src \
     -d /dev/nvme2n1
EOF
  exit 1
}

########################################
# Defaults (can be overridden with flags)
########################################

CURRENT_DIR="/home/lee/scripts"
BUILD_DIR="/home/lee/git/LeanStoreOOPW/build"
TPCC_DIR="/home/lee/git/LeanStoreOOPW/build/benchmark"
RESULT_DIR="/home/lee/result/ycsb/m0/mysql/800"
DEVICE="/dev/nvme2n1"
SRC_DIR="/home/lee/git/LeanStoreOOPW/src"

SUDO_PWD=""

exec_time=10800
thread_cnt=64
ver=0
skew=(0.8)

ssdsz="894"
dbsz=894

ssdsize=(
  #"--ycsb_dataset_size_gb=10 --bm_physical_gb=1 --max_wal_capacity_gb=64 --ycsb_record_count=5200000"
  "--ycsb_dataset_size_gb=800 --bm_physical_gb=40 --max_wal_capacity_gb=40 --ycsb_record_count=4160000000"
)

version=(
  "--use_out_of_place_write=false --write_buffer_partition_cnt=1 --bm_evict_batch_size=64  --block_size_mb=8192 --enable_doublewrite=true"
)

# Filesystem mountpoint for LeanStore data on \$DEVICE
LS_MOUNTPOINT="/home/lee/test_data"

# SMART / monitoring
SMART_SCRIPT="/home/lee/scripts/get_smart_info.sh"
SMART_INTERVAL_SEC="${SMART_INTERVAL_SEC:-480}"
SMART_DEVICE=""      # will default to DEVICE later
SMART_PASSWORD=""    # will default to SUDO_PWD later

# ----------------- MySQL config -----------------
MYSQL_BASE="/home/lee/mysql-local"
MYSQL_BIN="\$MYSQL_BASE/bin"
MYSQLD="\$MYSQL_BIN/mysqld"
MYSQLADMIN="\$MYSQL_BIN/mysqladmin"
MYSQL_CLIENT="\$MYSQL_BIN/mysql"
MYSQL_CNF="/home/lee/mysql-server/my.cnf"
SOCKET="/tmp/mysql.sock"    # must match my.cnf
HOST="127.0.0.1"
MYSQL_PORT=3306

# Datadir MUST match your my.cnf
DATADIR="/home/lee/test_data"
INNODB_LOG_DIR="/home/lee/test_log/mysql"

LOAD_BUFFER_POOL="160G"   # for load phase; run phase uses my.cnf

########################################
# Parse arguments
########################################

while getopts "c:t:r:s:d:M:F:D:L:l:P:S:n:p:h" opt; do
  case "\$opt" in
    c) CURRENT_DIR=\$OPTARG ;;
    t) TPCC_DIR=\$OPTARG ;;
    r) RESULT_DIR=\$OPTARG ;;
    s) SRC_DIR=\$OPTARG ;;
    d) DEVICE=\$OPTARG ;;
    M) MYSQL_BASE=\$OPTARG ;;
    F) MYSQL_CNF=\$OPTARG ;;
    D) DATADIR=\$OPTARG ;;
    L) LS_MOUNTPOINT=\$OPTARG ;;
    l) INNODB_LOG_DIR=\$OPTARG ;;
    P) SUDO_PWD=\$OPTARG ;;
    S) SMART_SCRIPT=\$OPTARG ;;
    n) SMART_DEVICE=\$OPTARG ;;
    p) SMART_PASSWORD=\$OPTARG ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Derive MySQL bin paths after MYSQL_BASE possibly changed
MYSQL_BIN="\$MYSQL_BASE/bin"
MYSQLD="\$MYSQL_BIN/mysqld"
MYSQLADMIN="\$MYSQL_BIN/mysqladmin"
MYSQL_CLIENT="\$MYSQL_BIN/mysql"

# Defaults for SMART if not provided
if [[ -z "\$SMART_DEVICE" ]]; then
  SMART_DEVICE="\$DEVICE"
fi

if [[ -z "\$SUDO_PWD" ]]; then
  read -s -p "Enter sudo password: " SUDO_PWD
  echo
fi

if [[ -z "\$SMART_PASSWORD" ]]; then
  SMART_PASSWORD="\$SUDO_PWD"
fi

########################################
# Prechecks
########################################

if [ ! -x "\$MYSQLD" ]; then
  echo "ERROR: mysqld not found or not executable at: \$MYSQLD" >&2
  exit 1
fi

if [ ! -f "\$MYSQL_CNF" ]; then
  echo "ERROR: MySQL config not found at: \$MYSQL_CNF" >&2
  exit 1
fi

if [ ! -x "\$MYSQLADMIN" ]; then
  echo "ERROR: mysqladmin not found or not executable at: \$MYSQLADMIN" >&2
  exit 1
fi

if [ ! -x "\$MYSQL_CLIENT" ]; then
  echo "ERROR: mysql client not found or not executable at: \$MYSQL_CLIENT" >&2
  exit 1
fi

mkdir -p "\$RESULT_DIR" "\$DATADIR" "\$LS_MOUNTPOINT" "\$INNODB_LOG_DIR"

echo "==> MySQL datadir set to: \$DATADIR"

RUN_LOG="\$RESULT_DIR/mysql_run.log"
LOAD_LOG="\$RESULT_DIR/mysql_load.log"

# Optional: export LD_LIBRARY_PATH if needed by MySQL_YCSB
export LD_LIBRARY_PATH="\$MYSQL_BASE/lib:\${LD_LIBRARY_PATH:-}"

########################################
# Helper functions
########################################

wait_for_mysql() {
  local label="\$1"
  echo "==> Waiting for MySQL to be ready (\$label)..."
  for _ in {1..60}; do
    if "\$MYSQLADMIN" -u root -S "\$SOCKET" ping >/dev/null 2>&1; then
      echo "MySQL is ready (\$label, root has NO password)."
      return 0
    fi
    sleep 1
  done
  echo "ERROR: MySQL did not become ready within 60 seconds (\$label)." >&2
  return 1
}

create_leanstore_user() {
  echo "==> Ensuring MySQL user 'leanstore' exists with EMPTY password..."

  "\$MYSQL_CLIENT" -u root -S "\$SOCKET" <<SQL
CREATE DATABASE IF NOT EXISTS test;

CREATE USER IF NOT EXISTS 'leanstore'@'localhost';
CREATE USER IF NOT EXISTS 'leanstore'@'%';

ALTER USER 'leanstore'@'localhost' IDENTIFIED BY '';
ALTER USER 'leanstore'@'%'         IDENTIFIED BY '';

GRANT ALL PRIVILEGES ON test.* TO 'leanstore'@'localhost';
GRANT ALL PRIVILEGES ON test.* TO 'leanstore'@'%';

GRANT PROCESS ON *.* TO 'leanstore'@'localhost';
GRANT PROCESS ON *.* TO 'leanstore'@'%';

GRANT SELECT ON performance_schema.* TO 'leanstore'@'localhost';
GRANT SELECT ON performance_schema.* TO 'leanstore'@'%';

FLUSH PRIVILEGES;
SQL
}

########################################
# Main experiment loops
########################################

for z in "\${skew[@]}"; do
  for j in "\${ssdsize[@]}"; do
    for i in "\${version[@]}"; do
      cd "\$RESULT_DIR" || exit 1
      mkdir -p "\$ver"
      echo "\$i"
      echo "Start YCSB/MySQL (ver=\$ver)"

      # ---------- Prepare SSD for this run ----------
      cd "\$CURRENT_DIR"

      echo "\$SUDO_PWD" | sudo -S killall -9 mysqld 2>/dev/null || true
      sleep 5

      echo "\$SUDO_PWD" | sudo -S umount "\$LS_MOUNTPOINT" 2>/dev/null || true

      echo "\$SUDO_PWD" | sudo -S blkdiscard -f "\$DEVICE"
      echo "\$SUDO_PWD" | sudo -S mkfs.ext4 "\$DEVICE"
      echo "\$SUDO_PWD" | sudo -S mount "\$DEVICE" "\$LS_MOUNTPOINT"
      echo "\$SUDO_PWD" | sudo -S chown -R "\$USER:\$USER" "\$LS_MOUNTPOINT"

      # ----------------- First-time initialization if needed -----------------
      if [ ! -f "\$DATADIR/ibdata1" ] || [ ! -d "\$DATADIR/mysql" ]; then
        echo "==> No existing InnoDB system tablespace found in \$DATADIR"
        echo "==> Cleaning existing contents of datadir and InnoDB log dir..."
        rm -rf "\$DATADIR"/* "\$INNODB_LOG_DIR"/*

        echo "==> Initializing new MySQL datadir (mysqld --initialize-insecure)..."
        pkill -f "\$MYSQLD" 2>/dev/null || true

        "\$MYSQLD" \
          --defaults-file="\$MYSQL_CNF" \
          --basedir="\$MYSQL_BASE" \
          --initialize-insecure

        echo "==> Initialization complete (root has EMPTY password)."
      else
        echo "==> Existing MySQL data found in \$DATADIR; skipping initialization."
      fi

      ################################################################
      # 1) LOAD PHASE: buffer pool = 160G
      ################################################################
      echo "==> Starting mysqld for LOAD phase (innodb_buffer_pool_size=\$LOAD_BUFFER_POOL)..."
      "\$MYSQLD" \
        --defaults-file="\$MYSQL_CNF" \
        --basedir="\$MYSQL_BASE" \
        --innodb_buffer_pool_size="\$LOAD_BUFFER_POOL" \
        >"\$LOAD_LOG" 2>&1 &

      MYSQLD_PID=$!
      echo "mysqld LOAD PID: \$MYSQLD_PID"

      wait_for_mysql "LOAD"
      create_leanstore_user

      export MYSQL_HOST="\$HOST"
      export MYSQL_TCP_PORT="\$MYSQL_PORT"
      export MYSQL_UNIX_PORT="\$SOCKET"

      # ---------- Run the YCSB loader ----------
      cd "\$TPCC_DIR"

      echo "==> Running MySQL_YCSB LOAD phase (big buffer pool)..."
      ./MySQL_YCSB \
        --SSD_OP=0.15 \
        --max_ssd_capacity_gb="\$ssdsz" \
        --ycsb_exec_seconds=0 \
        --bm_virtual_gb="\$dbsz" \
        --db_path="\$LS_MOUNTPOINT" \
        --wal_path="" \
        \$i \$j \
        --measure_waf=true \
        --ycsb_read_ratio=50 \
        --ycsb_zipf_theta="\$z" \
        --worker_count="\$thread_cnt"

      echo "==> LOAD phase finished. Shutting down MySQL (LOAD)..."
      "\$MYSQLADMIN" -u root -S "\$SOCKET" shutdown || {
        echo "mysqladmin shutdown (LOAD) failed; trying kill on PID \$MYSQLD_PID" >&2
        kill "\$MYSQLD_PID" 2>/dev/null || true
      }

      #################################################################
      # 2) RUN PHASE
      #################################################################
      echo "==> Starting mysqld for RUN phase (using \$MYSQL_CNF)..."
      "\$MYSQLD" \
        --defaults-file="\$MYSQL_CNF" \
        --basedir="\$MYSQL_BASE" \
        >"\$RUN_LOG" 2>&1 &

      MYSQLD_PID=$!
      echo "mysqld RUN PID: \$MYSQLD_PID"

      wait_for_mysql "RUN"
      create_leanstore_user

      # ---------- Provenance ----------
      cd "\$SRC_DIR"
      cp config.cc "\$CURRENT_DIR/run_ycsb_s0.sh" "\$RESULT_DIR" 2>/dev/null || true
      {
        echo "\$z"
        echo "\$j"
        echo "\$i"
        echo "access full 0.99"
      } >> "\$RESULT_DIR/\$ver/ver.out"

      # iostat logger
      cd "\$CURRENT_DIR"
      iostat -x 60 1080 > "\$RESULT_DIR/\$ver/iostat.out" &

      SMART_LOG="\$RESULT_DIR/\$ver/smart.log"
      echo "[INFO] Starting SMART monitor (every \${SMART_INTERVAL_SEC}s) for \$SMART_DEVICE -> \$SMART_LOG"
      (
        while true; do
          echo "===== \$(date -Is) =====" >> "\$SMART_LOG"
          "\$SMART_SCRIPT" "\$SMART_PASSWORD" "\$RESULT_DIR/\$ver/" "\$SMART_DEVICE" >> "\$SMART_LOG" 2>&1 || true
          sleep "\$SMART_INTERVAL_SEC"
        done
      ) &

      # ---------- RUN phase workload ----------
      cd "\$TPCC_DIR"
      echo "==> Running MySQL_YCSB RUN phase..."
      ./MySQL_YCSB \
        --SSD_OP=0.15 \
        --max_ssd_capacity_gb="\$ssdsz" \
        --ycsb_exec_seconds="\$exec_time" \
        --bm_virtual_gb="\$dbsz" \
        --db_path="\$LS_MOUNTPOINT" \
        --wal_path="" \
        \$i \$j \
        --measure_waf=true \
        --ycsb_read_ratio=50 \
        --ycsb_zipf_theta="\$z" \
        --worker_count="\$thread_cnt" \
        | tee "\$RESULT_DIR/\$ver/trx.csv"

      sleep 20
      killall -9 LeanStore_YCSB 2>/dev/null || true

      echo "\$SUDO_PWD" | sudo -S umount "\$LS_MOUNTPOINT" || true

      echo "Done evaluation"
      sleep 300
      ver=\$((ver + 10))
    done
    ver=\$((ver + 160))
  done
  ver=\$((ver + 1000))
done

echo "==> Shutting down MySQL (final)..."
"\$MYSQLADMIN" -u root -S "\$SOCKET" shutdown || {
  echo "mysqladmin shutdown failed; trying kill on PID \$MYSQLD_PID" >&2
  kill "\$MYSQLD_PID" 2>/dev/null || true
}

echo "==> All done."
