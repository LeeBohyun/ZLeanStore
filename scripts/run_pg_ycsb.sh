#!/usr/bin/env bash
set -euo pipefail

########################################
# Usage / argument parsing
########################################
usage() {
  cat <<EOF
Usage: $0 [options]

Paths / dirs (default to your current setup):

  -c  Current scripts dir        [default: /home/lee/scripts]
  -b  Benchmark dir              [default: /home/lee/git/LeanStoreOOPW/build/benchmark]
  -r  Result dir                 [default: /home/lee/result/ycsb/postgres/800/nofullpw]
  -s  Source dir                 [default: /home/lee/git/LeanStoreOOPW/src]
  -d  NVMe device                [default: /dev/nvme2n1]
  -m  Mountpoint (LS_MOUNTPOINT) [default: /home/lee/test_data]

Postgres config:

  -g  PG bin dir (pg_ctl/initdb/psql) [default: /home/lee/pg-debug/bin]
  -C  Postgres LOAD config            [default: /home/lee/pgsql.conf]
  -R  Postgres RUN config             [default: /home/lee/pgsql.conf]
  -D  PGDATA (data dir)               [default: same as mountpoint]
  -L  PG log dir                      [default: /home/lee/test_log]

Sudo / password:

  -P  Sudo password (for blkdiscard/mkfs/mount/killall)
      If omitted, you will be prompted.

Other:

  -h  Show this help and exit

Example:
  $0 -P your_sudo_pw \
     -c /home/lee/scripts \
     -b /home/lee/git/LeanStoreOOPW/build/benchmark \
     -r /home/lee/result/ycsb/postgres/800/nofullpw \
     -s /home/lee/git/LeanStoreOOPW/src \
     -d /dev/nvme2n1
EOF
  exit 1
}

########################################
# Defaults (your current values)
########################################

current_dir="/home/lee/scripts"
build_dir="/home/lee/git/LeanStoreOOPW/build"
benchmark_dir="/home/lee/git/LeanStoreOOPW/build/benchmark"
result_dir="/home/lee/result/ycsb/postgres/800/nofullpw"
device="/dev/nvme2n1"
src_dir="/home/lee/git/LeanStoreOOPW/src"

SUDO_PWD=""

exec_time=10800
thread_cnt=64
ver=100
skew=(0.8)

ssdsz="894"
dbsz=894
ssdsize=(
  #"--ycsb_dataset_size_gb=10 --bm_physical_gb=1 --max_wal_capacity_gb=4 --ycsb_record_count=8372093"
  "--ycsb_dataset_size_gb=800 --bm_physical_gb=80 --max_wal_capacity_gb=80 --ycsb_record_count=4200000000"
)

version=(
  "--use_out_of_place_write=false --write_buffer_partition_cnt=1 --bm_evict_batch_size=64  --block_size_mb=8192 --enable_doublewrite=true"
)

# Filesystem mountpoint for Postgres data on $device
LS_MOUNTPOINT="/home/lee/test_data"

# ----------------- Postgres config -----------------
PG_BIN_DIR="/home/lee/pg-debug/bin"
PG_CTL="$PG_BIN_DIR/pg_ctl"
INITDB="$PG_BIN_DIR/initdb"
PSQL="$PG_BIN_DIR/psql"

PG_PORT=5432
PG_SUPERUSER="$USER"

PG_CONF_LOAD="/home/lee/pgsql.conf"
PG_CONF_RUN="/home/lee/pgsql.conf"

PGLOG_DIR="/home/lee/test_log"
PG_WAL_DIR="$PGLOG_DIR/pg_wal"

# Optional: tell libpq defaults (mainly for manual psql)
export PGHOST="/home/lee/pgsock"
export PGPORT="$PG_PORT"
export PGUSER="postgres"
export PGDATABASE="postgres"

########################################
# Parse CLI arguments
########################################

while getopts "c:b:r:s:d:m:g:C:R:D:L:P:h" opt; do
  case "$opt" in
    c) current_dir=$OPTARG ;;
    b) benchmark_dir=$OPTARG ;;
    r) result_dir=$OPTARG ;;
    s) src_dir=$OPTARG ;;
    d) device=$OPTARG ;;
    m) LS_MOUNTPOINT=$OPTARG ;;
    g) PG_BIN_DIR=$OPTARG ;;
    C) PG_CONF_LOAD=$OPTARG ;;
    R) PG_CONF_RUN=$OPTARG ;;
    D) PGDATA=$OPTARG ;;
    L) PGLOG_DIR=$OPTARG ;;
    P) SUDO_PWD=$OPTARG ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Derive paths that depend on others
PG_CTL="$PG_BIN_DIR/pg_ctl"
INITDB="$PG_BIN_DIR/initdb"
PSQL="$PG_BIN_DIR/psql"

if [[ -z "${PGDATA:-}" ]]; then
  PGDATA="$LS_MOUNTPOINT"
fi
PG_WAL_DIR="$PGLOG_DIR/pg_wal"

# Ask for sudo password if not provided
if [[ -z "$SUDO_PWD" ]]; then
  read -s -p "Enter sudo password: " SUDO_PWD
  echo
fi

########################################
# Helper functions
########################################

run_psql() {
  PGPASSWORD="" "$PSQL" -h "127.0.0.1" -p "$PG_PORT" -U "$PG_SUPERUSER" "$@"
}

wait_for_postgres() {
  local label="$1"
  echo "==> Waiting for Postgres to be ready ($label)..."
  for _ in {1..60}; do
    if run_psql -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
      echo "Postgres is ready ($label)."
      return 0
    fi
    sleep 1
  done
  echo "ERROR: Postgres did not become ready within 60 seconds ($label)." >&2
  return 1
}

create_ycsb_user() {
  echo "==> Ensuring Postgres role 'postgres' exists for YCSB..."

  run_psql -d postgres <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
    CREATE ROLE postgres WITH LOGIN SUPERUSER;
  END IF;
END
$$;
SQL
}

########################################
# Prechecks
########################################

if [ ! -x "$PG_CTL" ] || [ ! -x "$INITDB" ] || [ ! -x "$PSQL" ]; then
  echo "ERROR: pg_ctl/initdb/psql not found or not executable in PG_BIN_DIR=$PG_BIN_DIR" >&2
  exit 1
fi

if [ ! -f "$PG_CONF_RUN" ]; then
  echo "ERROR: Postgres RUN config not found at: $PG_CONF_RUN" >&2
  exit 1
fi

if [ ! -f "$PG_CONF_LOAD" ]; then
  echo "WARNING: Postgres LOAD config not found at: $PG_CONF_LOAD" >&2
  echo "         Using RUN config for both LOAD and RUN."
  PG_CONF_LOAD="$PG_CONF_RUN"
fi

mkdir -p "$result_dir" "$PGLOG_DIR" "$LS_MOUNTPOINT"

echo "==> Postgres PGDATA set to: $PGDATA"

########################################
# Main experiment loops
########################################

for z in "${skew[@]}"; do
  for j in "${ssdsize[@]}"; do
    for i in "${version[@]}"; do
      cd "$result_dir" || exit 1
      mkdir -p "$ver"
      echo "$i"
      echo "Start YCSB/Postgres (ver=$ver)"

      # ---------- Prepare SSD / Postgres for this run ----------
      cd "$current_dir"

      # Stop any running Postgres on this data dir / port
      "$PG_CTL" -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true
      echo "$SUDO_PWD" | sudo -S killall -9 postgres 2>/dev/null || true
      sleep 5

      # FS + initdb section left commented out as in your original script
      # so you can re-enable it when you want a fresh cluster each time.

      # echo "$SUDO_PWD" | sudo -S umount "$LS_MOUNTPOINT" 2>/dev/null || true
      # echo "$SUDO_PWD" | sudo -S blkdiscard -f "$device"
      # echo "$SUDO_PWD" | sudo -S mkfs.ext4 "$device"
      # echo "$SUDO_PWD" | sudo -S mount "$device" "$LS_MOUNTPOINT"
      # echo "$SUDO_PWD" | sudo -S chown -R "$USER:$USER" "$LS_MOUNTPOINT"

      # mkdir -p "$PGLOG_DIR"
      # rm -rf "$PG_WAL_DIR"
      # rm -rf "$PGDATA"/*

      # echo "==> Initializing PostgreSQL cluster in $PGDATA..."
      # "$INITDB" -D "$PGDATA" -U "$PG_SUPERUSER" > "$result_dir/$ver/initdb.log" 2>&1

      # echo "==> Relocating WAL to $PG_WAL_DIR ..."
      # mv "$PGDATA/pg_wal" "$PG_WAL_DIR"
      # ln -s "$PG_WAL_DIR" "$PGDATA/pg_wal"
      # ls -ld "$PGDATA/pg_wal"

      # =====================================================================
      # 1) LOAD PHASE (currently commented out in your script)
      # =====================================================================
      # LOAD_LOG="$result_dir/$ver/postgres_load.log"
      # echo "==> Starting PostgreSQL for LOAD phase (shared_buffers=200GB)..."
      # echo "    (Logs: $LOAD_LOG)"
      #
      # "$PG_CTL" -D "$PGDATA" -l "$LOAD_LOG" \
      #   -o "-p $PG_PORT -c config_file=$PG_CONF_LOAD -c shared_buffers=200GB" \
      #   start
      #
      # wait_for_postgres "LOAD"
      # create_ycsb_user
      #
      # cd "$benchmark_dir"
      # echo "==> Running Postgres_YCSB LOAD phase (exec_seconds=0)..."
      # ./Postgres_YCSB \
      #   --SSD_OP=0.15 \
      #   --max_ssd_capacity_gb="$ssdsz" \
      #   --ycsb_exec_seconds=0 \
      #   --bm_virtual_gb="$dbsz" \
      #   --db_path="$LS_MOUNTPOINT" \
      #   --wal_path="" \
      #   $i $j \
      #   --measure_waf=true \
      #   --ycsb_read_ratio=50 \
      #   --ycsb_zipf_theta="$z" \
      #   --worker_count="$thread_cnt"
      #
      # echo "==> LOAD phase finished. Shutting down Postgres (LOAD)..."
      # "$PG_CTL" -D "$PGDATA" -m fast -t 600 stop || {
      #   echo "pg_ctl stop (LOAD) failed; trying killall postgres" >&2
      #   echo "$SUDO_PWD" | sudo -S killall -9 postgres 2>/dev/null || true
      # }

      # =====================================================================
      # 2) RUN PHASE: use PG_CONF_RUN + shared_buffers=80GB
      # =====================================================================
      RUN_LOG="$result_dir/$ver/postgres_run.log"
      echo "==> Starting PostgreSQL for RUN phase (shared_buffers=80GB)..."
      echo "    (Logs: $RUN_LOG)"

      "$PG_CTL" -D "$PGDATA" -l "$RUN_LOG" \
        -o "-p $PG_PORT -c config_file=$PG_CONF_RUN -c shared_buffers=80GB" \
        start

      wait_for_postgres "RUN"
      create_ycsb_user

      # ---------- Provenance ----------
      cd "$src_dir"
      cp config.cc "$current_dir/run_ycsb_postgres_s0.sh" "$result_dir"
      {
        echo "$z"
        echo "$j"
        echo "$i"
        echo "access full 0.99"
      } >> "$result_dir/$ver/ver.out"

      # iostat logger in background
      cd "$current_dir"
      iostat -x 60 1080 > "$result_dir/$ver/iostat.out" &

      # ---------- Run the Postgres_YCSB benchmark (RUN phase) ----------
      cd "$benchmark_dir"
      echo "==> Running Postgres_YCSB RUN phase..."
      ./Postgres_YCSB \
        --SSD_OP=0.15 \
        --max_ssd_capacity_gb="$ssdsz" \
        --ycsb_exec_seconds="$exec_time" \
        --bm_virtual_gb="$dbsz" \
        --db_path="$LS_MOUNTPOINT" \
        --wal_path="" \
        $i $j \
        --measure_waf=true \
        --ycsb_read_ratio=50 \
        --ycsb_zipf_theta="$z" \
        --worker_count="$thread_cnt" \
        | tee "$result_dir/$ver/trx.csv"

      sleep 20
      killall -9 Postgres_YCSB 2>/dev/null || true

      echo "$SUDO_PWD" | sudo -S umount "$LS_MOUNTPOINT" 2>/dev/null || true

      echo "Done evaluation"
      sleep 300
      ver=$((ver + 10))
    done
    ver=$((ver + 160))
  done
  ver=$((ver + 1000))
done

# ----------------- Shutdown Postgres -----------------
echo "==> Shutting down Postgres (final)..."
"$PG_CTL" -D "$PGDATA" -m fast stop || {
  echo "pg_ctl stop failed; trying killall postgres" >&2
  echo "$SUDO_PWD" | sudo -S killall -9 postgres 2>/dev/null || true
}

pkill -f get_smart_info.sh 2>/dev/null || true

echo "==> All done."
