#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  script.sh <result_dir> <device>
            [--duration SECONDS] [--interval SECONDS]
            [--stop-on-stable] [--stable-samples N]
            [--flash-unit-bytes N] [--host-unit-bytes N]
            [--sudo-mode auto|require|never]

Examples:
  # Run for 30 minutes, sample every 10s
  script.sh /tmp/results /dev/nvme2n1 --duration 1800 --interval 10

  # Run until flash counter stable for last 300 samples
  script.sh /tmp/results /dev/nvme2n1 --stop-on-stable --stable-samples 300 --interval 1

  # If your device reports "physical media units written" in 512,000-byte units (common):
  script.sh /tmp/results /dev/nvme2n1 --flash-unit-bytes 512000 --host-unit-bytes 512000

Notes:
  - WAF is computed as (delta_flash_bytes / delta_host_bytes).
  - Files written:
      flash_raw.out   : raw flash counter as reported by nvme ocp smart-add-log
      host_raw.out    : raw host DUW units as reported by smartctl (NOT bytes)
      flash_bytes.out : converted flash bytes
      host_bytes.out  : converted host bytes
      waf.csv         : per-interval + running average + cumulative WAF
      blocksize.txt   : block size info + script settings
      usedspace.out   : nvme list line for the device (append)
  - This script does NOT accept a sudo password argument (safer).
    It will use sudo if needed (unless --sudo-mode never).
EOF
}

# ---- Args ----
if [ "$#" -lt 2 ]; then usage; exit 1; fi

RESULT_DIR="$1"
DEVICE="$2"
shift 2

DURATION_SECS=0          # 0 => no fixed duration limit
INTERVAL_SECS=1
STOP_ON_STABLE=0
STABLE_SAMPLES=300

# Unit sizes (bytes per "unit")
# NVMe spec DUW is typically 512,000 bytes per unit.
FLASH_UNIT_BYTES=512000
HOST_UNIT_BYTES=512000

SUDO_MODE="auto"         # auto|require|never

while [ "$#" -gt 0 ]; do
  case "$1" in
    --duration)          DURATION_SECS="$2"; shift 2;;
    --interval)          INTERVAL_SECS="$2"; shift 2;;
    --stop-on-stable)    STOP_ON_STABLE=1; shift 1;;
    --stable-samples)    STABLE_SAMPLES="$2"; shift 2;;
    --flash-unit-bytes)  FLASH_UNIT_BYTES="$2"; shift 2;;
    --host-unit-bytes)   HOST_UNIT_BYTES="$2"; shift 2;;
    --sudo-mode)         SUDO_MODE="$2"; shift 2;;
    -h|--help)           usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

mkdir -p "$RESULT_DIR"

FLASH_RAW_OUT="$RESULT_DIR/flash_raw.out"
HOST_RAW_OUT="$RESULT_DIR/host_raw.out"
FLASH_BYTES_OUT="$RESULT_DIR/flash_bytes.out"
HOST_BYTES_OUT="$RESULT_DIR/host_bytes.out"
SPACE_OUT="$RESULT_DIR/usedspace.out"
WAF_OUT="$RESULT_DIR/waf.csv"
BS_OUT="$RESULT_DIR/blocksize.txt"

# ---- Derive controller device robustly (handles partitions) ----
# /dev/nvme2n1p3 -> base nvme2n1 -> controller nvme2
devbase="$(basename "$DEVICE")"
devbase="${devbase%%p[0-9]*}"          # strip partition suffix if any
controller_base="${devbase%%n[0-9]*}"  # strip namespace suffix
CONTROLLER_DEVICE="/dev/$controller_base"

# ---- Helper: sudo wrapper ----
need_sudo=0
if [ "${EUID:-9999}" -ne 0 ]; then
  need_sudo=1
fi

run() {
  if [ "$need_sudo" -eq 1 ]; then
    case "$SUDO_MODE" in
      never)
        "$@"
        ;;
      auto)
        # Use cached credentials if available; otherwise prompt once via sudo -v below.
        sudo -n true 2>/dev/null || sudo -v
        sudo -n "$@"
        ;;
      require)
        sudo -v
        sudo "$@"
        ;;
      *)
        echo "Invalid --sudo-mode: $SUDO_MODE (expected auto|require|never)" >&2
        exit 1
        ;;
    esac
  else
    "$@"
  fi
}

# ---- Check block sizes (logical + physical) ----
get_block_sizes() {
  local logical physical out

  logical=""
  physical=""

  if [ -r "/sys/block/$devbase/queue/logical_block_size" ]; then
    logical="$(cat "/sys/block/$devbase/queue/logical_block_size" 2>/dev/null || true)"
  fi
  if [ -r "/sys/block/$devbase/queue/physical_block_size" ]; then
    physical="$(cat "/sys/block/$devbase/queue/physical_block_size" 2>/dev/null || true)"
  fi

  if [ -z "${logical:-}" ] || [ -z "${physical:-}" ]; then
    out="$(lsblk -ndo LOG-SEC,PHY-SEC "$DEVICE" 2>/dev/null || true)"
    logical="${logical:-$(awk '{print $1}' <<<"$out")}"
    physical="${physical:-$(awk '{print $2}' <<<"$out")}"
  fi

  if [ -z "${logical:-}" ]; then logical="512"; fi
  if [ -z "${physical:-}" ]; then physical="$logical"; fi

  echo "$logical" "$physical"
}

read -r LOGICAL_BS PHYSICAL_BS < <(get_block_sizes)

{
  echo "DEVICE=$DEVICE"
  echo "CONTROLLER_DEVICE=$CONTROLLER_DEVICE"
  echo "LOGICAL_BLOCK_SIZE=$LOGICAL_BS"
  echo "PHYSICAL_BLOCK_SIZE=$PHYSICAL_BS"
  echo "INTERVAL_SECS=$INTERVAL_SECS"
  echo "DURATION_SECS=$DURATION_SECS"
  echo "STOP_ON_STABLE=$STOP_ON_STABLE"
  echo "STABLE_SAMPLES=$STABLE_SAMPLES"
  echo "FLASH_UNIT_BYTES=$FLASH_UNIT_BYTES"
  echo "HOST_UNIT_BYTES=$HOST_UNIT_BYTES"
  echo "SUDO_MODE=$SUDO_MODE"
} > "$BS_OUT"

# ---- Initialize WAF output ----
echo "sample,delta_flash_bytes,delta_host_bytes,interval_waf,running_avg_waf,cumulative_waf,epoch" > "$WAF_OUT"

# ---- WAF state ----
prev_flash_bytes=""
prev_host_bytes=""
base_flash_bytes=""
base_host_bytes=""

sum_interval_waf="0.000000"
interval_count=0
sample=0

start_epoch="$(date +%s)"

# ---- Stabilization ring buffer for RAW flash units ----
declare -a flash_buf=()

# ---- Helper: extract last integer on matching line ----
extract_last_int_on_line() {
  awk -v re="$1" '
    $0 ~ re {
      for (i=NF; i>=1; i--) {
        if ($i ~ /^[0-9]+$/) { print $i; exit }
      }
    }'
}

# ---- Main loop ----
while true; do
  now_epoch="$(date +%s)"

  if [ "$DURATION_SECS" -gt 0 ]; then
    elapsed=$((now_epoch - start_epoch))
    if [ "$elapsed" -ge "$DURATION_SECS" ]; then
      echo "Reached duration limit: ${DURATION_SECS}s. Stopping."
      break
    fi
  fi

  # 1) OCP: Physical media units written (RAW units)
  set +e
  flash_raw="$(
    run nvme ocp smart-add-log "$CONTROLLER_DEVICE" 2>/dev/null \
    | extract_last_int_on_line 'Physical[[:space:]]+media[[:space:]]+units[[:space:]]+written' \
    | tail -n 1
  )"
  rc=$?
  set -e
  if [ $rc -ne 0 ] || [ -z "${flash_raw:-}" ]; then flash_raw="0"; fi
  echo "$flash_raw" >> "$FLASH_RAW_OUT"

  # 2) smartctl: Data Units Written (RAW DUW units, not bytes)
  set +e
  host_units="$(
    run smartctl -A "$DEVICE" 2>/dev/null \
      | awk '/^Data Units Written/{
          for(i=1;i<=NF;i++){
            if ($i ~ /^[0-9,]+$/){ gsub(",","",$i); print $i; exit }
          }
        }' \
      | tail -n 1
  )"
  rc=$?
  set -e
  if [ $rc -ne 0 ] || [ -z "${host_units:-}" ]; then host_units="0"; fi
  echo "$host_units" >> "$HOST_RAW_OUT"

  # 3) Space usage line (best-effort)
  run nvme list 2>/dev/null | awk -v d="$DEVICE" '$1==d {print}' >> "$SPACE_OUT" || true

  # ---- Convert to bytes ----
  # shell arithmetic is int64; if you expect huge numbers, consider using awk for multiplication.
  flash_bytes="$(awk -v u="$flash_raw" -v b="$FLASH_UNIT_BYTES" 'BEGIN{ printf "%.0f\n", u*b }')"
  host_bytes="$(awk -v u="$host_units" -v b="$HOST_UNIT_BYTES" 'BEGIN{ printf "%.0f\n", u*b }')"

  echo "$flash_bytes" >> "$FLASH_BYTES_OUT"
  echo "$host_bytes" >> "$HOST_BYTES_OUT"

  # ---- Compute WAF ----
  sample=$((sample + 1))

  if [ -z "$prev_flash_bytes" ]; then
    prev_flash_bytes="$flash_bytes"
    prev_host_bytes="$host_bytes"
    base_flash_bytes="$flash_bytes"
    base_host_bytes="$host_bytes"
  else
    df="$(awk -v a="$flash_bytes" -v b="$prev_flash_bytes" 'BEGIN{ printf "%.0f\n", a-b }')"
    dh="$(awk -v a="$host_bytes" -v b="$prev_host_bytes" 'BEGIN{ printf "%.0f\n", a-b }')"

    # Only log WAF when host advanced
    if awk -v x="$dh" 'BEGIN{ exit !(x>0) }'; then
      interval_waf="$(awk -v f="$df" -v h="$dh" 'BEGIN{ printf "%.6f", f/h }')"

      interval_count=$((interval_count + 1))
      sum_interval_waf="$(awk -v s="$sum_interval_waf" -v w="$interval_waf" 'BEGIN{ printf "%.6f", s+w }')"
      running_avg="$(awk -v s="$sum_interval_waf" -v n="$interval_count" 'BEGIN{ printf "%.6f", s/n }')"

      cum_flash="$(awk -v a="$flash_bytes" -v b="$base_flash_bytes" 'BEGIN{ printf "%.0f\n", a-b }')"
      cum_host="$(awk -v a="$host_bytes" -v b="$base_host_bytes" 'BEGIN{ printf "%.0f\n", a-b }')"

      if awk -v x="$cum_host" 'BEGIN{ exit !(x>0) }'; then
        cumulative_waf="$(awk -v f="$cum_flash" -v h="$cum_host" 'BEGIN{ printf "%.6f", f/h }')"
      else
        cumulative_waf="nan"
      fi

      echo "$sample,$df,$dh,$interval_waf,$running_avg,$cumulative_waf,$now_epoch" >> "$WAF_OUT"
    fi

    prev_flash_bytes="$flash_bytes"
    prev_host_bytes="$host_bytes"
  fi

  # ---- Optional stabilization check (on RAW flash units) ----
  if [ "$STOP_ON_STABLE" -eq 1 ]; then
    flash_buf+=("$flash_raw")
    if [ "${#flash_buf[@]}" -gt "$STABLE_SAMPLES" ]; then
      flash_buf=("${flash_buf[@]:1}")
    fi

    if [ "${#flash_buf[@]}" -eq "$STABLE_SAMPLES" ]; then
      all_same="yes"
      first="${flash_buf[0]}"
      for v in "${flash_buf[@]}"; do
        if [ "$v" != "$first" ]; then
          all_same="no"
          break
        fi
      done
      if [ "$all_same" = "yes" ]; then
        echo "Flash counter stabilized for last $STABLE_SAMPLES samples. Stopping."
        break
      fi
    fi
  fi

  sleep "$INTERVAL_SECS"
done

echo "Done."
echo "Outputs:"
echo "  $FLASH_RAW_OUT"
echo "  $HOST_RAW_OUT"
echo "  $FLASH_BYTES_OUT"
echo "  $HOST_BYTES_OUT"
echo "  $SPACE_OUT"
echo "  $WAF_OUT"
echo "  $BS_OUT"
