#!/usr/bin/env bash
# Collect NVMe media/host write stats and stop when flashwrite stabilizes.

set -uo pipefail

# -------- Args --------
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <sudo_password> <result_dir> <device>"
  echo "Example: $0 mypass /tmp/results /dev/nvme2n1"
  exit 1
fi

SUDO_PASS="$1"
RESULT_DIR="$2"
DEVICE="$3"

# Strip namespace (e.g., /dev/nvme2n1 -> /dev/nvme2)
CONTROLLER_DEVICE="$(printf "%s" "$DEVICE" | sed -E 's/n[0-9]+$//')"

mkdir -p "$RESULT_DIR"

FLASH_OUT="$RESULT_DIR/flashwrite.out"
HOST_OUT="$RESULT_DIR/hostwrite.out"
SPACE_OUT="$RESULT_DIR/usedspace.out"

# ---- Helper: sudo if needed, otherwise run directly ----
run() {
  if [ "$EUID" -ne 0 ]; then
    # -S read password from stdin, -p "" suppress prompt
    printf '%s\n' "$SUDO_PASS" | sudo -S -p "" "$@"
  else
    "$@"
  fi
}

# ---- Note on units ----
# smartctl's "Data Units Written" for NVMe are typically *1000*×512-byte units.
# Make this configurable; default to 512000 bytes per unit.
: "${DUW_UNIT_BYTES:=512000}"

# ---- Main loop ----
while true; do
    # OCP log: Physical media units written (controller)
    # Example line:
    # Physical media units written -                0 4778461736910848
    run nvme ocp smart-add-log "$CONTROLLER_DEVICE" 2>/dev/null \
    | awk '/Physical[[:space:]]+media[[:space:]]+units[[:space:]]+written/{
        # grab the last numeric field (low 64 bits)
        for (i=NF; i>=1; i--) {
            if ($i ~ /^[0-9]+$/) { print $i; break }
        }
        }' \
    >> "$FLASH_OUT"


  # smartctl: Data Units Written (namespace device)
  run smartctl -A "$DEVICE" 2>/dev/null \
    | awk '/^Data Units Written/{
        for(i=1;i<=NF;i++){ if ($i ~ /^[0-9,]+$/){ gsub(",","",$i); num=$i; break } }
        if (num != "") print num
      }' \
    | awk -v BYTES="$DUW_UNIT_BYTES" '{printf "%.0f\n", $1*BYTES}' \
    >> "$HOST_OUT"

  # Log namespace usage from nvme list
  run nvme list 2>/dev/null \
    | awk -v d="$DEVICE" '$1==d {print}' \
    >> "$SPACE_OUT"

  # ---- Stabilization check on flashwrite.out ----
  if [ ! -s "$FLASH_OUT" ]; then
    sleep 10
    continue
  fi

  mapfile -t lines < <(tail -n 1800 "$FLASH_OUT")

  # NEW: If we don't yet have 1800 lines, skip check until we do
  if [ "${#lines[@]}" -lt 1800 ]; then
    sleep 10
    continue
  fi

  # Check if all last 1800 lines have the same value
  all_same="yes"
  first="${lines[0]}"
  for v in "${lines[@]}"; do
    if [ "$v" != "$first" ]; then
      all_same="no"
      break
    fi
  done

  if [ "$all_same" = "yes" ]; then
    echo "All of the last ${#lines[@]} lines of flashwrite.out have the same value. Exiting loop."
    break
  fi

  sleep 10
done
