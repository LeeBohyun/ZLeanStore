#!/bin/bash
set -x 

export FILENAME=$1
export FILESIZE= # let iob figure it out
export PREFIX=$2
UNI_BS=${3:-4K}
SEQ_BS=${4:-512K}

export IOENGINE=io_uring 
export FILL=1


# Get controller device by removing 'n' and digits after it
CONTROLLER_NAME=$(echo "$FILENAME" | sed -E 's/n[0-9]+$//')

sanitize_nvme() {
    echo "sanitize"
    sudo nvme sanitize --sanact=2 "$FILENAME"
    sleep 1m
    sudo nvme sanitize-log "$CONTROLLER_NAME"
    sleep 1m
    sudo blkdiscard -v "$FILENAME"
    sleep 1m
}

sanitize_nvme_and_init() {
	sanitize_nvme
	sudo -E INIT=disable IO_SIZE_PERCENT_FILE=1 IO_DEPTH=1  FILL=0.94  BS=$SEQ_BS THREADS=1 PATTERN=sequential-noshuffle RW=1 SSDFILL= ./iob >> iob-output-$PREFIX.csv
}
run_wa_exp_zipf() {
	ZIPF=$1
	sudo -E INIT=disable IO_SIZE_PERCENT_FILE=$OVERWRITE  IO_DEPTH=256 BS=$UNI_BS THREADS=4 PATTERN=zipf$SHUFFLE ZIPF=$ZIPF RW=1  ./iob >> iob-output-$PREFIX.csv
	sleep 1m
}
run_wa_exp_fiozipf() {
	ZIPF=$1
	sudo -E INIT=disable IO_SIZE_PERCENT_FILE=$OVERWRITE  IO_DEPTH=256 BS=$UNI_BS THREADS=1 PATTERN=fiozipf$SHUFFLE ZIPF=$ZIPF RW=1  ./iob >> iob-output-$PREFIX.csv
	sleep 1m
}
run_wa_exp_uniform() {
	sudo -E INIT=disable   IO_SIZE_PERCENT_FILE=$OVERWRITE IO_DEPTH=256 BS=$UNI_BS THREADS=4 PATTERN=uniform RW=1 ./iob >> iob-output-$PREFIX.csv
	sleep 1m
}
run_wa_exp_zones() {
	ZONES=$1
	sudo -E  FILENAME=$FILENAME INIT=false IO_SIZE_PERCENT_FILE=$OVERWRITE  IO_DEPTH=128 BS=$UNI_BS THREADS=4 PATTERN=zones$SHUFFLE ZONES=$ZONES RW=1  ./iob >> iob-output-$PREFIX.csv
	sleep 2m
}


SHUFFLE="-shuffle"

PREFIXBASE=$PREFIX

OVERWRITE=4
for BS in 64K ; do # PAGE_SIZE
for ZNS_ACTIVE in 1; do  #  do   #64K 256K 1M  8G 
for THR in 4 ; do 
for ZNS_SIZE in 512K 1G 2G 4G 8G 16G 32G ; do #1062M 2124M 4248M 8496M 
for IOD in 128 ; do 
	sanitize_nvme
	sudo -E  FILENAME=$FILENAME INIT=false FILL=1  IO_SIZE_PERCENT_FILE=$OVERWRITE IO_DEPTH="$IOD" BS=$BS THREADS="$THR" PATTERN=zns-noshuffle ZNS_ACTIVE_ZONES="$ZNS_ACTIVE" ZNS_ZONE_SIZE="$ZNS_SIZE" RATE=0 RW=1 ./iob >> iob-output-$PREFIX.csv 
done
done
done
done
done



# for FDP 17817403392
# for FDP /dev/nvme4n1 Reclaim Unit Nominal Size: 8908701696
# OVERWRITE=4
# for BS in 64K ; do # PAGE_SIZE
# for ZNS_ACTIVE in 1; do  #  do   #64K 256K 1M  8G 
# for THR in 4 ; do 
# for ZNS_SIZE in 1G 2G 4G 8G 16G 32G 64G 128G ; do #1062M 2124M 4248M 8496M 
# for IOD in 128 ; do 
# #for ZNS_ACTIVE in 1 4 16 64 ; do 
# 	sanitize_nvme_and_init
# 	sudo -E  FILENAME=$FILENAME INIT=false FILL=1  IO_SIZE_PERCENT_FILE=$OVERWRITE IO_DEPTH="$IOD" BS=$BS THREADS="$THR" PATTERN=zns-noshuffle ZNS_ACTIVE_ZONES="$ZNS_ACTIVE" ZNS_ZONE_SIZE="$ZNS_SIZE" RATE=0 RW=1 ./iob >> iob-output-$PREFIX.csv 
# done
# done
# done
# done
# done

# --- Post-process: find minimum pagesPerZone with WAF always < 1.1 ---
LOGFILE="iob-log-$PREFIX.csv"
THRESH=1.1

if [[ ! -f "$LOGFILE" ]]; then
  echo "ERROR: '$LOGFILE' not found"
  exit 1
fi

min_ppz=$(
  awk -F',' -v thr="$THRESH" '
    # Return extracted integer pagesPerZone from the quoted field
    # Example field: "activeZones: 1 pagesPerZone: 16384"
    function get_ppz(s,    m) {
      if (match(s, /pagesPerZone:[[:space:]]*([0-9]+)/, m)) return m[1]
      return ""
    }

    {
      waf = $(NF) + 0.0
      # Find the column containing pagesPerZone (we search all fields)
      ppz = ""
      for (i=1; i<=NF; i++) {
        if ($i ~ /pagesPerZone:/) { ppz = get_ppz($i); break }
      }
      if (ppz == "") next

      # Track if this ppz ever violates threshold
      seen[ppz] = 1
      if (waf >= thr) bad[ppz] = 1
    }

    END {
      min = ""
      for (p in seen) {
        if (!(p in bad)) {
          if (min == "" || p+0 < min+0) min = p
        }
      }
      if (min == "") print "NONE"
      else print min
    }
  ' "$LOGFILE"
)

if [[ "$min_ppz" == "NONE" ]]; then
  echo "No pagesPerZone found where WAF always stays below $THRESH in $LOGFILE"
else
  # Convert BS (e.g., 64K, 4K, 1M) to bytes
  bs_bytes=$(
    echo "$BS" | awk '
      /K$/ { print substr($0,1,length($0)-1) * 1024 }
      /M$/ { print substr($0,1,length($0)-1) * 1024 * 1024 }
      /G$/ { print substr($0,1,length($0)-1) * 1024 * 1024 * 1024 }
      /^[0-9]+$/ { print $0 }
    '
  )

  # pagesPerZone × BS → bytes → GB
  zone_bytes=$(( min_ppz * bs_bytes ))
  zone_gb=$(awk -v b="$zone_bytes" 'BEGIN { printf "%.2f", b / (1024^3) }')

  echo "Minimum SSD append granularity with WAF ~= 1:"
  echo "  Zone size    = ${zone_gb} GB"
fi
