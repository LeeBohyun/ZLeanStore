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
	sudo -E INIT=disable IO_SIZE_PERCENT_FILE=1 IO_DEPTH=1  FILL=0.99  BS=$SEQ_BS THREADS=1 PATTERN=sequential-noshuffle RW=1 ./iob >> iob-output-$PREFIX.csv
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
	sudo -E  FILENAME=$FILENAME INIT=false IO_SIZE_PERCENT_FILE=$OVERWRITE  IO_DEPTH=128 BS=$UNI_BS THREADS=4 PATTERN=zones$SHUFFLE ZONES=$ZONES RW=1 ./iob >> iob-output-$PREFIX.csv
	sleep 2m
}

run_wa_exp_nowa() {
	ZONES=$1
	sudo -E  FILENAME=$FILENAME INIT=false IO_SIZE_PERCENT_FILE=$OVERWRITE  IO_DEPTH=128 BS=$UNI_BS THREADS=4 PATTERN=zones$SHUFFLE ZONES=$ZONES RW=1 ./iob >> iob-output-$PREFIX.csv
	sleep 2m
}

SHUFFLE="-shuffle"


PREFIXBASE=$PREFIX

OVERWRITE=4
for BS in 256K ; do # PAGE_SIZE
for THR in 16 ; do 
for ZNS_SIZE in 512K ; do #1062M 2124M 4248M 8496M 
for IOD in 128 ; do 
for ZNS_ACTIVE in 16384 ; do  #16384 
  sanitize_nvme
  echo "Let's test if NoWA pattern gives you SSD WAF of 1 with BS=${BS}, ZNS_SIZE=${ZNS_SIZE}, and ${ZNS_ACTIVE} number of active zones!"
	FILENAME="$FILENAME" INIT=false FILL=1 IO_SIZE_PERCENT_FILE="$OVERWRITE" \
    IO_DEPTH="$IOD" BS="$BS" THREADS="$THR" PATTERN=nowa-noshuffle \
    ZNS_ACTIVE_ZONES="$ZNS_ACTIVE" ZNS_ZONE_SIZE="$ZNS_SIZE" RATE=0 RW=1 \
    sudo -E ./iob >> "iob-output-$PREFIX.csv"
done
done
done
done
done

