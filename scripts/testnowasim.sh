#!/bin/bash
GC="greedy"


for BS in 512K ; do
for ZNS_SIZE in 512K; do
for ZNS_ACTIVE in 2048 ; do

  pattern="nowa-noshuffle"
  zone=""          # empty unless PATTERN=zones
  szone="$ZNS_SIZE"

  echo "SIM: GC=${GC} PATTERN=${pattern} ZNS_SIZE=${szone} ZNS_ACTIVE=${ZNS_ACTIVE}"

  WRITES=500 \
  PATTERN="$pattern" \
  ZIPF="$zipf" \
  ZONES="$zone" \
  GC="$GC" \
  ZNS_ACTIVE_ZONES="$ZNS_ACTIVE" \
  ZNS_ZONE_SIZE="$ZNS_SIZE" \
  BS="$BS" LOAD=false \
  ./sim sim-"$GC"-"$pattern"-$zipf-"$szone" | tee sim-"$GC"-"$pattern"-$zipf-"$szone".csv

done
done
done


### ZNS-like write pattern
## simulator erase granularity is 8MB, so under 1 active zone, WAF will hit 1 when it reaches 8MB zone size

for BS in 256K ; do
for ZNS_SIZE in 4M ; do
for ZNS_ACTIVE in 1 ; do

  pattern="zns-noshuffle"
  szone="$ZNS_SIZE"

  echo "SIM: GC=${GC} PATTERN=${pattern} ZNS_SIZE=${szone} ZNS_ACTIVE=${ZNS_ACTIVE}"

  WRITES=500 \
  PATTERN="$pattern" \
  ZIPF="$zipf" \
  ZONES="$zone" \
  GC="$GC" \
  ZNS_ACTIVE_ZONES="$ZNS_ACTIVE" \
  ZNS_ZONE_SIZE="$ZNS_SIZE" LOAD=false \
  BS="$BS" \
  ./sim sim-"$GC"-"$pattern"-$zipf-"$szone" | tee sim-"$GC"-"$pattern"-$zipf-"$szone".csv

done
done
done