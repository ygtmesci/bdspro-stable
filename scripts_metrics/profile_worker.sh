#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-etcd}"   # etcd o plain
DURATION=30
OUT="results/profile_${MODE}.csv"

echo "second,cpu_percent,mem_mb" > "$OUT"

echo "Starting measurement for mode: $MODE"

for i in $(seq 1 $DURATION); do
  LINE=$(docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" | grep nes)

  CPU=$(echo "$LINE" | awk -F',' '{print $2}' | tr -d '%')
  MEM_RAW=$(echo "$LINE" | awk -F',' '{print $3}' | awk '{print $1}')

  # Si está en MiB lo dejamos; si está en GiB lo convertimos
  if [[ "$MEM_RAW" == *"GiB"* ]]; then
      MEM=$(echo "$MEM_RAW" | sed 's/GiB//' | awk '{print $1*1024}')
  else
      MEM=$(echo "$MEM_RAW" | sed 's/MiB//')
  fi

  echo "$i,$CPU,$MEM" >> "$OUT"
  sleep 1
done

echo "Saved to $OUT"