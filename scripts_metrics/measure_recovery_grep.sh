#!/usr/bin/env bash
set -euo pipefail

LOGFILE="$1"
T0_MS="$2"
TIMEOUT_SEC="${3:-60}"

PATTERN="SingleNodeWorker: started local query"

start_sec=$(date +%s)
while true; do
  if [[ -f "$LOGFILE" ]] && grep -q "$PATTERN" "$LOGFILE"; then
    t1_ms=$(date +%s%3N)
    echo $((t1_ms - T0_MS))
    exit 0
  fi

  now_sec=$(date +%s)
  if (( now_sec - start_sec >= TIMEOUT_SEC )); then
    echo "TIMEOUT waiting for recovery pattern in $LOGFILE" >&2
    exit 1
  fi

  sleep 0.2
done
