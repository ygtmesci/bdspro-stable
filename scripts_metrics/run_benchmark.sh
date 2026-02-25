#!/usr/bin/env bash
set -euo pipefail

ITERATIONS="${1:-5}"
OUT="results/benchmark.csv"

echo "iteration,recovery_ms" > "$OUT"

# Asegura entorno arriba
./scripts_metrics/revive.sh

for i in $(seq 1 "$ITERATIONS"); do
  TAG="iter${i}"
  echo "=== Iteration $i/$ITERATIONS ==="

  # 1) arrancar worker
  ./scripts_metrics/start_worker.sh "$TAG"

  # 2) submit query
  ./scripts_metrics/submit_query.sh "$TAG" || true

  # (pequeña pausa opcional para asegurar que etcd tiene keys)
  sleep 2

  # 3) marca tiempo crash
  CRASH_TS="$(python3 -c 'import datetime; print(datetime.datetime.now().isoformat(timespec="seconds"))')"
  T0_MS=$(date +%s%3N)

  # 4) crash real (docker kill)
  ./scripts_metrics/crash_worker.sh || true

  # 5) revive contenedor + etcd
  ./scripts_metrics/revive.sh
  sleep 3


  # 6) arrancar worker otra vez (mismo TAG para log de "después")
  ./scripts_metrics/start_worker.sh "${TAG}_after"

  # 7) medir recovery mirando el log del worker "after"
  #WORKER_LOG="results/worker_${TAG}_after.log"
  LOG_AFTER="results/worker_${TAG}_after.log"
  RECOVERY_MS="$(./scripts_metrics/measure_recovery_grep.sh "$LOG_AFTER" "$T0_MS" 60)"


  echo "$i,$RECOVERY_MS" | tee -a "$OUT"
done

echo "Saved results to: $OUT"
