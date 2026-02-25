#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="etcd-workers.yaml"
SERVICE="nes"
TAG="${1:-run}"

MOUNT="/mnt/c/Users/Paula/nebula"
LOG_IN_CONTAINER="${MOUNT}/results/worker_${TAG}.log"
PID_IN_CONTAINER="${MOUNT}/results/worker_${TAG}.pid"

WORKER_CMD="./nes-single-node-worker --connection=sink-node:9090 --grpc=sink-node:9091 --enableEtcdReconciler=true --etcdEndpoints=http://etcd:2379"

docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" bash -lc "
  mkdir -p ${MOUNT}/results
  echo \"START \$(date -Is)\" > ${LOG_IN_CONTAINER}

  cd ${MOUNT}/cmake-build-debug_docker/nes-single-node-worker



  # Arranque en background, log directo a fichero
  nohup ${WORKER_CMD} >> ${LOG_IN_CONTAINER} 2>&1 &

  echo \$! > ${PID_IN_CONTAINER}
  echo \"Worker started, pid=\$!, log=${LOG_IN_CONTAINER}\"
"
