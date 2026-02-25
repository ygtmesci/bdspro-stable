#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="etcd-workers.yaml"
SERVICE="nes"
TAG="${1:-run}"

QUERYFILE_HOST="results/query_${TAG}.sql"
OUT_HOST="results/repl_${TAG}.out"

# Guardamos tus comandos del REPL en un fichero
cat > "$QUERYFILE_HOST" << 'EOF'
CREATE WORKER "sink-node:9090" AT "sink-node:9091";
CREATE LOGICAL SOURCE endless(ts UINT64);
CREATE PHYSICAL SOURCE FOR endless
TYPE Generator
SET(
    'ALL' as `SOURCE`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as PARSER.`TYPE`,
    'emit_rate 10' AS `SOURCE`.GENERATOR_RATE_CONFIG,
    10000000 AS `SOURCE`.MAX_RUNTIME_MS,
    "sink-node:9090" AS `SOURCE`.`HOST`,
    1 AS `SOURCE`.SEED,
    'SEQUENCE UINT64 0 10000000 1' AS `SOURCE`.GENERATOR_SCHEMA
);
CREATE SINK someSink(ENDLESS.TS UINT64)
TYPE File
SET(
    'out.csv' as `SINK`.FILE_PATH,
    'CSV' as `SINK`.INPUT_FORMAT,
    "sink-node:9090" AS `SINK`.`HOST`
);
SELECT TS FROM ENDLESS INTO SOMESINK;
EOF

# Ejecutamos el REPL y le pasamos el fichero por stdin
# Si el repl es 100% interactivo, esto normalmente funciona igual porque al llegar EOF se cierra.
docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" bash -lc "
  cd ./cmake-build-debug_docker/nes-nebuli/apps
  cat ${PWD}/${QUERYFILE_HOST} | ./nes-repl-embedded
" > "$OUT_HOST" 2>&1 || true

echo "Saved REPL output to: $OUT_HOST"
