#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="etcd-workers.yaml"

# Levanta servicios otra vez
docker compose -f "$COMPOSE_FILE" up -d
