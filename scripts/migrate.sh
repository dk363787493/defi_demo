#!/bin/bash
set -euo pipefail

DB_DSN="${DB_DSN:-postgres://defi:defi_secret@localhost:5432/defi_lending?sslmode=disable}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-./migrations}"

case "${1:-up}" in
  up)
    migrate -path "$MIGRATIONS_DIR" -database "$DB_DSN" up
    ;;
  down)
    migrate -path "$MIGRATIONS_DIR" -database "$DB_DSN" down "${2:-1}"
    ;;
  version)
    migrate -path "$MIGRATIONS_DIR" -database "$DB_DSN" version
    ;;
  *)
    echo "Usage: $0 {up|down [N]|version}"
    exit 1
    ;;
esac
