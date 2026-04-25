#!/bin/sh
set -eu

APP_DATA_DIR="${APP_DATA_DIR:-/data}"
DEFAULTS_DIR="${APP_HOME:-/opt/ai-discovery}/defaults"

mkdir -p "${APP_DATA_DIR}/.omx/data"

if [ ! -f "${APP_DATA_DIR}/sub_sites.md" ] && [ -f "${DEFAULTS_DIR}/sub_sites.md" ]; then
  cp "${DEFAULTS_DIR}/sub_sites.md" "${APP_DATA_DIR}/sub_sites.md"
fi

if [ ! -f "${APP_DATA_DIR}/.env" ] && [ -f "${DEFAULTS_DIR}/.env.example" ]; then
  cp "${DEFAULTS_DIR}/.env.example" "${APP_DATA_DIR}/.env"
fi

cd "${APP_DATA_DIR}"
exec "$@"
