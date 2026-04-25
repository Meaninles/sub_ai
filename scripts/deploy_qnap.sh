#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

QNAP_HOST="${QNAP_HOST:-192.168.110.15}"
QNAP_PORT="${QNAP_PORT:-22}"
QNAP_USER="${QNAP_USER:-erjiguan}"
REMOTE_BASE="${REMOTE_BASE:-/share/Container/ai-discovery}"
REMOTE_APP_DIR="${REMOTE_BASE}/app"
REMOTE_DATA_DIR="${REMOTE_BASE}/data"
IMAGE_NAME="${IMAGE_NAME:-ai-project-discovery:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-ai-project-discovery}"
ADMIN_PORT="${ADMIN_PORT:-8765}"

REMOTE_SHELL_PREFIX=""
if [[ "${USE_SUDO:-1}" == "1" ]]; then
  REMOTE_SHELL_PREFIX="sudo "
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd ssh
require_cmd scp
require_cmd tar

echo "[1/6] verifying remote docker environment"
ssh -p "${QNAP_PORT}" -o StrictHostKeyChecking=no "${QNAP_USER}@${QNAP_HOST}" \
  "${REMOTE_SHELL_PREFIX}sh -lc 'test -S /var/run/docker.sock && docker version >/dev/null'"

echo "[2/6] creating remote directories"
ssh -p "${QNAP_PORT}" -o StrictHostKeyChecking=no "${QNAP_USER}@${QNAP_HOST}" \
  "${REMOTE_SHELL_PREFIX}sh -lc 'mkdir -p \"${REMOTE_APP_DIR}\" \"${REMOTE_DATA_DIR}/.omx/data\"'"

echo "[3/6] uploading application files"
tar -C "${ROOT_DIR}" \
  --exclude='.git' \
  --exclude='.venv' \
  --exclude='.pytest_cache' \
  --exclude='__pycache__' \
  --exclude='.omx/data' \
  -cf - \
  Dockerfile docker-entrypoint.sh .dockerignore pyproject.toml README.md .env.example sub_sites.md src tests \
| ssh -p "${QNAP_PORT}" -o StrictHostKeyChecking=no "${QNAP_USER}@${QNAP_HOST}" \
  "${REMOTE_SHELL_PREFIX}tar -C \"${REMOTE_APP_DIR}\" -xf -"

echo "[4/6] uploading persistent config and data"
if [[ -f "${ROOT_DIR}/.env" ]]; then
  scp -P "${QNAP_PORT}" -o StrictHostKeyChecking=no "${ROOT_DIR}/.env" "${QNAP_USER}@${QNAP_HOST}:${REMOTE_DATA_DIR}/.env"
fi
if [[ -f "${ROOT_DIR}/sub_sites.md" ]]; then
  scp -P "${QNAP_PORT}" -o StrictHostKeyChecking=no "${ROOT_DIR}/sub_sites.md" "${QNAP_USER}@${QNAP_HOST}:${REMOTE_DATA_DIR}/sub_sites.md"
fi
if [[ -d "${ROOT_DIR}/.omx/data" ]]; then
  tar -C "${ROOT_DIR}" -cf - .omx/data \
  | ssh -p "${QNAP_PORT}" -o StrictHostKeyChecking=no "${QNAP_USER}@${QNAP_HOST}" \
    "${REMOTE_SHELL_PREFIX}tar -C \"${REMOTE_DATA_DIR}\" -xf - --strip-components=2"
fi

echo "[5/6] building image and recreating container"
ssh -p "${QNAP_PORT}" -o StrictHostKeyChecking=no "${QNAP_USER}@${QNAP_HOST}" "${REMOTE_SHELL_PREFIX}sh -lc '
  cd \"${REMOTE_APP_DIR}\"
  docker build -t \"${IMAGE_NAME}\" .
  docker rm -f \"${CONTAINER_NAME}\" >/dev/null 2>&1 || true
  docker run -d \
    --name \"${CONTAINER_NAME}\" \
    --restart unless-stopped \
    --network host \
    --workdir /data \
    -e TZ=Asia/Shanghai \
    -e HTTP_PROXY=http://127.0.0.1:7890 \
    -e HTTPS_PROXY=http://127.0.0.1:7890 \
    -e ALL_PROXY=http://127.0.0.1:7890 \
    -e http_proxy=http://127.0.0.1:7890 \
    -e https_proxy=http://127.0.0.1:7890 \
    -e all_proxy=http://127.0.0.1:7890 \
    -e NO_PROXY=127.0.0.1,localhost,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12 \
    -e no_proxy=127.0.0.1,localhost,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12 \
    -v \"${REMOTE_DATA_DIR}:/data\" \
    \"${IMAGE_NAME}\"
'"

echo "[6/6] verifying container health"
ssh -p "${QNAP_PORT}" -o StrictHostKeyChecking=no "${QNAP_USER}@${QNAP_HOST}" "${REMOTE_SHELL_PREFIX}sh -lc '
  docker ps --filter \"name=${CONTAINER_NAME}\" --format \"table {{.Names}}\t{{.Status}}\t{{.Networks}}\"
  sleep 3
  curl -fsS \"http://127.0.0.1:${ADMIN_PORT}/api/status\" >/dev/null
'"

echo "deployment complete: http://${QNAP_HOST}:${ADMIN_PORT}/"
