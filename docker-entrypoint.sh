#!/bin/bash
set -euo pipefail

: "${HEIMDALL_CONFIG_PATH:=/etc/heimdall/heimdall.yaml}"
VECTOR_CONFIG_OUT="/tmp/heimdall/vector.yaml"

if [[ ! -f "${HEIMDALL_CONFIG_PATH}" ]]; then
    echo "heimdall: config not found at ${HEIMDALL_CONFIG_PATH}" >&2
    echo "heimdall: mount your heimdall.yaml into the container at that path" >&2
    exit 1
fi

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
    echo "heimdall: ANTHROPIC_API_KEY is not set" >&2
    exit 1
fi

mkdir -p "$(dirname "${VECTOR_CONFIG_OUT}")"
heimdall --extract-vector-config "${VECTOR_CONFIG_OUT}"

vector --config "${VECTOR_CONFIG_OUT}" &
VECTOR_PID=$!

heimdall &
HEIMDALL_PID=$!

shutdown() {
    echo "heimdall: received signal, shutting down" >&2
    kill -TERM "${VECTOR_PID}" "${HEIMDALL_PID}" 2>/dev/null || true
}
trap shutdown TERM INT

# Exit as soon as either process dies so the container restarts cleanly.
wait -n "${VECTOR_PID}" "${HEIMDALL_PID}"
EXIT=$?
shutdown
wait || true
exit "${EXIT}"
