#!/bin/bash
set -euo pipefail

: "${HEIMDALL_CONFIG_PATH:=/etc/heimdall/heimdall.yaml}"
: "${HEIMDALL_VECTOR_CONFIG_PATH:=/tmp/heimdall/vector.yaml}"
export HEIMDALL_VECTOR_CONFIG_PATH

if [[ ! -f "${HEIMDALL_CONFIG_PATH}" ]]; then
    echo "heimdall: config not found at ${HEIMDALL_CONFIG_PATH}" >&2
    echo "heimdall: mount your heimdall.yaml into the container at that path" >&2
    exit 1
fi

# Analyzer authenticates via the Claude Code CLI (`claude --print`). Options:
#   - CLAUDE_CODE_OAUTH_TOKEN env var (long-lived subscription token), or
#   - bind-mounted ~/.claude + ~/.claude.json from the host.
# No Anthropic API key is required. If none of the above are provided,
# `claude` itself will fail the first analysis run and heimdall will log
# the error — we don't hard-fail here so dev can still boot the container.

mkdir -p "$(dirname "${HEIMDALL_VECTOR_CONFIG_PATH}")"

# Heimdall writes the vector config sub-tree to HEIMDALL_VECTOR_CONFIG_PATH
# during init() if heimdall.yaml contains a `vector:` block. Start heimdall
# first, wait briefly for the file to appear, then launch vector.
heimdall &
HEIMDALL_PID=$!

for _ in $(seq 1 50); do
    if [[ -f "${HEIMDALL_VECTOR_CONFIG_PATH}" ]]; then
        break
    fi
    sleep 0.1
done

if [[ ! -f "${HEIMDALL_VECTOR_CONFIG_PATH}" ]]; then
    echo "heimdall: vector config was not written to ${HEIMDALL_VECTOR_CONFIG_PATH}" >&2
    echo "heimdall: ensure heimdall.yaml contains a top-level 'vector:' block" >&2
    kill -TERM "${HEIMDALL_PID}" 2>/dev/null || true
    exit 1
fi

vector --config "${HEIMDALL_VECTOR_CONFIG_PATH}" &
VECTOR_PID=$!

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
