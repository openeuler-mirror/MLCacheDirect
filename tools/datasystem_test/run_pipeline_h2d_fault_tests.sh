#!/bin/bash
# Pipeline H2D Fault Test - Full Scenario Runner
# 一键跑完所有流程4/6/7/8故障场景

set -euo pipefail

LOCAL_IP="${1:-127.0.0.1}"
REMOTE_IP="${2:-}"
PORT="${PORT:-${3:-18481}}"
INJECT_DELAY_MS="${INJECT_DELAY_MS:-${4:-3000}}"
TIMEOUT_MS="${TIMEOUT_MS:-${5:-30000}}"
TEST_DIR="$(dirname "$0")"
TEST_BIN="${TEST_DIR}/pipeline_h2d_fault_test"

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    echo "ERROR: invalid port: $PORT"
    exit 1
fi
if ! [[ "$INJECT_DELAY_MS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: invalid inject delay: $INJECT_DELAY_MS"
    exit 1
fi
if ! [[ "$TIMEOUT_MS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: invalid timeout: $TIMEOUT_MS"
    exit 1
fi

echo "========================================"
echo "Pipeline H2D Fault Test - Full Run"
echo "Local (Get side):  $LOCAL_IP"
echo "Dataserver port:   $PORT"
echo "Inject delay:      ${INJECT_DELAY_MS}ms"
echo "Timeout:           ${TIMEOUT_MS}ms"
if [ -n "$REMOTE_IP" ]; then
    echo "Remote (Set side): $REMOTE_IP"
else
    echo "Mode: Single-node"
fi
echo "========================================"

# 基本检查
if [ ! -x "$TEST_BIN" ]; then
    echo "ERROR: $TEST_BIN not found or not executable"
    exit 1
fi
if ! command -v mlcd_inject_cli &>/dev/null; then
    echo "ERROR: mlcd_inject_cli not found in PATH"
    exit 1
fi

# 清理函数
clear_injections() {
    mlcd_inject_cli clearall >/dev/null 2>&1 || true
    if [ -n "$REMOTE_IP" ]; then
        ssh -o BatchMode=yes -o ConnectTimeout=5 \
            "$REMOTE_IP" "mlcd_inject_cli clearall" >/dev/null 2>&1 || true
    fi
}

# 场景定义：ID:名称
trap clear_injections EXIT

SCENARIOS=(
    "0:normal"
    "1:recv_delay"
    "2:recv_error"
    "3:send_delay"
    "4:send_error"
    "5:urma_delay"
    "6:urma_error"
    "7:cuda_delay"
    "8:cuda_error"
    "9:chain"
    "20:urma_recv_error"
    "21:send_first_chunk_error"
    "22:task_group_alloc_error"
)

PASSED=0
FAILED=0

for entry in "${SCENARIOS[@]}"; do
    IFS=':' read -r id name <<< "$entry"

    echo ""
    echo "========================================"
    echo "Scenario $id: $name"
    echo "========================================"
    clear_injections

    ARGS=("$LOCAL_IP")
    [ -n "$REMOTE_IP" ] && ARGS+=("--remote-worker=$REMOTE_IP")
    ARGS+=("--port=$PORT" "--scenario=$id" "--count=1" "--timeout=$TIMEOUT_MS")

    if [[ "$name" == *"delay"* ]] || [[ "$name" == *"chain"* ]]; then
        ARGS+=("--inject_delay_ms=$INJECT_DELAY_MS")
    fi

    if "$TEST_BIN" "${ARGS[@]}"; then
        echo "[PASS] Scenario $id ($name)"
        ((++PASSED))
    else
        echo "[FAIL] Scenario $id ($name)"
        ((++FAILED))
    fi
done

echo ""
echo "========================================"
echo "           Test Summary"
echo "========================================"
echo "Total : $((PASSED + FAILED))"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo "========================================"

exit $((FAILED > 0 ? 1 : 0))
