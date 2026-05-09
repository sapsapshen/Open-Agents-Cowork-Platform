#!/usr/bin/env bash
set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ROOT="$ROOT/target/platform-runtime"
LOG_ROOT="$RUN_ROOT/logs"
PID_FILE="$RUN_ROOT/processes.json"
CONTROL_PLANE_URL="http://127.0.0.1:9000"
CONTROL_PLANE_PORT=9000

info()  { printf "\r  [ \033[00;34m..\033[0m ] %s\n" "$*"; }
ok()    { printf "\r  [ \033[00;32mOK\033[0m ] %s\n" "$*"; }
fail()  { printf "\r  [\033[0;31mFAIL\033[0m] %s\n" "$*"; exit 1; }

# ── helpers ──────────────────────────────────────────────

kill_platform_ports() {
  local pids
  pids="$(lsof -ti "tcp:$CONTROL_PLANE_PORT" 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 0.3
    for pid in $pids; do
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    done
  fi
  rm -f "$PID_FILE"
}

wait_http() {
  local url="$1" timeout="${2:-20}"
  local deadline
  deadline=$(date +%s)
  deadline=$((deadline + timeout))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if curl -sf -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

start_managed() {
  local name="$1" bin="$2" health_url="$3"
  local stdout="$LOG_ROOT/${name}.stdout.log"
  local stderr="$LOG_ROOT/${name}.stderr.log"
  shift 3

  "$bin" "$@" >"$stdout" 2>"$stderr" &
  local pid=$!
  sleep 0.7

  if ! kill -0 "$pid" 2>/dev/null; then
    fail "$name failed to start. See $stderr"
  fi

  if ! wait_http "$health_url" 20; then
    fail "$name health check timed out. See $stderr"
  fi

  echo "$name::$pid"
}

# ── main ─────────────────────────────────────────────────

mkdir -p "$LOG_ROOT"

info "Cleaning up any leftover processes from previous runs…"
kill_platform_ports

export PLATFORM_API_TOKEN="${PLATFORM_API_TOKEN:-local-review-token}"
export PLATFORM_ALLOWED_HOSTS="${PLATFORM_ALLOWED_HOSTS:-127.0.0.1,localhost}"
export RUST_LOG="${RUST_LOG:-info}"

info "Building workspace…"
cd "$ROOT"
cargo build --workspace
ok "Build complete."

BIN_ROOT="$ROOT/target/debug"
[ -x "$BIN_ROOT/control-plane" ] || fail "Missing binary: $BIN_ROOT/control-plane"

NAMES=()
PIDS=()

add_process() {
  local name="$1" pid="$2"
  NAMES+=("$name")
  PIDS+=("$pid")
}

info "Starting control-plane…"
read -r name_pid < <(start_managed "control-plane" "$BIN_ROOT/control-plane" \
  "$CONTROL_PLANE_URL/health" \
  --bind "127.0.0.1:$CONTROL_PLANE_PORT")
name_set="${name_pid%%::*}"
pid_val="${name_pid##*::}"
add_process "$name_set" "$pid_val"
unset name_set pid_val

# All processes are started and healthy.
# Now disable the cleanup trap: if the script exits from here on,
# processes should keep running in background.
trap - EXIT INT TERM

# Write PID file
{
  printf "["
  first=true
  for i in "${!NAMES[@]}"; do
    $first || printf ","
    first=false
    printf '{"Name":"%s","Id":%s,"StartedAtUtc":"%s"}' \
      "${NAMES[$i]}" "${PIDS[$i]}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  done
  printf "]\n"
} > "$PID_FILE"

# Wait for the control-plane web index
wait_http "$CONTROL_PLANE_URL/" 10

echo ""
echo "  Platform is running in background processes."
echo ""
echo "  Web endpoints:"
echo "    Dashboard       $CONTROL_PLANE_URL/dashboard"
echo "    Control Plane   $CONTROL_PLANE_URL/"
echo ""
echo "  Health:         $CONTROL_PLANE_URL/health"
echo "  Logs:           $LOG_ROOT"
echo "  Stop command:   ./stop-platform.sh"
echo ""
echo "  Agent Management:"
echo "    Open the Dashboard and navigate to Settings to add and launch AI agents."
echo ""
