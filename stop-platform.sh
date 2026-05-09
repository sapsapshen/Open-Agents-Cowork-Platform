#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$ROOT/target/platform-runtime/processes.json"
RUNTIME_PORTS=(9000)

info()  { printf "\r  [ \033[00;34m..\033[0m ] %s\n" "$*"; }
ok()    { printf "\r  [ \033[00;32mOK\033[0m ] %s\n" "$*"; }
warn()  { printf "\r  [\033[00;33mWARN\033[0m] %s\n" "$*"; }

stopped=0

# ── 1. Kill by PID file ─────────────────────────────────
if [ -f "$PID_FILE" ]; then
  while IFS='' read -r name pid; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 0.3
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
      info "Stopped $name (PID $pid)."
      stopped=$((stopped + 1))
    fi
  done < <(python3 -c "
import json, sys
try:
    with open('$PID_FILE') as f:
        entries = json.load(f)
    if not isinstance(entries, list):
        entries = [entries]
    for e in entries:
        print(e.get('Name','?'), e.get('Id',0))
except Exception:
    pass
" 2>/dev/null)
  rm -f "$PID_FILE"
fi

# ── 2. Port-force-kill fallback ─────────────────────────
orphan_killed=0
for port in "${RUNTIME_PORTS[@]}"; do
  pids="$(lsof -ti "tcp:$port" 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    while IFS= read -r pid; do
      # Use non-empty trimmed PID
      pid="${pid//[[:space:]]/}"
      [ -z "$pid" ] && continue
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        sleep 0.3
        if kill -0 "$pid" 2>/dev/null; then
          kill -9 "$pid" 2>/dev/null || true
        fi
        orphan_killed=$((orphan_killed + 1))
      fi
    done <<< "$pids"
  fi
done

# ── 3. Report ──────────────────────────────────────────
if [ "$stopped" -eq 0 ] && [ "$orphan_killed" -eq 0 ]; then
  warn "No platform processes were running."
else
  [ "$stopped" -gt 0 ]      && ok "Stopped $stopped tracked process(es)."
  [ "$orphan_killed" -gt 0 ] && ok "Force-killed $orphan_killed orphan process(es) still holding platform ports."
fi
