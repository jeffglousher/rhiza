#!/usr/bin/env bash
# Concrete D1 adapter for the established disposable Hiqlite deployment owner.
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
run_id="${HIQLITE_STEADY_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)-$$}"
target="${HIQLITE_STEADY_TARGET_DIR:-$repo_root/target/hiqlite-steady/$run_id}"
concurrency="${HIQLITE_STEADY_CONCURRENCY:-1}"
case "$concurrency" in 1|4|16) ;; *) echo 'HIQLITE_STEADY_CONCURRENCY must be 1, 4, or 16' >&2; exit 1 ;; esac
mkdir -p "$target"; chmod 700 "$target"
HIQLITE_STEADY_MODE=1 HIQLITE_STEADY_CONCURRENCY="$concurrency" \
  HIQLITE_RECOVERY_TARGET_DIR="$target" HIQLITE_RECOVERY_CLUSTER="hiqlite-steady-$run_id" \
  "$repo_root/scripts/e2e-hiqlite-recovery.sh"
[ -f "$target/steady-summary.json" ] || { echo 'Hiqlite steady owner did not emit a summary' >&2; exit 1; }
cat "$target/steady-summary.json"
