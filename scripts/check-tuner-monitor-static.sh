#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
monitor="$repo_root/scripts/tuner-monitor.sh"

sleep() { return 42; }
export -f sleep

run_watch() {
  set +e
  WATCH_OUTPUT=$(TERM=xterm bash "$monitor" http://unused.example token --watch 2>&1)
  WATCH_STATUS=$?
  set -e
  [ "$WATCH_STATUS" -eq 42 ]
  grep -Fq 'Last updated:' <<<"$WATCH_OUTPUT"
}

# shellcheck disable=SC2329 # Invoked in the exported child-shell environment.
curl() {
  case "$*" in
    *membership/status*)
      printf '%s\n' '{"cluster_id":"c1","epoch":7,"node":"ready","members":["n1"],"execution_profile":"sql"}'
      ;;
    *tuner/metrics*)
      printf '%s\n' '{"total_samples":123,"is_fresh":true,"cold_start_gates_passed":true}'
      ;;
  esac
}
export -f curl
valid_output=$(bash "$monitor" http://unused.example token)
grep -Fq 'c1' <<<"$valid_output"
grep -Fq '123' <<<"$valid_output"
grep -Fq 'Tuner is active and learning' <<<"$valid_output"

# shellcheck disable=SC2329 # Invoked in the exported child-shell environment.
curl() {
  local argument
  for argument in "$@"; do
    case "$argument" in
      --fail|--fail-with-body) return 22 ;;
    esac
    if [[ "$argument" == -?* && "$argument" != --* && "${argument#-}" == *f* ]]; then
      return 22
    fi
  done
  case "$*" in
    *membership/status*) printf '%s\n' '{}' ;;
    *tuner/metrics*) printf '%s\n' '{"error":"tuner not configured"}' ;;
  esac
}
export -f curl
if curl -fsS http://unused.example/v1/admin/tuner/metrics >/dev/null; then
  echo 'curl failure flags were not modeled as an HTTP error' >&2
  exit 1
fi
unavailable_output=$(bash "$monitor" http://unused.example token)
grep -Fq 'Tuner not available: tuner not configured' <<<"$unavailable_output"
if grep -Fq 'Recommendations' <<<"$unavailable_output"; then
  echo 'unavailable tuner unexpectedly emitted recommendations' >&2
  exit 1
fi

# shellcheck disable=SC2329 # Invoked in the exported child-shell environment.
curl() { printf '%s\n' 'not-json'; }
export -f curl
run_watch
grep -Fq 'Invalid cluster status response' <<<"$WATCH_OUTPUT"
grep -Fq 'Invalid tuner metrics response' <<<"$WATCH_OUTPUT"

# shellcheck disable=SC2329 # Invoked in the exported child-shell environment.
curl() {
  case "$*" in
    *membership/status*|*tuner/metrics*) printf '%s\n' '{}' ;;
  esac
}
export -f curl
run_watch
grep -Fq 'Invalid tuner metrics response' <<<"$WATCH_OUTPUT"

# shellcheck disable=SC2329 # Invoked in the exported child-shell environment.
curl() {
  case "$*" in
    *membership/status*) printf '%s\n' '{}' ;;
    *tuner/metrics*) printf '%s\n' '{"error":""}' ;;
  esac
}
export -f curl
run_watch
grep -Fq 'Invalid tuner metrics response' <<<"$WATCH_OUTPUT"

# shellcheck disable=SC2329 # Invoked in the exported child-shell environment.
curl() {
  case "$*" in
    *membership/status*) printf '%s\n' '{}' ;;
    *tuner/metrics*) printf '%s\n' '{"total_samples":"oops","is_fresh":true,"cold_start_gates_passed":false}' ;;
  esac
}
export -f curl
run_watch
grep -Fq 'Invalid tuner metrics response' <<<"$WATCH_OUTPUT"

curl() { return 7; }
export -f curl
run_watch
grep -Fq 'Cannot fetch cluster status' <<<"$WATCH_OUTPUT"
grep -Fq 'Cannot fetch tuner metrics' <<<"$WATCH_OUTPUT"

echo 'tuner monitor static checks passed'
