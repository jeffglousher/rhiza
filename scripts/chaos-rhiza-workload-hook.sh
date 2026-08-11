#!/usr/bin/env bash
# Rhiza-only acknowledgement boundary probe for scripts/chaos-k8s.sh.
set -euo pipefail

die() { printf 'chaos-rhiza-hook: %s\n' "$*" >&2; exit 1; }
[ $# = 3 ] || die 'expected PHASE CUTPOINT OUTPUT_DIR'
phase="$1"; cutpoint="$2"; output_dir="$3"
case "$phase:$cutpoint" in prepare:pre-ack|verify:pre-ack|prepare:post-ack|verify:post-ack) ;; *) die 'invalid boundary' ;; esac
if [ -z "${CHAOS_K8S_CONTEXT:-}" ] || [ -z "${CHAOS_NAMESPACE:-}" ] || [ -z "${CHAOS_RUN_ID:-}" ]; then
  die 'missing exact cluster scope'
fi
[ "$(kubectl config current-context)" = "$CHAOS_K8S_CONTEXT" ] || die 'current context changed'

state="$output_dir/.rhiza-$cutpoint-state.json"
raw="$output_dir/rhiza-$cutpoint-$phase.raw"
selector="app.kubernetes.io/part-of=rhiza-chaos,chaos.rhiza.io/run=$CHAOS_RUN_ID,chaos.rhiza.io/role=voter"

ready_pod() {
  kubectl --context "$CHAOS_K8S_CONTEXT" -n "$CHAOS_NAMESPACE" get pods -l "$selector" -o json |
    jq -er '[.items[] | select(.status.phase=="Running" and any(.status.conditions[]?; .type=="Ready" and .status=="True")) | .metadata.name] | sort | .[0] // empty'
}

client() {
  local pod
  pod="$(ready_pod)" || die 'no ready Rhiza voter pod'
  kubectl --context "$CHAOS_K8S_CONTEXT" -n "$CHAOS_NAMESPACE" exec "$pod" -- rhiza "$@" --url http://127.0.0.1:8080
}

wait_for_recovered_quorum() {
  local key="$1" evidence="$2" attempt pods_json ready
  : > "$evidence"
  for attempt in $(seq 1 180); do
    if pods_json="$(kubectl --context "$CHAOS_K8S_CONTEXT" -n "$CHAOS_NAMESPACE" \
      get pods -l "$selector" -o json 2>> "$evidence")"; then
      ready="$(jq '[.items[] | select(.status.phase=="Running" and any(.status.conditions[]?; .type=="Ready" and .status=="True"))] | length' \
        <<< "$pods_json")"
    else
      ready=unavailable
    fi
    printf 'attempt=%s ready_voters=%s\n' "$attempt" "$ready" >> "$evidence"
    if [ "$ready" = 3 ] && client read --key "$key" --consistency read_barrier >> "$evidence" 2>&1 \
      && tail -n 1 "$evidence" | grep -q '^value=null '; then
      return 0
    fi
    sleep 1
  done
  die 'three-voter read-barrier convergence did not recover after fault removal'
}

if [ "$phase" = prepare ]; then
  id="$CHAOS_RUN_ID-$cutpoint-$(date -u +%Y%m%d%H%M%S)-$$"
  key="chaos-$id"
  value="verified-$id"
  if [ "$cutpoint" = pre-ack ]; then
    client read --key "$key" --consistency read_barrier > "$raw"
    grep -q '^value=null ' "$raw" || die 'fresh pre-ack key was unexpectedly present'
    durable=false
  else
    wait_for_recovered_quorum "$key" "$output_dir/rhiza-post-ack-recovery.raw"
    client write --request-id "$id" --key "$key" --value "$value" > "$raw"
    grep -q '^applied_index=' "$raw" || die 'write returned no acknowledgement'
    durable=true
  fi
  tmp="$state.tmp"
  jq -cn --arg id "$id" --arg key "$key" --arg value "$value" --argjson durable "$durable" \
    '{schema_version:1,id:$id,key:$key,value:$value,durable_ack_observed:$durable}' > "$tmp"
  chmod 600 "$tmp"; mv "$tmp" "$state"
else
  [ -f "$state" ] || die 'boundary state is missing'
  id="$(jq -er '.id' "$state")"; key="$(jq -er '.key' "$state")"; value="$(jq -er '.value' "$state")"
  if [ "$cutpoint" = pre-ack ]; then
    client read --key "$key" --consistency read_barrier > "$raw"
    grep -q '^value=null ' "$raw" || die 'unacknowledged value became visible'
    durable=false
  else
    client read --key "$key" --consistency read_barrier --expect "$value" > "$raw"
    grep -q '^value=' "$raw" || die 'acknowledged value was not readable'
    durable=true
  fi
fi

jq -cn --arg phase "$phase" --arg cutpoint "$cutpoint" --arg id "$id" --argjson durable "$durable" \
  '{schema_version:1,kind:"chaos-workload-boundary",phase:$phase,cutpoint:$cutpoint,fault_observed:($phase=="verify"),entries:[{system:"rhiza",id:$id,ack_kind:"consensus-and-proof-quorum",durable_ack_observed:$durable}]}'
