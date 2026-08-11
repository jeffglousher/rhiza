#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

grep -Fq 'http|tcp-postcard|tcp-postcard-rpc)' scripts/bench-vind.sh
grep -Fq 'RHIZA_RECORDER_TLS=on' scripts/bench-vind.sh
grep -Fq 'RHIZA_RECORDER_TLS_SECRET=rhiza-sql-c1-recorder-tls' scripts/bench-vind.sh
# vcluster may materialize image.tar.gz in its working directory.
# shellcheck disable=SC2016
grep -Fq '(cd "$target" && vcluster node load-image "$node" --image "$image")' scripts/bench-vind.sh
[ "$(grep -Fc 'vcluster node load-image' scripts/bench-vind.sh)" -eq 1 ] || {
  echo "every benchmark image load must run inside the target directory" >&2
  exit 1
}
grep -Fq 'recorder_tls_server_name:' scripts/bench-vind.sh
# shellcheck disable=SC2016 # Literal source check.
grep -Fq -- '--from-file=tls.key="$recorder_tls_dir/tls.key"' scripts/bench-vind.sh
grep -Fq 'recorder_tcp_addr:' scripts/bench-vind.sh
grep -Fq '":1F92[[:space:]]+[0-9A-F]+:[0-9A-F]+[[:space:]]+0A"' scripts/bench-vind.sh
grep -Fq '! grep -Eqi ":1F91' scripts/bench-vind.sh

# shellcheck disable=SC2016 # These are literal source checks.
grep -Fq 'peer_tokens="$(for _ in 1 2 3; do openssl rand -hex 24; done' scripts/bench-vind.sh
# shellcheck disable=SC2016 # These are literal jq source checks.
grep -Fq 'token:$tokens[$n]' scripts/bench-vind.sh
grep -Fq 'export RHIZA_S3_ENDPOINT=http://rustfs:9000 RHIZA_OBJECT_SECRET=rustfs-credentials' scripts/bench-vind.sh
grep -Fq 'export RHIZA_S3_ALLOW_HTTP=true' scripts/bench-vind.sh

# shellcheck disable=SC1091 # Repository-local source; callers run from repo root.
source scripts/bench-vind.sh

[ "$(resource_continuity_budget_seconds 2)" = 13 ]
[ "$(resource_coverage_wait_budget_seconds)" = 17 ]
grep -Fq 'timeout --kill-after=' scripts/bench-vind.sh

render_verified_provenance_json() {
  render_provenance_json "$@" 0 0 ok ok
}

declare -F endpoint_ready >/dev/null
(
  stall_port_file="$tmp/stall-port"
  python3 -c 'import socket,sys,time; s=socket.socket(); s.bind(("127.0.0.1",0)); s.listen(); open(sys.argv[1],"w").write(str(s.getsockname()[1])); c,_=s.accept(); time.sleep(10)' \
    "$stall_port_file" & stall_server_pid=$!
  trap 'kill "$stall_server_pid" 2>/dev/null || true; wait "$stall_server_pid" 2>/dev/null || true' EXIT
  for _ in $(seq 1 500); do [ -s "$stall_port_file" ] && break; sleep 0.01; done
  [ -s "$stall_port_file" ]
  if endpoint_ready "http://127.0.0.1:$(cat "$stall_port_file")"; then
    echo "stalled readiness probe was accepted" >&2
    exit 1
  fi
  kill -0 "$stall_server_pid"
)

: > "$tmp/empty-resources.jsonl"
if validate_resource_sample_schema "$tmp/empty-resources.jsonl"; then
  echo "empty resource evidence was accepted" >&2
  exit 1
fi
component_sample() {
  local pod="$1" container="$2" epoch="$3" cpu="${4:-$3}" identity="${5:-original}"
  local restart_count="${6:-0}"
  local collection_batch="${7:-$epoch}"
  local app=simulator
  [ "$container" != rhiza ] || app=rhiza
  jq -cn --arg app "$app" --arg pod "$pod" --arg container "$container" \
    --arg identity "$identity" --argjson epoch "$epoch" --argjson cpu "$cpu" \
    --argjson restart_count "$restart_count" --argjson collection_batch "$collection_batch" \
    '{timestamp:"2026-07-13T00:00:00Z",timestamp_epoch_seconds:$epoch,
      collection_batch:$collection_batch,source:"containerd_cri_stats",app:$app,pod:$pod,
      pod_uid:($pod + "-uid-" + $identity),container:$container,
      container_id:($container + "-id-" + $identity),restart_count:$restart_count,
      cpu_usage_usec:$cpu,memory_bytes:2}'
}
resource_cycle() {
  local epoch="$1" omit="${2:-}" meter="${3:-false}" cpu="${4:-$1}" pod
  local collection_batch="${5:-$epoch}"
  for pod in rhiza-sql-c1-0 rhiza-sql-c1-1 rhiza-sql-c1-2; do
    [ "$pod" = "$omit" ] || component_sample "$pod" rhiza "$epoch" "$cpu" original 0 \
      "$collection_batch"
  done
  [ "$omit" = rustfs ] || component_sample rustfs-abc rustfs "$epoch" "$cpu" original 0 \
    "$collection_batch"
  if [ "$meter" = true ] && [ "$omit" != object-meter ]; then
    component_sample rustfs-abc object-meter "$epoch" "$cpu" original 0 "$collection_batch"
  fi
}

# A collection started before the boundary may return counters gathered after it.
# The runtime-provided CRI timestamps, not collector invocation time, classify them.
jq -cn '{stats:[{attributes:{id:"container-id",metadata:{name:"rhiza"},
    labels:{"io.kubernetes.pod.namespace":"bench",
      "io.kubernetes.pod.name":"rhiza-sql-c1-0",
      "io.kubernetes.pod.uid":"pod-uid"},
    annotations:{"io.kubernetes.container.restartCount":"0"}},
  cpu:{timestamp:"121250000000",usageCoreNanoSeconds:{value:"1000"}},
  memory:{timestamp:"121250000000",workingSetBytes:{value:"2"}}}]}' |
  resource_samples_from_cri_stats bench 1 > "$tmp/delayed-collector.jsonl"
jq -cn '{stats:[{attributes:{id:"container-id",metadata:{name:"rhiza"},
    labels:{"io.kubernetes.pod.namespace":"bench",
      "io.kubernetes.pod.name":"rhiza-sql-c1-0",
      "io.kubernetes.pod.uid":"pod-uid"},
    annotations:{"io.kubernetes.container.restartCount":"0"}},
  cpu:{timestamp:"130000000000",usageCoreNanoSeconds:{value:"3000"}},
  memory:{timestamp:"130000000000",workingSetBytes:{value:"2"}}}]}' |
  resource_samples_from_cri_stats bench 2 >> "$tmp/delayed-collector.jsonl"
jq -cn '{stats:[{attributes:{id:"pre-container-id",metadata:{name:"rhiza"},
    labels:{"io.kubernetes.pod.namespace":"bench",
      "io.kubernetes.pod.name":"rhiza-sql-c1-0",
      "io.kubernetes.pod.uid":"pre-pod-uid"},
    annotations:{"io.kubernetes.container.restartCount":"0"}},
  cpu:{timestamp:"119000000000",usageCoreNanoSeconds:{value:"0"}},
  memory:{timestamp:"119000000000",workingSetBytes:{value:"2"}}}]}' |
  resource_samples_from_cri_stats bench 0 >> "$tmp/delayed-collector.jsonl"
jq -e '.timestamp_epoch_seconds == 121.25' \
  <(head -1 "$tmp/delayed-collector.jsonl") >/dev/null
summarize_resource_samples "$tmp/delayed-collector.jsonl" 120 129 \
  > "$tmp/delayed-collector-summary.json"
jq -e '.container_cpu_usage_usec_deltas[0] |
  .baseline == "born_in_window" and .delta_usec == 3' \
  "$tmp/delayed-collector-summary.json" >/dev/null
if jq -cn '{stats:[{attributes:{metadata:{name:"rhiza"},
    labels:{"io.kubernetes.pod.namespace":"bench",
      "io.kubernetes.pod.name":"rhiza-sql-c1-0"}},
  cpu:{timestamp:0},memory:{timestamp:1}}]}' |
  resource_samples_from_cri_stats bench 1 >/dev/null 2>&1; then
  echo "resource sample without positive CRI timestamps was accepted" >&2
  exit 1
fi

# CRI timestamps may be staggered within one stats response. Memory remains one
# collection snapshot and must be summed by collection batch, not timestamp.
{
  for pod in rhiza-sql-c1-0 rhiza-sql-c1-1 rhiza-sql-c1-2; do
    component_sample "$pod" rhiza 140 140 original 0 9 |
      jq -c 'if .pod == "rhiza-sql-c1-0" then .memory_bytes = 10
        elif .pod == "rhiza-sql-c1-1" then .memory_bytes = 20
        else .memory_bytes = 30 end'
  done
  component_sample rhiza-sql-c1-0 rhiza 150 150 original 0 10 |
    jq -c '.memory_bytes = 10'
  component_sample rhiza-sql-c1-1 rhiza 151 151 original 0 10 |
    jq -c '.memory_bytes = 20'
  component_sample rhiza-sql-c1-2 rhiza 152 152 original 0 10 |
    jq -c '.memory_bytes = 30'
  for pod in rhiza-sql-c1-0 rhiza-sql-c1-1 rhiza-sql-c1-2; do
    component_sample "$pod" rhiza 160 160 original 0 11 |
      jq -c 'if .pod == "rhiza-sql-c1-0" then .memory_bytes = 10
        elif .pod == "rhiza-sql-c1-1" then .memory_bytes = 20
        else .memory_bytes = 30 end'
  done
} > "$tmp/staggered-memory-resources.jsonl"
summarize_resource_samples "$tmp/staggered-memory-resources.jsonl" 149 153 \
  > "$tmp/staggered-memory-summary.json"
jq -e '.apps[] | select(.app == "rhiza") |
  .memory_samples == 3 and .average_memory_bytes == 60 and .peak_memory_bytes == 60' \
  "$tmp/staggered-memory-summary.json" >/dev/null

# Selecting the measurement window per container can split every complete CRI
# response when valid timestamp skew crosses both boundaries. Memory evidence
# must instead retain complete collection batches.
{
  for batch in 1 2 3 4; do
    case "$batch" in
      1) first=100; later=100 ;;
      2) first=118; later=122 ;;
      3) first=119; later=123 ;;
      4) first=140; later=140 ;;
    esac
    component_sample rhiza-sql-c1-0 rhiza "$first" "$first" original 0 "$batch"
    component_sample rhiza-sql-c1-1 rhiza "$later" "$later" original 0 "$batch"
    component_sample rhiza-sql-c1-2 rhiza "$later" "$later" original 0 "$batch"
    component_sample rustfs-abc rustfs "$first" "$first" original 0 "$batch"
  done
} > "$tmp/batch-window-skew-resources.jsonl"
validate_resource_samples "$tmp/batch-window-skew-resources.jsonl" 120 121 50 0
summarize_resource_samples "$tmp/batch-window-skew-resources.jsonl" 120 121 \
  > "$tmp/batch-window-skew-summary.json"
jq -e '.apps[] | select(.app == "rhiza") |
  .memory_samples == 4 and .average_memory_bytes == 6 and .peak_memory_bytes == 6' \
  "$tmp/batch-window-skew-summary.json" >/dev/null
jq -c 'select(.collection_batch != 4)' "$tmp/batch-window-skew-resources.jsonl" \
  > "$tmp/unbracketed-memory-resources.jsonl"
if summarize_resource_samples "$tmp/unbracketed-memory-resources.jsonl" 120 121 \
  >/dev/null 2>&1; then
  echo "memory evidence without a complete successor batch was accepted" >&2
  exit 1
fi

{
  for epoch in 118 124 130 136 142; do
    resource_cycle "$epoch" "" false "$epoch" | jq -c \
      'if .app == "rhiza" then .memory_bytes = 10 else . end'
  done
} > "$tmp/complete-memory-batches.jsonl"
jq -c 'select(.collection_batch != 130 or .pod != "rhiza-sql-c1-2")' \
  "$tmp/complete-memory-batches.jsonl" > "$tmp/partial-memory-batch.jsonl"
if validate_resource_samples "$tmp/partial-memory-batch.jsonl" 120 140 2 0; then
  echo "incomplete non-fault collection batch was accepted" >&2
  exit 1
fi
summarize_resource_samples "$tmp/partial-memory-batch.jsonl" 120 140 \
  > "$tmp/partial-memory-summary.json"
jq -e '.apps[] | select(.app == "rhiza") |
  .memory_samples == 4 and .average_memory_bytes == 30 and .peak_memory_bytes == 30' \
  "$tmp/partial-memory-summary.json" >/dev/null

resource_cycle 150 > "$tmp/valid-collection-batch.jsonl"
jq -c 'del(.collection_batch)' "$tmp/valid-collection-batch.jsonl" \
  > "$tmp/missing-collection-batch.jsonl"
if validate_resource_sample_schema "$tmp/missing-collection-batch.jsonl" 0; then
  echo "resource evidence without a collection batch was accepted" >&2
  exit 1
fi
jq -c '.collection_batch = 1.5' "$tmp/valid-collection-batch.jsonl" \
  > "$tmp/invalid-collection-batch.jsonl"
if validate_resource_sample_schema "$tmp/invalid-collection-batch.jsonl" 0; then
  echo "resource evidence with an invalid collection batch was accepted" >&2
  exit 1
fi
{
  resource_cycle 150 "" false 150 10
  resource_cycle 151 "" false 151 10
} > "$tmp/reused-collection-batch.jsonl"
if validate_resource_sample_schema "$tmp/reused-collection-batch.jsonl" 0; then
  echo "separate collections sharing a batch identity were accepted" >&2
  exit 1
fi
gap_fixture() {
  local file="$1" omitted="$2" epoch
  {
    resource_cycle 118
    resource_cycle 124
    resource_cycle 130
    for epoch in $(seq 136 6 184); do resource_cycle "$epoch" "$omitted"; done
    for epoch in 190 196 202; do
      resource_cycle "$epoch" "$omitted"
      component_sample "$omitted" rhiza "$epoch" "$epoch" replacement
    done
  } > "$file"
}
component_sample rhiza-sql-c1-0 rhiza 100 > "$tmp/resources.jsonl"
if validate_resource_sample_schema "$tmp/resources.jsonl" 0; then
  echo "resource evidence without simulator samples was accepted" >&2
  exit 1
fi
component_sample rustfs-abc rustfs 100 >> "$tmp/resources.jsonl"
if validate_resource_samples "$tmp/resources.jsonl" 120 200 2; then
  echo "single resource sample per app was accepted" >&2
  exit 1
fi
cp "$tmp/resources.jsonl" "$tmp/stale-resources.jsonl"
component_sample rhiza-sql-c1-0 rhiza 150 >> "$tmp/stale-resources.jsonl"
component_sample rustfs-abc rustfs 150 >> "$tmp/stale-resources.jsonl"
if validate_resource_samples "$tmp/stale-resources.jsonl" 120 200 2; then
  echo "resource evidence that ends before measurement coverage was accepted" >&2
  exit 1
fi
{
  resource_cycle 150
  resource_cycle 199
} > "$tmp/late-resources.jsonl"
if validate_resource_samples "$tmp/late-resources.jsonl" 120 200 2 0; then
  echo "resource evidence that starts after measurement was accepted" >&2
  exit 1
fi
{
  resource_cycle 118
  resource_cycle 122
  resource_cycle 198
  resource_cycle 202
} > "$tmp/gapped-resources.jsonl"
if validate_resource_samples "$tmp/gapped-resources.jsonl" 120 200 2 0; then
  echo "resource evidence with a measurement outage was accepted" >&2
  exit 1
fi
component_sample rhiza-sql-c1-0 rhiza 201 >> "$tmp/resources.jsonl"
component_sample rustfs-abc rustfs 201 >> "$tmp/resources.jsonl"
{
  for epoch in $(seq 118 6 202); do
    resource_cycle "$epoch"
  done
} > "$tmp/jittered-resources.jsonl"
validate_resource_sample_schema "$tmp/jittered-resources.jsonl" 0
validate_resource_samples "$tmp/jittered-resources.jsonl" 120 200 2 0

cp "$tmp/jittered-resources.jsonl" "$tmp/unexpected-restart-resources.jsonl"
jq -c 'if .pod == "rhiza-sql-c1-2" and .timestamp_epoch_seconds >= 150
  then .restart_count = 1 else . end' "$tmp/unexpected-restart-resources.jsonl" \
  > "$tmp/unexpected-restart-resources.tmp"
mv "$tmp/unexpected-restart-resources.tmp" "$tmp/unexpected-restart-resources.jsonl"
if validate_resource_samples "$tmp/unexpected-restart-resources.jsonl" 120 200 2 0; then
  echo "unexpected restart in a no-fault run was accepted" >&2
  exit 1
fi

cp "$tmp/jittered-resources.jsonl" "$tmp/non-fault-identity-resources.jsonl"
jq -c 'if .pod == "rhiza-sql-c1-2" and .timestamp_epoch_seconds >= 150
  then .pod_uid = "unexpected-uid" | .container_id = "unexpected-container"
  else . end' "$tmp/non-fault-identity-resources.jsonl" \
  > "$tmp/non-fault-identity-resources.tmp"
mv "$tmp/non-fault-identity-resources.tmp" "$tmp/non-fault-identity-resources.jsonl"
if validate_resource_samples "$tmp/non-fault-identity-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185; then
  echo "non-fault component identity transition was accepted" >&2
  exit 1
fi

if validate_resource_samples "$tmp/jittered-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185; then
  echo "fault evidence without the required identity transition was accepted" >&2
  exit 1
fi
if validate_resource_samples "$tmp/jittered-resources.jsonl" 120 200 2 0 \
  rustfs-abc 135 185; then
  echo "non-rhiza fault component was accepted" >&2
  exit 1
fi

{
  resource_cycle 100
  for epoch in $(seq 122 6 200); do
    resource_cycle "$epoch"
  done
  resource_cycle 202
} > "$tmp/stale-boundary-resources.jsonl"
if validate_resource_samples "$tmp/stale-boundary-resources.jsonl" 120 200 2 0; then
  echo "resource evidence with a stale measurement boundary was accepted" >&2
  exit 1
fi

{
  resource_cycle 0 "" false 999999
  resource_cycle 100
  resource_cycle 150
  resource_cycle 200
  resource_cycle 999 "" false 999999
} > "$tmp/window-resources.jsonl"
validate_resource_samples "$tmp/window-resources.jsonl" 120 190 50 0
summarize_resource_samples "$tmp/window-resources.jsonl" 120 190 > "$tmp/resource-summary.json"
jq -e '.samples == 12 and
  ([.apps[] | {key:.app,value:.peak_memory_bytes}] | from_entries) ==
    {rhiza:6,simulator:2} and
  ([.apps[] | {key:.app,value:.average_memory_bytes}] | from_entries) ==
    {rhiza:6,simulator:2} and
  ([.apps[] | {key:.app,value:.cpu_usage_usec}] | from_entries) ==
    {rhiza:300,simulator:100}' \
  "$tmp/resource-summary.json" >/dev/null

gap_fixture "$tmp/fault-gap-resources.jsonl" rhiza-sql-c1-1
validate_resource_samples "$tmp/fault-gap-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185
summarize_resource_samples "$tmp/fault-gap-resources.jsonl" 120 200 \
  > "$tmp/fault-gap-summary.json"
jq -e '.apps[] | select(.app == "rhiza") |
  .memory_samples == 6 and .average_memory_bytes == 6 and .peak_memory_bytes == 6' \
  "$tmp/fault-gap-summary.json" >/dev/null
jq -c 'select(.pod != "rhiza-sql-c1-1" or .timestamp_epoch_seconds != 130)' \
  "$tmp/fault-gap-resources.jsonl" > "$tmp/outside-fault-window-gap-resources.jsonl"
if validate_resource_samples "$tmp/outside-fault-window-gap-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185; then
  echo "incomplete fault batch outside the verified fault window was accepted" >&2
  exit 1
fi
cp "$tmp/fault-gap-resources.jsonl" "$tmp/fault-with-other-restart-resources.jsonl"
jq -c 'if .pod == "rhiza-sql-c1-2" and .timestamp_epoch_seconds >= 150
  then .restart_count = 1 else . end' "$tmp/fault-with-other-restart-resources.jsonl" \
  > "$tmp/fault-with-other-restart-resources.tmp"
mv "$tmp/fault-with-other-restart-resources.tmp" "$tmp/fault-with-other-restart-resources.jsonl"
if validate_resource_samples "$tmp/fault-with-other-restart-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185; then
  echo "restart of a non-fault component was accepted" >&2
  exit 1
fi
{
  for epoch in $(seq 118 6 160); do resource_cycle "$epoch"; done
  for epoch in $(seq 166 6 184); do resource_cycle "$epoch" rhiza-sql-c1-1; done
  for epoch in 190 196 202; do
    resource_cycle "$epoch" rhiza-sql-c1-1
    component_sample rhiza-sql-c1-1 rhiza "$epoch" "$epoch" replacement
  done
} > "$tmp/fault-outlives-measurement-resources.jsonl"
validate_resource_samples "$tmp/fault-outlives-measurement-resources.jsonl" 120 160 2 0 \
  rhiza-sql-c1-1 135 185
sed 's/-replacement"/-original"/g' "$tmp/fault-gap-resources.jsonl" \
  > "$tmp/fault-omission-resources.jsonl"
if validate_resource_samples "$tmp/fault-omission-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185; then
  echo "collection omission was mistaken for a pod replacement" >&2
  exit 1
fi
if validate_resource_samples "$tmp/fault-gap-resources.jsonl" 120 200 2 0 "" "" ""; then
  echo "pod absence without an explicit fault window was accepted" >&2
  exit 1
fi
gap_fixture "$tmp/non-fault-gap-resources.jsonl" rhiza-sql-c1-2
if validate_resource_samples "$tmp/non-fault-gap-resources.jsonl" 120 200 2 0 \
  rhiza-sql-c1-1 135 185; then
  echo "collection omission for a non-faulted pod was accepted" >&2
  exit 1
fi

{
  resource_cycle 118 rhiza-sql-c1-2
  resource_cycle 124 rhiza-sql-c1-2
  resource_cycle 202 rhiza-sql-c1-2
} > "$tmp/missing-component-resources.jsonl"
if validate_resource_sample_schema "$tmp/missing-component-resources.jsonl" 0; then
  echo "resource evidence without every rhiza ordinal was accepted" >&2
  exit 1
fi
if validate_resource_samples "$tmp/jittered-resources.jsonl" 120 200 2 1 "" "" ""; then
  echo "resource evidence without the enabled object meter was accepted" >&2
  exit 1
fi
cp "$tmp/window-resources.jsonl" "$tmp/meter-resources.jsonl"
for epoch in 0 100 150 200 999; do
  component_sample rustfs-abc object-meter "$epoch" "$epoch" >> "$tmp/meter-resources.jsonl"
done
validate_resource_samples "$tmp/meter-resources.jsonl" 120 190 50 1 "" "" ""
{
  component_sample rhiza-sql-c1-0 rhiza 100 100 original
  component_sample rhiza-sql-c1-0 rhiza 150 130 original
  component_sample rhiza-sql-c1-0 rhiza 170 20 replacement
  component_sample rhiza-sql-c1-0 rhiza 200 50 replacement
  for epoch in 100 150 200; do
    component_sample rhiza-sql-c1-1 rhiza "$epoch" "$epoch"
    component_sample rhiza-sql-c1-2 rhiza "$epoch" "$epoch"
    component_sample rustfs-abc rustfs "$epoch" "$epoch"
  done
} > "$tmp/lifecycle-resources.jsonl"
summarize_resource_samples "$tmp/lifecycle-resources.jsonl" 120 190 \
  > "$tmp/lifecycle-summary.json"
jq -e '
  ([.container_cpu_usage_usec_deltas[] |
    select(.pod == "rhiza-sql-c1-0") | {key:.container_id,value:.delta_usec}] |
    from_entries) == {"rhiza-id-original":30,"rhiza-id-replacement":50}
' "$tmp/lifecycle-summary.json" >/dev/null

component_sample rhiza-sql-c1-2 rhiza 180 100 >> "$tmp/lifecycle-resources.jsonl"
if summarize_resource_samples "$tmp/lifecycle-resources.jsonl" 120 190 \
  > "$tmp/regressed-counter-summary.json" 2>/dev/null; then
  echo "same-container CPU counter regression was accepted" >&2
  exit 1
fi

: > "$tmp/empty-access.jsonl"
printf '%s\n' '{"metering":{"enabled":true,"status":"failed","requests":0},"retained":{"status":"ok","object_count":0,"retained_bytes":0}}' > "$tmp/invalid-meter.json"
if validate_object_evidence "$tmp/missing-access.jsonl" "$tmp/invalid-meter.json"; then
  echo "unreadable object meter output was accepted" >&2
  exit 1
fi
printf '%s\n' '{"metering":{"enabled":true,"status":"ok","requests":0},"retained":{"status":"ok","object_count":0,"retained_bytes":0}}' > "$tmp/empty-usage.json"
validate_object_evidence "$tmp/empty-access.jsonl" "$tmp/empty-usage.json"
printf '%s\n' '{"metering":{"enabled":true,"status":"ok","requests":0},"retained":{"status":"failed","object_count":null,"retained_bytes":null}}' > "$tmp/invalid-usage.json"
if validate_object_evidence "$tmp/empty-access.jsonl" "$tmp/invalid-usage.json"; then
  echo "null object inventory was accepted" >&2
  exit 1
fi
printf '%s\n' '{"method":"PUT","status":"200","request_bytes":"1","response_bytes":"2"}' > "$tmp/access.jsonl"
printf '%s\n' '{"metering":{"enabled":true,"status":"ok","requests":1},"retained":{"status":"ok","object_count":1,"retained_bytes":2}}' > "$tmp/usage.json"
validate_object_evidence "$tmp/access.jsonl" "$tmp/usage.json"
[ "$(evidence_exit_status 0 failed ok)" = 1 ]
[ "$(evidence_exit_status 7 ok ok)" = 7 ]
[ "$(evidence_exit_status 0 ok ok failed)" = 1 ]
failed_evidence="$(render_evidence_json failed ok true true)"
jq -e '.status == "failed" and .resource_sampling.status == "failed" and
  .object_metering.status == "ok"' <<< "$failed_evidence" >/dev/null
jq -e '.status == "disabled" and
  (.resource_sampling.enabled | not) and (.object_metering.enabled | not)' \
  <<< "$(render_evidence_json disabled disabled false false)" >/dev/null
[ "$(cleanup_outcome 0 1)" = ok ]
[ "$(cleanup_outcome 1 1)" = failed ]
[ "$(cleanup_outcome 0 0)" = failed ]
jq -e '.status == "failed" and (.cleaned_up | not)' \
  <<< "$(render_cleanup_json failed failed ok)" >/dev/null
jq -e '(.requested | not) and (.cleaned_up | not) and .namespace == "retained"' \
  <<< "$(render_cleanup_json skipped retained retained)" >/dev/null
jq -e '.started_at_epoch_seconds == 120 and .finished_at_epoch_seconds == 200' \
  <<< "$(render_measurement_window_json 120 200)" >/dev/null
printf '%s\n' '{"measurement":{"measurement_window":{"started_at_epoch_seconds":120.25,"finished_at_epoch_seconds":200.75}}}' > "$tmp/benchmark-report.json"
jq -e '.started_at_epoch_seconds == 120.25 and .finished_at_epoch_seconds == 200.75' \
  <<< "$(measurement_window_from_report "$tmp/benchmark-report.json")" >/dev/null
printf '%s\n' '{"fault":{"tag":"pod-delete","command_completed":true,"command_start_offset_seconds":10.5,"command_elapsed_seconds":20.25}}' > "$tmp/fault-report.json"
jq -e '.started_at_epoch_seconds == 130.5 and .finished_at_epoch_seconds == 150.75' \
  <<< "$(resource_fault_window_from_report "$tmp/fault-report.json" 120)" >/dev/null

image_digest="sha256:$(printf 'a%.0s' {1..64})"
image_id="docker-pullable://rhiza@${image_digest}"
repo_digest="example/rhiza@sha256:$(printf 'b%.0s' {1..64})"
source_commit="$(printf 'c%.0s' {1..40})"
client_sha256="$(printf 'd%.0s' {1..64})"
rustc_vv=$'rustc 1.90.0\nbinary: rustc\ncommit-hash: fixture'
cargo_version='cargo 1.90.0 (fixture)'
inspect="$(jq -cn --arg id "$image_id" --arg digest "$repo_digest" '[{Id:$id,RepoDigests:[$digest]}]')"
matching_inspect="$(jq -cn --arg id "$image_id" --arg digest "$repo_digest" --arg revision "$source_commit" \
  '[{Id:$id,RepoDigests:[$digest],Config:{Labels:{"org.opencontainers.image.revision":$revision}}}]')"
runtime_images="$(jq -cn \
  --arg rhiza "containerd://${image_digest}" \
  --arg rustfs "docker-pullable://rustfs@sha256:$(printf '2%.0s' {1..64})" \
  --arg meter "containerd://sha256:$(printf '3%.0s' {1..64})" \
  --arg inventory "docker-pullable://aws@sha256:$(printf '4%.0s' {1..64})" \
  '{rhiza:[$rhiza,$rhiza,$rhiza],rustfs:[$rustfs],
    object_meter:[$meter],aws_cli_inventory:[$inventory]}')"
pod_status_fixture="$(jq -cn --argjson identities "$runtime_images" '{items:[
  {metadata:{labels:{"app.kubernetes.io/name":"rhiza"}},status:{containerStatuses:[
    {name:"rhiza",imageID:$identities.rhiza[0]}]}},
  {metadata:{labels:{"app.kubernetes.io/name":"rhiza"}},status:{containerStatuses:[
    {name:"rhiza",imageID:$identities.rhiza[1]}]}},
  {metadata:{labels:{"app.kubernetes.io/name":"rhiza"}},status:{containerStatuses:[
    {name:"rhiza",imageID:$identities.rhiza[2]}]}},
  {metadata:{labels:{"app.kubernetes.io/name":"rustfs"}},status:{containerStatuses:[
    {name:"rustfs",imageID:$identities.rustfs[0]},
    {name:"object-meter",imageID:$identities.object_meter[0]}]}}
]}')"
observed_runtime="$(printf '%s\n' "$pod_status_fixture" | runtime_image_ids_from_pods)"
printf '%s\n' "$observed_runtime" | jq -e --argjson expected "$runtime_images" '
  .rhiza == $expected.rhiza and .rustfs == $expected.rustfs and
  .object_meter == $expected.object_meter and .aws_cli_inventory == []
' >/dev/null
built_provenance="$(render_verified_provenance_json "$source_commit" false built rhiza:dev "$inspect" \
  "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
printf '%s\n' "$built_provenance" | jq -e --arg id "$image_digest" --arg digest "$repo_digest" '
  .publishable == true and .source.dirty == false and .image.build_mode == "built" and
  .image.content_id == $id and .image.repo_digests == [$digest] and .reasons == [] and
  (.execution.benchmark_client.sha256 | test("^[0-9a-f]{64}$")) and
  (.execution.toolchain.rustc_vv | startswith("rustc ")) and
  (.execution.toolchain.cargo_version | startswith("cargo ")) and
  all(.execution.runtime_images[]; .status == "verified" and (.image_digests | length) == 1)
' >/dev/null
failed_run_provenance="$(render_provenance_json "$source_commit" false built rhiza:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true \
  9 9 failed failed)"
printf '%s\n' "$failed_run_provenance" | jq -e '
  .publishable == false and
  (["benchmark_failed","run_failed","evidence_failed","cleanup_failed"] - .reasons == [])
' >/dev/null
kept_run_provenance="$(render_provenance_json "$source_commit" false built rhiza:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true \
  0 0 ok skipped)"
printf '%s\n' "$kept_run_provenance" | jq -e '
  .publishable == false and (.reasons | index("cleanup_not_verified") != null)
' >/dev/null
heterogeneous_runtime="$(printf '%s\n' "$runtime_images" | jq -c \
  --arg image "containerd://sha256:$(printf '5%.0s' {1..64})" '.rhiza[2] = $image')"
heterogeneous_provenance="$(render_verified_provenance_json "$source_commit" false built rhiza:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$heterogeneous_runtime" true)"
printf '%s\n' "$heterogeneous_provenance" | jq -e '
  .publishable == false and (.reasons | index("heterogeneous_rhiza_runtime_images") != null)
' >/dev/null
short_runtime="$(printf '%s\n' "$runtime_images" | jq -c '.rhiza = .rhiza[:2]')"
short_provenance="$(render_verified_provenance_json "$source_commit" false built rhiza:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$short_runtime" true)"
printf '%s\n' "$short_provenance" | jq -e '
  .publishable == false and (.reasons | index("unexpected_rhiza_runtime_image_count") != null)
' >/dev/null
mismatched_image="containerd://sha256:$(printf '6%.0s' {1..64})"
mismatched_runtime="$(printf '%s\n' "$runtime_images" | jq -c --arg image "$mismatched_image" \
  '.rhiza = [$image,$image,$image]')"
mismatched_provenance="$(render_verified_provenance_json "$source_commit" false built rhiza:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$mismatched_runtime" true)"
printf '%s\n' "$mismatched_provenance" | jq -e '
  .publishable == false and (.reasons | index("rhiza_runtime_image_mismatch") != null)
' >/dev/null
matching_provenance="$(render_verified_provenance_json "$source_commit" false skip-build rhiza:dev \
  "$matching_inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
printf '%s\n' "$matching_provenance" | jq -e --arg commit "$source_commit" '
  .publishable == true and .image.source_revision == $commit and .reasons == []
' >/dev/null
for revision in missing mismatch; do
  if [ "$revision" = missing ]; then candidate="$inspect"
  else candidate="$(jq -cn --arg id "$image_id" --arg revision deadbeef \
    '[{Id:$id,RepoDigests:[],Config:{Labels:{"org.opencontainers.image.revision":$revision}}}]')"
  fi
  unverified="$(render_verified_provenance_json "$source_commit" false skip-build rhiza:dev "$candidate" \
    "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
  printf '%s\n' "$unverified" | jq -e \
    '.publishable == false and .reasons == ["unverified_image_source"]' >/dev/null
done
for identity in benchmark_client rustc cargo; do
  identity_sha="$client_sha256" identity_rustc="$rustc_vv" identity_cargo="$cargo_version"
  case "$identity" in
    benchmark_client) identity_sha="" ;;
    rustc) identity_rustc="" ;;
    cargo) identity_cargo="" ;;
  esac
  missing_identity="$(render_verified_provenance_json "$source_commit" false built rhiza:dev "$inspect" \
    "$identity_sha" "$identity_rustc" "$identity_cargo" "$runtime_images" true)"
  reason="missing_or_invalid_${identity}_$( [ "$identity" = benchmark_client ] && printf sha256 || printf version )"
  printf '%s\n' "$missing_identity" | jq -e --arg reason "$reason" \
    '.publishable == false and (.reasons | index($reason) != null)' >/dev/null
done
for component in rhiza rustfs object_meter aws_cli_inventory; do
  for invalid in missing mutable; do
    if [ "$invalid" = missing ]; then value='[]'; else value='["latest"]'; fi
    invalid_runtime="$(printf '%s\n' "$runtime_images" | jq -c --arg component "$component" \
      --argjson value "$value" '.[$component] = $value')"
    invalid_provenance="$(render_verified_provenance_json "$source_commit" false built rhiza:dev \
      "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$invalid_runtime" true)"
    reason="missing_or_invalid_${component}_runtime_image"
    printf '%s\n' "$invalid_provenance" | jq -e --arg reason "$reason" \
      '.publishable == false and (.reasons | index($reason) != null)' >/dev/null
  done
done
disabled_runtime="$(printf '%s\n' "$runtime_images" | jq -c \
  '.object_meter = ["latest"] | .aws_cli_inventory = []')"
disabled_provenance="$(render_verified_provenance_json "$source_commit" false built rhiza:dev "$inspect" \
  "$client_sha256" "$rustc_vv" "$cargo_version" "$disabled_runtime" false)"
printf '%s\n' "$disabled_provenance" | jq -e '
  .publishable == true and .execution.runtime_images.object_meter.status == "not_applicable" and
  .execution.runtime_images.aws_cli_inventory.status == "not_applicable"
' >/dev/null
dirty_provenance="$(render_verified_provenance_json "$source_commit" true built rhiza:dev "$inspect" \
  "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
printf '%s\n' "$dirty_provenance" | jq -e \
  '.publishable == false and (.reasons | index("dirty_source") != null)' >/dev/null
missing_provenance="$(render_verified_provenance_json "$source_commit" false skip-build rhiza:dev \
  '[{"Id":"","RepoDigests":[]}]' "$client_sha256" "$rustc_vv" "$cargo_version" \
  "$runtime_images" true)"
printf '%s\n' "$missing_provenance" | jq -e \
  '.publishable == false and (.reasons | index("missing_immutable_image_identity") != null)' >/dev/null

build_line="$(grep -n 'cargo build --release --locked --manifest-path bench/Cargo.toml --bin rhiza-bench' scripts/bench-vind.sh | cut -d: -f1)"
# shellcheck disable=SC2016 # Literal source-order check.
sample_line="$(grep -n ': > "$resources_jsonl"' scripts/bench-vind.sh | cut -d: -f1)"
meter_line="$(grep -n "k exec .*': > /var/log/nginx/s3-access.log'" scripts/bench-vind.sh | cut -d: -f1)"
[ -n "$build_line" ] && [ "$build_line" -lt "$sample_line" ] && [ "$build_line" -lt "$meter_line" ]
# shellcheck disable=SC2016 # Literal direct-execution check.
grep -Fq 'RHIZA_CLIENT_TOKEN="$client_token" "$bench_binary" "${bench_args[@]}"' scripts/bench-vind.sh
grep -Fq 'run: scripts/check-ci.sh' .github/workflows/ci.yml
grep -Fq 'run scripts/check-bench-vind-static.sh' scripts/check-ci.sh
grep -Fq 'YQ_VERSION: v4.47.2' .github/workflows/ci.yml

printf '%s\n' 'fixture port-forward failure' > "$tmp/port-forward-0.log"
if failure="$(
  # shellcheck disable=SC2034 # Read by assert_port_forward_alive from the sourced script.
  target="$tmp"
  # shellcheck disable=SC2034 # Read by assert_port_forward_alive from the sourced script.
  admin_endpoint_urls=(http://127.0.0.1:18080)
  false &
  port_forward_pids=("$!")
  while kill -0 "${port_forward_pids[0]}" 2>/dev/null; do :; done
  assert_port_forward_alive 0 2>&1
)"; then
  echo "dead port-forward was accepted" >&2
  exit 1
fi
grep -Fq 'port-forward exited with status' <<< "$failure"
grep -Fq 'http://127.0.0.1:18080' <<< "$failure"
grep -Fq 'fixture port-forward failure' <<< "$failure"

printf '%s\n' 'fixture non-target failure' > "$tmp/port-forward-1.log"
if failure="$(
  target="$tmp"
  admin_endpoint_urls=(http://127.0.0.1:18080 http://127.0.0.1:18081)
  sleep 30 & live_pid=$!
  trap 'kill "$live_pid" 2>/dev/null || true' EXIT
  false & dead_pid=$!
  port_forward_pids=("$live_pid" "$dead_pid")
  while kill -0 "$dead_pid" 2>/dev/null; do :; done
  assert_all_port_forwards_alive 2>&1
)"; then
  echo "dead non-target port-forward was accepted" >&2
  exit 1
fi
grep -Fq 'http://127.0.0.1:18081' <<< "$failure"
grep -Fq 'fixture non-target failure' <<< "$failure"

fake_bin="$tmp/fake-bin"
mkdir "$fake_bin"
# shellcheck disable=SC2016 # Written into the fake kubectl script verbatim.
printf '%s\n' '#!/usr/bin/env bash' \
  'printf "%s\n" "$*" >> "$KUBECTL_LOG"' \
  'case " $* " in' \
  '  *" get pod "*)' \
  '    if [ -n "${KUBECTL_REPLACEMENT_UID:-}" ] && [ -e "$KUBECTL_OLD_UID_SEEN" ]; then printf "%s" "$KUBECTL_REPLACEMENT_UID"; else : > "$KUBECTL_OLD_UID_SEEN"; printf old-pod-uid; fi ;;' \
  '  *" delete pod "*) [ "${KUBECTL_DELETE_FAIL:-1}" = 0 ] || exit 17 ;;' \
  '  *" wait "*) exit 0 ;;' \
  'esac' > "$fake_bin/kubectl"
chmod +x "$fake_bin/kubectl"
export KUBECTL_LOG="$tmp/kubectl.log"
export KUBECTL_OLD_UID_SEEN="$tmp/old-uid-seen"
failed_delete_command="$(build_pod_delete_fault_command fixture fixture rhiza-sql-c1-1 '' '' '')"
if PATH="$fake_bin:$PATH" sh -c "$failed_delete_command"; then
  echo "failed pod deletion was accepted" >&2
  exit 1
fi
grep -Fq 'delete pod rhiza-sql-c1-1' "$KUBECTL_LOG"
if grep -Fq ' wait ' "$KUBECTL_LOG"; then
  echo "replacement wait ran after failed pod deletion" >&2
  exit 1
fi
printf '%s\n' '#!/bin/sh' 'printf "1\n"' > "$fake_bin/seq"
printf '%s\n' '#!/bin/sh' 'exit 0' > "$fake_bin/sleep"
chmod +x "$fake_bin/seq" "$fake_bin/sleep"

checkpoint_fixture="$tmp/checkpoint-drain"
mkdir "$checkpoint_fixture"
(
  target="$checkpoint_fixture"
  checkpoint_drain_json="$checkpoint_fixture/result.json"
  admin_endpoint_urls=(http://127.0.0.1:18080 http://127.0.0.1:18081 http://127.0.0.1:18082)
  admin_token=fixture
  checkpoint_fixture_mode=stale
  seq() { printf '1\n'; }
  sleep() { :; }
  curl() {
    local url="${!#}" index=6
    if [ "$checkpoint_fixture_mode" = stale ] && [[ "$url" == *:18080/* ]]; then
      index=5
    fi
    jq -cn --argjson index "$index" \
      '{qlog_root:{index:$index,hash:[range(32)]},
        checkpoint_root:{index:$index,hash:[range(32)]}}'
  }
  if wait_for_checkpoint_drain; then
    echo "a stale endpoint with a locally matching checkpoint was accepted" >&2
    exit 1
  fi
  jq -e '.qlog_root == null and .checkpoint_root == null and
    (.endpoints | length) == 3' "$checkpoint_drain_json" >/dev/null

  checkpoint_fixture_mode=converged
  wait_for_checkpoint_drain
  jq -e '.qlog_root == {index:6,hash:[range(32)]} and
    .checkpoint_root == .qlog_root and (.endpoints | length) == 3 and
    all(.endpoints[]; .qlog_root == {index:6,hash:[range(32)]} and
      .checkpoint_root == .qlog_root)' "$checkpoint_drain_json" >/dev/null

  checkpoint_fixture_mode="invalid-hash"
  curl() {
    jq -cn '{qlog_root:{index:6,hash:"not-serde-log-hash"},
      checkpoint_root:{index:6,hash:"not-serde-log-hash"}}'
  }
  if wait_for_checkpoint_drain; then
    echo "checkpoint drain accepted a string hash" >&2
    exit 1
  fi

  curl() {
    jq -cn '{qlog_root:{index:6,hash:([range(31)] + [256])},
      checkpoint_root:{index:6,hash:([range(31)] + [256])}}'
  }
  if wait_for_checkpoint_drain; then
    echo "checkpoint drain accepted an invalid byte-array hash" >&2
    exit 1
  fi

  curl() {
    jq -cn '{qlog_root:{index:6,hash:[range(31)]},
      checkpoint_root:{index:6,hash:[range(31)]}}'
  }
  if wait_for_checkpoint_drain; then
    echo "checkpoint drain accepted a short byte-array hash" >&2
    exit 1
  fi
)

(
  admin_endpoint_urls=(http://127.0.0.1:18080 http://127.0.0.1:18081 http://127.0.0.1:18082)
  multi_endpoint=0
  configure_endpoint_topology
  [ "${#workload_endpoint_urls[@]}" -eq 1 ]
  [ "${workload_endpoint_urls[0]}" = "${admin_endpoint_urls[0]}" ]
  multi_endpoint=1
  configure_endpoint_topology
  [ "${workload_endpoint_urls[*]}" = "${admin_endpoint_urls[*]}" ]
  admin_endpoint_urls=(http://127.0.0.1:18080 http://127.0.0.1:18081)
  if configure_endpoint_topology; then
    echo "incomplete admin evidence topology was accepted" >&2
    exit 1
  fi
  fault=none
  fault_pod=rhiza-sql-c1-1
  [ -z "$(selected_resource_fault_pod)" ]
  fault="pod-delete"
  [ "$(selected_resource_fault_pod)" = rhiza-sql-c1-1 ]
)

: > "$KUBECTL_LOG"
if KUBECTL_DELETE_FAIL=0 PATH="$fake_bin:$PATH" sh -c "$failed_delete_command"; then
  echo "same-identity replacement pod was accepted" >&2
  exit 1
fi
if grep -Fq ' wait ' "$KUBECTL_LOG"; then
  echo "replacement wait ran for the deleted pod identity" >&2
  exit 1
fi
: > "$KUBECTL_LOG"
rm -f "$KUBECTL_OLD_UID_SEEN"
if ! KUBECTL_DELETE_FAIL=0 KUBECTL_REPLACEMENT_UID=new-pod-uid \
  PATH="$fake_bin:$PATH" sh -c "$failed_delete_command"; then
  echo "different ready replacement pod was rejected" >&2
  exit 1
fi
grep -Fq 'wait --for=condition=Ready pod/rhiza-sql-c1-1' "$KUBECTL_LOG"

rebind_fixture="$tmp/rebind"
mkdir "$rebind_fixture"
(
  target="$rebind_fixture"
  admin_endpoint_urls=(http://127.0.0.1:18081)
  printf '0\n' > "$rebind_fixture/starts"
  k() {
    local invocation
    invocation=$(( $(cat "$rebind_fixture/starts") + 1 ))
    printf '%s\n' "$invocation" > "$rebind_fixture/starts"
    : > "$rebind_fixture/started-$invocation"
    trap ': > "$rebind_fixture/stopped-$invocation"; exit 0' TERM INT
    while true; do sleep 1; done
  }
  curl() { [ -e "$rebind_fixture/allow-ready" ]; }
  wait_for_fixture() {
    for _ in $(seq 1 500); do [ -e "$1" ] && return; sleep 0.01; done
    return 1
  }
  supervise_rebinding_port_forward 0 rhiza-sql-c1-1 18081 & supervisor_pid=$!
  trap 'kill "$supervisor_pid" 2>/dev/null || true; wait "$supervisor_pid" 2>/dev/null || true' EXIT
  wait_for_fixture "$rebind_fixture/started-1"
  : > "$rebind_fixture/port-forward-0.rebind-request"
  wait_for_fixture "$rebind_fixture/stopped-1"
  [ ! -e "$rebind_fixture/started-2" ]
  : > "$rebind_fixture/port-forward-0.replacement-ready"
  wait_for_fixture "$rebind_fixture/started-2"
  : > "$rebind_fixture/allow-ready"
  wait_for_fixture "$rebind_fixture/port-forward-0.rebound"
  kill -0 "$supervisor_pid"
)

benchmark_line="$(grep -n 'RHIZA_CLIENT_TOKEN=.*bench_binary' scripts/bench-vind.sh | tail -n 1 | cut -d: -f1)"
final_forward_check_line="$(grep -n 'assert_all_port_forwards_ready' scripts/bench-vind.sh | tail -n 1 | cut -d: -f1)"
[ -n "$final_forward_check_line" ] && [ "$benchmark_line" -lt "$final_forward_check_line" ]

jq -n '{config_id:1,members:[range(3) as $n | {
  node_id:"node-\($n + 1)",
  url:"http://rhiza-sql-c1-\($n).rhiza-sql-c1:8081",
  log_url:"http://rhiza-sql-c1-\($n).rhiza-sql-c1:8080",
  token:"fixture-peer-\($n + 1)"
}]}' > "$tmp/config.json"
jq -e '(.members | length) == 3 and ([.members[].token] | unique | length) == 3' \
  "$tmp/config.json" >/dev/null

export RHIZA_IMAGE=rhiza:fixture RHIZA_CLUSTER_ID=rhiza-vind
export RHIZA_RECOVERY_GENERATION=1
export RHIZA_S3_ENDPOINT=http://rustfs:9000 RHIZA_OBJECT_SECRET=rustfs-credentials
export RHIZA_S3_ALLOW_HTTP=true
RHIZA_OBJECT_JOB_RENDER_ONLY="$tmp/object-job.yaml" \
  scripts/k8s-object-job.sh 1 "$tmp/config.json" init-checkpoint
RHIZA_RECORDER_TRANSPORT=http \
  scripts/render-k8s-config.sh 1 3 "$tmp/config.json" "$tmp/cluster.yaml"
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") | .env[] |
  select(.name == "RHIZA_STARTUP_MODE") | .value == "rejoin"' \
  "$tmp/cluster.yaml" >/dev/null
if grep -Eq 'RHIZA_STARTUP_MODE=bootstrap|set env statefulset/rhiza-sql-c1 RHIZA_STARTUP_MODE=rejoin' \
  scripts/bench-vind.sh; then
  echo "benchmark mutates startup mode instead of rendering rejoin from the start" >&2
  exit 1
fi

jq '.members |= (to_entries | map(.value + {
  recorder_tcp_addr:("rhiza-sql-c1-" + (.key | tostring) + ".rhiza-sql-c1:8082")
}))' "$tmp/config.json" > "$tmp/config-tcp.json"
RHIZA_RECORDER_TRANSPORT=tcp-postcard \
  scripts/render-k8s-config.sh 1 3 "$tmp/config-tcp.json" "$tmp/cluster-tcp.yaml"
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") | .env[] |
  select(.name == "RHIZA_RECORDER_TRANSPORT") | .value == "tcp-postcard"' \
  "$tmp/cluster-tcp.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") | .env[] |
  select(.name == "RHIZA_RECORDER_TCP_LISTEN") | .value == "0.0.0.0:8082"' \
  "$tmp/cluster-tcp.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") | .ports[] |
  select(.name == "recorder-tcp") | .containerPort == 8082' \
  "$tmp/cluster-tcp.yaml" >/dev/null
yq eval -e 'select(.kind == "Service" and .metadata.name == "rhiza-sql-c1") |
  .spec.ports[] |
  select(.name == "recorder-tcp" and .port == 8082 and .targetPort == "recorder-tcp")' \
  "$tmp/cluster-tcp.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .env[] | select(.name == "RHIZA_RECORDER_TLS") | .value == "off"' \
  "$tmp/cluster-tcp.yaml" >/dev/null
for tls_env_name in RHIZA_RECORDER_TLS_CERT_FILE RHIZA_RECORDER_TLS_KEY_FILE \
  RHIZA_RECORDER_TLS_CA_FILE; do
  if TLS_ENV_NAME="$tls_env_name" yq eval -e 'select(.kind == "StatefulSet") |
    .spec.template.spec.containers[] | select(.name == "rhiza") |
    .env[] | select(.name == strenv(TLS_ENV_NAME))' "$tmp/cluster-tcp.yaml" \
      >/dev/null 2>&1; then
    echo "plaintext Recorder manifest contains $tls_env_name" >&2
    exit 1
  fi
done
if yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .volumeMounts[] | select(.name == "recorder-tls")' "$tmp/cluster-tcp.yaml" \
    >/dev/null 2>&1; then
  echo "plaintext Recorder manifest contains a TLS volume mount" >&2
  exit 1
fi

RHIZA_RECORDER_TRANSPORT=tcp-postcard-rpc \
  scripts/render-k8s-config.sh 1 3 "$tmp/config-tcp.json" "$tmp/cluster-postcard-rpc.yaml"
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .env[] | select(.name == "RHIZA_RECORDER_TRANSPORT") |
  .value == "tcp-postcard-rpc"' "$tmp/cluster-postcard-rpc.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .ports[] | select(.name == "recorder-tcp") | .containerPort == 8082' \
  "$tmp/cluster-postcard-rpc.yaml" >/dev/null

jq '.members |= (to_entries | map(.value + {
  recorder_tcp_addr:("rhiza-sql-c1-" + (.key | tostring) + ".rhiza-sql-c1:8082"),
  recorder_tls_server_name:("rhiza-sql-c1-" + (.key | tostring) + ".rhiza-sql-c1")
}))' "$tmp/config.json" > "$tmp/config-tls.json"
tls_names=(
  rhiza-sql-c1-0.rhiza-sql-c1
  rhiza-sql-c1-1.rhiza-sql-c1
  rhiza-sql-c1-2.rhiza-sql-c1
)
tls_dir="$tmp/recorder-tls"
generate_recorder_tls_material "$tls_dir" "${tls_names[@]}"
openssl verify -CAfile "$tls_dir/ca-bundle.pem" "$tls_dir/tls.crt" >/dev/null
for dns_name in "${tls_names[@]}"; do
  openssl x509 -in "$tls_dir/tls.crt" -noout -checkhost "$dns_name" >/dev/null
done
expected_sans="$(printf '%s\n' "${tls_names[@]}" | sort)"
actual_sans="$(openssl x509 -in "$tls_dir/tls.crt" -noout -text |
  grep -o 'DNS:[^, ]*' | cut -c5- | sort)"
[ "$actual_sans" = "$expected_sans" ]
[ "$(find "$tls_dir" -type f | wc -l | tr -d ' ')" = 3 ]

RHIZA_RECORDER_TRANSPORT=tcp-postcard \
RHIZA_RECORDER_TLS=on \
RHIZA_RECORDER_TLS_SECRET=rhiza-sql-c1-recorder-tls \
  scripts/render-k8s-config.sh 1 3 "$tmp/config-tls.json" "$tmp/cluster-tls.yaml"
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .env[] | select(.name == "RHIZA_RECORDER_TRANSPORT") |
  .value == "tcp-postcard"' "$tmp/cluster-tls.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .env[] | select(.name == "RHIZA_RECORDER_TLS") |
  .value == "on"' "$tmp/cluster-tls.yaml" >/dev/null
for tls_env in \
  RHIZA_RECORDER_TLS_CERT_FILE=/run/secrets/rhiza/recorder-tls/tls.crt \
  RHIZA_RECORDER_TLS_KEY_FILE=/run/secrets/rhiza/recorder-tls/tls.key \
  RHIZA_RECORDER_TLS_CA_FILE=/run/secrets/rhiza/recorder-tls/ca-bundle.pem; do
  TLS_ENV_NAME="${tls_env%%=*}" TLS_ENV_VALUE="${tls_env#*=}" \
    yq eval -e 'select(.kind == "StatefulSet") |
      .spec.template.spec.containers[] | select(.name == "rhiza") |
      .env[] | select(.name == strenv(TLS_ENV_NAME)) |
      .value == strenv(TLS_ENV_VALUE)' "$tmp/cluster-tls.yaml" >/dev/null
done
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") | .volumeMounts[] |
  select(.name == "recorder-tls" and .readOnly == true)' \
  "$tmp/cluster-tls.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.volumes[] | select(.name == "recorder-tls") |
  .secret.secretName == "rhiza-sql-c1-recorder-tls"' \
  "$tmp/cluster-tls.yaml" >/dev/null

RHIZA_RECORDER_TRANSPORT=tcp-postcard-rpc \
RHIZA_RECORDER_TLS=on \
RHIZA_RECORDER_TLS_SECRET=rhiza-sql-c1-recorder-tls \
  scripts/render-k8s-config.sh 1 3 "$tmp/config-tls.json" \
    "$tmp/cluster-postcard-rpc-tls.yaml"
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .env[] | select(.name == "RHIZA_RECORDER_TRANSPORT") |
  .value == "tcp-postcard-rpc"' "$tmp/cluster-postcard-rpc-tls.yaml" >/dev/null
yq eval -e 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "rhiza") |
  .env[] | select(.name == "RHIZA_RECORDER_TLS") |
  .value == "on"' "$tmp/cluster-postcard-rpc-tls.yaml" >/dev/null
tls_secret_keys="$(yq eval -r 'select(.kind == "StatefulSet") |
  .spec.template.spec.volumes[] | select(.name == "recorder-tls") |
  .secret.items[].key' "$tmp/cluster-tls.yaml" | sed '/^$/d; /^---$/d' | sort | paste -sd, -)"
[ "$tls_secret_keys" = ca-bundle.pem,tls.crt,tls.key ]
if grep -Eq 'BEGIN (CERTIFICATE|.*PRIVATE KEY)|^[[:space:]]*data:' "$tmp/cluster-tls.yaml"; then
  echo "Recorder TLS manifest contains certificate or key material" >&2
  exit 1
fi

for manifest in "$tmp/object-job.yaml" "$tmp/cluster.yaml"; do
  yq eval -e '
    .spec.template.spec.containers[0].env[] |
    select(.name == "RHIZA_S3_ENDPOINT") |
    .value == "http://rustfs:9000"
  ' "$manifest" >/dev/null
  yq eval -e '
    .spec.template.spec.containers[0].env[] |
    select(.name == "RHIZA_S3_ALLOW_HTTP") |
    .value == "true"
  ' "$manifest" >/dev/null
  [ "$(yq eval -r '
    [.spec.template.spec.containers[0].env[] |
      select(.name == "RHIZA_S3_ACCESS_KEY" or .name == "RHIZA_S3_SECRET_KEY") |
      .valueFrom.secretKeyRef.name] | unique | .[]
  ' "$manifest")" = rustfs-credentials ]
done

echo "vind benchmark static checks passed"
