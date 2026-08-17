#!/usr/bin/env bash
set -euo pipefail
export RHIZA_EXECUTION_PROFILE=sql

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
cluster="${RHIZA_VIND_CLUSTER:-rhiza-bench-${run_id}}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-bench}"
image="${RHIZA_IMAGE:-rhiza:dev}"
rustfs_image="${RHIZA_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.8}"
aws_image="${RHIZA_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
nginx_image="${RHIZA_NGINX_IMAGE:-nginx:1.27-alpine}"
object_metering="${RHIZA_BENCH_OBJECT_USAGE_METERING:-1}"
resource_sampling="${RHIZA_BENCH_RESOURCE_SAMPLING:-1}"
multi_endpoint="${RHIZA_BENCH_MULTI_ENDPOINT:-0}"
durability_mode="${RHIZA_DURABILITY_MODE-sync}"
durability_max_lag="${RHIZA_DURABILITY_MAX_LAG-}"
durability_interval="${RHIZA_DURABILITY_INTERVAL-}"
durability_max_lag_set="${RHIZA_DURABILITY_MAX_LAG+x}"
durability_interval_set="${RHIZA_DURABILITY_INTERVAL+x}"
recorder_transport="${RHIZA_RECORDER_TRANSPORT:-http}"
recorder_tls="${RHIZA_RECORDER_TLS:-off}"
recorder_tls_dir=""
target_base="${RHIZA_BENCH_TARGET_DIR:-target/rhiza-bench}"
duration=30s
warmup=5s
concurrency=4
target_rate=""
workload=mixed
d1_exact_write="${RHIZA_BENCH_D1_EXACT_WRITE:-0}"
write_percent=50
fault=none
fault_offset=10s
fault_pod=rhiza-sql-c1-1
sample_interval=2
resource_sample_timeout=3
resource_sample_kill_after=1
resource_sample_jitter=1
rhiza_cpu_request="${RHIZA_BENCH_RHIZA_CPU_REQUEST:-250m}"
rhiza_cpu_limit="${RHIZA_BENCH_RHIZA_CPU_LIMIT:-1000m}"
rhiza_memory_request="${RHIZA_BENCH_RHIZA_MEMORY_REQUEST:-512Mi}"
rhiza_memory_limit="${RHIZA_BENCH_RHIZA_MEMORY_LIMIT:-1Gi}"
rustfs_cpu_request="${RHIZA_BENCH_RUSTFS_CPU_REQUEST:-250m}"
rustfs_cpu_limit="${RHIZA_BENCH_RUSTFS_CPU_LIMIT:-1000m}"
rustfs_memory_request="${RHIZA_BENCH_RUSTFS_MEMORY_REQUEST:-512Mi}"
rustfs_memory_limit="${RHIZA_BENCH_RUSTFS_MEMORY_LIMIT:-1Gi}"
keep=false
context=""
previous_context=""
created_cluster=false
namespace_created=false
port_forward_pids=()
sampler_pid=""
benchmark_status=255
cleanup_status=0
cleaned_up=false
cleanup_verification_status=skipped
namespace_cleanup_status=not_created
vcluster_cleanup_status=not_created
resource_evidence_status=disabled
object_evidence_status=disabled
object_meter_reset_status=disabled
measurement_started_at_epoch_seconds=""
measurement_finished_at_epoch_seconds=""
resource_fault_started_at_epoch_seconds=""
resource_fault_finished_at_epoch_seconds=""
source_git_commit=""
source_dirty=true
image_build_mode="unknown"
image_inspect_json='[]'
benchmark_binary_sha256=""
rustc_vv=""
cargo_version=""
runtime_image_ids_json='{"rhiza":[],"rustfs":[],"object_meter":[],"aws_cli_inventory":[]}'
[ "$resource_sampling" = 0 ] || resource_evidence_status=pending
[ "$object_metering" = 0 ] || {
  object_evidence_status=pending
  object_meter_reset_status=pending
}

usage() {
  printf '%s\n' \
    'usage: scripts/bench-vind.sh [options]' \
    '  --duration D --warmup D --concurrency N --target-rate R' \
    '  --workload read|write|mixed --write-percent N' \
    '  --fault none|pod-delete' \
    '  --fault-offset D --fault-pod POD' \
    '  --sample-interval SECONDS --keep' \
    '' \
    'Resource defaults are 250m/512Mi requests and 1000m/1Gi limits for each' \
    'rhiza or RustFS container. Override with RHIZA_BENCH_{RHIZA,RUSTFS}_*' \
    'CPU_{REQUEST,LIMIT} and MEMORY_{REQUEST,LIMIT} environment variables.' \
    'Set RHIZA_BENCH_RESOURCE_SAMPLING=0 to omit containerd CRI sampling.' \
    'Set RHIZA_BENCH_OBJECT_USAGE_METERING=0 to omit the nginx S3 counting proxy.' \
    'Set RHIZA_BENCH_MULTI_ENDPOINT=1 to route retries across all three nodes.' \
    'Durability defaults to sync. Set RHIZA_DURABILITY_MODE=bounded with' \
    'RHIZA_DURABILITY_MAX_LAG, or periodic with RHIZA_DURABILITY_INTERVAL.' \
    'Set RHIZA_RECORDER_TRANSPORT=tcp-postcard|tcp-postcard-rpc to benchmark a TCP transport.' \
    'Set RHIZA_RECORDER_TLS=on to enable server-authenticated TLS for either TCP transport.' \
    '' \
    'It creates a vind cluster, deploys RustFS plus a three-node rhiza cluster,' \
    'runs bench/rhiza-bench through a local port-forward, and emits artifacts.json.' >&2
}

die() { echo "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || die "missing required command: $1"; }
shell_quote() { printf '%q' "$1"; }

generate_recorder_tls_material() {
  local directory="$1" dns_name sans=""
  shift
  [ "$#" -gt 0 ] || die "recorder TLS requires at least one DNS name"
  for dns_name in "$@"; do
    case "$dns_name" in
      ''|*[!A-Za-z0-9.-]*) die "invalid recorder TLS DNS name" ;;
    esac
    sans="${sans:+$sans,}DNS:$dns_name"
  done
  (
    umask 077
    mkdir -p "$directory"
    chmod 700 "$directory"
    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
      -subj '/CN=rhiza Recorder benchmark CA' \
      -addext 'basicConstraints=critical,CA:TRUE' \
      -addext 'keyUsage=critical,keyCertSign,cRLSign' \
      -keyout "$directory/ca.key" -out "$directory/ca-bundle.pem" \
      >/dev/null 2>&1 || die "cannot generate recorder TLS benchmark CA"
    openssl req -new -newkey rsa:2048 -nodes -sha256 \
      -subj '/CN=rhiza Recorder benchmark' \
      -keyout "$directory/tls.key" -out "$directory/server.csr" \
      >/dev/null 2>&1 || die "cannot generate recorder TLS benchmark key"
    printf '%s\n' \
      "subjectAltName=$sans" \
      'basicConstraints=critical,CA:FALSE' \
      'keyUsage=critical,digitalSignature,keyEncipherment' \
      'extendedKeyUsage=serverAuth' > "$directory/server.ext"
    openssl x509 -req -sha256 -days 1 -in "$directory/server.csr" \
      -CA "$directory/ca-bundle.pem" -CAkey "$directory/ca.key" -CAcreateserial \
      -extfile "$directory/server.ext" -out "$directory/tls.crt" \
      >/dev/null 2>&1 || die "cannot sign recorder TLS benchmark certificate"
    rm -f "$directory/ca.key" "$directory/ca-bundle.srl" \
      "$directory/server.csr" "$directory/server.ext"
  )
}

endpoint_ready() {
  curl --connect-timeout 1 --max-time 3 -fsS "$1/readyz" >/dev/null 2>&1
}

assert_port_forward_alive() {
  local index="$1" pid="${port_forward_pids[$1]}" status=0
  kill -0 "$pid" 2>/dev/null && return
  if wait "$pid"; then status=0; else status=$?; fi
  echo "port-forward exited with status $status: ${admin_endpoint_urls[$index]}" >&2
  sed 's/^/  /' "$target/port-forward-$index.log" >&2
  exit 1
}

assert_all_port_forwards_alive() {
  local index
  for index in "${!admin_endpoint_urls[@]}"; do assert_port_forward_alive "$index"; done
}

assert_all_port_forwards_ready() {
  local endpoint_url
  assert_all_port_forwards_alive
  for endpoint_url in "${admin_endpoint_urls[@]}"; do
    endpoint_ready "$endpoint_url" ||
      die "port-forward did not remain ready: $endpoint_url"
  done
}

configure_endpoint_topology() {
  [ "${#admin_endpoint_urls[@]}" -eq 3 ] || return 1
  workload_endpoint_urls=("${admin_endpoint_urls[0]}")
  [ "$multi_endpoint" = 0 ] || workload_endpoint_urls=("${admin_endpoint_urls[@]}")
}

selected_resource_fault_pod() {
  if [ "$fault" = pod-delete ]; then printf '%s' "$fault_pod"; fi
}

supervise_rebinding_port_forward() {
  local index="$1" pod="$2" port="$3" child="" status=0
  local request="$target/port-forward-$index.rebind-request"
  local replacement_ready="$target/port-forward-$index.replacement-ready"
  local rebound="$target/port-forward-$index.rebound"
  local log="$target/port-forward-$index.log"
  trap 'exit 0' TERM INT
  trap 'status=$?; trap - EXIT; if [ -n "$child" ]; then kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi; exit "$status"' EXIT

  : > "$log"
  k port-forward "pod/$pod" "${port}:8080" >> "$log" 2>&1 &
  child=$!
  while true; do
    while kill -0 "$child" 2>/dev/null && [ ! -e "$request" ]; do sleep 1; done
    if [ ! -e "$request" ]; then
      if wait "$child"; then status=0; else status=$?; fi
      child=""
      return "$status"
    fi

    kill "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
    child=""
    while [ ! -e "$replacement_ready" ]; do sleep 1; done
    while [ -e "$request" ]; do
      k port-forward "pod/$pod" "${port}:8080" >> "$log" 2>&1 &
      child=$!
      for _ in $(seq 1 60); do
        if kill -0 "$child" 2>/dev/null && endpoint_ready "http://127.0.0.1:${port}"; then
          rm -f "$request"
          rm -f "$replacement_ready"
          : > "$rebound"
          break
        fi
        kill -0 "$child" 2>/dev/null || break
        sleep 1
      done
      if [ -e "$request" ]; then
        kill "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
        child=""
        sleep 1
      fi
    done
  done
}

build_pod_delete_fault_command() {
  local context="$1" namespace="$2" pod="$3"
  local request="${4:-}" replacement_ready="${5:-}" rebound="${6:-}"
  local kubectl_command pod_arg pod_resource command
  kubectl_command="kubectl --context $(shell_quote "$context") -n $(shell_quote "$namespace")"
  pod_arg="$(shell_quote "$pod")"
  pod_resource="$(shell_quote "pod/$pod")"
  command="old_pod_uid=\$($kubectl_command get pod $pod_arg -o jsonpath='{.metadata.uid}') && [ -n \"\$old_pod_uid\" ]"
  if [ -n "$request" ]; then
    command+=" && rm -f $(shell_quote "$rebound") $(shell_quote "$replacement_ready") && touch $(shell_quote "$request")"
  fi
  command+=" && $kubectl_command delete pod $pod_arg --wait=true >/dev/null"
  command+=" && { replacement_pod_uid=; for attempt in \$(seq 1 240); do replacement_pod_uid=\$($kubectl_command get pod $pod_arg -o jsonpath='{.metadata.uid}' 2>/dev/null || true); [ -n \"\$replacement_pod_uid\" ] && [ \"\$replacement_pod_uid\" != \"\$old_pod_uid\" ] && break; sleep 1; done; [ -n \"\$replacement_pod_uid\" ] && [ \"\$replacement_pod_uid\" != \"\$old_pod_uid\" ]; }"
  command+=" && $kubectl_command wait --for=condition=Ready $pod_resource --timeout=240s >/dev/null"
  if [ -n "$request" ]; then
    command+=" && touch $(shell_quote "$replacement_ready") && { for attempt in \$(seq 1 30); do [ -e $(shell_quote "$rebound") ] && break; sleep 1; done; [ -e $(shell_quote "$rebound") ]; }"
  fi
  printf '%s\n' "$command"
}

validate_resource_sample_schema() {
  local meter_enabled="${2:-$object_metering}" expected
  expected="$(expected_resource_components "$meter_enabled")"
  jq -s -e --argjson expected "$expected" --argjson meter_enabled "$meter_enabled" '
    def component: if .app == "rhiza" then .pod else .container end;
    . as $samples |
    length > 0 and all(.[];
      (.timestamp | type == "string" and length > 0) and
      (.timestamp_epoch_seconds | type == "number" and . >= 0) and
      (.collection_batch | type == "number" and . >= 0 and floor == .) and
      .source == "containerd_cri_stats" and
      ([.pod,.pod_uid,.container,.container_id] |
        all(type == "string" and length > 0)) and
      ((.app == "rhiza" and .container == "rhiza" and
          (.pod | test("^rhiza-sql-c1-[0-2]$"))) or
        (.app == "simulator" and
          (.container == "rustfs" or .container == "object-meter") and
          (.pod | startswith("rustfs-")))) and
      (.restart_count | type == "number" and . >= 0 and floor == .) and
      ([.cpu_usage_usec,.memory_bytes] | all(type == "number" and . >= 0))) and
    all($expected[]; . as $required | any($samples[]; component == $required)) and
    ($samples | group_by(.collection_batch) | all(.[]; . as $batch |
      ([$batch[] | component] | unique | length) == ($batch | length))) and
    ([$samples[] | select(.container == "rustfs") | .pod] | unique | length) == 1 and
    (if $meter_enabled == 1 then
       ([$samples[] | select(.container == "object-meter") | .pod] | unique) ==
       ([$samples[] | select(.container == "rustfs") | .pod] | unique)
     else true end)
  ' "$1" >/dev/null 2>&1
}

expected_resource_components() {
  if [ "$1" = 1 ]; then
    echo '["rhiza-sql-c1-0","rhiza-sql-c1-1","rhiza-sql-c1-2","rustfs","object-meter"]'
  else
    echo '["rhiza-sql-c1-0","rhiza-sql-c1-1","rhiza-sql-c1-2","rustfs"]'
  fi
}

resource_continuity_budget_seconds() {
  local interval="${1:-$sample_interval}"
  echo $((2 * (interval + resource_sample_timeout + resource_sample_kill_after) +
    resource_sample_jitter))
}

resource_coverage_wait_budget_seconds() {
  echo $(($(resource_continuity_budget_seconds) + resource_sample_timeout +
    resource_sample_kill_after))
}

validate_resource_samples() {
  local meter_enabled="${5:-$object_metering}" fault_pod_name="${6:-}"
  local fault_start="${7:-}" fault_end="${8:-}"
  local continuity_budget
  continuity_budget="$(resource_continuity_budget_seconds "$4")"
  local expected
  expected="$(expected_resource_components "$meter_enabled")"
  validate_resource_sample_schema "$1" "$meter_enabled" && jq -s -e \
    --argjson start "$2" --argjson end "$3" --argjson interval "$4" \
    --argjson max_gap "$continuity_budget" --arg fault_pod "$fault_pod_name" \
    --arg fault_start "$fault_start" --arg fault_end "$fault_end" \
    --argjson expected "$expected" '
    def component: if .app == "rhiza" then .pod else .container end;
    def identity: [.pod_uid,.container_id,.restart_count];
    def fault_gap($component; $left; $right):
      $component == $fault_pod and
      ($left.pod_uid != $right.pod_uid or $left.container_id != $right.container_id) and
      ($fault_start | test("^[0-9]+([.][0-9]+)?$")) and
      ($fault_end | test("^[0-9]+([.][0-9]+)?$")) and
      ($fault_end | tonumber) >= ($fault_start | tonumber) and
      $left.timestamp_epoch_seconds <= ($fault_end | tonumber) and
      $right.timestamp_epoch_seconds >= ($fault_start | tonumber) and
      (($fault_start | tonumber) - $left.timestamp_epoch_seconds) <= $max_gap and
      ($right.timestamp_epoch_seconds - ($fault_end | tonumber)) <= $max_gap;
    def complete_or_fault_batch:
      . as $batch |
      ([$batch[] | component] | unique | sort) as $components |
      $components == ($expected | sort) or
      ($fault_pod != "" and
       ($fault_start | test("^[0-9]+([.][0-9]+)?$")) and
       ($fault_end | test("^[0-9]+([.][0-9]+)?$")) and
       $components == ($expected | map(select(. != $fault_pod)) | sort) and
       all($batch[];
         .timestamp_epoch_seconds >= ($fault_start | tonumber) and
         .timestamp_epoch_seconds <= ($fault_end | tonumber)));
    . as $samples |
    $start >= 0 and $end >= $start and $interval > 0 and
    ($fault_pod == "" or ($fault_pod | test("^rhiza-sql-c1-[0-2]$"))) and
    all(($samples | group_by(.collection_batch))[]; complete_or_fault_batch) and
    all($expected[];
      . as $component |
      ([$samples[] | select(component == $component)] |
        unique_by([.timestamp_epoch_seconds,.container_id]) |
        sort_by(.timestamp_epoch_seconds)) as $observed |
      (if $component == $fault_pod and
          ($fault_end | test("^[0-9]+([.][0-9]+)?$")) and
          ($fault_end | tonumber) > $end
       then ($fault_end | tonumber) else $end end) as $coverage_end |
      ([$observed[] | select(.timestamp_epoch_seconds <= $start)] | last) as $before |
      ([$observed[] | select(.timestamp_epoch_seconds >= $coverage_end)] | first) as $after |
      ($observed | length) >= 2 and $before != null and $after != null and
      ($start - $before.timestamp_epoch_seconds) <= $max_gap and
      ($after.timestamp_epoch_seconds - $coverage_end) <= $max_gap and
      (([$observed[] | select(.timestamp_epoch_seconds >= $before.timestamp_epoch_seconds and
          .timestamp_epoch_seconds <= $after.timestamp_epoch_seconds)]) as $covered |
        ($covered | length) >= 2 and
        all(range(1; $covered | length);
          . as $i | ($covered[$i].timestamp_epoch_seconds -
            $covered[$i - 1].timestamp_epoch_seconds) <= $max_gap or
            fault_gap($component; $covered[$i - 1]; $covered[$i])) and
        (if $component == $fault_pod then
           ([$covered[] | identity] | unique | length) == 2 and
           ([range(1; $covered | length) as $i |
             select(($covered[$i - 1] | identity) != ($covered[$i] | identity))] | length) == 1 and
           all(range(1; $covered | length);
             . as $i |
             (($covered[$i - 1] | identity) == ($covered[$i] | identity)) or
             fault_gap($component; $covered[$i - 1]; $covered[$i]))
         else
           ([$covered[] | identity] | unique | length) == 1
         end)))
  ' "$1" >/dev/null 2>&1
}

summarize_resource_samples() {
  jq -s --argjson start "$2" --argjson end "$3" '
    def component: if .app == "rhiza" then .pod else .container end;
    . as $samples |
    ($samples | group_by(.app) |
      map({key:.[0].app,value:(map(component) | unique | sort)}) |
      from_entries) as $expected_by_app |
    ($samples | sort_by([.app,.pod_uid,.container,.container_id,.timestamp_epoch_seconds]) |
      group_by([.app,.pod_uid,.container,.container_id]) |
      map(sort_by(.timestamp_epoch_seconds) as $g |
        ([$g[] | select(.timestamp_epoch_seconds <= $start)] | last) as $before |
        ([$g[] | select(.timestamp_epoch_seconds >= $start and
          .timestamp_epoch_seconds <= $end)]) as $inside |
        ([$g[] | select(.timestamp_epoch_seconds >= $end)] | first) as $after |
        select(($inside | length) > 0 or ($before != null and $after != null)) |
        ([$before] + $inside + [$after] | map(select(. != null)) |
          unique_by([.timestamp_epoch_seconds,.collection_batch])))) as $groups |
    ($groups | map(.[])) as $window |
    def regressed:
      . as $g | any(range(1; length); . as $i |
        $g[$i].cpu_usage_usec < $g[$i - 1].cpu_usage_usec);
    def cpu_deltas: $groups |
      map(. as $g |
        ([$g[] | select(.timestamp_epoch_seconds <= $start)] | last) as $baseline |
        ($baseline.cpu_usage_usec // 0) as $first |
        {app:$g[0].app,pod:$g[0].pod,pod_uid:$g[0].pod_uid,container:$g[0].container,
         container_id:$g[0].container_id,first:$first,last:$g[-1].cpu_usage_usec,
         baseline:(if $baseline == null then "born_in_window" else "preexisting" end),
         delta_usec:($g[-1].cpu_usage_usec - $first)});
    def complete_memory_batches: group_by([.collection_batch,.app]) |
      map(. as $batch |
        ([$batch[] | component] | unique | sort) as $components |
        select($components == $expected_by_app[$batch[0].app] and
          ($components | length) == ($batch | length)) |
        {collection_batch:$batch[0].collection_batch,app:$batch[0].app,
         started_at_epoch_seconds:($batch | map(.timestamp_epoch_seconds) | min),
         finished_at_epoch_seconds:($batch | map(.timestamp_epoch_seconds) | max),
         memory_bytes:($batch | map(.memory_bytes) | add)});
    def memory_window: group_by(.app) |
      map(sort_by([.started_at_epoch_seconds,.finished_at_epoch_seconds,
          .collection_batch]) as $batches |
        ([$batches[] | select(.finished_at_epoch_seconds < $start)] | last) as $before |
        ([$batches[] | select(.finished_at_epoch_seconds >= $start and
          .started_at_epoch_seconds <= $end)]) as $inside |
        ([$batches[] | select(.started_at_epoch_seconds > $end)] | first) as $after |
        ([$before] + $inside + [$after] | map(select(. != null)) |
          unique_by(.collection_batch))) | map(.[]);
    if any($samples[]; ((.collection_batch | type != "number") or
        .collection_batch < 0 or (.collection_batch | floor) != .collection_batch)) then
      error("resource sample collection batch is missing or invalid")
    elif any($groups[]; regressed) then error("container CPU counter regressed") else
    (cpu_deltas) as $cpu |
    ($samples | complete_memory_batches) as $complete_memory |
    ($complete_memory | memory_window) as $memory |
    if any(($expected_by_app | keys)[]; . as $app |
        (any($complete_memory[]; .app == $app and
          .finished_at_epoch_seconds <= $start) | not) or
        (any($complete_memory[]; .app == $app and
          .started_at_epoch_seconds >= $end) | not)) then
      error("complete memory batches do not bracket the measurement window")
    else
    {status:"ok",measurement_window:{started_at_epoch_seconds:$start,
      finished_at_epoch_seconds:$end},samples:($window | length),
     container_cpu_usage_usec_deltas:$cpu,
     apps:(["rhiza","simulator"] | map(. as $app |
       ($memory | map(select(.app == $app))) as $app_memory |
       {app:$app,cpu_usage_usec:($cpu | map(select(.app == $app) | .delta_usec) | add // 0),
        memory_samples:($app_memory | length),
        average_memory_bytes:(if ($app_memory | length) == 0 then null else
          (($app_memory | map(.memory_bytes) | add) / ($app_memory | length) | floor) end),
        peak_memory_bytes:($app_memory | map(.memory_bytes) | max // null)}))} end
    end
  ' "$1"
}

runtime_image_ids_from_pods() {
  jq -c '
    def image_id($container):
      (([.status.containerStatuses[]? | select(.name == $container) | (.imageID // "")][0]) // "");
    {rhiza:[.items[]? | select(.metadata.labels["app.kubernetes.io/name"] == "rhiza") |
        image_id("rhiza")],
     rustfs:[.items[]? | select(.metadata.labels["app.kubernetes.io/name"] == "rustfs") |
        image_id("rustfs")],
     object_meter:[.items[]? | select(.metadata.labels["app.kubernetes.io/name"] == "rustfs") |
        image_id("object-meter")],
     aws_cli_inventory:[]}
  '
}

validate_object_evidence() {
  local request_count
  [ -r "$1" ] || return 1
  request_count="$(jq -s -er '
    def numeric: type == "number" or (type == "string" and test("^[0-9]+$"));
    if all(.[];
      (.method | type == "string" and length > 0) and
      (.status | numeric) and (.request_bytes | numeric) and (.response_bytes | numeric))
    then length else error("invalid meter output") end
  ' "$1" 2>/dev/null)" || return 1
  jq -e --argjson request_count "$request_count" '
      .metering.enabled == true and .metering.status == "ok" and
      .metering.requests == $request_count and
      .retained.status == "ok" and
      (.retained.object_count | type == "number" and . >= 0) and
      (.retained.retained_bytes | type == "number" and . >= 0)
    ' "$2" >/dev/null 2>&1
}

evidence_overall_status() {
  if [ "$1" = failed ] || [ "$2" = failed ]; then
    echo failed
  elif [ "$1" = disabled ] && [ "$2" = disabled ]; then
    echo disabled
  else
    echo ok
  fi
}

evidence_exit_status() {
  if [ "$1" -ne 0 ]; then echo "$1"
  elif [ "$(evidence_overall_status "$2" "$3")" = failed ] || [ "${4:-ok}" = failed ]; then echo 1
  else echo 0
  fi
}

render_evidence_json() {
  jq -n --arg resource "$1" --arg object "$2" \
    --argjson resource_enabled "$3" --argjson object_enabled "$4" \
    --arg overall "$(evidence_overall_status "$1" "$2")" \
    '{status:$overall,
      resource_sampling:{enabled:$resource_enabled,status:$resource},
      object_metering:{enabled:$object_enabled,status:$object}}'
}

cleanup_outcome() {
  if [ "$1" -eq 0 ] && [ "$2" -ne 0 ]; then echo ok; else echo failed; fi
}

render_cleanup_json() {
  jq -n --arg status "$1" --arg namespace_status "$2" --arg vcluster_status "$3" \
    '{requested:($status != "skipped"),status:$status,cleaned_up:($status == "ok"),
      namespace:$namespace_status,vcluster:$vcluster_status}'
}

render_measurement_window_json() {
  jq -n --arg started "$1" --arg finished "$2" \
    '{started_at_epoch_seconds:(if $started == "" then null else ($started | tonumber) end),
      finished_at_epoch_seconds:(if $finished == "" then null else ($finished | tonumber) end)}'
}

measurement_window_from_report() {
  jq -ce '
    .measurement.measurement_window as $window |
    $window |
    select(($window.started_at_epoch_seconds | type == "number" and . >= 0) and
      ($window.finished_at_epoch_seconds | type == "number" and . >= 0) and
      ($window.finished_at_epoch_seconds >= $window.started_at_epoch_seconds))
  ' "$1"
}

resource_fault_window_from_report() {
  jq -ce --argjson measurement_start "$2" '
    .fault as $fault |
    select($fault.tag == "pod-delete" and $fault.command_completed == true and
      ($fault.command_start_offset_seconds | type == "number" and . >= 0) and
      ($fault.command_elapsed_seconds | type == "number" and . >= 0)) |
    {started_at_epoch_seconds:($measurement_start + $fault.command_start_offset_seconds),
     finished_at_epoch_seconds:($measurement_start + $fault.command_start_offset_seconds +
       $fault.command_elapsed_seconds)}
  ' "$1"
}

render_provenance_json() {
  jq -n --arg commit "$1" --argjson dirty "$2" --arg build_mode "$3" \
    --arg image_reference "$4" --argjson inspect "$5" --arg benchmark_sha256 "$6" \
    --arg rustc_vv "$7" --arg cargo_version "$8" --argjson runtime_image_ids "$9" \
    --argjson object_enabled "${10}" --argjson benchmark_exit "${11}" \
    --argjson run_exit "${12}" --arg evidence_status "${13}" \
    --arg cleanup_status "${14}" '
    def immutable_digest:
      if type == "string" then
        if test("sha256:[0-9a-f]{64}$")
        then capture("(?<digest>sha256:[0-9a-f]{64})$").digest else null end
      else null end;
    def runtime_component($value; $required):
      (if ($value | type) == "array" then $value else [] end) as $ids |
      [$ids[] | immutable_digest] as $normalized |
      if ($required | not) then {status:"not_applicable",observed_instances:0,image_digests:[]}
      else {status:(if ($ids | length) > 0 and all($normalized[]; . != null)
          then "verified" else "missing_or_invalid" end),
        observed_instances:($ids | length),
        image_digests:([$normalized[] | select(. != null)] | unique)} end;
    ($inspect[0].Id // "") as $content_id_raw |
    ($content_id_raw | immutable_digest) as $content_id |
    (($inspect[0].RepoDigests // []) | map(select(type == "string"))) as $repo_digests |
    (($inspect[0].Config.Labels["org.opencontainers.image.revision"] // "") |
      if type == "string" then . else "" end) as $source_revision |
    runtime_component($runtime_image_ids.rhiza; true) as $rhiza_runtime |
    runtime_component($runtime_image_ids.rustfs; true) as $rustfs_runtime |
    runtime_component($runtime_image_ids.object_meter; $object_enabled) as $meter_runtime |
    runtime_component($runtime_image_ids.aws_cli_inventory; $object_enabled) as $inventory_runtime |
    ([if $benchmark_exit != 0 then "benchmark_failed" else empty end,
      if $run_exit != 0 then "run_failed" else empty end,
      if $evidence_status == "failed" then "evidence_failed" else empty end,
      if $cleanup_status == "failed" then "cleanup_failed"
        elif $cleanup_status != "ok" then "cleanup_not_verified" else empty end,
      if ($commit | test("^[0-9a-f]{40,64}$") | not) then "missing_git_commit" else empty end,
      if $dirty then "dirty_source" else empty end,
      if $content_id == null
        then "missing_immutable_image_identity" else empty end,
      if $build_mode == "skip-build" and $source_revision != $commit
        then "unverified_image_source" else empty end,
      if ($benchmark_sha256 | test("^[0-9a-f]{64}$") | not)
        then "missing_or_invalid_benchmark_client_sha256" else empty end,
      if ($rustc_vv | startswith("rustc ") | not)
        then "missing_or_invalid_rustc_version" else empty end,
      if ($cargo_version | startswith("cargo ") | not)
        then "missing_or_invalid_cargo_version" else empty end,
      if $rhiza_runtime.status != "verified"
        then "missing_or_invalid_rhiza_runtime_image" else empty end,
      if $rhiza_runtime.status == "verified" and $rhiza_runtime.observed_instances != 3
        then "unexpected_rhiza_runtime_image_count" else empty end,
      if $rhiza_runtime.status == "verified" and
        ($rhiza_runtime.image_digests | length) != 1
        then "heterogeneous_rhiza_runtime_images" else empty end,
      if $rhiza_runtime.status == "verified" and
        ($rhiza_runtime.image_digests | length) == 1 and $content_id != null and
        $rhiza_runtime.image_digests[0] != $content_id
        then "rhiza_runtime_image_mismatch" else empty end,
      if $rustfs_runtime.status != "verified"
        then "missing_or_invalid_rustfs_runtime_image" else empty end,
      if $meter_runtime.status == "missing_or_invalid"
        then "missing_or_invalid_object_meter_runtime_image" else empty end,
      if $inventory_runtime.status == "missing_or_invalid"
        then "missing_or_invalid_aws_cli_inventory_runtime_image" else empty end]) as $reasons |
    {publishable:($reasons | length == 0),reasons:$reasons,
     source:{git_commit:(if $commit == "" then null else $commit end),dirty:$dirty,clean:($dirty | not)},
     image:{reference:$image_reference,build_mode:$build_mode,
       content_id:$content_id,repo_digests:$repo_digests,
       source_revision:(if $source_revision == "" then null else $source_revision end)},
     execution:{benchmark_client:{sha256:(if $benchmark_sha256 == "" then null else $benchmark_sha256 end)},
       toolchain:{rustc_vv:(if $rustc_vv == "" then null else $rustc_vv end),
         cargo_version:(if $cargo_version == "" then null else $cargo_version end)},
       runtime_images:{rhiza:$rhiza_runtime,rustfs:$rustfs_runtime,
         object_meter:$meter_runtime,aws_cli_inventory:$inventory_runtime}}}
  '
}

wait_for_checkpoint_drain() {
  local status elapsed endpoint_url
  local start_epoch=$SECONDS
  local status_file="$target/.checkpoint-status.json"
  local statuses_file="$target/.checkpoint-statuses.jsonl"
  for _ in $(seq 1 120); do
    : > "$statuses_file"
    for endpoint_url in "${admin_endpoint_urls[@]}"; do
      status="$(curl --max-time 3 -fsS -H 'x-rhiza-version: 1' -H "Authorization: Bearer $admin_token" \
        "$endpoint_url/v1/admin/membership/status" 2>/dev/null || true)"
      [ -n "$status" ] || continue
      jq -cse --arg endpoint "$endpoint_url" '
        if length == 1 and (.[0] | type) == "object"
        then {endpoint:$endpoint,status:.[0]}
        else error("invalid status response") end
      ' \
        <<< "$status" >> "$statuses_file" 2>/dev/null || true
    done
    elapsed=$((SECONDS - start_epoch))
    jq -s '.' "$statuses_file" > "$status_file"
    if jq -e --argjson expected "${#admin_endpoint_urls[@]}" '
      def hash:
        type == "array" and length == 32 and
        all(.[]; type == "number" and . >= 0 and . <= 255 and floor == .);
      . as $statuses |
      ($statuses[0].status.qlog_root // null) as $root |
      length == $expected and
      ($root | type) == "object" and
      ($root.index | type) == "number" and $root.index >= 0 and ($root.index | floor) == $root.index and
      ($root.hash | hash) and
      all($statuses[];
        (.status | type) == "object" and
        (.status.qlog_root | type) == "object" and
        (.status.checkpoint_root | type) == "object" and
        (.status.qlog_root.hash | hash) and
        (.status.checkpoint_root.hash | hash) and
        .status.qlog_root == $root and .status.checkpoint_root == $root)
    ' "$status_file" >/dev/null 2>&1; then
      jq --argjson wait_seconds "$elapsed" \
        '. as $statuses | $statuses[0].status.qlog_root as $root |
         {wait_seconds:$wait_seconds,qlog_root:$root,checkpoint_root:$root,
          endpoints:[$statuses[] | {endpoint,qlog_root:.status.qlog_root,
            checkpoint_root:.status.checkpoint_root}]}' \
        "$status_file" > "$checkpoint_drain_json"
      rm -f "$status_file" "$statuses_file"
      return 0
    fi
    sleep 1
  done
  jq --argjson wait_seconds "$((SECONDS - start_epoch))" \
    '{wait_seconds:$wait_seconds,qlog_root:null,checkpoint_root:null,
      endpoints:[.[] | {endpoint,qlog_root:(.status.qlog_root // null),
        checkpoint_root:(.status.checkpoint_root // null)}]}' \
    "$status_file" > "$checkpoint_drain_json" 2>/dev/null || true
  rm -f "$status_file" "$statuses_file"
  return 1
}

resource_samples_from_cri_stats() {
  local sample_namespace="$1" collection_batch="${2:-}"
  case "$collection_batch" in ''|*[!0-9]*) return 1 ;; esac
  jq -c --arg namespace "$sample_namespace" --argjson collection_batch "$collection_batch" '
    def required_integer:
      if type == "number" and floor == . then .
      elif type == "string" and test("^[0-9]+$") then tonumber
      else error("CRI stats timestamp is not an integer") end;
    .stats[] |
    select(.attributes.labels["io.kubernetes.pod.namespace"] == $namespace) |
    .attributes.metadata.name as $container |
    .attributes.labels["io.kubernetes.pod.name"] as $pod |
    select($container == "rhiza" or $container == "rustfs" or $container == "object-meter") |
    select($container != "rhiza" or ($pod | startswith("rhiza-sql-c1-"))) |
    (.cpu.timestamp | required_integer) as $cpu_timestamp_ns |
    (.memory.timestamp | required_integer) as $memory_timestamp_ns |
    if $cpu_timestamp_ns <= 0 or $memory_timestamp_ns <= 0 or
        $cpu_timestamp_ns != $memory_timestamp_ns then
      error("CRI CPU and memory stats must share a positive timestamp")
    else
      ($cpu_timestamp_ns / 1000000000) as $cpu_timestamp |
      {timestamp:($cpu_timestamp | floor | todateiso8601),
       timestamp_epoch_seconds:$cpu_timestamp,
       collection_batch:$collection_batch,
       source:"containerd_cri_stats",
       app:(if $container == "rhiza" then "rhiza" else "simulator" end),
       pod:$pod,
       pod_uid:(.attributes.labels["io.kubernetes.pod.uid"] // ""),container:$container,
       container_id:.attributes.id,
       restart_count:(.attributes.annotations["io.kubernetes.container.restartCount"] // "0" | tonumber),
       cpu_usage_usec:((.cpu.usageCoreNanoSeconds.value // "0" | tonumber) / 1000 | floor),
       memory_bytes:(.memory.workingSetBytes.value // .memory.usageBytes.value // "0" | tonumber)}
    end
  '
}

# Allow the static check to exercise process-failure handling without starting a cluster.
[ "${BASH_SOURCE[0]}" = "$0" ] || return 0

validate_duration() {
  local name="$1" value="$2" amount
  case "$value" in
    *ms) amount="${value%ms}" ;;
    *s|*m|*h) amount="${value%?}" ;;
    *) die "$name must be a positive duration with ms/s/m/h suffix" ;;
  esac
  case "$amount" in ''|*[!0-9]*) die "$name must be a positive duration with ms/s/m/h suffix" ;; esac
  [ -n "${amount//0/}" ] || die "$name must be a positive duration with ms/s/m/h suffix"
}

case "$durability_mode" in
  sync)
    [ -z "$durability_max_lag_set" ] || die "RHIZA_DURABILITY_MAX_LAG is irrelevant for sync durability"
    [ -z "$durability_interval_set" ] || die "RHIZA_DURABILITY_INTERVAL is irrelevant for sync durability"
    ;;
  bounded)
    if [ -z "$durability_max_lag_set" ] || [ -z "$durability_max_lag" ]; then
      die "RHIZA_DURABILITY_MAX_LAG is required for bounded durability"
    fi
    [ -z "$durability_interval_set" ] || die "RHIZA_DURABILITY_INTERVAL is irrelevant for bounded durability"
    validate_duration RHIZA_DURABILITY_MAX_LAG "$durability_max_lag"
    ;;
  periodic)
    if [ -z "$durability_interval_set" ] || [ -z "$durability_interval" ]; then
      die "RHIZA_DURABILITY_INTERVAL is required for periodic durability"
    fi
    [ -z "$durability_max_lag_set" ] || die "RHIZA_DURABILITY_MAX_LAG is irrelevant for periodic durability"
    validate_duration RHIZA_DURABILITY_INTERVAL "$durability_interval"
    ;;
  *) die "RHIZA_DURABILITY_MODE must be sync|bounded|periodic" ;;
esac

while [ "$#" -gt 0 ]; do
  case "$1" in
    --duration|--warmup|--concurrency|--target-rate|--workload|--write-percent|--fault|--fault-offset|--fault-pod|--sample-interval)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      case "$1" in
        --duration) duration="$2" ;;
        --warmup) warmup="$2" ;;
        --concurrency) concurrency="$2" ;;
        --target-rate) target_rate="$2" ;;
        --workload) workload="$2" ;;
        --write-percent) write_percent="$2" ;;
        --fault) fault="$2" ;;
        --fault-offset) fault_offset="$2" ;;
        --fault-pod) fault_pod="$2" ;;
        --sample-interval) sample_interval="$2" ;;
      esac
      shift 2 ;;
    --keep) keep=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

case "$fault" in none|pod-delete) ;; *) die "--fault must be none or pod-delete";; esac
case "$object_metering" in 0|1) ;; *) die "RHIZA_BENCH_OBJECT_USAGE_METERING must be 0 or 1";; esac
case "$resource_sampling" in 0|1) ;; *) die "RHIZA_BENCH_RESOURCE_SAMPLING must be 0 or 1";; esac
case "$multi_endpoint" in 0|1) ;; *) die "RHIZA_BENCH_MULTI_ENDPOINT must be 0 or 1";; esac
case "$recorder_transport" in
  http|tcp-postcard|tcp-postcard-rpc) ;;
  *) die "RHIZA_RECORDER_TRANSPORT must be http|tcp-postcard|tcp-postcard-rpc" ;;
esac
case "$recorder_tls" in
  on|off) ;;
  *) die "RHIZA_RECORDER_TLS must be on|off" ;;
esac
[ "$recorder_tls" != on ] || [ "$recorder_transport" != http ] ||
  die "RHIZA_RECORDER_TLS=on requires RHIZA_RECORDER_TRANSPORT=tcp-postcard|tcp-postcard-rpc"
case "$sample_interval" in ''|*[!0-9]*) die "--sample-interval must be a positive integer";; esac
[ "$sample_interval" -gt 0 ] || die "--sample-interval must be a positive integer"
for tool in cargo curl docker jq kubectl openssl rustc sed timeout vcluster yq; do require "$tool"; done

target="$repo_root/$target_base/$run_id"
benchmark_json="$target/benchmark.json"
resources_jsonl="$target/resources.jsonl"
resource_summary="$target/resource-summary.json"
resource_sampler_log="$target/resource-sampler.log"
checkpoint_drain_json="$target/checkpoint-drain.json"
object_access_log="$target/s3-access.jsonl"
object_usage_json="$target/object-usage.json"
artifacts_json="$target/artifacts.json"
runtime_pods_artifact="$target/runtime-pods.json"
rendered_rustfs="$target/rustfs.yaml"
rendered_cluster="$target/rhiza-sql-c1.yaml"
stop_sampler="$target/.stop-sampler"

k() { kubectl --context "$context" -n "$namespace" "$@"; }

sample_resources() {
  local collection_batch=0
  printf 'resource sampler started: context=%s namespace=%s\n' "$context" "$namespace"
  while [ ! -e "$stop_sampler" ]; do
    summary="$(timeout --kill-after="${resource_sample_kill_after}s" \
      "${resource_sample_timeout}s" docker exec "vcluster.cp.$cluster" \
      crictl stats -o json 2>/dev/null || true)"
    if ! jq -e --arg namespace "$namespace" '
      any(.stats[]?; .attributes.labels["io.kubernetes.pod.namespace"] == $namespace)
    ' <<< "$summary" >/dev/null 2>&1; then
      printf 'containerd stats unavailable\n'
      sleep "$sample_interval"
      continue
    fi
    collection_batch=$((collection_batch + 1))
    resource_samples_from_cri_stats "$namespace" "$collection_batch" \
      <<< "$summary" >> "$resources_jsonl"
    sleep "$sample_interval"
  done
}

wait_for_resource_coverage() {
  local deadline=$((SECONDS + $(resource_coverage_wait_budget_seconds)))
  while ! validate_resource_samples "$resources_jsonl" \
    "$measurement_started_at_epoch_seconds" "$measurement_finished_at_epoch_seconds" \
    "$sample_interval" "$object_metering" "$(selected_resource_fault_pod)" \
    "$resource_fault_started_at_epoch_seconds" "$resource_fault_finished_at_epoch_seconds"; do
    kill -0 "$sampler_pid" 2>/dev/null || return 1
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 1
  done
}

collect_object_usage() {
  local pod phase usage_pod meter_enabled meter_status inventory_status retained retained_output usage_tmp
  local inventory_image_id
  meter_enabled=false
  [ "$object_metering" = 1 ] && meter_enabled=true
  if [ -z "$context" ] || ! k get service rustfs >/dev/null 2>&1; then
    : > "$object_access_log"
    jq -n --argjson enabled "$meter_enabled" \
      '{metering:{enabled:$enabled,status:(if $enabled then "failed" else "disabled" end),
        error:(if $enabled then "rustfs service unavailable" else null end),
        source:(if $enabled then "nginx_access_log" else null end),requests:0,
        request_bytes:0,response_bytes:0,by_method_status:[]},
        retained:{status:"failed",object_count:null,retained_bytes:null}}' > "$object_usage_json"
    if [ "$object_metering" = 0 ]; then return 0; else return 1; fi
  fi
  pod="$(k get pod -l app.kubernetes.io/name=rustfs -o json 2>/dev/null | jq -r \
    '.items[] | select(any(.spec.containers[]; .name == "object-meter")) | .metadata.name' | head -n 1 || true)"
  meter_status=disabled
  if [ "$object_metering" = 1 ] && [ -n "$pod" ]; then
    if k exec "$pod" -c object-meter -- cat /var/log/nginx/s3-access.log > "$object_access_log" 2>/dev/null; then
      meter_status=ok
    else
      meter_status=failed
    fi
  elif [ "$object_metering" = 1 ]; then
    meter_status=failed
    : > "$object_access_log"
  else
    : > "$object_access_log"
  fi

  usage_pod="bench-object-usage"
  k delete pod "$usage_pod" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  inventory_status=failed
  if jq -n --arg image "$aws_image" '{apiVersion:"v1",kind:"Pod",metadata:{name:"bench-object-usage"},spec:{
    automountServiceAccountToken:false,enableServiceLinks:false,restartPolicy:"Never",containers:[{
      name:"aws-cli",image:$image,imagePullPolicy:"IfNotPresent",
      command:["/bin/sh","-c"],args:["aws --endpoint-url http://rustfs:9000 s3api list-objects-v2 --bucket rhiza --output json"],
      env:[
        {name:"AWS_ACCESS_KEY_ID",valueFrom:{secretKeyRef:{name:"rustfs-credentials",key:"access-key"}}},
        {name:"AWS_SECRET_ACCESS_KEY",valueFrom:{secretKeyRef:{name:"rustfs-credentials",key:"secret-key"}}},
        {name:"AWS_DEFAULT_REGION",value:"us-east-1"},{name:"AWS_EC2_METADATA_DISABLED",value:"true"}
      ]}]}}' | k apply -f - >/dev/null 2>&1; then
    inventory_status=pending
  fi
  phase=""
  if [ "$inventory_status" = pending ]; then
    for _ in $(seq 1 90); do
      phase="$(k get pod "$usage_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      case "$phase" in Succeeded|Failed) break ;; esac
      sleep 1
    done
  fi
  inventory_image_id="$(k get pod "$usage_pod" -o json 2>/dev/null | jq -r \
    '(([.status.containerStatuses[]? | select(.name == "aws-cli") | (.imageID // "")][0]) // "")' || true)"
  runtime_image_ids_json="$(printf '%s\n' "$runtime_image_ids_json" | jq -c \
    --arg image_id "$inventory_image_id" '.aws_cli_inventory = [$image_id]')"
  retained='{"object_count":null,"retained_bytes":null}'
  if [ "$phase" = Succeeded ]; then
    if retained_output="$(k logs "$usage_pod" 2>/dev/null)" &&
      retained="$(jq -ce \
        '{object_count:((.Contents // []) | length),retained_bytes:((.Contents // []) | map(.Size) | add // 0)} |
         select((.object_count | type == "number" and . >= 0) and
           (.retained_bytes | type == "number" and . >= 0))' <<< "$retained_output" 2>/dev/null)"; then
      inventory_status=ok
    fi
  fi
  [ "$inventory_status" = ok ] || inventory_status=failed
  usage_tmp="$object_usage_json.tmp"
  if ! jq -s --argjson enabled "$meter_enabled" --arg meter_status "$meter_status" \
    --arg inventory_status "$inventory_status" --argjson retained "$retained" '
    {metering:{enabled:$enabled,status:$meter_status,
      source:(if $enabled then "nginx_access_log" else null end),
      requests:length,request_bytes:(map(.request_bytes | tonumber) | add // 0),
      response_bytes:(map(.response_bytes | tonumber) | add // 0),
      by_method_status:(group_by([.method,.status]) | map({method:.[0].method,status:(.[0].status | tonumber),
        requests:length,request_bytes:(map(.request_bytes | tonumber) | add),
        response_bytes:(map(.response_bytes | tonumber) | add)}))},
     retained:($retained + {status:$inventory_status})}' \
    "$object_access_log" > "$usage_tmp"; then
    jq -n --argjson enabled "$meter_enabled" \
      '{metering:{enabled:$enabled,status:(if $enabled then "failed" else "disabled" end),
        error:"invalid meter output",source:(if $enabled then "nginx_access_log" else null end),
        requests:0,request_bytes:0,response_bytes:0,by_method_status:[]},
       retained:{status:"failed",object_count:null,retained_bytes:null}}' > "$usage_tmp"
  fi
  mv "$usage_tmp" "$object_usage_json"
  k delete pod "$usage_pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  [ "$object_metering" = 0 ] || validate_object_evidence "$object_access_log" "$object_usage_json"
}

emit_artifacts() {
  local resource_enabled=false object_enabled=false evidence cleanup measurement_window provenance
  [ "$resource_sampling" = 0 ] || resource_enabled=true
  [ "$object_metering" = 0 ] || object_enabled=true
  evidence="$(render_evidence_json "$resource_evidence_status" "$object_evidence_status" \
    "$resource_enabled" "$object_enabled")"
  cleanup="$(render_cleanup_json "$cleanup_verification_status" \
    "$namespace_cleanup_status" "$vcluster_cleanup_status")"
  measurement_window="$(render_measurement_window_json \
    "$measurement_started_at_epoch_seconds" "$measurement_finished_at_epoch_seconds")"
  provenance="$(render_provenance_json "$source_git_commit" "$source_dirty" \
    "$image_build_mode" "$image" "$image_inspect_json" "$benchmark_binary_sha256" \
    "$rustc_vv" "$cargo_version" "$runtime_image_ids_json" "$object_enabled" \
    "$benchmark_status" "$cleanup_status" \
    "$(evidence_overall_status "$resource_evidence_status" "$object_evidence_status")" \
    "$cleanup_verification_status")"
  jq -n \
    --arg run_id "$run_id" \
    --arg cluster "$cluster" \
    --arg namespace "$namespace" \
    --arg benchmark "$benchmark_json" \
    --arg resources "$resources_jsonl" \
    --arg resource_summary "$resource_summary" \
    --arg checkpoint_drain "$checkpoint_drain_json" \
    --arg object_access_log "$object_access_log" \
    --arg object_usage "$object_usage_json" \
    --arg rustfs_manifest "$rendered_rustfs" \
    --arg cluster_manifest "$rendered_cluster" \
    --arg runtime_pods "$runtime_pods_artifact" \
    --arg durability_mode "$durability_mode" \
    --arg durability_max_lag "$durability_max_lag" \
    --arg durability_interval "$durability_interval" \
    --arg recorder_transport "$recorder_transport" \
    --arg recorder_tls "$recorder_tls" \
    --argjson benchmark_exit "$benchmark_status" \
    --argjson run_exit "$cleanup_status" \
    --argjson evidence "$evidence" \
    --argjson cleanup "$cleanup" \
    --argjson measurement_window "$measurement_window" \
    --argjson provenance "$provenance" \
    --argjson cleaned_up "$cleaned_up" \
    '{run_id:$run_id,cluster:$cluster,namespace:$namespace,benchmark_exit_status:$benchmark_exit,
      exit_status:$run_exit,evidence:$evidence,cleanup:$cleanup,provenance:$provenance,
      measurement_window:$measurement_window,
      configuration:{recorder_transport:$recorder_transport,recorder_tls:$recorder_tls,
        durability:{mode:$durability_mode,
        max_lag:(if $durability_max_lag == "" then null else $durability_max_lag end),
        interval:(if $durability_interval == "" then null else $durability_interval end)}},
      cleaned_up:$cleaned_up,artifacts:{benchmark_json:$benchmark,resource_samples_jsonl:$resources,
      resource_summary_json:$resource_summary,checkpoint_drain_json:$checkpoint_drain,
      object_access_log_jsonl:$object_access_log,
      object_usage_json:$object_usage,rustfs_manifest:$rustfs_manifest,cluster_manifest:$cluster_manifest,runtime_pods_json:$runtime_pods}}' > "$artifacts_json"
}

cleanup_run() {
  cleanup_status="$1"
  local runtime_pods_json observed_runtime_image_ids
  mkdir -p "$target"
  [ -z "$recorder_tls_dir" ] || rm -rf "$recorder_tls_dir"
  if [ "$resource_sampling" = 1 ] &&
    { [ -z "$sampler_pid" ] || ! kill -0 "$sampler_pid" 2>/dev/null; }; then
    resource_evidence_status=failed
  fi
  touch "$stop_sampler" 2>/dev/null || true
  [ -z "$sampler_pid" ] || kill "$sampler_pid" 2>/dev/null || true
  [ -z "$sampler_pid" ] || wait "$sampler_pid" 2>/dev/null || true
  for pid in "${port_forward_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${port_forward_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [ "$namespace_created" = true ] &&
    runtime_pods_json="$(k get pods -o json 2>/dev/null)" &&
    observed_runtime_image_ids="$(printf '%s\n' "$runtime_pods_json" | runtime_image_ids_from_pods)"; then
    printf '%s\n' "$runtime_pods_json" > "$runtime_pods_artifact"
    runtime_image_ids_json="$observed_runtime_image_ids"
  fi
  if [ "$object_metering" = 0 ]; then
    : > "$object_access_log"
    jq -n '{metering:{enabled:false,status:"disabled",source:null,requests:0,
      request_bytes:0,response_bytes:0,by_method_status:[]},
      retained:{status:"disabled",object_count:null,retained_bytes:null}}' > "$object_usage_json"
  elif collect_object_usage && [ "$object_meter_reset_status" = ok ]; then
    object_evidence_status=ok
  else
    object_evidence_status=failed
  fi
  if [ "$resource_sampling" = 0 ]; then
    jq -n '{status:"disabled",samples:0,container_cpu_usage_usec_deltas:[],apps:[]}' \
      > "$resource_summary"
  elif [ "$resource_evidence_status" != failed ] &&
    validate_resource_samples "$resources_jsonl" "$measurement_started_at_epoch_seconds" \
      "$measurement_finished_at_epoch_seconds" "$sample_interval" "$object_metering" \
      "$(selected_resource_fault_pod)" "$resource_fault_started_at_epoch_seconds" \
      "$resource_fault_finished_at_epoch_seconds" &&
    summarize_resource_samples "$resources_jsonl" "$measurement_started_at_epoch_seconds" \
      "$measurement_finished_at_epoch_seconds" > "$resource_summary"; then
    resource_evidence_status=ok
  else
    resource_evidence_status=failed
    jq -n '{status:"failed",error:"resource samples unavailable or invalid",samples:0,
      container_cpu_usage_usec_deltas:[],apps:[]}' > "$resource_summary"
  fi
  if [ "$keep" = false ]; then
    if [ "$namespace_created" = true ]; then
      namespace_delete_status=0
      kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null 2>&1 ||
        namespace_delete_status=$?
      namespace_present_status=0
      if namespace_output="$(kubectl --context "$context" get namespace "$namespace" \
        --ignore-not-found -o name 2>/dev/null)" && [ -z "$namespace_output" ]; then
        namespace_present_status=1
      fi
      namespace_cleanup_status="$(cleanup_outcome "$namespace_delete_status" \
        "$namespace_present_status")"
    fi
    if [ "$created_cluster" = true ]; then
      vcluster_delete_status=0
      vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || vcluster_delete_status=$?
      vcluster_present_status=0
      if vcluster_output="$(vcluster list --driver docker --output json 2>/dev/null)" &&
        ! grep -Fq "\"${cluster}\"" <<< "$vcluster_output"; then
        vcluster_present_status=1
      fi
      vcluster_cleanup_status="$(cleanup_outcome "$vcluster_delete_status" \
        "$vcluster_present_status")"
    fi
    if { [ "$namespace_cleanup_status" = ok ] || [ "$namespace_cleanup_status" = not_created ]; } &&
      { [ "$vcluster_cleanup_status" = ok ] || [ "$vcluster_cleanup_status" = not_created ] ||
        [ "$vcluster_cleanup_status" = not_owned ]; }; then
      cleanup_verification_status=ok
      cleaned_up=true
    else
      cleanup_verification_status=failed
    fi
  fi
  [ -z "$previous_context" ] || kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  cleanup_status="$(evidence_exit_status "$cleanup_status" \
    "$resource_evidence_status" "$object_evidence_status" "$cleanup_verification_status")"
  if ! emit_artifacts; then
    echo "failed to write benchmark artifacts" >&2
    [ "$cleanup_status" -ne 0 ] || cleanup_status=1
  fi
  if [ "$cleanup_status" -eq 0 ]; then
    cat "$artifacts_json"
  else
    echo "benchmark artifacts: $artifacts_json" >&2
  fi
}

on_exit() {
  status=$?
  trap - EXIT
  cleanup_run "$status"
  exit "$cleanup_status"
}
trap on_exit EXIT

cd "$repo_root"
source_git_commit="$(git rev-parse --verify HEAD 2>/dev/null || true)"
source_status=""
if source_status="$(git status --porcelain --untracked-files=normal 2>/dev/null)" &&
  [ -z "$source_status" ]; then
  source_dirty=false
fi
mkdir -p "$target"
chmod 700 "$target"
previous_context="$(kubectl config current-context 2>/dev/null || true)"

if [ "${RHIZA_VIND_SKIP_BUILD:-0}" = 1 ]; then
  docker image inspect "$image" >/dev/null 2>&1 || die "missing local image: $image"
  image_build_mode=skip-build
else
  docker build -t "$image" .
  image_build_mode=built
fi
image_inspect_json="$(docker image inspect "$image" 2>/dev/null || printf '[]')"
vcluster use driver docker >/dev/null
if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
  [ "${RHIZA_VIND_REUSE_EXISTING:-0}" = 1 ] || die "vind cluster already exists: $cluster"
  vcluster connect "$cluster" --driver docker >/dev/null
  vcluster_cleanup_status=not_owned
else
  vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster" >/dev/null
  created_cluster=true
  vcluster_cleanup_status=retained
fi
context="$(kubectl config current-context 2>/dev/null || true)"
[ -n "$context" ] || die "vcluster did not select a Kubernetes context"

if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  managed="$(kubectl --context "$context" get namespace "$namespace" -o go-template='{{index .metadata.labels "rhiza.dev/bench-managed"}}')"
  [ "$managed" = true ] || die "refusing to replace unmanaged namespace $namespace"
  kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null
fi
kubectl --context "$context" create namespace "$namespace" >/dev/null
namespace_created=true
namespace_cleanup_status=retained
kubectl --context "$context" label namespace "$namespace" rhiza.dev/bench-managed=true \
  "rhiza.dev/bench-run-id=$run_id" >/dev/null

node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node"
(cd "$target" && vcluster node load-image "$node" --image "$image") >/dev/null

client_token="$(openssl rand -hex 24)"
admin_token="$(openssl rand -hex 24)"
tail_token="$(openssl rand -hex 24)"
peer_tokens="$(for _ in 1 2 3; do openssl rand -hex 24; done | jq -Rsc 'split("\n")[:-1]')"
k create secret generic rhiza-auth --from-literal=client-token="$client_token" \
  --from-literal=admin-token="$admin_token" \
  --from-literal=tail-token="$tail_token" >/dev/null
k apply -f deploy/k8s/rhiza-client-services.yaml >/dev/null
sed -e "s|__RUSTFS_IMAGE__|$rustfs_image|g" -e "s|__AWS_CLI_IMAGE__|$aws_image|g" \
  deploy/k8s/rustfs-e2e.yaml > "$rendered_rustfs"
yq eval '.' "$rendered_rustfs" >/dev/null
export RUSTFS_CPU_REQUEST="$rustfs_cpu_request" RUSTFS_CPU_LIMIT="$rustfs_cpu_limit"
export RUSTFS_MEMORY_REQUEST="$rustfs_memory_request" RUSTFS_MEMORY_LIMIT="$rustfs_memory_limit"
yq eval -i '(select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.containers[] | select(.name == "rustfs") | .resources) = {"requests": {"cpu": strenv(RUSTFS_CPU_REQUEST), "memory": strenv(RUSTFS_MEMORY_REQUEST)}, "limits": {"cpu": strenv(RUSTFS_CPU_LIMIT), "memory": strenv(RUSTFS_MEMORY_LIMIT)}}' "$rendered_rustfs"
if [ "$object_metering" = 1 ]; then
  # shellcheck disable=SC2016 # nginx expands these access-log variables.
  nginx_config='events {}
http {
  log_format s3 escape=json '\''{"method":"$request_method","status":$status,"request_bytes":$request_length,"response_bytes":$bytes_sent}'\'';
  access_log /var/log/nginx/s3-access.log s3;
  server {
    listen 9002;
    client_max_body_size 0;
    location / {
      proxy_request_buffering off;
      proxy_buffering off;
      proxy_http_version 1.1;
      proxy_set_header Host $http_host;
      proxy_set_header Connection "";
      proxy_pass http://127.0.0.1:9000;
    }
  }
}'
  k create configmap rustfs-object-meter --from-literal=nginx.conf="$nginx_config" >/dev/null
  export NGINX_IMAGE="$nginx_image"
  yq eval -i '
    (select(.kind == "Service" and .metadata.name == "rustfs") | .spec.ports[] | select(.name == "s3") | .targetPort) = "s3-meter" |
    (select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.volumes) += [{"name":"object-meter-config","configMap":{"name":"rustfs-object-meter"}},{"name":"object-meter-log","emptyDir":{}}] |
    (select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.containers) += [{
      "name":"object-meter","image":strenv(NGINX_IMAGE),"imagePullPolicy":"IfNotPresent",
      "ports":[{"name":"s3-meter","containerPort":9002}],
      "volumeMounts":[{"name":"object-meter-config","mountPath":"/etc/nginx/nginx.conf","subPath":"nginx.conf","readOnly":true},{"name":"object-meter-log","mountPath":"/var/log/nginx"}],
      "readinessProbe":{"tcpSocket":{"port":"s3-meter"},"initialDelaySeconds":1,"periodSeconds":2}
    }]' "$rendered_rustfs"
fi
k apply -f "$rendered_rustfs" >/dev/null
k rollout status deployment/rustfs --timeout=240s >/dev/null
k wait --for=condition=complete job/rustfs-create-bucket --timeout=240s >/dev/null

bundle="$target/config-c1.json"
jq -n --argjson tokens "$peer_tokens" --arg recorder_transport "$recorder_transport" \
  --arg recorder_tls "$recorder_tls" '
  {config_id:1,members:[range(3) as $n | {
    node_id:("node-" + ($n + 1 | tostring)),
    url:("http://rhiza-sql-c1-" + ($n|tostring) + ".rhiza-sql-c1:8081"),
    log_url:("http://rhiza-sql-c1-" + ($n|tostring) + ".rhiza-sql-c1:8080"), token:$tokens[$n]
  } + (if $recorder_transport != "http" then {
    recorder_tcp_addr:("rhiza-sql-c1-" + ($n|tostring) + ".rhiza-sql-c1:8082")
  } else {} end) + (if $recorder_tls == "on" then {
    recorder_tls_server_name:("rhiza-sql-c1-" + ($n|tostring) + ".rhiza-sql-c1")
  } else {} end)]}
' > "$bundle"
chmod 600 "$bundle"
k create secret generic rhiza-sql-c1-bundle --from-file=config.json="$bundle" --dry-run=client -o yaml |
  yq eval '.immutable = true' - | k create -f - >/dev/null

if [ "$recorder_tls" = on ]; then
  recorder_tls_dir="$(mktemp -d "${TMPDIR:-/tmp}/rhiza-recorder-tls.XXXXXX")"
  recorder_tls_names=()
  while IFS= read -r dns_name; do recorder_tls_names+=("$dns_name"); done \
    < <(jq -r '.members[].recorder_tls_server_name' "$bundle")
  generate_recorder_tls_material "$recorder_tls_dir" "${recorder_tls_names[@]}"
  k create secret generic rhiza-sql-c1-recorder-tls \
    --from-file=tls.crt="$recorder_tls_dir/tls.crt" \
    --from-file=tls.key="$recorder_tls_dir/tls.key" \
    --from-file=ca-bundle.pem="$recorder_tls_dir/ca-bundle.pem" >/dev/null
  export RHIZA_RECORDER_TLS_SECRET=rhiza-sql-c1-recorder-tls
else
  unset RHIZA_RECORDER_TLS_SECRET
fi

export RHIZA_IMAGE="$image" RHIZA_KUBE_CONTEXT="$context" RHIZA_K8S_NAMESPACE="$namespace"
export RHIZA_CLUSTER_ID=rhiza-vind RHIZA_RECOVERY_GENERATION=1
export RHIZA_S3_ENDPOINT=http://rustfs:9000 RHIZA_OBJECT_SECRET=rustfs-credentials
export RHIZA_S3_ALLOW_HTTP=true
export RHIZA_RECORDER_TRANSPORT="$recorder_transport"
export RHIZA_RECORDER_TLS="$recorder_tls"
scripts/k8s-object-job.sh 1 "$bundle" init-checkpoint >/dev/null
scripts/render-k8s-config.sh 1 3 "$bundle" "$rendered_cluster"
export RHIZA_CPU_REQUEST="$rhiza_cpu_request" RHIZA_CPU_LIMIT="$rhiza_cpu_limit"
export RHIZA_MEMORY_REQUEST="$rhiza_memory_request" RHIZA_MEMORY_LIMIT="$rhiza_memory_limit"
yq eval -i '(select(.kind == "StatefulSet" and .metadata.name == "rhiza-sql-c1") | .spec.template.spec.containers[] | select(.name == "rhiza") | .resources) = {"requests": {"cpu": strenv(RHIZA_CPU_REQUEST), "memory": strenv(RHIZA_MEMORY_REQUEST)}, "limits": {"cpu": strenv(RHIZA_CPU_LIMIT), "memory": strenv(RHIZA_MEMORY_LIMIT)}}' "$rendered_cluster"
k create -f "$rendered_cluster" >/dev/null
scripts/wait-k8s-statefulset-ready.sh rhiza-sql-c1 3 1
if [ "$recorder_transport" != http ]; then
  for ordinal in 0 1 2; do
    k exec "rhiza-sql-c1-$ordinal" -- /bin/sh -ec '
      grep -Eqi ":1F92[[:space:]]+[0-9A-F]+:[0-9A-F]+[[:space:]]+0A" /proc/net/tcp /proc/net/tcp6
      ! grep -Eqi ":1F91[[:space:]]+[0-9A-F]+:[0-9A-F]+[[:space:]]+0A" /proc/net/tcp /proc/net/tcp6
    '
  done
fi
[ -z "$(k get persistentvolumeclaims -o name)" ] || die "benchmark deployment created a PVC"

local_port="${RHIZA_BENCH_PORT:-18080}"
admin_endpoint_urls=()
workload_endpoint_urls=()
fault_endpoint_index=""
for ordinal in 0 1 2; do
  port=$((local_port + ordinal))
  if [ "$fault" = pod-delete ] && [ "$fault_pod" = "rhiza-sql-c1-$ordinal" ]; then
    fault_endpoint_index="$ordinal"
    supervise_rebinding_port_forward "$ordinal" "$fault_pod" "$port" &
  else
    k port-forward "pod/rhiza-sql-c1-$ordinal" "${port}:8080" \
      > "$target/port-forward-$ordinal.log" 2>&1 &
  fi
  port_forward_pids+=("$!")
  admin_endpoint_urls+=("http://127.0.0.1:${port}")
done
configure_endpoint_topology || die "benchmark requires exactly three admin endpoints"

for index in "${!admin_endpoint_urls[@]}"; do
  endpoint_url="${admin_endpoint_urls[$index]}"
  for _ in $(seq 1 60); do
    endpoint_ready "$endpoint_url" && break
    assert_port_forward_alive "$index"
    sleep 1
  done
  if ! endpoint_ready "$endpoint_url"; then
    assert_port_forward_alive "$index"
    die "port-forward did not become ready: $endpoint_url"
  fi
done

setup_body="$(jq -n --arg request_id "$run_id-setup" '
  {request_id:$request_id,statements:[
    {sql:"CREATE TABLE IF NOT EXISTS rhiza_bench (request_id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",parameters:[]},
    {sql:"INSERT INTO rhiza_bench (request_id, value) VALUES (?, ?)",parameters:[
      {type:"text",value:"rhiza-bench-seed"},{type:"text",value:"value-rhiza-bench-seed"}
    ]}
  ]}
')"
curl -fsS -H 'x-rhiza-version: 1' -H "Authorization: Bearer $client_token" \
  -H 'Content-Type: application/json' \
  --data "$setup_body" "http://127.0.0.1:${local_port}/v1/sql/execute" >/dev/null

bench_target_dir="$(cargo metadata --locked --manifest-path bench/Cargo.toml --format-version 1 --no-deps | jq -r .target_directory)"
cargo build --release --locked --manifest-path bench/Cargo.toml --bin rhiza-bench
bench_binary="$bench_target_dir/release/rhiza-bench"
[ -x "$bench_binary" ] || die "benchmark binary was not built: $bench_binary"
benchmark_binary_sha256="$(openssl dgst -sha256 -r "$bench_binary")"
benchmark_binary_sha256="${benchmark_binary_sha256%% *}"
rustc_vv="$(rustc -Vv)"
cargo_version="$(cargo --version)"

: > "$resources_jsonl"
if [ "$resource_sampling" = 1 ]; then
  sample_resources >"$resource_sampler_log" 2>&1 &
  sampler_pid=$!
  initial_resource_deadline=$((SECONDS + $(resource_coverage_wait_budget_seconds)))
  while ! validate_resource_sample_schema "$resources_jsonl" "$object_metering"; do
    kill -0 "$sampler_pid" 2>/dev/null || break
    [ "$SECONDS" -lt "$initial_resource_deadline" ] || break
    sleep 1
  done
  validate_resource_sample_schema "$resources_jsonl" "$object_metering" ||
    die "resource sampler did not produce a valid initial sample"
else
  printf 'resource sampling disabled\n' > "$resource_sampler_log"
fi
if [ "$object_metering" = 1 ]; then
  meter_pod="$(k get pod -l app.kubernetes.io/name=rustfs -o json | jq -r \
    '.items[] | select(any(.spec.containers[]; .name == "object-meter")) | .metadata.name' | head -n 1)"
  if k exec "$meter_pod" -c object-meter -- sh -c ': > /var/log/nginx/s3-access.log'; then
    object_meter_reset_status=ok
  else
    object_meter_reset_status=failed
    die "failed to reset object meter"
  fi
fi
bench_args=(--duration "$duration" --warmup "$warmup" --concurrency "$concurrency"
  --workload "$workload" --write-percent "$write_percent" --skip-setup)
[ "$d1_exact_write" = 0 ] || [ "$d1_exact_write" = 1 ] || die "RHIZA_BENCH_D1_EXACT_WRITE must be 0 or 1"
[ "$d1_exact_write" = 0 ] || bench_args+=(--retry-ambiguous-writes --d1-exact-write)
for endpoint_url in "${workload_endpoint_urls[@]}"; do bench_args+=(--endpoint "$endpoint_url"); done
[ -z "$target_rate" ] || bench_args+=(--target-rate "$target_rate")
case "$fault" in
  pod-delete)
    fault_rebind_request=""
    fault_replacement_ready=""
    fault_rebound=""
    if [ -n "$fault_endpoint_index" ]; then
      fault_rebind_request="$target/port-forward-$fault_endpoint_index.rebind-request"
      fault_replacement_ready="$target/port-forward-$fault_endpoint_index.replacement-ready"
      fault_rebound="$target/port-forward-$fault_endpoint_index.rebound"
    fi
    fault_command="$(build_pod_delete_fault_command "$context" "$namespace" "$fault_pod" \
      "$fault_rebind_request" "$fault_replacement_ready" "$fault_rebound")"
    bench_args+=(--fault "$fault_offset" pod-delete "$fault_command") ;;
esac

if RHIZA_CLIENT_TOKEN="$client_token" "$bench_binary" "${bench_args[@]}" > "$benchmark_json"; then
  benchmark_status=0
else
  benchmark_status=$?
fi
assert_all_port_forwards_ready
[ "$benchmark_status" -eq 0 ] || exit "$benchmark_status"
measurement_window="$(measurement_window_from_report "$benchmark_json")" ||
  die "benchmark report has no valid measurement window"
measurement_started_at_epoch_seconds="$(jq -r .started_at_epoch_seconds <<< "$measurement_window")"
measurement_finished_at_epoch_seconds="$(jq -r .finished_at_epoch_seconds <<< "$measurement_window")"
if [ "$fault" = pod-delete ]; then
  resource_fault_window="$(resource_fault_window_from_report "$benchmark_json" \
    "$measurement_started_at_epoch_seconds")" || die "benchmark report has no valid fault window"
  resource_fault_started_at_epoch_seconds="$(jq -r .started_at_epoch_seconds \
    <<< "$resource_fault_window")"
  resource_fault_finished_at_epoch_seconds="$(jq -r .finished_at_epoch_seconds \
    <<< "$resource_fault_window")"
fi
if [ "$resource_sampling" = 1 ]; then
  wait_for_resource_coverage || die "resource sampler did not cover the measurement end"
fi
wait_for_checkpoint_drain || die "checkpoint did not drain to the committed qlog tip"
assert_all_port_forwards_ready
